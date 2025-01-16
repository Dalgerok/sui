// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sui_pg_db::Db;
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    metrics::IndexerMetrics,
    pipeline::logging::{LoggerWatermark, WatermarkLogger},
    watermarks::PrunerWatermark,
};

use super::{Handler, PrunerConfig};

// TODO: Consider making this configurable.
const MAX_CONCURRENT_PRUNER_TASKS: usize = 10;

/// The pruner task is responsible for deleting old data from the database. It will periodically
/// check the `watermarks` table to see if there is any data that should be pruned between the
/// `pruner_hi` (inclusive), and `reader_lo` (exclusive) checkpoints. This task will also provide a
/// mapping of the pruned checkpoints to their corresponding epoch and tx, which the handler can
/// then use to delete the corresponding data from the database.
///
/// To ensure that the pruner does not interfere with reads that are still in flight, it respects
/// the watermark's `pruner_timestamp`, which records the time that `reader_lo` was last updated.
/// The task will not prune data until at least `config.delay()` has passed since `pruner_timestamp`
/// to give in-flight reads time to land.
///
/// The task regularly traces its progress, outputting at a higher log level every
/// [LOUD_WATERMARK_UPDATE_INTERVAL]-many checkpoints.
///
/// The task will shutdown if the `cancel` token is signalled. If the `config` is `None`, the task
/// will shutdown immediately.
pub(super) fn pruner<H: Handler + Send + Sync + 'static>(
    handler: Arc<H>,
    config: Option<PrunerConfig>,
    db: Db,
    metrics: Arc<IndexerMetrics>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(config) = config else {
            info!(pipeline = H::NAME, "Skipping pruner task");
            return;
        };

        info!(
            pipeline = H::NAME,
            "Starting pruner with config: {:?}", config
        );

        // The pruner can pause for a while, waiting for the delay imposed by the
        // `pruner_timestamp` to expire. In that case, the period between ticks should not be
        // compressed to make up for missed ticks.
        let mut poll = interval(config.interval());
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // The pruner task will periodically output a log message at a higher log level to
        // demonstrate that it is making progress.
        let mut logger = WatermarkLogger::new("pruner", LoggerWatermark::default());

        // Maintains the list of chunks that are ready to be pruned but not yet pruned.
        // This map can contain ranges that were attempted to be pruned in previous iterations,
        // but failed due to errors.
        let mut pending_prune_ranges = BTreeMap::new();

        loop {
            // (1) Get the latest pruning bounds from the database.
            let mut watermark = tokio::select! {
                _ = cancel.cancelled() => {
                    info!(pipeline = H::NAME, "Shutdown received");
                    break;
                }

                _ = poll.tick() => {
                    let guard = metrics
                        .watermark_pruner_read_latency
                        .with_label_values(&[H::NAME])
                        .start_timer();

                    let Ok(mut conn) = db.connect().await else {
                        warn!(pipeline = H::NAME, "Pruner failed to connect, while fetching watermark");
                        continue;
                    };

                    match PrunerWatermark::get(&mut conn, H::NAME, config.delay()).await {
                        Ok(Some(current)) => {
                            guard.stop_and_record();
                            current
                        }

                        Ok(None) => {
                            guard.stop_and_record();
                            warn!(pipeline = H::NAME, "No watermark for pipeline, skipping");
                            continue;
                        }

                        Err(e) => {
                            guard.stop_and_record();
                            warn!(pipeline = H::NAME, "Failed to get watermark: {e}");
                            continue;
                        }
                    }
                }
            };

            // (2) Wait until this information can be acted upon.
            if let Some(wait_for) = watermark.wait_for() {
                debug!(pipeline = H::NAME, ?wait_for, "Waiting to prune");
                tokio::select! {
                    _ = tokio::time::sleep(wait_for) => {}
                    _ = cancel.cancelled() => {
                        info!(pipeline = H::NAME, "Shutdown received");
                        break;
                    }
                }
            }

            // Keep a copy of the watermark for the db_watermark.
            // This is because we can only advance db_watermark when all checkpoints
            // up to it have been pruned.
            let mut db_watermark = watermark.clone();

            // (3) Collect all the new chunks that are ready to be pruned.
            // This will also advance the watermark.
            while let Some((from, to_exclusive)) = watermark.next_chunk(config.max_chunk_size) {
                pending_prune_ranges.insert(from, to_exclusive);
            }

            debug!(
                pipeline = H::NAME,
                "Number of chunks to prune: {}",
                pending_prune_ranges.len()
            );

            // (3) Prune chunk by chunk to avoid the task waiting on a long-running database
            // transaction, between tests for cancellation.
            // Spawn all tasks in parallel, but limit the number of concurrent tasks.
            let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PRUNER_TASKS));
            let mut tasks = FuturesUnordered::new();
            for (from, to_exclusive) in pending_prune_ranges.iter() {
                let semaphore = semaphore.clone();
                let cancel = cancel.child_token();
                let db = db.clone();
                let metrics = metrics.clone();
                let handler = handler.clone();
                let from = *from;
                let to_exclusive = *to_exclusive;

                tasks.push(tokio::spawn(async move {
                    let _permit = tokio::select! {
                        permit = semaphore.acquire() => {
                            permit.unwrap()
                        }
                        _ = cancel.cancelled() => {
                            return ((from, to_exclusive), Err(anyhow::anyhow!("Cancelled")));
                        }
                    };
                    let result = prune_task_impl(metrics, db, handler, from, to_exclusive).await;
                    ((from, to_exclusive), result)
                }));
            }

            // (4) Wait for all tasks to finish.
            // For each task, if it succeeds, remove the range from the pending_prune_ranges.
            // Otherwise the range will remain in the map and will be retried in the next iteration.
            while let Some(r) = tasks.next().await {
                let ((from, to_exclusive), result) = r.unwrap();
                match result {
                    Ok(()) => {
                        let removed = pending_prune_ranges.remove(&from);
                        assert_eq!(
                            removed,
                            Some(to_exclusive),
                            "Removed range is not the same as the one we expected to remove"
                        );
                        // The latest pruner_hi can be derived fromthe first checkpoint that has not yet been pruned.
                        // This would be the first key in the pending_prune_ranges map.
                        // It guarantees that all checkpoints up to it have been pruned.
                        // If the map is empty, then pruner_hi is the last checkpoint that has been pruned.
                        db_watermark.pruner_hi = pending_prune_ranges
                            .keys()
                            .next()
                            .cloned()
                            .unwrap_or(to_exclusive)
                            as i64;
                        metrics
                            .watermark_pruner_hi
                            .with_label_values(&[H::NAME])
                            .set(db_watermark.pruner_hi);
                    }
                    Err(e) => {
                        error!(
                            pipeline = H::NAME,
                            "Failed to prune data for range: {from} to {to_exclusive}: {e}"
                        );
                    }
                }
            }

            // (4) Update the pruner watermark
            let guard = metrics
                .watermark_pruner_write_latency
                .with_label_values(&[H::NAME])
                .start_timer();

            let Ok(mut conn) = db.connect().await else {
                warn!(
                    pipeline = H::NAME,
                    "Pruner failed to connect, while updating watermark"
                );
                continue;
            };

            match db_watermark.update(&mut conn).await {
                Err(e) => {
                    let elapsed = guard.stop_and_record();
                    error!(
                        pipeline = H::NAME,
                        elapsed_ms = elapsed * 1000.0,
                        "Failed to update pruner watermark: {e}"
                    )
                }

                Ok(true) => {
                    let elapsed = guard.stop_and_record();
                    logger.log::<H>(&db_watermark, elapsed);

                    metrics
                        .watermark_pruner_hi_in_db
                        .with_label_values(&[H::NAME])
                        .set(db_watermark.pruner_hi);
                }
                Ok(false) => {}
            }
        }

        info!(pipeline = H::NAME, "Stopping pruner");
    })
}

async fn prune_task_impl<H: Handler + Send + Sync + 'static>(
    metrics: Arc<IndexerMetrics>,
    db: Db,
    handler: Arc<H>,
    from: u64,
    to_exclusive: u64,
) -> Result<(), anyhow::Error> {
    metrics
        .total_pruner_chunks_attempted
        .with_label_values(&[H::NAME])
        .inc();

    let guard = metrics
        .pruner_delete_latency
        .with_label_values(&[H::NAME])
        .start_timer();

    let mut conn = db.connect().await?;

    debug!(pipeline = H::NAME, "Pruning from {from} to {to_exclusive}");

    let affected = match handler.prune(from, to_exclusive, &mut conn).await {
        Ok(affected) => {
            guard.stop_and_record();
            affected
        }

        Err(e) => {
            guard.stop_and_record();
            return Err(e);
        }
    };

    metrics
        .total_pruner_chunks_deleted
        .with_label_values(&[H::NAME])
        .inc();

    metrics
        .total_pruner_rows_deleted
        .with_label_values(&[H::NAME])
        .inc_by(affected as u64);

    Ok(())
}
