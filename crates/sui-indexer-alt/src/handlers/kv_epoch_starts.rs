// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::Range;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use sui_indexer_alt_framework::{
    models::cp_sequence_numbers::epoch_interval,
    pipeline::{concurrent::Handler, Processor},
};
use sui_indexer_alt_schema::{epochs::StoredEpochStart, schema::kv_epoch_starts};
use sui_pg_db as db;
use sui_types::{
    full_checkpoint_content::CheckpointData,
    sui_system_state::{get_sui_system_state, SuiSystemStateTrait},
    transaction::{TransactionDataAPI, TransactionKind},
};

#[derive(Default)]
pub(crate) struct KvEpochStarts;

impl Processor for KvEpochStarts {
    const NAME: &'static str = "kv_epoch_starts";

    type Value = StoredEpochStart;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> Result<Vec<Self::Value>> {
        let CheckpointData {
            checkpoint_summary,
            transactions,
            ..
        } = checkpoint.as_ref();

        // If this is the last checkpoint in the current epoch, it will contain enough information
        // about the start of the next epoch.
        if !checkpoint_summary.is_last_checkpoint_of_epoch() {
            return Ok(vec![]);
        }

        let Some(transaction) = transactions.iter().find(|tx| {
            matches!(
                tx.transaction.intent_message().value.kind(),
                TransactionKind::ChangeEpoch(_) | TransactionKind::EndOfEpochTransaction(_)
            )
        }) else {
            bail!(
                "Failed to get end of epoch transaction in checkpoint {} with EndOfEpochData",
                checkpoint_summary.sequence_number,
            );
        };

        let system_state = get_sui_system_state(&transaction.output_objects.as_slice())
            .context("Failed to find system state object output from end of epoch transaction")?;

        Ok(vec![StoredEpochStart {
            epoch: system_state.epoch() as i64,
            protocol_version: system_state.protocol_version() as i64,
            cp_lo: checkpoint_summary.sequence_number as i64 + 1,
            start_timestamp_ms: system_state.epoch_start_timestamp_ms() as i64,
            reference_gas_price: system_state.reference_gas_price() as i64,
            system_state: bcs::to_bytes(&system_state)
                .context("Failed to serialize SystemState")?,
        }])
    }
}

#[async_trait::async_trait]
impl Handler for KvEpochStarts {
    const MIN_EAGER_ROWS: usize = 1;

    async fn commit(values: &[Self::Value], conn: &mut db::Connection<'_>) -> Result<usize> {
        Ok(diesel::insert_into(kv_epoch_starts::table)
            .values(values)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?)
    }

    async fn prune(
        &self,
        from: u64,
        to_exclusive: u64,
        conn: &mut db::Connection<'_>,
    ) -> Result<usize> {
        let Range {
            start: from_epoch,
            end: to_epoch,
        } = epoch_interval(conn, from..to_exclusive).await?;
        if from_epoch < to_epoch {
            let filter = kv_epoch_starts::table
                .filter(kv_epoch_starts::epoch.between(from_epoch as i64, to_epoch as i64 - 1));
            Ok(diesel::delete(filter).execute(conn).await?)
        } else {
            Ok(0)
        }
    }
}

/// kv_epoch_starts necessitates emulating a live network due to EndOfEpochData presence and a
/// particular transaction kind, so these tests use Simulacrum instead of TestCheckpointDataBuilder.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{ConcurrentLayer, IndexerConfig},
        start_indexer,
    };
    use anyhow::Result;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use simulacrum::Simulacrum;
    use std::{ops::Range, path::PathBuf, time::Duration};
    use sui_indexer_alt_framework::{
        ingestion::{ClientArgs, IngestionConfig},
        models::cp_sequence_numbers::tx_interval,
        pipeline::{
            concurrent::{ConcurrentConfig, PrunerConfig},
            CommitterConfig,
        },
        IndexerArgs,
    };
    use sui_indexer_alt_schema::schema::kv_epoch_starts;
    use sui_pg_db::{
        temp::{get_available_port, TempDb},
        Connection, Db, DbArgs,
    };
    use tempfile::TempDir;
    use tokio::{task::JoinHandle, time::timeout};
    use tokio_util::sync::CancellationToken;

    fn load_indexer_config() -> IndexerConfig {
        let mut base_config = IndexerConfig::default();
        base_config.ingestion = IngestionConfig {
            checkpoint_buffer_size: 10,
            ingest_concurrency: 2,
            retry_interval_ms: 200,
            ..Default::default()
        }
        .into();

        base_config.committer = CommitterConfig {
            write_concurrency: 2,
            collect_interval_ms: 500,
            watermark_interval_ms: 500,
            ..Default::default()
        }
        .into();

        base_config.pipeline.cp_sequence_numbers = ConcurrentLayer::default().into();
        base_config.pipeline.kv_epoch_starts = Some(
            ConcurrentConfig {
                pruner: Some(
                    PrunerConfig {
                        interval_ms: 200,
                        delay_ms: 100,
                        retention: 1,
                        max_chunk_size: 1,
                    }
                    .into(),
                ),
                ..Default::default()
            }
            .into(),
        );
        base_config
    }

    /// The TempDir and TempDb need to be kept alive for the duration of the test, otherwise parts of
    /// the test env will hang indefinitely.
    async fn setup_temp_resources() -> (TempDb, TempDir) {
        let temp_db = TempDb::new().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        (temp_db, temp_dir)
    }

    async fn setup_test_env(
        db_url: String,
        data_ingestion_path: PathBuf,
        indexer_config: IndexerConfig,
    ) -> (
        Simulacrum<StdRng>,
        Db,
        JoinHandle<anyhow::Result<()>>,
        CancellationToken,
    ) {
        // Set up simulacrum
        let rng = StdRng::from_seed([12; 32]);
        let mut sim = Simulacrum::new_with_rng(rng);
        sim.set_data_ingestion_path(data_ingestion_path.clone());

        // Set up direct db pool for test assertions
        let db = Db::for_write(DbArgs {
            database_url: db_url.parse().unwrap(),
            db_connection_pool_size: 1,
            connection_timeout_ms: 60_000,
        })
        .await
        .unwrap();

        // Set up indexer
        let db_args = DbArgs {
            database_url: db_url.parse().unwrap(),
            db_connection_pool_size: 10,
            connection_timeout_ms: 60_000,
        };

        let prom_address = format!("127.0.0.1:{}", get_available_port())
            .parse()
            .unwrap();
        let indexer_args = IndexerArgs {
            metrics_address: prom_address,
            ..Default::default()
        };

        let client_args = ClientArgs {
            remote_store_url: None,
            local_ingestion_path: Some(data_ingestion_path),
        };

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Spawn the indexer in a separate task
        let indexer_handle = tokio::spawn(async move {
            start_indexer(
                db_args,
                indexer_args,
                client_args,
                indexer_config,
                true,
                Some(cancel_clone),
            )
            .await
        });

        (sim, db, indexer_handle, cancel)
    }

    /// Even though the indexer consists of several independent pipelines, the `cp_sequence_numbers`
    /// table governs checkpoint -> tx and epoch lookups and provides such information for prunable
    /// tables. This waits for the lookup table to be updated with the expected changes.
    async fn wait_for_tx_interval(
        conn: &mut Connection<'_>,
        duration: Duration,
        cp_range: Range<u64>,
    ) -> anyhow::Result<()> {
        timeout(duration, async {
            loop {
                match tx_interval(conn, cp_range.clone()).await {
                    Ok(_) => break Ok(()),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Timeout occurred while waiting for tx interval of checkpoints [{}, {})",
                cp_range.start,
                cp_range.end
            )
        })?
    }

    async fn get_all_kv_epoch_starts(conn: &mut Connection<'_>) -> Result<Vec<i64>> {
        let query = kv_epoch_starts::table
            .select(kv_epoch_starts::epoch)
            .load(conn)
            .await?;
        Ok(query)
    }

    async fn wait_for_table_changes(
        conn: &mut Connection<'_>,
        duration: Duration,
        epochs: Vec<i64>,
    ) -> anyhow::Result<()> {
        timeout(duration, async {
            loop {
                match get_all_kv_epoch_starts(conn).await {
                    Ok(fetched_epochs) => {
                        if fetched_epochs == epochs {
                            break Ok(());
                        }
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Timeout occurred while waiting for table changes of epochs {:?}",
                epochs
            )
        })?
    }

    async fn cleanup_test_env(
        cancel: CancellationToken,
        indexer_handle: JoinHandle<anyhow::Result<()>>,
    ) {
        cancel.cancel();
        let _ = indexer_handle.await.expect("Indexer task panicked");
    }

    /// Test that the `cp_sequence_numbers` is correctly committed to.
    #[tokio::test]
    pub async fn test_kv_epoch_starts_advance_multiple_epochs() -> () {
        let indexer_config = load_indexer_config();
        let (temp_db, temp_dir) = setup_temp_resources().await;
        let db_url = temp_db.database().url().as_str().to_owned();
        let data_ingestion_path = temp_dir.path().to_path_buf();
        let (mut sim, db, indexer_handle, cancel) =
            setup_test_env(db_url, data_ingestion_path, indexer_config).await;

        // we start at epoch 0
        sim.advance_epoch(true);
        sim.advance_epoch(true);
        sim.advance_epoch(true);

        let mut conn = db
            .connect()
            .await
            .expect("Failed to retrieve DB connection");

        if let Err(e) = wait_for_tx_interval(&mut conn, Duration::from_secs(5), 0..3).await {
            cleanup_test_env(cancel, indexer_handle).await;
            panic!("{:?}", e);
        }

        // Each advance_epoch produces a tx and chkpt, so at epoch 0, we'll have 1 chkpt that
        // advances to epoch 1. This means that with a retention of 1 checkpoint, we'll end up with
        // 2 epochs in the table.
        if let Err(e) = wait_for_table_changes(&mut conn, Duration::from_secs(15), vec![2, 3]).await
        {
            cleanup_test_env(cancel, indexer_handle).await;
            panic!("{:?}", e);
        }

        cleanup_test_env(cancel, indexer_handle).await;
    }

    /// The checkpoint-based pruner watermark continuously updates the `pruner_hi`, but we don't
    /// want to prune epoch-related data until the `[from, to)` checkpoints are across epochs. Once
    /// at epoch 1, committing checkpoints should not prune the epoch.
    #[tokio::test]
    pub async fn test_kv_epoch_starts_same_epoch() -> () {
        let indexer_config = load_indexer_config();
        let (temp_db, temp_dir) = setup_temp_resources().await;
        let db_url = temp_db.database().url().as_str().to_owned();
        let data_ingestion_path = temp_dir.path().to_path_buf();
        let (mut sim, db, indexer_handle, cancel) =
            setup_test_env(db_url, data_ingestion_path, indexer_config).await;

        sim.advance_epoch(true);
        sim.create_checkpoint();
        sim.create_checkpoint();
        sim.create_checkpoint();

        let mut conn = db
            .connect()
            .await
            .expect("Failed to retrieve DB connection");

        // epoch 0 has been pruned
        if let Err(e) = wait_for_table_changes(&mut conn, Duration::from_secs(5), vec![1]).await {
            cleanup_test_env(cancel, indexer_handle).await;
            panic!("{:?}", e);
        }

        // Once we've reached the expected state of having only epoch 1, manually attempt to prune.
        // No data should be pruned.
        let kv_epoch_starts = KvEpochStarts::default();
        let rows_pruned = kv_epoch_starts.prune(0, 4, &mut conn).await.unwrap();
        let epochs = get_all_kv_epoch_starts(&mut conn).await.unwrap();
        assert_eq!(epochs, vec![1]);
        assert_eq!(rows_pruned, 0);

        cleanup_test_env(cancel, indexer_handle).await;
    }
}
