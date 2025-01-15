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
use sui_indexer_alt_schema::{epochs::StoredEpochEnd, schema::kv_epoch_ends};
use sui_pg_db as db;
use sui_types::{
    event::SystemEpochInfoEvent,
    full_checkpoint_content::CheckpointData,
    transaction::{TransactionDataAPI, TransactionKind},
};

#[derive(Default)]
pub(crate) struct KvEpochEnds;

impl Processor for KvEpochEnds {
    const NAME: &'static str = "kv_epoch_ends";

    type Value = StoredEpochEnd;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> Result<Vec<Self::Value>> {
        let CheckpointData {
            checkpoint_summary,
            transactions,
            ..
        } = checkpoint.as_ref();

        let Some(end_of_epoch) = checkpoint_summary.end_of_epoch_data.as_ref() else {
            return Ok(vec![]);
        };

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

        if let Some(SystemEpochInfoEvent {
            total_stake,
            storage_fund_reinvestment,
            storage_charge,
            storage_rebate,
            storage_fund_balance,
            stake_subsidy_amount,
            total_gas_fees,
            total_stake_rewards_distributed,
            leftover_storage_fund_inflow,
            ..
        }) = transaction
            .events
            .iter()
            .flat_map(|events| &events.data)
            .find_map(|event| {
                event
                    .is_system_epoch_info_event()
                    .then(|| bcs::from_bytes(&event.contents))
            })
            .transpose()
            .context("Failed to deserialize SystemEpochInfoEvent")?
        {
            Ok(vec![StoredEpochEnd {
                epoch: checkpoint_summary.epoch as i64,
                cp_hi: checkpoint_summary.sequence_number as i64 + 1,
                tx_hi: checkpoint_summary.network_total_transactions as i64,
                end_timestamp_ms: checkpoint_summary.timestamp_ms as i64,

                safe_mode: false,

                total_stake: Some(total_stake as i64),
                storage_fund_balance: Some(storage_fund_balance as i64),
                storage_fund_reinvestment: Some(storage_fund_reinvestment as i64),
                storage_charge: Some(storage_charge as i64),
                storage_rebate: Some(storage_rebate as i64),
                stake_subsidy_amount: Some(stake_subsidy_amount as i64),
                total_gas_fees: Some(total_gas_fees as i64),
                total_stake_rewards_distributed: Some(total_stake_rewards_distributed as i64),
                leftover_storage_fund_inflow: Some(leftover_storage_fund_inflow as i64),

                epoch_commitments: bcs::to_bytes(&end_of_epoch.epoch_commitments)
                    .context("Failed to serialize EpochCommitment-s")?,
            }])
        } else {
            Ok(vec![StoredEpochEnd {
                epoch: checkpoint_summary.epoch as i64,
                cp_hi: checkpoint_summary.sequence_number as i64 + 1,
                tx_hi: checkpoint_summary.network_total_transactions as i64,
                end_timestamp_ms: checkpoint_summary.timestamp_ms as i64,

                safe_mode: true,

                total_stake: None,
                storage_fund_balance: None,
                storage_fund_reinvestment: None,
                storage_charge: None,
                storage_rebate: None,
                stake_subsidy_amount: None,
                total_gas_fees: None,
                total_stake_rewards_distributed: None,
                leftover_storage_fund_inflow: None,

                epoch_commitments: bcs::to_bytes(&end_of_epoch.epoch_commitments)
                    .context("Failed to serialize EpochCommitment-s")?,
            }])
        }
    }
}

#[async_trait::async_trait]
impl Handler for KvEpochEnds {
    const MIN_EAGER_ROWS: usize = 1;

    async fn commit(values: &[Self::Value], conn: &mut db::Connection<'_>) -> Result<usize> {
        Ok(diesel::insert_into(kv_epoch_ends::table)
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
            let filter = kv_epoch_ends::table
                .filter(kv_epoch_ends::epoch.between(from_epoch as i64, to_epoch as i64 - 1));
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
        base_config.pipeline.kv_epoch_ends = Some(
            ConcurrentConfig {
                pruner: Some(
                    PrunerConfig {
                        interval_ms: 200,
                        delay_ms: 100,
                        retention: 10, // This is so we don't prune immediately after writing epoch end data
                        max_chunk_size: 10,
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

    async fn get_all_kv_epoch_ends(conn: &mut Connection<'_>) -> Result<Vec<i64>> {
        let result = kv_epoch_ends::table
            .select(kv_epoch_ends::epoch)
            .load(conn)
            .await?;
        Ok(result)
    }

    async fn wait_for_table_changes(
        conn: &mut Connection<'_>,
        duration: Duration,
        epochs: Vec<i64>,
    ) -> anyhow::Result<()> {
        timeout(duration, async {
            loop {
                match get_all_kv_epoch_ends(conn).await {
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
    pub async fn test_kv_epoch_ends_advance_multiple_epochs() -> () {
        let indexer_config = load_indexer_config();
        let (temp_db, temp_dir) = setup_temp_resources().await;
        let db_url = temp_db.database().url().as_str().to_owned();
        let data_ingestion_path = temp_dir.path().to_path_buf();
        let (mut sim, db, indexer_handle, cancel) =
            setup_test_env(db_url, data_ingestion_path, indexer_config).await;
        let kv_epoch_ends = KvEpochEnds::default();

        // we start at epoch 0
        sim.advance_epoch(true);
        sim.advance_epoch(true);
        sim.advance_epoch(true);
        // and progress to epoch 3

        let mut conn = db
            .connect()
            .await
            .expect("Failed to retrieve DB connection");

        if let Err(e) = wait_for_tx_interval(&mut conn, Duration::from_secs(5), 0..3).await {
            cleanup_test_env(cancel, indexer_handle).await;
            panic!("{:?}", e);
        }

        // This test is a bit different from kv_epoch_starts - due to the longer 10 chpkt retention, at this point all epochs should still be available.
        if let Err(e) =
            wait_for_table_changes(&mut conn, Duration::from_secs(15), vec![0, 1, 2]).await
        {
            cleanup_test_env(cancel, indexer_handle).await;
            panic!("{:?}", e);
        }

        let rows_pruned = kv_epoch_ends.prune(0, 3, &mut conn).await.unwrap();
        let epochs = get_all_kv_epoch_ends(&mut conn).await.unwrap();
        assert_eq!(epochs, vec![2]);
        assert_eq!(rows_pruned, 2);

        cleanup_test_env(cancel, indexer_handle).await;
    }

    /// Epoch end table retention must be larger than one epoch's worth of checkpoints - otherwise
    /// we'll prune the entry for the previous epoch at boundary shortly after writing it.
    #[tokio::test]
    pub async fn test_kv_epoch_ends_same_epoch() -> () {
        let indexer_config = load_indexer_config();
        let (temp_db, temp_dir) = setup_temp_resources().await;
        let db_url = temp_db.database().url().as_str().to_owned();
        let data_ingestion_path = temp_dir.path().to_path_buf();
        let (mut sim, db, indexer_handle, cancel) =
            setup_test_env(db_url, data_ingestion_path, indexer_config).await;

        sim.create_checkpoint();
        sim.create_checkpoint();
        sim.advance_epoch(true);
        sim.create_checkpoint();
        sim.create_checkpoint();
        sim.create_checkpoint();

        let mut conn = db
            .connect()
            .await
            .expect("Failed to retrieve DB connection");

        // Assert that we didn't write epoch 1, still have epoch 0 because within retention.
        if let Err(e) = wait_for_table_changes(&mut conn, Duration::from_secs(5), vec![0]).await {
            cleanup_test_env(cancel, indexer_handle).await;
            panic!("{:?}", e);
        }

        // Once we've reached the expected state, manually attempt to prune. Data will be pruned.
        let kv_epoch_ends = KvEpochEnds::default();
        let rows_pruned = kv_epoch_ends.prune(0, 4, &mut conn).await.unwrap();
        let epochs = get_all_kv_epoch_ends(&mut conn).await.unwrap();
        assert_eq!(epochs, Vec::<i64>::new());
        assert_eq!(rows_pruned, 1);

        cleanup_test_env(cancel, indexer_handle).await;
    }
}
