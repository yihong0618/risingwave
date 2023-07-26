// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use risingwave_pb::hummock::{group_delta, HummockVersionDelta};
use tokio::sync::{mpsc, Mutex};

use crate::hummock::sstable_store::SstableStoreRef;
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

const REFILL_DATA_FILE_CACHE_CONCURRENCY: usize = 100;
const _REFILL_DATA_FILE_CACHE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct CacheRefillPolicyConfig {
    pub sstable_store: SstableStoreRef,
    pub metrics: Arc<CompactorMetrics>,

    pub max_preload_wait_time_mill: u64,

    pub refill_data_file_cache_levels: HashSet<u32>,
}

pub struct CacheRefillPolicy {
    sstable_store: SstableStoreRef,
    _metrics: Arc<CompactorMetrics>,

    _max_preload_wait_time_mill: u64,

    refill_data_file_cache_levels: HashSet<u32>,

    concurrency: Arc<Concurrency>,
}

impl CacheRefillPolicy {
    pub fn new(config: CacheRefillPolicyConfig) -> Self {
        Self {
            sstable_store: config.sstable_store,
            _metrics: config.metrics,

            _max_preload_wait_time_mill: config.max_preload_wait_time_mill,

            refill_data_file_cache_levels: config.refill_data_file_cache_levels,

            concurrency: Arc::new(Concurrency::new(REFILL_DATA_FILE_CACHE_CONCURRENCY)),
        }
    }

    pub async fn execute(self: &Arc<Self>, delta: HummockVersionDelta, _max_level: u32) {
        let policy = self.clone();

        for group_deltas in delta.group_deltas.values() {
            for group_delta in &group_deltas.group_deltas {
                if let Some(group_delta::DeltaType::IntraLevel(level_delta)) =
                    group_delta.delta_type.as_ref()
                {
                    if level_delta.inserted_table_infos.is_empty() {
                        continue;
                    }

                    let mut refill_data = false;
                    if let Some(filter) = policy.sstable_store.data_file_cache_refill_filter() && policy.refill_data_file_cache_levels.contains(&level_delta.level_idx) {
                        for id in &level_delta.removed_table_object_ids {
                            if filter.contains(id) {
                                refill_data = true;
                                break;
                            }
                        }
                    }

                    for sst_info in &level_delta.inserted_table_infos {
                        let sst_info = sst_info.clone();
                        let policy = policy.clone();
                        tokio::spawn(async move {
                            let sst = match policy
                                .sstable_store
                                .may_fill_meta_file_cache(&sst_info)
                                .await
                            {
                                Ok(res) => res,
                                Err(e) => {
                                    tracing::warn!("refill meta error: {}", e);
                                    return;
                                }
                            };
                            if refill_data {
                                for block_index in 0..sst.block_count() {
                                    policy.concurrency.acquire().await;

                                    let policy = policy.clone();
                                    let sst = sst.clone();

                                    tokio::spawn(async move {
                                        let mut stat = StoreLocalStatistic::default();
                                        if let Err(e) = policy
                                            .sstable_store
                                            .may_fill_data_file_cache(&sst, block_index, &mut stat)
                                            .await
                                        {
                                            tracing::warn!("refill data error: {}", e);
                                        }
                                        policy.concurrency.release();
                                    });
                                }
                            }
                        });
                    }
                }
            }
        }
    }
}

pub struct Concurrency {
    tx: mpsc::UnboundedSender<()>,
    rx: Mutex<mpsc::UnboundedReceiver<()>>,
}

impl Concurrency {
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for _ in 0..concurrency {
            tx.send(()).unwrap();
        }
        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    pub async fn acquire(&self) {
        self.rx.lock().await.recv().await.unwrap();
    }

    pub fn release(&self) {
        self.tx.send(()).unwrap();
    }
}
