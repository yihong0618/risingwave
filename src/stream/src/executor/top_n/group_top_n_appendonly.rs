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

// Copyright 2023 Singularity Data
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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::HashKey;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::StateStore;

use super::top_n_cache::AppendOnlyTopNCacheTrait;
use super::utils::*;
use super::{ManagedTopNState, TopNCache};
use crate::cache::{new_indexed_unbounded, ManagedIndexedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorResult;
use crate::executor::{
    ActorContextRef, Executor, ExecutorInfo, PkIndices, Watermark, BUCKET_NUMBER,
    DEFAULT_GHOST_CAP_MUTIPLE, HACK_JOIN_KEY_SIZE, INIT_GHOST_CAP, REAL_UPDATE_INTERVAL_TOP_N,
    SAMPLE_NUM_IN_TEN_K,
};
use crate::task::AtomicU64Ref;

/// If the input is append-only, `AppendOnlyGroupTopNExecutor` does not need
/// to keep all the rows seen. As long as a record
/// is no longer in the result set, it can be deleted.
pub type AppendOnlyGroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    AppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let info = input.info();
        Ok(TopNExecutorWrapper {
            input,
            ctx: ctx.clone(),
            inner: InnerAppendOnlyGroupTopNExecutor::new(
                info,
                storage_key,
                offset_and_limit,
                order_by,
                executor_id,
                group_by,
                state_table,
                watermark_epoch,
                ctx,
            )?,
        })
    }
}

pub struct InnerAppendOnlyGroupTopNExecutor<K: HashKey, S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

    /// `LIMIT XXX`. None means no limit.
    limit: usize,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The storage key indices of the `AppendOnlyGroupTopNExecutor`
    storage_key_indices: PkIndices,

    managed_state: ManagedTopNState<S>,

    /// which column we used to group the data.
    group_by: Vec<usize>,

    /// group key -> cache for this group
    caches: IndexedGroupTopNCache<K, WITH_TIES>,

    traces: Vec<u64>,
    cur_count: i32,
    file: File,
    trace_status: i32,

    /// Used for serializing pk into CacheKey.
    cache_key_serde: CacheKeySerde,

    stats: ExecutionStats,

    ctx: ActorContextRef,
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool>
    InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        ctx: ActorContextRef,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input_info;

        let cache_key_serde = create_cache_key_serde(&storage_key, &schema, &order_by, &group_by);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());
        let stats: ExecutionStats = ExecutionStats::new();
        let cache_metrics_info = MetricsInfo::new(
            ctx.streaming_metrics.clone(),
            managed_state.state_table.table_id(),
            ctx.id,
            "appendonlytopn result table",
        );

        let traces: Vec<u64> = vec![];
        let cur_count = 0;

        let filename = "data.bin";
        let file = File::create(filename).expect("Error creating file");
        let trace_status = 0;

        Ok(Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("AppendOnlyGroupTopNExecutor {:X}", executor_id),
            },
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            group_by,
            caches: IndexedGroupTopNCache::new(
                watermark_epoch,
                cache_metrics_info,
                INIT_GHOST_CAP,
                REAL_UPDATE_INTERVAL_TOP_N,
                BUCKET_NUMBER,
            ),
            traces,
            cur_count,
            file,
            trace_status,
            cache_key_serde,
            stats,
            ctx,
        })
    }

    fn flush_stats(&mut self) {
        let actor_id_str = self.ctx.id.to_string();
        let table_id_str = self.managed_state.state_table.table_id().to_string();

        self.ctx
            .streaming_metrics
            .lookup_new_count
            .with_label_values(&[&table_id_str, &actor_id_str, "appendonlytopn"])
            .inc_by(self.stats.lookup_miss_count - self.stats.lookup_real_miss_count);

        self.ctx
            .streaming_metrics
            .group_top_n_appendonly_total_query_cache_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(self.stats.total_lookup_count);

        self.ctx
            .streaming_metrics
            .group_top_n_appendonly_cache_miss_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(self.stats.lookup_miss_count);

        self.ctx
            .streaming_metrics
            .group_top_n_appendonly_cache_real_miss_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .inc_by(self.stats.lookup_real_miss_count);

        self.stats.total_lookup_count = 0;
        self.stats.lookup_miss_count = 0;
        self.stats.lookup_real_miss_count = 0;
        let cache_entry_count = self.caches.len();

        for i in 0..=BUCKET_NUMBER {
            let count = self.stats.bucket_counts[i];
            self.ctx
                .streaming_metrics
                .cache_real_resue_distance_bucket_count
                .with_label_values(&[
                    &actor_id_str,
                    &table_id_str,
                    "appendonlytopn",
                    &self.stats.bucket_ids[i],
                ])
                .inc_by(count as u64);
            self.stats.bucket_counts[i] = 0;

            let ghost_count = self.stats.ghost_bucket_counts[i];
            self.ctx
                .streaming_metrics
                .cache_ghost_resue_distance_bucket_count
                .with_label_values(&[
                    &actor_id_str,
                    &table_id_str,
                    "appendonlytopn",
                    &self.stats.bucket_ids[i],
                ])
                .inc_by(ghost_count as u64);
            self.stats.ghost_bucket_counts[i] = 0;
        }

        self.ctx
            .streaming_metrics
            .mrc_bucket_info
            .with_label_values(&[&table_id_str, &actor_id_str, "appendonlytopn", "bucket"])
            .set(self.caches.bucket_count() as i64);
        self.ctx
            .streaming_metrics
            .mrc_bucket_info
            .with_label_values(&[
                &table_id_str,
                &actor_id_str,
                "appendonlytopn",
                "ghost_bucket",
            ])
            .set(self.caches.ghost_bucket_count() as i64);
        self.ctx
            .streaming_metrics
            .mrc_bucket_info
            .with_label_values(&[
                &table_id_str,
                &actor_id_str,
                "appendonlytopn",
                "ghost_start",
            ])
            .set(self.stats.ghost_start as i64);
        self.ctx
            .streaming_metrics
            .mrc_bucket_info
            .with_label_values(&[&table_id_str, &actor_id_str, "appendonlytopn", "ghost_cap"])
            .set(self.caches.ghost_cap() as i64);

        Self::update_bucket_size(&mut self.caches, &mut self.stats, cache_entry_count);
    }

    fn update_bucket_size(
        cache: &mut IndexedGroupTopNCache<K, WITH_TIES>,
        stats: &mut ExecutionStats,
        entry_count: usize,
    ) {
        let old_entry_count = stats.bucket_size * BUCKET_NUMBER;
        if (old_entry_count as f64 * 1.2 < entry_count as f64
            || old_entry_count as f64 * 0.7 > entry_count as f64)
            && entry_count > 100
        {
            let mut ghost_cap_multiple = DEFAULT_GHOST_CAP_MUTIPLE;
            let k_size = cache.key_size.unwrap_or(HACK_JOIN_KEY_SIZE);
            if let Some(kv_size) = cache.get_avg_kv_size() {
                let v_size = kv_size - k_size;
                let multiple = v_size / k_size;
                ghost_cap_multiple = usize::min(usize::max(multiple, 1), ghost_cap_multiple);
            }
            let ghost_cap = ghost_cap_multiple * entry_count;

            stats.bucket_size = std::cmp::max(
                (entry_count as f64 * 1.1 / BUCKET_NUMBER as f64).round() as usize,
                1,
            );
            stats.ghost_bucket_size = std::cmp::max(
                ((entry_count as f64 * 0.3 + ghost_cap as f64) / BUCKET_NUMBER as f64).round()
                    as usize,
                1,
            );
            stats.ghost_start = std::cmp::max((entry_count as f64 * 0.8).round() as usize, 1);
            info!(
                "WKXLOG ghost_start switch to {}, old_entry_count: {}, new_entry_count: {}",
                stats.ghost_start, old_entry_count, entry_count
            );
            cache.set_ghost_cap(ghost_cap);
        }
    }
}

#[async_trait]
impl<K: HashKey, S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerAppendOnlyGroupTopNExecutor<K, S, WITH_TIES>
where
    TopNCache<WITH_TIES>: AppendOnlyTopNCacheTrait,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        if self.cur_count < 26000000 + 16384 && self.traces.len() >= 16384 {
            write_u64_array_to_file(&mut self.file, &self.traces).expect("Error writing file");
            self.traces.clear();
            if self.trace_status == 0 {
                tracing::info!("[WKXLOG]_trace Started to output traces");
                self.trace_status += 1;
            }
            if self.cur_count < 26000000 + 16384 && self.traces.len() >= 16384 {
                write_u64_array_to_file(&mut self.file, &self.traces).expect("Error writing file");
                self.traces.clear();
                if self.trace_status == 0 {
                    tracing::info!("[WKXLOG]_trace Started to output traces");
                    self.trace_status += 1;
                }
            }

            if self.cur_count >= 26000000 + 16384 && self.trace_status == 1 {
                self.trace_status += 1;
            }

            if self.trace_status == 2 {
                tracing::info!("[WKXLOG]_trace Finished to output traces");
                self.trace_status += 1;
            }
        }

        if self.cur_count >= 26000000 + 16384 && self.trace_status == 1 {
            self.trace_status += 1;
        }

        if self.trace_status == 2 {
            tracing::info!("[WKXLOG]_trace Finished to output traces");
            self.trace_status += 1;
        }

        let mut res_ops = Vec::with_capacity(self.limit);
        let mut res_rows = Vec::with_capacity(self.limit);
        let chunk = chunk.compact();
        let keys = K::build(&self.group_by, chunk.data_chunk())?;

        let data_types = self.schema().data_types();
        let row_deserializer = RowDeserializer::new(data_types.clone());
        let table_id_str = self.managed_state.state_table.table_id().to_string();
        let actor_id_str = self.ctx.id.to_string();
        for ((op, row_ref), group_cache_key) in chunk.rows().zip_eq_debug(keys.iter()) {
            // The pk without group by
            let pk_row = row_ref.project(&self.storage_key_indices[self.group_by.len()..]);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);

            let group_key = row_ref.project(&self.group_by);
            self.stats.total_lookup_count += 1;
            self.cur_count += 1;

            let mut hasher = DefaultHasher::new();
            group_cache_key.hash(&mut hasher);
            let hashed_key = hasher.finish();
            let sampled = hashed_key % 10000 < SAMPLE_NUM_IN_TEN_K;

            if self.cur_count > 15000000 && self.cur_count < 26000000 {
                self.traces.push(hashed_key);
            }

            // If 'self.caches' does not already have a cache for the current group, create a new
            // cache for it and insert it into `self.caches`

            let (exist, dis) = self.caches.contains_sampled(group_cache_key, sampled);
            if let Some((distance, is_ghost)) = dis {
                if is_ghost {
                    let bucket_index = if distance < self.stats.ghost_start as u32 {
                        0
                    } else if distance
                        > (self.stats.ghost_start + self.stats.ghost_bucket_size * BUCKET_NUMBER)
                            as u32
                    {
                        BUCKET_NUMBER
                    } else {
                        (distance as usize - self.stats.ghost_start) / self.stats.ghost_bucket_size
                    };
                    self.stats.ghost_bucket_counts[bucket_index] += 1;
                } else if sampled {
                    let bucket_index = if distance > (self.stats.bucket_size * BUCKET_NUMBER) as u32
                    {
                        BUCKET_NUMBER
                    } else {
                        distance as usize / self.stats.bucket_size
                    };
                    self.stats.bucket_counts[bucket_index] += 1;
                }
            }
            if !exist {
                self.stats.lookup_miss_count += 1;
                let mut topn_cache = TopNCache::new(self.offset, self.limit, data_types.clone());
                self.managed_state
                    .init_topn_cache(Some(group_key), &mut topn_cache)
                    .await?;
                if !topn_cache.is_empty() {
                    self.stats.lookup_real_miss_count += 1;
                }
                self.caches.put(group_cache_key.clone(), topn_cache);
            }

            let mut cache = self.caches.peek_mut(group_cache_key).unwrap();

            debug_assert_eq!(op, Op::Insert);
            cache.insert(
                cache_key,
                row_ref,
                &mut res_ops,
                &mut res_rows,
                &mut self.managed_state,
                &row_deserializer,
            )?;
        }
        self.ctx
            .streaming_metrics
            .group_top_n_appendonly_cached_entry_count
            .with_label_values(&[&table_id_str, &actor_id_str])
            .set(self.caches.len() as i64);
        generate_output(res_rows, res_ops, self.schema())
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.flush_stats();
        self.managed_state.flush(epoch).await
    }

    fn info(&self) -> &ExecutorInfo {
        &self.info
    }

    fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        let (_previous_vnode_bitmap, cache_may_stale) = self
            .managed_state
            .state_table
            .update_vnode_bitmap(vnode_bitmap);

        if cache_may_stale {
            self.caches.clear();
        }
    }

    fn evict(&mut self) {
        self.caches.evict()
    }

    fn update_epoch(&mut self, epoch: u64) {
        self.caches.update_epoch(epoch)
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        Ok(())
    }

    async fn handle_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if watermark.col_idx == self.group_by[0] {
            self.managed_state
                .state_table
                .update_watermark(watermark.val.clone(), false);
            Some(watermark)
        } else {
            None
        }
    }

    fn update_size_limit(&mut self, table_cache_sizes: &HashMap<u32, u64>) {
        let table_id = self.managed_state.state_table.table_id();
        if let Some(cache_size) = table_cache_sizes.get(&table_id) {
            self.caches.update_size_limit(*cache_size as usize);
            tracing::info!(
                "WKXLOG: Success update_size_limit table_id {}, to cache size: {}",
                table_id,
                cache_size
            );
        } else {
            tracing::warn!(
                "WKXLOG: WARN!!! update_size_limit cannot find table_id {} in table_cache_size: {:?}",
                table_id,
                table_cache_sizes
            );
        }
        tracing::info!(
            "WKXNB! agg table cache updated! table_cache_sizes: {:?}, fragment_id: {}",
            table_cache_sizes,
            self.ctx.fragment_id
        );
    }
}

struct ExecutionStats {
    lookup_miss_count: u64,
    lookup_real_miss_count: u64,
    total_lookup_count: u64,

    bucket_size: usize,
    ghost_bucket_size: usize,
    ghost_start: usize,

    bucket_ids: Vec<String>,
    bucket_counts: Vec<usize>,
    ghost_bucket_counts: Vec<usize>,
}

impl ExecutionStats {
    fn new() -> Self {
        let mut bucket_ids = vec![];
        for i in 0..=BUCKET_NUMBER {
            bucket_ids.push(i.to_string());
        }
        let bucket_counts = vec![0; BUCKET_NUMBER + 1];
        let ghost_bucket_counts = vec![0; BUCKET_NUMBER + 1];
        Self {
            lookup_miss_count: 0,
            lookup_real_miss_count: 0,
            total_lookup_count: 0,
            bucket_size: 1,
            ghost_bucket_size: 1,
            ghost_start: 0,
            bucket_ids,
            bucket_counts,
            ghost_bucket_counts,
        }
    }
}

pub struct IndexedGroupTopNCache<K: HashKey, const WITH_TIES: bool> {
    data: ManagedIndexedLruCache<K, TopNCache<WITH_TIES>>,
}

impl<K: HashKey, const WITH_TIES: bool> IndexedGroupTopNCache<K, WITH_TIES> {
    pub fn new(
        watermark_epoch: AtomicU64Ref,
        metrics_info: MetricsInfo,
        ghost_cap: usize,
        update_interval: u32,
        ghost_bucket_count: usize,
    ) -> Self {
        let cache = new_indexed_unbounded(
            watermark_epoch,
            metrics_info,
            ghost_cap,
            update_interval,
            ghost_bucket_count,
        );
        Self { data: cache }
    }
}

impl<K: HashKey, const WITH_TIES: bool> Deref for IndexedGroupTopNCache<K, WITH_TIES> {
    type Target = ManagedIndexedLruCache<K, TopNCache<WITH_TIES>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<K: HashKey, const WITH_TIES: bool> DerefMut for IndexedGroupTopNCache<K, WITH_TIES> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

fn write_u64_array_to_file(file: &mut File, data: &[u64]) -> std::io::Result<()> {
    file.write_all(bytemuck::cast_slice(data))?;
    Ok(())
}
