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

use prometheus::core::{
    AtomicF64, AtomicI64, AtomicU64, Collector, GenericCounterVec, GenericGaugeVec,
};
use prometheus::{
    exponential_buckets, histogram_opts, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, Histogram,
    HistogramVec, IntCounter, IntGauge, Registry,
};

pub struct StreamingMetrics {
    pub registry: Registry,
    pub executor_row_count: GenericCounterVec<AtomicU64>,
    pub actor_execution_time: GenericGaugeVec<AtomicF64>,
    pub actor_output_buffer_blocking_duration_ns: GenericCounterVec<AtomicU64>,
    pub actor_input_buffer_blocking_duration_ns: GenericCounterVec<AtomicU64>,
    pub actor_scheduled_duration: GenericGaugeVec<AtomicF64>,
    pub actor_scheduled_cnt: GenericGaugeVec<AtomicI64>,
    pub actor_fast_poll_duration: GenericGaugeVec<AtomicF64>,
    pub actor_fast_poll_cnt: GenericGaugeVec<AtomicI64>,
    pub actor_slow_poll_duration: GenericGaugeVec<AtomicF64>,
    pub actor_slow_poll_cnt: GenericGaugeVec<AtomicI64>,
    pub actor_poll_duration: GenericGaugeVec<AtomicF64>,
    pub actor_poll_cnt: GenericGaugeVec<AtomicI64>,
    pub actor_idle_duration: GenericGaugeVec<AtomicF64>,
    pub actor_idle_cnt: GenericGaugeVec<AtomicI64>,
    pub actor_memory_usage: GenericGaugeVec<AtomicI64>,
    pub actor_in_record_cnt: GenericCounterVec<AtomicU64>,
    pub actor_out_record_cnt: GenericCounterVec<AtomicU64>,
    pub actor_sampled_deserialize_duration_ns: GenericCounterVec<AtomicU64>,
    pub source_output_row_count: GenericCounterVec<AtomicU64>,
    pub source_row_per_barrier: GenericCounterVec<AtomicU64>,

    // Exchange (see also `compute::ExchangeServiceMetrics`)
    pub exchange_frag_recv_size: GenericCounterVec<AtomicU64>,

    // Streaming Join
    pub join_lookup_miss_count: GenericCounterVec<AtomicU64>,
    pub join_total_lookup_count: GenericCounterVec<AtomicU64>,
    pub join_insert_cache_miss_count: GenericCounterVec<AtomicU64>,
    pub join_actor_input_waiting_duration_ns: GenericCounterVec<AtomicU64>,
    pub join_match_duration_ns: GenericCounterVec<AtomicU64>,
    pub join_barrier_align_duration: HistogramVec,
    pub join_cached_entries: GenericGaugeVec<AtomicI64>,
    pub join_cached_rows: GenericGaugeVec<AtomicI64>,
    pub join_cached_estimated_size: GenericGaugeVec<AtomicI64>,

    // Streaming Aggregation
    // pub agg_lookup_miss_count: GenericCounterVec<AtomicU64>,
    // pub agg_total_lookup_count: GenericCounterVec<AtomicU64>,
    // pub agg_cached_keys: GenericGaugeVec<AtomicI64>,
    // pub agg_chunk_lookup_miss_count: GenericCounterVec<AtomicU64>,
    // pub agg_chunk_total_lookup_count: GenericCounterVec<AtomicU64>,

    // Backfill
    pub backfill_snapshot_read_row_count: GenericCounterVec<AtomicU64>,
    pub backfill_upstream_output_row_count: GenericCounterVec<AtomicU64>,

    /// The duration from receipt of barrier to all actors collection.
    /// And the max of all node `barrier_inflight_latency` is the latency for a barrier
    /// to flow through the graph.
    pub barrier_inflight_latency: Histogram,
    /// The duration of sync to storage.
    pub barrier_sync_latency: Histogram,

    pub sink_commit_duration: HistogramVec,

    pub sink_output_row_count: GenericCounterVec<AtomicU64>,

    // Memory management
    // FIXME(yuhao): use u64 here
    pub lru_current_watermark_time_ms: IntGauge,
    pub lru_physical_now_ms: IntGauge,
    pub lru_runtime_loop_count: IntCounter,
    pub lru_watermark_step: IntGauge,
    pub jemalloc_allocated_bytes: IntGauge,
    pub jemalloc_active_bytes: IntGauge,

    /// User compute error reporting
    pub user_compute_error_count: GenericCounterVec<AtomicU64>,

    // Materialize
    pub materialize_cache_hit_count: GenericCounterVec<AtomicU64>,
    pub materialize_cache_total_count: GenericCounterVec<AtomicU64>,
}

impl StreamingMetrics {
    pub fn new(registry: Registry) -> Self {
        let executor_row_count = register_int_counter_vec_with_registry!(
            "stream_executor_row_count",
            "Total number of rows that have been output from each executor",
            &["actor_id", "executor_id"],
            registry
        )
        .unwrap();

        let source_output_row_count = register_int_counter_vec_with_registry!(
            "stream_source_output_rows_counts",
            "Total number of rows that have been output from source",
            &["source_id", "source_name"],
            registry
        )
        .unwrap();

        let source_row_per_barrier = register_int_counter_vec_with_registry!(
            "stream_source_rows_per_barrier_counts",
            "Total number of rows that have been output from source per barrier",
            &["actor_id", "executor_id"],
            registry
        )
        .unwrap();

        let actor_execution_time = register_gauge_vec_with_registry!(
            "stream_actor_actor_execution_time",
            "Total execution time (s) of an actor",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_output_buffer_blocking_duration_ns = register_int_counter_vec_with_registry!(
            "stream_actor_output_buffer_blocking_duration_ns",
            "Total blocking duration (ns) of output buffer",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_input_buffer_blocking_duration_ns = register_int_counter_vec_with_registry!(
            "stream_actor_input_buffer_blocking_duration_ns",
            "Total blocking duration (ns) of input buffer",
            &["actor_id", "upstream_fragment_id"],
            registry
        )
        .unwrap();

        let exchange_frag_recv_size = register_int_counter_vec_with_registry!(
            "stream_exchange_frag_recv_size",
            "Total size of messages that have been received from upstream Fragment",
            &["up_fragment_id", "down_fragment_id"],
            registry
        )
        .unwrap();

        let actor_fast_poll_duration = register_gauge_vec_with_registry!(
            "stream_actor_fast_poll_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_fast_poll_cnt = register_int_gauge_vec_with_registry!(
            "stream_actor_fast_poll_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_slow_poll_duration = register_gauge_vec_with_registry!(
            "stream_actor_slow_poll_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_slow_poll_cnt = register_int_gauge_vec_with_registry!(
            "stream_actor_slow_poll_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_poll_duration = register_gauge_vec_with_registry!(
            "stream_actor_poll_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_poll_cnt = register_int_gauge_vec_with_registry!(
            "stream_actor_poll_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_scheduled_duration = register_gauge_vec_with_registry!(
            "stream_actor_scheduled_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_scheduled_cnt = register_int_gauge_vec_with_registry!(
            "stream_actor_scheduled_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_idle_duration = register_gauge_vec_with_registry!(
            "stream_actor_idle_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_idle_cnt = register_int_gauge_vec_with_registry!(
            "stream_actor_idle_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_in_record_cnt = register_int_counter_vec_with_registry!(
            "stream_actor_in_record_cnt",
            "Total number of rows actor received",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_out_record_cnt = register_int_counter_vec_with_registry!(
            "stream_actor_out_record_cnt",
            "Total number of rows actor sent",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_sampled_deserialize_duration_ns = register_int_counter_vec_with_registry!(
            "actor_sampled_deserialize_duration_ns",
            "Duration (ns) of sampled chunk deserialization",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_memory_usage = register_int_gauge_vec_with_registry!(
            "actor_memory_usage",
            "Memory usage (bytes)",
            &["actor_id"],
            registry,
        )
        .unwrap();

        let join_lookup_miss_count = register_int_counter_vec_with_registry!(
            "stream_join_lookup_miss_count",
            "Join executor lookup miss duration",
            &["side", "join_table_id", "degree_table_id", "actor_id"],
            registry
        )
        .unwrap();

        let join_total_lookup_count = register_int_counter_vec_with_registry!(
            "stream_join_lookup_total_count",
            "Join executor lookup total operation",
            &["side", "join_table_id", "degree_table_id", "actor_id"],
            registry
        )
        .unwrap();

        let join_insert_cache_miss_count = register_int_counter_vec_with_registry!(
            "stream_join_insert_cache_miss_count",
            "Join executor cache miss when insert operation",
            &["side", "join_table_id", "degree_table_id", "actor_id"],
            registry
        )
        .unwrap();

        let join_actor_input_waiting_duration_ns = register_int_counter_vec_with_registry!(
            "stream_join_actor_input_waiting_duration_ns",
            "Total waiting duration (ns) of input buffer of join actor",
            &["actor_id"],
            registry
        )
        .unwrap();

        let join_match_duration_ns = register_int_counter_vec_with_registry!(
            "stream_join_match_duration_ns",
            "Matching duration for each side",
            &["actor_id", "side"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "stream_join_barrier_align_duration",
            "Duration of join align barrier",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let join_barrier_align_duration =
            register_histogram_vec_with_registry!(opts, &["actor_id", "wait_side"], registry)
                .unwrap();

        let join_cached_entries = register_int_gauge_vec_with_registry!(
            "stream_join_cached_entries",
            "Number of cached entries in streaming join operators",
            &["actor_id", "side"],
            registry
        )
        .unwrap();

        let join_cached_rows = register_int_gauge_vec_with_registry!(
            "stream_join_cached_rows",
            "Number of cached rows in streaming join operators",
            &["actor_id", "side"],
            registry
        )
        .unwrap();

        let join_cached_estimated_size = register_int_gauge_vec_with_registry!(
            "stream_join_cached_estimated_size",
            "Estimated size of all cached entries in streaming join operators",
            &["actor_id", "side"],
            registry
        )
        .unwrap();

        // let agg_lookup_miss_count = register_int_counter_vec_with_registry!(
        //     "stream_agg_lookup_miss_count",
        //     "Aggregation executor lookup miss duration",
        //     &["table_id", "actor_id"],
        //     registry
        // )
        // .unwrap();

        // let agg_total_lookup_count = register_int_counter_vec_with_registry!(
        //     "stream_agg_lookup_total_count",
        //     "Aggregation executor lookup total operation",
        //     &["table_id", "actor_id"],
        //     registry
        // )
        // .unwrap();

        // let agg_cached_keys = register_int_gauge_vec_with_registry!(
        //     "stream_agg_cached_keys",
        //     "Number of cached keys in streaming aggregation operators",
        //     &["table_id", "actor_id"],
        //     registry
        // )
        // .unwrap();

        // let agg_chunk_lookup_miss_count = register_int_counter_vec_with_registry!(
        //     "stream_agg_chunk_lookup_miss_count",
        //     "Aggregation executor chunk-level lookup miss duration",
        //     &["table_id", "actor_id"],
        //     registry
        // )
        // .unwrap();

        // let agg_chunk_total_lookup_count = register_int_counter_vec_with_registry!(
        //     "stream_agg_chunk_lookup_total_count",
        //     "Aggregation executor chunk-level lookup total operation",
        //     &["table_id", "actor_id"],
        //     registry
        // )
        // .unwrap();

        let backfill_snapshot_read_row_count = register_int_counter_vec_with_registry!(
            "stream_backfill_snapshot_read_row_count",
            "Total number of rows that have been read from the backfill snapshot",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();

        let backfill_upstream_output_row_count = register_int_counter_vec_with_registry!(
            "stream_backfill_upstream_output_row_count",
            "Total number of rows that have been output from the backfill upstream",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "stream_barrier_inflight_duration_seconds",
            "barrier_inflight_latency",
            exponential_buckets(0.1, 1.5, 16).unwrap() // max 43s
        );
        let barrier_inflight_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "stream_barrier_sync_storage_duration_seconds",
            "barrier_sync_latency",
            exponential_buckets(0.1, 1.5, 16).unwrap() // max 43s
        );
        let barrier_sync_latency = register_histogram_with_registry!(opts, registry).unwrap();
        let sink_commit_duration = register_histogram_vec_with_registry!(
            "sink_commit_duration",
            "Duration of commit op in sink",
            &["executor_id", "connector"],
            registry
        )
        .unwrap();

        let sink_output_row_count = register_int_counter_vec_with_registry!(
            "stream_sink_output_rows_counts",
            "Total number of rows that have been output to sink",
            &["sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let lru_current_watermark_time_ms = register_int_gauge_with_registry!(
            "lru_current_watermark_time_ms",
            "Current LRU manager watermark time(ms)",
            registry
        )
        .unwrap();

        let lru_physical_now_ms = register_int_gauge_with_registry!(
            "lru_physical_now_ms",
            "Current physical time in Risingwave(ms)",
            registry
        )
        .unwrap();

        let lru_runtime_loop_count = register_int_counter_with_registry!(
            "lru_runtime_loop_count",
            "The counts of the eviction loop in LRU manager per second",
            registry
        )
        .unwrap();

        let lru_watermark_step = register_int_gauge_with_registry!(
            "lru_watermark_step",
            "The steps increase in 1 loop",
            registry
        )
        .unwrap();

        let jemalloc_allocated_bytes = register_int_gauge_with_registry!(
            "jemalloc_allocated_bytes",
            "The allocated memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_active_bytes = register_int_gauge_with_registry!(
            "jemalloc_active_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let user_compute_error_count = register_int_counter_vec_with_registry!(
            "user_compute_error_count",
            "Compute errors in the system, queryable by tags",
            &["error_type", "error_msg", "executor_name", "fragment_id"],
            registry,
        )
        .unwrap();

        let materialize_cache_hit_count = register_int_counter_vec_with_registry!(
            "stream_materialize_cache_hit_count",
            "Materialize executor cache hit count",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();

        let materialize_cache_total_count = register_int_counter_vec_with_registry!(
            "stream_materialize_cache_total_count",
            "Materialize executor cache total operation",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();
        Self {
            registry,
            executor_row_count,
            actor_execution_time,
            actor_output_buffer_blocking_duration_ns,
            actor_input_buffer_blocking_duration_ns,
            actor_scheduled_duration,
            actor_scheduled_cnt,
            actor_fast_poll_duration,
            actor_fast_poll_cnt,
            actor_slow_poll_duration,
            actor_slow_poll_cnt,
            actor_poll_duration,
            actor_poll_cnt,
            actor_idle_duration,
            actor_idle_cnt,
            actor_memory_usage,
            actor_in_record_cnt,
            actor_out_record_cnt,
            actor_sampled_deserialize_duration_ns,
            source_output_row_count,
            source_row_per_barrier,
            exchange_frag_recv_size,
            join_lookup_miss_count,
            join_total_lookup_count,
            join_insert_cache_miss_count,
            join_actor_input_waiting_duration_ns,
            join_match_duration_ns,
            join_barrier_align_duration,
            join_cached_entries,
            join_cached_rows,
            join_cached_estimated_size,
            // agg_lookup_miss_count,
            // agg_total_lookup_count,
            // agg_cached_keys,
            // agg_chunk_lookup_miss_count,
            // agg_chunk_total_lookup_count,
            backfill_snapshot_read_row_count,
            backfill_upstream_output_row_count,
            barrier_inflight_latency,
            barrier_sync_latency,
            sink_commit_duration,
            sink_output_row_count,
            lru_current_watermark_time_ms,
            lru_physical_now_ms,
            lru_runtime_loop_count,
            lru_watermark_step,
            jemalloc_allocated_bytes,
            jemalloc_active_bytes,
            user_compute_error_count,
            materialize_cache_hit_count,
            materialize_cache_total_count,
        }
    }

    /// Create a new `StreamingMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use paste::paste;
use prometheus::core::{Desc, GenericCounter, GenericGauge};
use prometheus::{opts, IntCounterVec, IntGaugeVec};

use crate::task::ActorId;

macro_rules! for_all_actor_metrics {
        ($macro:ident) => {
            $macro! {
                // Streaming Aggregation
                { agg_lookup_miss_count, GenericCounterVec<AtomicU64>, GenericCounter<AtomicU64> },
                { agg_total_lookup_count, GenericCounterVec<AtomicU64>, GenericCounter<AtomicU64> },
                { agg_cached_keys, GenericGaugeVec<AtomicI64>, GenericGauge<AtomicI64> },
                { agg_chunk_lookup_miss_count, GenericCounterVec<AtomicU64>, GenericCounter<AtomicU64> },
                { agg_chunk_total_lookup_count, GenericCounterVec<AtomicU64>, GenericCounter<AtomicU64> },
            }
        };
    }

macro_rules! def_actor_metrics {
        ($({ $metric:ident, $type:ty, $_collector_type:ty},)*) => {
            #[derive(Clone)]
            pub struct ActorMetrics {
                descs: Vec<Desc>,
                delete_actor: Arc<Mutex<Vec<ActorId>>>,
                register_labels: Arc<Mutex<HashMap<ActorId, Vec<Box<dyn Fn() + Send>>>>>,

                $( pub $metric: $type, )*
            }
        };
    }

for_all_actor_metrics!(def_actor_metrics);

impl ActorMetrics {
    pub fn new(registry: Registry) -> Self {
        let mut descs = Vec::with_capacity(16);

        let agg_lookup_miss_count = IntCounterVec::new(
            opts!(
                "stream_agg_lookup_miss_count",
                "Aggregation executor lookup miss duration",
            ),
            &["table_id", "actor_id"],
        )
        .unwrap();
        descs.extend(agg_lookup_miss_count.desc().into_iter().cloned());

        let agg_total_lookup_count = IntCounterVec::new(
            opts!(
                "stream_agg_lookup_total_count",
                "Aggregation executor lookup total operation",
            ),
            &["table_id", "actor_id"],
        )
        .unwrap();
        descs.extend(agg_total_lookup_count.desc().into_iter().cloned());

        let agg_cached_keys = IntGaugeVec::new(
            opts!(
                "stream_agg_cached_keys",
                "Number of cached keys in streaming aggregation operators",
            ),
            &["table_id", "actor_id"],
        )
        .unwrap();
        descs.extend(agg_cached_keys.desc().into_iter().cloned());

        let agg_chunk_lookup_miss_count = IntCounterVec::new(
            opts!(
                "stream_agg_chunk_lookup_miss_count",
                "Aggregation executor chunk-level lookup miss duration",
            ),
            &["table_id", "actor_id"],
        )
        .unwrap();
        descs.extend(agg_chunk_lookup_miss_count.desc().into_iter().cloned());

        let agg_chunk_total_lookup_count = IntCounterVec::new(
            opts!(
                "stream_agg_chunk_lookup_total_count",
                "Aggregation executor chunk-level lookup total operation",
            ),
            &["table_id", "actor_id"],
        )
        .unwrap();
        descs.extend(agg_chunk_total_lookup_count.desc().into_iter().cloned());

        let metrics = Self {
            descs,
            delete_actor: Arc::new(Mutex::new(Vec::new())),
            register_labels: Arc::new(Mutex::new(HashMap::new())),
            agg_lookup_miss_count,
            agg_total_lookup_count,
            agg_cached_keys,
            agg_chunk_lookup_miss_count,
            agg_chunk_total_lookup_count,
        };
        registry.register(Box::new(metrics.clone())).unwrap();
        metrics
    }

    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }

    fn clean_metrics(&self) {
        let delete_actor: Vec<ActorId> = {
            let mut delete_actor = self.delete_actor.lock();
            if delete_actor.is_empty() {
                return;
            }
            std::mem::take(delete_actor.as_mut())
        };
        let delete_labels = {
            let mut register_labels = self.register_labels.lock();
            let mut delete_labels = Vec::with_capacity(delete_actor.len());
            for id in delete_actor {
                if let Some(callback) = register_labels.remove(&id) {
                    delete_labels.push(callback);
                }
            }
            delete_labels
        };
        delete_labels
            .into_iter()
            .for_each(|delete_labels| delete_labels.into_iter().for_each(|callback| callback()));
    }

    pub fn add_delete_actor(&self, actor_id: ActorId) {
        self.delete_actor.lock().push(actor_id);
    }
}

macro_rules! impl_collector_trait_for_actor_metrics {
        ($({ $metric:ident, $_type:ty, $_collector_type:ty},)*) => {
            impl Collector for ActorMetrics {
                fn desc(&self) -> Vec<&Desc> {
                    self.descs.iter().collect()
                }

                fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
                    let mut mfs = Vec::with_capacity(16);

                    $( mfs.extend(self.$metric.collect()); )*

                    self.clean_metrics();

                    mfs
                }
            }
        };
    }

for_all_actor_metrics!(impl_collector_trait_for_actor_metrics);

pub type ActorMetricsWithLabels = Arc<ActorMetricsWithLabelsInner>;

pub struct ActorMetricsWithLabelsInner {
    actor_metrics: Arc<ActorMetrics>,
    actor_id: ActorId,
    labels: Vec<String>,
}

macro_rules! def_create_actor_collector {
        ($({ $metric:ident, $type:ty, $collector_type:ty },)*) => {
            paste! {
                $(
                    pub fn [<create_collector_for_$metric>](&self, executor_label: Vec<String>) -> $collector_type {
                        let mut owned_task_labels = self.labels.clone();
                        owned_task_labels.extend(executor_label);
                        let task_labels = owned_task_labels.iter().map(String::as_str).collect_vec();

                        let collector = self
                            .actor_metrics
                            .$metric
                            .with_label_values(&task_labels);

                        let metrics = self.actor_metrics.$metric.clone();

                        self.actor_metrics
                            .register_labels
                            .lock()
                            .entry(self.actor_id.clone())
                            .or_default()
                            .push(Box::new(move || {
                                metrics.remove_label_values(
                                    &owned_task_labels.iter().map(String::as_str).collect_vec(),
                                ).expect("Collector with same label only can be created once. It should never have case of duplicate remove");
                            }));

                        collector
                    }
                )*
            }
        };
    }

impl ActorMetricsWithLabelsInner {
    for_all_actor_metrics!(def_create_actor_collector);

    pub fn new(actor_id: ActorId, actor_metrics: Arc<ActorMetrics>) -> Self {
        Self {
            actor_metrics,
            actor_id,
            labels: vec![],
        }
    }
}

impl Drop for ActorMetricsWithLabelsInner {
    fn drop(&mut self) {
        self.actor_metrics.delete_actor.lock().push(self.actor_id);
    }
}
