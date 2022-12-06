// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;

use tokio::task::JoinHandle;
use tokio_metrics::RuntimeMonitor;

/// `CompactionExecutor` is a dedicated runtime for compaction's CPU intensive jobs.
pub struct CompactionExecutor {
    /// Runtime for compaction tasks.
    runtime: &'static tokio::runtime::Runtime,
}

impl CompactionExecutor {
    pub fn new(worker_threads_num: Option<usize>) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.thread_name("risingwave-compaction");
            if let Some(worker_threads_num) = worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder.enable_all().build().unwrap()
        };

        // print runtime metrics every 1000ms
        {
            use std::time::Duration;
            let runtime_monitor = RuntimeMonitor::new(runtime.handle());
            tokio::spawn(async move {
                for runtime_interval in runtime_monitor.intervals() {
                    // pretty-print the metric interval
                    // println!("{:?}", interval);
                    tracing::info!(
                        "busy_ratio {:?} runtime_monitor {:?}",
                        runtime_interval.busy_ratio(),
                        runtime_interval
                    );

                    // wait 500ms
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            });
        }

        Self {
            // Leak the runtime to avoid runtime shutting-down in the main async context.
            // TODO: may manually shutdown the runtime gracefully.
            runtime: Box::leak(Box::new(runtime)),
        }
    }

    /// Send a request to the executor, returns a [`JoinHandle`] to retrieve the result.
    pub fn spawn<F, T>(&self, t: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.runtime.spawn(t)
    }

    pub fn runtime(&self) -> &'static tokio::runtime::Runtime {
        self.runtime
    }
}
