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

use std::io::{Error, ErrorKind, Result};

use prometheus::core::{Collector, Desc};
use prometheus::{proto, IntCounter, IntGauge, Opts, Registry};


/// Monitors current process.
pub fn monitor_process(registry: &Registry) -> Result<()> {
    let pc = ProcessCollector::new();
    registry
        .register(Box::new(pc))
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

pub const ACTOR_INFO_DESC: &[&str] = &[
    "Actor id to fragment id mapping",
    "Table id to table name and actor id mapping"
];

pub struct ActorInfoToCollect {
    new_actors: Vec<MetricFamily>,
    new_tables: Vec<MetricFamily>
}

/// A collector to collect actor info
pub struct ActorInfoCollector {
    actor_info_to_collect: Mutex<ActorInfoToCollect>
}

impl Default for ActorInfoCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorInfoCollector {
    pub fn new() -> Self {
        Self {
            actor_info_to_collect: Mutex::new(ActorInfoToCollect::default())
        }
    }
}

impl Collector for ActorInfoCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let p = match procfs::process::Process::myself() {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };

        // memory
        self.vsize.set(p.stat.vsize as i64);
        self.rss.set(p.stat.rss * *PAGESIZE);

        // cpu
        let cpu_total_mfs = {
            let total = (p.stat.utime + p.stat.stime) / *CLOCK_TICK;
            let past = self.cpu_total.get();
            self.cpu_total.inc_by(total - past);
            self.cpu_total.collect()
        };

        self.cpu_core_num
            .set(resource_util::cpu::total_cpu_available() as i64);

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.cpu_core_num.collect());
        mfs
    }
}
