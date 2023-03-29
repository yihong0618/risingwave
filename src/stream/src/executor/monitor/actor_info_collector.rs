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
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc, Describer};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{proto, Opts, Registry};
use risingwave_common::catalog::TableId;

use crate::task::{ActorId, FragmentId};

/// Monitors actor info.
pub fn monitor_actor_info(registry: &Registry, collector: Arc<ActorInfoCollector>) -> Result<()> {
    let ac = ActorInfoCollectorWrapper { inner: collector };
    registry
        .register(Box::new(ac))
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

const ACTOR_ID_LABEL: &str = "actor_id";
const FRAGMENT_ID_LABEL: &str = "fragment_id";
const TABLE_ID_LABEL: &str = "table_id";
const TABLE_NAME_LABEL: &str = "table_name";
const ADD_GAUGE_VALUE: f64 = 1.0;
const DROP_GAUGE_VALUE: f64 = 0.0;


/// A collector to collect actor info
pub struct ActorInfoCollector {
    desc: Vec<Desc>,
    actor_info_to_collect: Mutex<Vec<MetricFamily>>,
}

impl ActorInfoCollector {
    pub fn new() -> Self {
        Self {
            desc: vec![
                Opts::new("actor_id_info", "Mapping from actor id to fragment id")
                    .variable_labels(vec![
                        ACTOR_ID_LABEL.to_owned(),
                        FRAGMENT_ID_LABEL.to_owned(),
                    ])
                    .describe()
                    .unwrap(),
                Opts::new(
                    "state_table_id_info",
                    "Mapping from table id to table name and actor id",
                )
                .variable_labels(vec![
                    TABLE_ID_LABEL.to_owned(),
                    ACTOR_ID_LABEL.to_owned(),
                    TABLE_NAME_LABEL.to_owned(),
                ])
                .describe()
                .unwrap(),
            ],
            actor_info_to_collect: Mutex::new(vec![]),
        }
    }

    pub fn add_actor(&self, actor_id: ActorId, fragment_id: FragmentId) {
        self.actor_change(actor_id, fragment_id, ADD_GAUGE_VALUE);
    }

    pub fn add_table(&self, table_id: TableId, actor_id: ActorId, table_name: &str) {
        self.table_change(table_id, actor_id, table_name, ADD_GAUGE_VALUE);
    }

    pub fn drop_actor(&self, actor_id: ActorId, fragment_id: FragmentId) {
        self.actor_change(actor_id, fragment_id, DROP_GAUGE_VALUE);
    }

    pub fn drop_table(&self, table_id: TableId, actor_id: ActorId, table_name: &str) {
        self.table_change(table_id, actor_id, table_name, DROP_GAUGE_VALUE);
    }


    fn actor_change(&self, actor_id: ActorId, fragment_id: FragmentId, value: f64) {
        // Fake a metric family with the corresponding desc
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc[0].fq_name.clone());
        mf.set_help(self.desc[0].help.clone());
        mf.set_field_type(MetricType::GAUGE);

        // Fake a gauge metric with the corresponding label
        let mut m = Metric::default();
        let mut gauge = Gauge::default();
        gauge.set_value(value);
        m.set_gauge(gauge);
        let mut labels = Vec::with_capacity(2);
        let mut label = LabelPair::new();
        label.set_name(ACTOR_ID_LABEL.to_owned());
        label.set_value(actor_id.to_string());
        labels.push(label);
        let mut label = LabelPair::new();
        label.set_name(FRAGMENT_ID_LABEL.to_owned());
        label.set_value(fragment_id.to_string());
        labels.push(label);
        m.set_label(labels.into());
        mf.set_metric(vec![m].into());

        // Store the fake metric family for later collection
        self.actor_info_to_collect.lock().push(mf);
    }

    fn table_change(&self, table_id: TableId, actor_id: ActorId, table_name: &str, value: f64) {
        // Fake a metric family with the corresponding desc
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc[1].fq_name.clone());
        mf.set_help(self.desc[1].help.clone());
        mf.set_field_type(MetricType::GAUGE);

        // Fake a gauge metric with the corresponding label
        let mut m = Metric::default();
        let mut gauge = Gauge::default();
        gauge.set_value(value);
        m.set_gauge(gauge);
        let mut labels = Vec::with_capacity(3);
        let mut label = LabelPair::new();
        label.set_name(TABLE_ID_LABEL.to_owned());
        label.set_value(table_id.to_string());
        labels.push(label);
        let mut label = LabelPair::new();
        label.set_name(ACTOR_ID_LABEL.to_owned());
        label.set_value(actor_id.to_string());
        labels.push(label);
        let mut label = LabelPair::new();
        label.set_name(TABLE_NAME_LABEL.to_owned());
        label.set_value(table_name.to_string());
        labels.push(label);
        m.set_label(labels.into());
        mf.set_metric(vec![m].into());

        // Store the fake metric family for later collection
        self.actor_info_to_collect.lock().push(mf);
    }

    fn desc(&self) -> Vec<&Desc> {
        self.desc.iter().collect_vec()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.actor_info_to_collect.lock().split_off(0)
    }
}

pub struct ActorInfoCollectorWrapper {
    inner: Arc<ActorInfoCollector>,
}

impl Collector for ActorInfoCollectorWrapper {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.inner.collect()
    }
}
