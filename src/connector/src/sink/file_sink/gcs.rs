// Copyright 2024 RisingWave Labs
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
use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Gcs;
use opendal::Operator;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::FileSink;
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::{Result, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct GcsCommon {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,

    /// The base64 encoded credential key. If not set, ADC will be used.
    #[serde(rename = "gcs.credential")]
    pub credential: Option<String>,

    /// If credential/ADC is not set. The service account can be used to provide the credential info.
    #[serde(rename = "gcs.service_account", default)]
    pub service_account: Option<String>,

    /// The directory where the sink file is located
    #[serde(rename = "gcs.path", default)]
    pub path: String,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct GcsConfig {
    #[serde(flatten)]
    pub common: GcsCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

pub const GCS_SINK: &str = "gcs";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_gcs_sink(config: GcsConfig) -> Result<Operator> {
        // Create gcs builder.
        let mut builder = Gcs::default();

        builder.bucket(&config.common.bucket_name);

        // if credential env is set, use it. Otherwise, ADC will be used.
        if let Some(cred) = config.common.credential {
            builder.credential(&cred);
        } else {
            let cred = std::env::var("GOOGLE_APPLICATION_CREDENTIALS");
            if let Ok(cred) = cred {
                builder.credential(&cred);
            }
        }

        if let Some(service_account) = config.common.service_account {
            builder.service_account(&service_account);
        }
        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcsSink;

impl OpendalSinkBackend for GcsSink {
    type Properties = GcsConfig;

    const SINK_NAME: &'static str = GCS_SINK;

    fn from_btreemap(hash_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config = serde_json::from_value::<GcsConfig>(serde_json::to_value(hash_map).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }

    fn new_operator(properties: GcsConfig) -> Result<Operator> {
        FileSink::<GcsSink>::new_gcs_sink(properties)
    }

    fn get_path(properties: &Self::Properties) -> String {
        (*properties.common.path).to_string()
    }
}