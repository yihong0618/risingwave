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

use risingwave_common::array::ArrayError;
use risingwave_common::define_traced_error;
use risingwave_common::error::{BoxedError, Error, TrackingIssue};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::RpcError;
use risingwave_storage::error::StorageError;

use super::Barrier;
use crate::common::log_store::LogStoreError;

/// A specialized Result type for streaming executors.
pub type StreamExecutorResult<T> = std::result::Result<T, StreamExecutorError>;

define_traced_error!(
    StreamExecutorError,
    ErrorKind,
    "The error type for streaming executors."
);

#[derive(thiserror::Error, Debug)]
enum ErrorKind {
    #[error("failed to access storage")]
    Storage {
        #[backtrace]
        source: StorageError,
    },

    #[error("failed to access log store")]
    LogStoreError {
        #[backtrace]
        source: LogStoreError,
    },

    #[error("failed to operate data chunk")]
    ArrayError { source: ArrayError },

    #[error("failed to evaluate expression")]
    ExprError {
        #[backtrace]
        source: ExprError,
    },

    // TODO: remove this after state table is fully used
    #[error("failed to serialize/deserialize data")]
    SerdeError {
        #[backtrace]
        source: BoxedError,
    },

    #[error("failed to sink data")]
    SinkError {
        #[backtrace]
        source: SinkError,
    },

    #[error("failed to exchange data")]
    ExchangeError {
        #[backtrace]
        source: RpcError,
    },

    #[error("failed to access data connector")]
    ConnectorError {
        #[backtrace]
        source: BoxedError,
    },

    #[error("channel closed unexpectedly ({name})")]
    ChannelClosed {
        name: String,
        #[backtrace]
        source: Option<BoxedError>,
    },

    #[error("failed to align barrier: expected {0:?}, but got {1:?}")]
    AlignBarrier(Box<Barrier>, Box<Barrier>),

    #[error("feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),

    #[error(transparent)]
    Internal(anyhow::Error),
}

impl StreamExecutorError {
    fn serde_error(error: impl Error) -> Self {
        ErrorKind::SerdeError {
            source: error.into(),
        }
        .into()
    }

    pub fn channel_closed(name: impl Into<String>) -> Self {
        ErrorKind::ChannelClosed {
            name: name.into(),
            source: None,
        }
        .into()
    }

    pub fn channel_closed_with_source(name: impl Into<String>, cause: impl Error) -> Self {
        ErrorKind::ChannelClosed {
            name: name.into(),
            source: Some(cause.into()),
        }
        .into()
    }

    pub fn align_barrier(expected: Barrier, received: Barrier) -> Self {
        ErrorKind::AlignBarrier(expected.into(), received.into()).into()
    }

    pub fn connector_error(error: impl Error) -> Self {
        ErrorKind::ConnectorError {
            source: error.into(),
        }
        .into()
    }

    pub fn not_implemented(error: impl Into<String>, issue: impl Into<TrackingIssue>) -> Self {
        ErrorKind::NotImplemented(error.into(), issue.into()).into()
    }

    pub fn exchange(error: RpcError) -> Self {
        ErrorKind::ExchangeError { source: error }.into()
    }
}

/// Storage error.
impl From<StorageError> for StreamExecutorError {
    fn from(s: StorageError) -> Self {
        ErrorKind::Storage { source: s }.into()
    }
}

/// Log store error
impl From<LogStoreError> for StreamExecutorError {
    fn from(e: LogStoreError) -> Self {
        ErrorKind::LogStoreError { source: e }.into()
    }
}

/// Chunk operation error.
impl From<ArrayError> for StreamExecutorError {
    fn from(e: ArrayError) -> Self {
        ErrorKind::ArrayError { source: e }.into()
    }
}

impl From<ExprError> for StreamExecutorError {
    fn from(e: ExprError) -> Self {
        ErrorKind::ExprError { source: e }.into()
    }
}

/// Internal error.
impl From<anyhow::Error> for StreamExecutorError {
    fn from(a: anyhow::Error) -> Self {
        ErrorKind::Internal(a).into()
    }
}

/// Serialize/deserialize error.
impl From<memcomparable::Error> for StreamExecutorError {
    fn from(m: memcomparable::Error) -> Self {
        Self::serde_error(m)
    }
}
impl From<ValueEncodingError> for StreamExecutorError {
    fn from(e: ValueEncodingError) -> Self {
        Self::serde_error(e)
    }
}

/// Connector error.
impl From<ConnectorError> for StreamExecutorError {
    fn from(s: ConnectorError) -> Self {
        Self::connector_error(s)
    }
}

impl From<SinkError> for StreamExecutorError {
    fn from(e: SinkError) -> Self {
        ErrorKind::SinkError { source: e }.into()
    }
}

impl From<PbFieldNotFound> for StreamExecutorError {
    fn from(err: PbFieldNotFound) -> Self {
        Self::from(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
    }
}

static_assertions::const_assert_eq!(std::mem::size_of::<StreamExecutorError>(), 8);

#[cfg(test)]
mod tests {
    use risingwave_common::bail;

    use super::*;

    fn func_return_error() -> StreamExecutorResult<()> {
        bail!("test_error")
    }

    #[test]
    #[should_panic]
    #[ignore]
    fn executor_error_ui_test_1() {
        // For this test, ensure that we have only one backtrace from error when panic.
        func_return_error().unwrap();
    }

    #[test]
    #[ignore]
    fn executor_error_ui_test_2() {
        // For this test, ensure that we have only one backtrace from error when panic.
        func_return_error().map_err(|e| println!("{:?}", e)).ok();
    }
}
