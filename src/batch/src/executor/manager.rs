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

use async_stream::stream;
use futures::stream::StreamExt;
use futures::stream_select;
use minitrace::prelude::*;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode;
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio_stream::wrappers::WatchStream;
use tonic::codegen::Body;

use crate::executor::{BoxedDataChunkStream, BoxedExecutor, Executor};
use crate::task::ShutdownMsg;

/// `ManagerExecutor` build on top of the underlying executor. For now, it do two things:
/// 1. the duration of performance-critical operations will be traced, such as open/next/close.
/// 2. receive shutdown signal
pub struct ManagedExecutor {
    child: BoxedExecutor,
    /// Description of input executor
    input_desc: String,
    shutdown_rx: Receiver<ShutdownMsg>,
}

impl ManagedExecutor {
    pub fn new(
        child: BoxedExecutor,
        input_desc: String,
        shutdown_rx: Receiver<ShutdownMsg>,
    ) -> Self {
        Self {
            child,
            input_desc,
            shutdown_rx,
        }
    }
}

impl Executor for ManagedExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        self.child.identity()
    }

    fn execute(mut self: Box<Self>) -> BoxedDataChunkStream {
        let input_desc = self.input_desc.as_str();
        let span_name = format!("{input_desc}_next");

        let span = || {
            let mut span = Span::enter_with_local_parent("next");
            span.add_property(|| ("otel.name", span_name.to_string()));
            span.add_property(|| ("next", input_desc.to_string()));
            span
        };

        let mut child_stream = self.child.execute();

        let shutdown_stream = WatchStream::from_changes(self.shutdown_rx).map(|msg| match msg {
            ShutdownMsg::Abort(reason) => Err(ErrorCode::BatchError(reason.into()).into()),
            ShutdownMsg::Cancel => Err(ErrorCode::BatchError("".into()).into()),
            ShutdownMsg::Init => {
                unreachable!("Never receive Init message");
            }
        });

        stream_select!(shutdown_stream, child_stream).boxed()
    }
}
