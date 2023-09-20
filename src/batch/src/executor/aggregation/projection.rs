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

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::aggregate::{
    AggregateFunction, AggregateState, AggregateStateRef, BoxedAggregateFunction,
};
use risingwave_expr::Result;

pub struct Projection {
    inner: BoxedAggregateFunction,
    indices: Vec<usize>,
}

impl Projection {
    pub fn new(indices: Vec<usize>, inner: BoxedAggregateFunction) -> Self {
        Self { inner, indices }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Projection {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> AggregateState {
        self.inner.create_state()
    }

    async fn accumulate_and_retract(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
    ) -> Result<()> {
        self.inner
            .accumulate_and_retract(state, &input.project(&self.indices))
            .await
    }

    async fn grouped_accumulate_and_retract(
        &self,
        states: &[AggregateStateRef],
        input: &StreamChunk,
    ) -> Result<()> {
        self.inner
            .grouped_accumulate_and_retract(states, &input.project(&self.indices))
            .await
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        self.inner.get_result(state).await
    }
}
