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

use std::sync::Arc;

use risingwave_common::array::StreamChunk;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::aggregate::{
    AggregateFunction, AggregateState, AggregateStateRef, BoxedAggregateFunction,
};
use risingwave_expr::expr::Expression;
use risingwave_expr::Result;

/// A special aggregator that filters out rows that do not satisfy the given _condition_
/// and feeds the rows that satisfy to the _inner_ aggregator.
pub struct Filter {
    condition: Arc<dyn Expression>,
    inner: BoxedAggregateFunction,
}

impl Filter {
    pub fn new(condition: Arc<dyn Expression>, inner: BoxedAggregateFunction) -> Self {
        assert_eq!(condition.return_type(), DataType::Boolean);
        Self { condition, inner }
    }

    /// Return a new chunk with rows that satisfy the condition.
    async fn filter_input(&self, input: &StreamChunk) -> Result<StreamChunk> {
        let selection = self
            .condition
            .eval(input.data_chunk())
            .await?
            .as_bool()
            .to_bitmap();
        Ok(input.clone_with_vis(input.visibility() & selection))
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Filter {
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
        let inner_input = self.filter_input(input).await?;
        self.inner.accumulate_and_retract(state, &inner_input).await
    }

    async fn grouped_accumulate_and_retract(
        &self,
        states: &[AggregateStateRef],
        input: &StreamChunk,
    ) -> Result<()> {
        let inner_input = self.filter_input(input).await?;
        self.inner
            .grouped_accumulate_and_retract(states, &inner_input)
            .await
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        self.inner.get_result(state).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::aggregate::{build_append_only, AggCall};
    use risingwave_expr::expr::build_from_pretty;

    use super::*;

    #[tokio::test]
    async fn test_selective_agg_always_true() -> Result<()> {
        let agg = Filter::new(
            build_from_pretty("true:boolean").into(),
            build_append_only(&AggCall::from_pretty("(count:int8 $0:int8)")).unwrap(),
        );
        let mut state = agg.create_state();

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.accumulate_and_retract(&mut state, &chunk).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg() -> Result<()> {
        let agg = Filter::new(
            build_from_pretty("(greater_than:boolean $0:int8 5:int8)").into(),
            build_append_only(&AggCall::from_pretty("(count:int8 $0:int8)")).unwrap(),
        );
        let mut state = agg.create_state();

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.accumulate_and_retract(&mut state, &chunk).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_selective_agg_null_condition() -> Result<()> {
        let agg = Filter::new(
            build_from_pretty("(equal:boolean $0:int8 null:int8)").into(),
            build_append_only(&AggCall::from_pretty("(count:int8 $0:int8)")).unwrap(),
        );
        let mut state = agg.create_state();

        let chunk = StreamChunk::from_pretty(
            " I
            + 9
            + 5
            + 6
            + 1",
        );

        agg.accumulate_and_retract(&mut state, &chunk).await?;
        assert_eq!(agg.get_result(&state).await?.unwrap().into_int64(), 0);

        Ok(())
    }
}
