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

use std::collections::HashSet;

use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::aggregate::{
    AggStateDyn, AggregateFunction, AggregateState, AggregateStateRef, BoxedAggregateFunction,
};
use risingwave_expr::Result;

/// `Distinct` is a wrapper of `Aggregator` that only keeps distinct rows.
pub struct Distinct {
    inner: BoxedAggregateFunction,
}

/// The intermediate state for distinct aggregation.
#[derive(Debug)]
struct State {
    /// Inner aggregate function state.
    inner: AggregateState,
    /// The set of distinct rows.
    exists: HashSet<OwnedRow>, // TODO: optimize for small rows
    exists_estimated_heap_size: usize,
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_size()
            + self.exists.capacity() * std::mem::size_of::<OwnedRow>()
            + self.exists_estimated_heap_size
    }
}

impl AggStateDyn for State {}

impl Distinct {
    pub fn new(inner: BoxedAggregateFunction) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl AggregateFunction for Distinct {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> AggregateState {
        AggregateState::Any(Box::new(State {
            inner: self.inner.create_state(),
            exists: HashSet::new(),
            exists_estimated_heap_size: 0,
        }))
    }

    async fn accumulate_and_retract(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
    ) -> Result<()> {
        let state = state.downcast_mut::<State>();

        let mut bitmap_builder = BitmapBuilder::from(input.data_chunk().visibility().clone());
        for row_id in 0..input.capacity() {
            let (row_ref, vis) = input.data_chunk().row_at(row_id);
            if !vis {
                continue;
            }
            let row = row_ref.to_owned_row();
            let row_size = row.estimated_heap_size();
            let b = state.exists.insert(row);
            if b {
                state.exists_estimated_heap_size += row_size;
            }
            bitmap_builder.set(row_id, b);
        }
        let input = input.clone_with_vis(bitmap_builder.finish());
        self.inner
            .accumulate_and_retract(&mut state.inner, &input)
            .await
    }

    async fn grouped_accumulate_and_retract(
        &self,
        states: &[AggregateStateRef],
        input: &StreamChunk,
    ) -> Result<()> {
        let mut bitmap_builder = BitmapBuilder::from(input.data_chunk().visibility().clone());
        let mut inner_states = states.to_vec();
        for row_id in 0..input.capacity() {
            let (row_ref, vis) = input.data_chunk().row_at(row_id);
            if !vis {
                continue;
            }
            let state = unsafe { states[row_id].downcast_mut::<State>() };
            let row = row_ref.to_owned_row();
            let row_size = row.estimated_heap_size();
            let b = state.exists.insert(row);
            if b {
                state.exists_estimated_heap_size += row_size;
            }
            bitmap_builder.set(row_id, b);
            inner_states[row_id] = state.inner.as_ref();
        }
        let input = input.clone_with_vis(bitmap_builder.finish());
        self.inner
            .grouped_accumulate_and_retract(&inner_states, &input)
            .await
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        self.inner.get_result(&state.inner).await
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{Datum, Decimal};
    use risingwave_expr::aggregate::AggCall;

    use super::super::build;

    #[test]
    fn distinct_sum_int32() {
        let input = StreamChunk::from_pretty(
            " i
            + 1
            + 1
            + 3",
        );
        test_agg("(sum:int8 $0:int4 distinct)", input, Some(4i64.into()));
    }

    #[test]
    fn distinct_sum_int64() {
        let input = StreamChunk::from_pretty(
            " I
            + 1
            + 1
            + 3",
        );
        test_agg(
            "(sum:decimal $0:int8 distinct)",
            input,
            Some(Decimal::from(4).into()),
        );
    }

    #[test]
    fn distinct_min_float32() {
        let input = StreamChunk::from_pretty(
            " f
            + 1.0
            + 2.0
            + 3.0",
        );
        test_agg(
            "(min:float4 $0:float4 distinct)",
            input,
            Some(1.0f32.into()),
        );
    }

    #[test]
    fn distinct_min_char() {
        let input = StreamChunk::from_pretty(
            " T
            + b
            + aa",
        );
        test_agg(
            "(min:varchar $0:varchar distinct)",
            input,
            Some("aa".into()),
        );
    }

    #[test]
    fn distinct_max_char() {
        let input = StreamChunk::from_pretty(
            " T
            + b
            + aa",
        );
        test_agg("(max:varchar $0:varchar distinct)", input, Some("b".into()));
    }

    #[test]
    fn distinct_count_int32() {
        let input = StreamChunk::from_pretty(
            " i
            + 1
            + 1
            + 3",
        );
        test_agg("(count:int8 $0:int4 distinct)", input, Some(2i64.into()));

        let input = StreamChunk::from_pretty("i");
        test_agg("(count:int8 $0:int4 distinct)", input, Some(0i64.into()));

        let input = StreamChunk::from_pretty(
            " i
            + .",
        );
        test_agg("(count:int8 $0:int4 distinct)", input, Some(0i64.into()));
    }

    fn test_agg(pretty: &str, input: StreamChunk, expected: Datum) {
        let agg = build(&AggCall::from_pretty(pretty)).unwrap();
        let mut state = agg.create_state();
        agg.accumulate_and_retract(&mut state, &input)
            .now_or_never()
            .unwrap()
            .unwrap();
        let actual = agg.get_result(&state).now_or_never().unwrap().unwrap();
        assert_eq!(actual, expected);
    }
}
