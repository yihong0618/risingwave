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
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_async_stream::try_stream;
use risingwave_common::field_generator::FieldGeneratorImpl;
use serde_json::Value;

use crate::source::{SourceMessage, SplitId};

pub struct DatagenEventGenerator {
    fields_map: HashMap<String, FieldGeneratorImpl>,
    offset: u64,
    split_id: SplitId,
    partition_rows_per_second: u64,
    // If the user didn't specify, then u64::MAX by default.
    partition_num_events: u64,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_map: HashMap<String, FieldGeneratorImpl>,
        rows_per_second: u64,
        offset: u64,
        split_id: SplitId,
        split_num: u64,
        split_index: u64,
        num_events: Option<String>,
    ) -> Result<Self> {
        let partition_rows_per_second = if rows_per_second % split_num > split_index {
            rows_per_second / split_num + 1
        } else {
            rows_per_second / split_num
        };
        let partition_num_events = match num_events {
            Some(num_events) => {
                let num_events = num_events.parse::<u64>()?;
                if num_events % split_num > split_index {
                    num_events / split_num + 1
                } else {
                    num_events / split_num
                }
            }
            None => u64::MAX,
        };
        Ok(Self {
            fields_map,
            offset,
            split_id,
            partition_rows_per_second,
            partition_num_events,
        })
    }

    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            // generate `partition_rows_per_second` rows per second
            interval.tick().await;
            let mut msgs = vec![];
            let num_events_current_round =
                std::cmp::min(self.partition_rows_per_second, self.partition_num_events);
            self.partition_num_events -= num_events_current_round;
            for _ in 0..num_events_current_round {
                let value = Value::Object(
                    self.fields_map
                        .iter_mut()
                        .map(|(name, field_generator)| {
                            (name.to_string(), field_generator.generate(self.offset))
                        })
                        .collect(),
                );
                msgs.push(SourceMessage {
                    payload: Some(Bytes::from(value.to_string())),
                    offset: self.offset.to_string(),
                    split_id: self.split_id.clone(),
                });
                self.offset += 1;
            }
            yield msgs;
            if self.partition_num_events == 0 {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use itertools::Itertools;

    use super::*;

    async fn check_sequence_partition_result(
        split_num: u64,
        split_index: u64,
        rows_per_second: u64,
        num_events: Option<String>,
        expected_length: usize,
    ) {
        let split_id = format!("{}-{}", split_num, split_index).into();
        let mut fields_map = HashMap::new();
        fields_map.insert(
            "v1".to_string(),
            FieldGeneratorImpl::with_number_sequence(
                risingwave_common::types::DataType::Int32,
                Some("1".to_string()),
                Some("10".to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        fields_map.insert(
            "v2".to_string(),
            FieldGeneratorImpl::with_number_sequence(
                risingwave_common::types::DataType::Float32,
                Some("1".to_string()),
                Some("10".to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        let generator = DatagenEventGenerator::new(
            fields_map,
            rows_per_second,
            0,
            split_id,
            split_num,
            split_index,
            num_events,
        )
        .unwrap();

        let chunk = generator
            .into_stream()
            .boxed()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_length, chunk.len());
    }

    #[tokio::test]
    async fn test_one_partition_sequence() {
        check_sequence_partition_result(1, 0, 10, None, 10).await;
        check_sequence_partition_result(1, 0, 10, Some("8".to_string()), 8).await;
        check_sequence_partition_result(1, 0, 10, Some("0".to_string()), 0).await;
    }

    #[tokio::test]
    async fn test_two_partition_sequence() {
        let num_events_vec = [
            None,
            Some("11".to_string()),
            Some("9".to_string()),
            Some("1".to_string()),
            Some("0".to_string()),
        ];
        let expected_lengths_vec: [[usize; 2]; 5] = [[5, 5], [5, 5], [5, 4], [1, 0], [0, 0]];

        for (num_events, expected_lengths) in num_events_vec
            .into_iter()
            .zip_eq(expected_lengths_vec.into_iter())
        {
            check_sequence_partition_result(2, 0, 10, num_events.clone(), expected_lengths[0])
                .await;
            check_sequence_partition_result(2, 1, 10, num_events, expected_lengths[1]).await;
        }
    }

    #[tokio::test]
    async fn test_three_partition_sequence() {
        let num_events_vec = [
            None,
            Some("11".to_string()),
            Some("8".to_string()),
            Some("1".to_string()),
        ];
        let expected_lengths_vec: [[usize; 3]; 4] = [[4, 3, 3], [4, 3, 3], [3, 3, 2], [1, 0, 0]];
        for (num_events, expected_length) in num_events_vec
            .into_iter()
            .zip_eq(expected_lengths_vec.into_iter())
        {
            check_sequence_partition_result(3, 0, 10, num_events.clone(), expected_length[0]).await;
            check_sequence_partition_result(3, 1, 10, num_events.clone(), expected_length[1]).await;
            check_sequence_partition_result(3, 2, 10, num_events, expected_length[2]).await;
        }
    }
}
