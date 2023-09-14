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

use itertools::Itertools;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DefaultOrdered, ScalarImpl};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

/// Select the largest value from a list of any number of expressions.
///
/// NULL values in the list are ignored. The result will be NULL only if all the expressions evaluate to NULL.
///
/// ```slt
/// query I
/// SELECT greatest(1, null, 3);
/// ----
/// 3
///
/// query I
/// SELECT greatest(null, null);
/// ----
/// NULL
/// ```
#[function("greatest(...) -> any", type_infer = "type_infer")]
fn greatest(row: impl Row) -> Option<ScalarImpl> {
    let max_value = row.iter().flatten().map(DefaultOrdered).max();
    max_value.map(|v| v.0.into_scalar_impl())
}

/// Select the smallest value from a list of any number of expressions.
///
/// NULL values in the list are ignored. The result will be NULL only if all the expressions evaluate to NULL.
///
/// ```slt
/// query I
/// SELECT least(1, null, 3);
/// ----
/// 1
///
/// query I
/// SELECT least(null, null);
/// ----
/// NULL
/// ```
#[function("least(...) -> any", type_infer = "type_infer")]
fn least(row: impl Row) -> Option<ScalarImpl> {
    let min_value = row.iter().flatten().map(DefaultOrdered).min();
    min_value.map(|v| v.0.into_scalar_impl())
}

fn type_infer(args: &[DataType]) -> Result<DataType> {
    if args.is_empty() {
        return Err(ExprError::TooFewArguments);
    }
    let ty = args
        .iter()
        .all_equal_value()
        .expect("input type should be the same");
    Ok(ty.clone())
}
