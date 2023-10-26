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

use std::fmt::Debug;

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::Row;
use risingwave_common::types::{
    JsonbRef, JsonbVal, Scalar, ScalarImpl, ScalarRef, ScalarRefImpl, StructRef, F32, F64,
};
use risingwave_expr::aggregate::AggStateDyn;
use risingwave_expr::{function, ExprError, Result};

use crate::aggregate::ToJson;

#[function("to_jsonb(struct) -> jsonb")]
pub fn strict_to_jsonb(input: StructRef<'_>) -> JsonbVal {
    println!("to_jsonb: {:?}", input);
    JsonbVal::null()
}

#[function("to_jsonb(boolean) -> jsonb")]
#[function("to_jsonb(*int) -> jsonb")]
#[function("to_jsonb(*float) -> jsonb")]
#[function("to_jsonb(varchar) -> jsonb")]
pub fn to_jsonb(input: impl ToJson) -> JsonbVal {
    // println!("to_jsonb: {:?}", row);
    // let _res = row.iter().flatten().map(|_scalar| {
    //     // println!("to_jsonb: scalar {:?}", scalar);
    // });
    JsonbVal::null()
}
