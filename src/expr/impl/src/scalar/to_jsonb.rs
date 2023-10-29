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

use risingwave_common::types::{JsonbVal, ListRef, StructRef};
use risingwave_expr::function;

#[function("to_jsonb(boolean) -> jsonb")]
#[function("to_jsonb(*int) -> jsonb")]
#[function("to_jsonb(int256) -> jsonb")]
#[function("to_jsonb(*float) -> jsonb")]
#[function("to_jsonb(decimal) -> jsonb")]
#[function("to_jsonb(serial) -> jsonb")]
#[function("to_jsonb(date) -> jsonb")]
#[function("to_jsonb(time) -> jsonb")]
#[function("to_jsonb(timestamp) -> jsonb")]
#[function("to_jsonb(timestamptz) -> jsonb")]
#[function("to_jsonb(interval) -> jsonb")]
#[function("to_jsonb(varchar) -> jsonb")]
#[function("to_jsonb(bytea) -> jsonb")]
#[function("to_jsonb(jsonb) -> jsonb")]
#[function("to_jsonb(anyarray) -> jsonb")]
#[function("to_jsonb(struct) -> jsonb")]
pub fn to_jsonb(input: impl Into<JsonbVal>) -> JsonbVal {
    input.into()
}

// case:
// select to_jsonb(null);

// select to_jsonb('2199-01-02 00:00:00.000000'::timestamp);
// rw "2199-01-02 00:00:00"
// pg "2199-01-02T00:00:00"

// create table t(d1 int, c2 varchar);
// insert into t values(1,  '4'), (2, '5');

// create table t2(n1 int, c2 varchar);
// insert into t2 values(1,  'ffff'), (2, '5');

// create table t3(n1 int, c2 interval);
// insert into t3 values(1, '10 second'::interval), (2, '30 second'::interval);

// create table t4(n1 int, c2 interval);
// insert into t4 values(1, null);

// select to_jsonb(row(n1, c2)) from t3;

// select to_jsonb('30 second'::interval);
// -- select to_jsonb(null);

// select to_jsonb(row(n1, c2)) from t4;
