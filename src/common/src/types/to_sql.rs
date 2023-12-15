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

use std::error::Error;

use bytes::BytesMut;
use postgres_types::{BorrowToSql, ToSql};

use crate::types::{DatumRef, ScalarRefImpl};

pub fn datum_to_dyn_to_sql<'a>(
    scalar: &'a ScalarRefImpl<'_>,
) -> Result<&'a dyn ToSql, Box<dyn Error + Sync + Send>> {
    let ret: &dyn ToSql = match scalar {
            ScalarRefImpl::Int16(v) => v as &dyn ToSql,
            ScalarRefImpl::Int32(v) => v,
            ScalarRefImpl::Int64(v) => v,
            ScalarRefImpl::Serial(v) => v,
            ScalarRefImpl::Float32(v) => v,
            ScalarRefImpl::Float64(v) => v,
            ScalarRefImpl::Utf8(v) => v,
            ScalarRefImpl::Bool(v) => v,
            ScalarRefImpl::Decimal(v) => v,
            ScalarRefImpl::Interval(v) => v,
            ScalarRefImpl::Date(v) => v,
            ScalarRefImpl::Timestamp(v) => v,
            ScalarRefImpl::Timestamptz(v) => v,
            ScalarRefImpl::Time(v) => v,
            ScalarRefImpl::Bytea(v) => v,
            ScalarRefImpl::Jsonb(_) // jsonbb::Value doesn't implement ToSql yet
            | ScalarRefImpl::Int256(_)
            | ScalarRefImpl::Struct(_)
            | ScalarRefImpl::List(_) => {
                panic!()
                // bail_not_implemented!("the postgres encoding for {ty} is unsupported")
            }
        };
    Ok(ret)
}
