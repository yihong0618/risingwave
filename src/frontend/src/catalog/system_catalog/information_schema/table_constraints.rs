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

use risingwave_common::catalog::INFORMATION_SCHEMA_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const INFORMATION_SCHEMA_TABLE_CONSTRAINTS: BuiltinTable = BuiltinTable {
    name: "table_constraints",
    schema: INFORMATION_SCHEMA_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "constraint_catalog"),
        (DataType::Varchar, "constraint_schema"),
        (DataType::Varchar, "constraint_name"),
        (DataType::Varchar, "table_catalog"),
        (DataType::Varchar, "table_schema"),
        (DataType::Varchar, "table_name"),
        (DataType::Varchar, "constraint_type"),
        (DataType::Varchar, "is_deferrable"),
        (DataType::Varchar, "initially_deferred"),
        (DataType::Varchar, "enforced"),
        (DataType::Varchar, "nulls_distinct"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_information_schema_table_constraints(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }
}
