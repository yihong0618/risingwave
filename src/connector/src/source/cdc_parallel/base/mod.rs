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

pub mod split;

use risingwave_common::types::JsonbVal;

use crate::source::{SplitEnumerator, SplitId, SplitMetaData, SplitReader};

// TODO: define framework level interface here

// pub type CdcSplitReader = SplitReader;

trait CdcSplitEnumerator: SplitEnumerator {
    type Split;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CdcTableId {
    database: String,
    schema: String,
    table: String,
}

// Split a table into multiple chunks
trait CdcTableSplitter {
    type Split;

    ///
    fn generate_splits(&self, table_id: &CdcTableId) -> Vec<Self::Split>;
}

// implies that struct implements CdcSplitReader should also impl SplitReader
trait CdcSplitReader: SplitReader {}

pub type CdcSplitId = String;

// states for different types of split
#[derive(Clone, Debug, Default)]
pub(crate) struct CdcSnapshotSplit {
    id: CdcSplitId,
    table_id: CdcTableId,
}

impl CdcSnapshotSplit {
    pub fn new(id: CdcSplitId, table_id: CdcTableId) -> Self {
        Self { id, table_id }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CdcStreamSplit {
    id: CdcSplitId,
}

#[derive(Clone, Debug)]
pub(crate) enum CdcSplit {
    SnapshotSplit(CdcSnapshotSplit),
    StreamSplit(CdcStreamSplit),
}

impl CdcSplit {
    pub fn is_snapshot_split(&self) -> bool {
        match self {
            CdcSplit::SnapshotSplit(_) => true,
            CdcSplit::StreamSplit(_) => false,
        }
    }

    pub fn is_stream_split(&self) -> bool {
        match self {
            CdcSplit::SnapshotSplit(_) => false,
            CdcSplit::StreamSplit(_) => true,
        }
    }
}

impl CdcSplit {
    fn id(&self) -> SplitId {
        match &self {
            CdcSplit::SnapshotSplit(split) => split.id(),
            CdcSplit::StreamSplit(split) => split.id(),
        }
    }

    fn encode_to_json(&self) -> JsonbVal {
        match &self {
            CdcSplit::SnapshotSplit(split) => split.encode_to_json(),
            CdcSplit::StreamSplit(split) => split.encode_to_json(),
        }
    }

    // fn restore_from_json(&self, value: JsonbVal) -> anyhow::Result<Box<dyn SplitMetaData>> {
    //     match &self {
    //         CdcSplit::SnapshotSplit(_) =>
    // Ok(Box::new(CdcSnapshotSplit::restore_from_json(value)?)),
    //         CdcSplit::StreamSplit(_) => Ok(Box::new(CdcStreamSplit::restore_from_json(value)?)),
    //     }
    // }

    fn restore_from_json(&self, value: JsonbVal) -> anyhow::Result<CdcSplit> {
        match &self {
            CdcSplit::SnapshotSplit(_) => Ok(CdcSplit::SnapshotSplit(
                CdcSnapshotSplit::restore_from_json(value)?,
            )),
            CdcSplit::StreamSplit(_) => Ok(CdcSplit::StreamSplit(
                CdcStreamSplit::restore_from_json(value)?,
            )),
        }
    }
}

impl SplitMetaData for CdcSnapshotSplit {
    fn id(&self) -> SplitId {
        todo!()
    }

    fn encode_to_json(&self) -> JsonbVal {
        todo!()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        todo!()
    }
}

impl SplitMetaData for CdcStreamSplit {
    fn id(&self) -> SplitId {
        todo!()
    }

    fn encode_to_json(&self) -> JsonbVal {
        todo!()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        todo!()
    }
}

struct MySqlCdcTableSplitter {}

impl CdcTableSplitter for MySqlCdcTableSplitter {
    type Split = CdcSplit;

    fn generate_splits(&self, table_id: &CdcTableId) -> Vec<Self::Split> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::source::{SplitId, SplitReader};
    #[test]
    fn test_cdc_split() {
        let table_id = CdcTableId {
            database: "mydb".to_string(),
            schema: "public".to_string(),
            table: "mytable".to_string(),
        };

        let split1 = CdcSplit::SnapshotSplit(CdcSnapshotSplit::default());
        let split2 = CdcSplit::SnapshotSplit(CdcSnapshotSplit::new(
            "my-split".to_string(),
            table_id.clone(),
        ));

        println!("split1: {:?}", split1);
        println!("split2: {:?}", split2);
    }
}
