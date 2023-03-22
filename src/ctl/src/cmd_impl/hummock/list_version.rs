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

use std::collections::HashMap;

use bytes::{Buf, Bytes};
use risingwave_pb::hummock::{
    Level, LevelType, PinnedSnapshotsSummary, PinnedVersionsSummary, SstableInfo,
};
use risingwave_rpc_client::HummockMetaClient;

use crate::CtlContext;

pub async fn list_version(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let version = meta_client.get_current_version().await?;

    for (cg, levels) in &version.levels {
        #[allow(clippy::type_complexity)]
        let mut lsm_level_key_range: HashMap<u64, Vec<(u64, (u32, u16), u64)>> = HashMap::default();
        let mut level_index_vec = Vec::default();

        let mut small_key = Bytes::default();
        let mut large_key = Bytes::default();

        let left_key = |level: &Level| -> Vec<u8> {
            level
                .table_infos
                .first()
                .unwrap()
                .key_range
                .as_ref()
                .unwrap()
                .left
                .clone()
        };

        let right_key = |level: &Level| -> Vec<u8> {
            level
                .table_infos
                .last()
                .unwrap()
                .key_range
                .as_ref()
                .unwrap()
                .right
                .clone()
        };

        let get_table_id_and_vnode = |key: &Bytes| -> (u32, u16) {
            let mut data = &key[..];
            let table_id = data.get_u32();
            let vnode = data.get_u16();

            (table_id, vnode)
        };

        let key_range_length = |left: &Bytes, right: &Bytes| -> u64 {
            let (left_table_id, left_vnode) = get_table_id_and_vnode(left);
            let (right_table_id, right_vnode) = get_table_id_and_vnode(right);

            let diff_table_id = (right_table_id - left_table_id) as u64;
            let diff_vnode = if right_table_id == left_table_id {
                (right_vnode - left_vnode) as u64
            } else {
                (right_vnode + 256 - left_vnode) as u64
            };

            let diff_length = diff_table_id * 256 + diff_vnode;
            println!(
                "lt {} lv {} rt {} rv {} length {}",
                left_table_id, left_vnode, right_table_id, right_vnode, diff_length
            );

            diff_length
        };

        let sst_key_range_length = |sst: &SstableInfo| -> u64 {
            let left = Bytes::from(sst.key_range.as_ref().unwrap().left.clone());
            let right = Bytes::from(sst.key_range.as_ref().unwrap().right.clone());

            key_range_length(&left, &right)
        };

        if let Some(l0) = levels.l0.as_ref() {
            for l0_level in &l0.sub_levels {
                if l0_level.table_infos.is_empty()
                    || l0_level.level_type == LevelType::Overlapping as i32
                {
                    continue;
                }

                level_index_vec.push(l0_level.sub_level_id);

                lsm_level_key_range.insert(l0_level.sub_level_id, vec![]);
                let last_level = lsm_level_key_range.get_mut(&l0_level.sub_level_id).unwrap();
                for sst in &l0_level.table_infos {
                    let length = sst_key_range_length(sst);
                    let left = Bytes::from(sst.key_range.as_ref().unwrap().left.clone());
                    let (table_id, vnode) = get_table_id_and_vnode(&left);

                    last_level.push((sst.sst_id, (table_id, vnode), length));
                }

                let left = left_key(l0_level);
                let right = right_key(l0_level);

                if left < small_key || small_key.is_empty() {
                    small_key = Bytes::copy_from_slice(left.as_ref());
                }

                if right > large_key {
                    large_key = Bytes::copy_from_slice(right.as_ref());
                }
            }
        }

        for level in &levels.levels {
            if level.table_infos.is_empty() {
                continue;
            }

            level_index_vec.push(level.level_idx as u64);

            lsm_level_key_range.insert(level.level_idx as u64, vec![]);
            let last_level = lsm_level_key_range
                .get_mut(&(level.level_idx as u64))
                .unwrap();

            for sst in &level.table_infos {
                let length = sst_key_range_length(sst);

                let left = Bytes::from(sst.key_range.as_ref().unwrap().left.clone());
                let (table_id, vnode) = get_table_id_and_vnode(&left);

                last_level.push((sst.sst_id, (table_id, vnode), length));
            }

            let left = left_key(level);
            let right = right_key(level);

            if left < small_key || small_key.is_empty() {
                small_key = Bytes::copy_from_slice(left.as_ref());
            }

            if right > large_key {
                large_key = Bytes::copy_from_slice(right.as_ref());
            }
        }

        let (l_table_id, l_vnode) = get_table_id_and_vnode(&small_key);

        let global_length = key_range_length(&small_key, &large_key);

        println!(
            "cg {:?} small_key {:?} large_key {:?} global_length {:?}",
            cg, small_key, large_key, global_length
        );

        for level_idx in level_index_vec {
            let level_stat = lsm_level_key_range.get(&level_idx).unwrap();
            println!("cg{}-L{}", cg, level_idx);
            for (_sst, (table_id, vnode), length) in level_stat {
                let raw_offset =
                    (table_id - l_table_id) as u64 * 256 + (vnode + 256 - l_vnode) as u64;

                print!("({}, {}), ", raw_offset, raw_offset + length);
            }

            println!();
        }
    }

    println!("{:#?}", version);
    Ok(())
}

pub async fn list_pinned_versions(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let PinnedVersionsSummary {
        mut pinned_versions,
        workers,
    } = meta_client
        .risectl_get_pinned_versions_summary()
        .await?
        .summary
        .unwrap();
    pinned_versions.sort_by_key(|v| v.min_pinned_id);
    for pinned_version in pinned_versions {
        match workers.get(&pinned_version.context_id) {
            None => {
                println!(
                    "Worker {} may have been dropped, min_pinned_version_id {}",
                    pinned_version.context_id, pinned_version.min_pinned_id
                );
            }
            Some(worker) => {
                println!(
                    "Worker {} type {} min_pinned_version_id {}",
                    pinned_version.context_id,
                    worker.r#type().as_str_name(),
                    pinned_version.min_pinned_id
                );
            }
        }
    }
    Ok(())
}

pub async fn list_pinned_snapshots(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let PinnedSnapshotsSummary {
        mut pinned_snapshots,
        workers,
    } = meta_client
        .risectl_get_pinned_snapshots_summary()
        .await?
        .summary
        .unwrap();
    pinned_snapshots.sort_by_key(|s| s.minimal_pinned_snapshot);
    for pinned_snapshot in pinned_snapshots {
        match workers.get(&pinned_snapshot.context_id) {
            None => {
                println!(
                    "Worker {} may have been dropped, min_pinned_snapshot {}",
                    pinned_snapshot.context_id, pinned_snapshot.minimal_pinned_snapshot
                );
            }
            Some(worker) => {
                println!(
                    "Worker {} type {} min_pinned_snapshot {}",
                    pinned_snapshot.context_id,
                    worker.r#type().as_str_name(),
                    pinned_snapshot.minimal_pinned_snapshot
                );
            }
        }
    }
    Ok(())
}
