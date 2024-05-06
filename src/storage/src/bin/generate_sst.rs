use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::iter;

use bytes::Bytes;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext;
use risingwave_hummock_sdk::key::{get_table_id, next_key, FullKey, TableKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::hummock::{PbHummockVersionCheckpoint, SstableInfo};
use risingwave_storage::hummock::sstable::*;

trait KeyValueStatsProvider {
    fn ratio(&self, table_id: u32) -> f64;
    fn key_size(&self, table_id: u32) -> u64;
    fn value_size(&self, table_id: u32) -> u64;
}

/// When there's no per table stats available,
/// use `DefaultKeyValueStatsProvider` which is based on `SstableInfo`.
struct DefaultKeyValueStatsProvider {
    sst_total_key_count: u64,
    estimated_sst_total_size: u64,
    estimated_key_size: u64,
    estimated_value_size: u64,
    sst_member_tables: HashSet<u32>,
}

impl DefaultKeyValueStatsProvider {
    fn new(sst: &SstableInfo) -> Self {
        let key_range = sst.key_range.as_ref().unwrap();
        let sst_total_key_count = sst.total_key_count;
        let estimated_sst_total_size = sst.uncompressed_file_size;
        let estimated_key_size = ((key_range.left.len() + key_range.right.len()) / 2) as u64;
        let estimated_value_size = (estimated_sst_total_size
            .saturating_sub(estimated_key_size * sst_total_key_count))
            / sst_total_key_count;
        Self {
            sst_total_key_count,
            estimated_sst_total_size,
            estimated_key_size,
            estimated_value_size,
            sst_member_tables: sst.table_ids.iter().cloned().collect(),
        }
    }
}

impl KeyValueStatsProvider for DefaultKeyValueStatsProvider {
    fn ratio(&self, table_id: u32) -> f64 {
        if !self.sst_member_tables.contains(&table_id) {
            return 0.0;
        }
        1.0 / self.sst_member_tables.len() as f64
    }

    fn key_size(&self, _table_id: u32) -> u64 {
        self.estimated_key_size
    }

    fn value_size(&self, _table_id: u32) -> u64 {
        self.estimated_value_size
    }
}

fn generate_ssts(v: HummockVersion) {
    let opt = SstableBuilderOptions::default();
    let mut visited_object_ids: HashSet<HummockSstableObjectId> = HashSet::default();
    for level in v.get_combined_levels() {
        for sst in &level.table_infos {
            if !visited_object_ids.insert(sst.object_id) {
                continue;
            }
            generate_sst(sst);

            // TODO
            return;
        }
    }
}

fn generate_sst(sst: &SstableInfo) {
    let avg_epoch = std::cmp::max(
        sst.total_key_count / (sst.total_key_count - sst.stale_key_count),
        1,
    );
    let p = DefaultKeyValueStatsProvider::new(sst);
    let key_range: KeyRange = sst.key_range.as_ref().unwrap().into();
    let left_full_key = FullKey::decode(&key_range.left);
    let right_full_key = FullKey::decode(&key_range.right);

    println!(
        "member_table_ids: {:?}, key_count: {}, uncompressed_file_size: {}",
        sst.table_ids, sst.total_key_count, sst.uncompressed_file_size
    );
    for t in &sst.table_ids {
        println!("table: {}", t);
        println!("\tratio: {}", p.ratio(*t));
        println!("\tkey size: {}", p.key_size(*t));
        println!("\tvalue size: {}", p.value_size(*t));
        println!();
    }
    println!("avg_epoch: {}\n", avg_epoch);
    for full_key in vec![left_full_key.clone(), right_full_key.clone()] {
        let table_id = full_key.user_key.table_id;
        let epoch = Epoch::from(full_key.epoch_with_gap.pure_epoch());
        let key = full_key.user_key.table_key.key_part();
        let vnode = full_key.user_key.table_key.vnode_part();
        println!("full_key: {:?}, len={}", full_key, full_key.encoded_len());
        println!(
            "epoch: {} offset = {}",
            epoch,
            full_key.epoch_with_gap.offset(),
        );
        println!("table_id: {}, vnode: {:?}", table_id, vnode);
        println!("table_key: {:?}", key);
        println!();
    }

    fn add_key(table_id: u32, vnode_id: usize, key: &[u8], epoch: u64) {
        println!("table: {}, vnode: {}, key: {:?}, epoch: {}", table_id, vnode_id, key, epoch);
        // TODO
    }

    // left_key_table == right_key_table && left_key_vnode == right_key_vnode
    let c1 = || {
        let mut keys_to_generate = sst.total_key_count;
        let mut current_table_key = left_full_key.user_key.table_key.0.to_vec();
        let mut current_epoch = left_full_key.epoch_with_gap.as_u64();
        let mut epoch_counter = 0;
        let table_id = left_full_key.user_key.table_id.table_id;
        let vnode_id = left_full_key.user_key.get_vnode_id();
        assert!(current_epoch >= sst.min_epoch);
        assert!(current_table_key.as_slice()<right_full_key.user_key.table_key.0);
        while keys_to_generate > 0 {
            if epoch_counter == avg_epoch {
                epoch_counter = 0;
                current_epoch = sst.min_epoch;
                current_table_key = next_key_padding(&current_table_key);
                assert!(current_table_key.as_slice()<right_full_key.user_key.table_key.0);
            }
            assert!(current_epoch <= sst.max_epoch);
            add_key(
                table_id,
                vnode_id,
                &current_table_key,
                current_epoch,
            );
            current_epoch += 1;
            epoch_counter += 1;
            keys_to_generate -= 1;
        }
    };

    if left_full_key.user_key.table_id == right_full_key.user_key.table_id {
        if left_full_key.user_key.get_vnode_id() == right_full_key.user_key.get_vnode_id() {
            // #1
            c1();
        } else {
            // #2
            // TODO
            // let vnode_count = right_full_key.user_key.get_vnode_id() - left_full_key.user_key.get_vnode_id() + 1;
        }
    } else {
        // #3
        // TODO
    }
}

fn next_key_padding(key: &[u8]) -> Vec<u8> {
    let mut next_key = next_key(key);
    assert!(!next_key.is_empty());
    if key.len() > next_key.len() {
        next_key.extend(iter::repeat(0).take(key.len() - next_key.len()));
    }
    next_key
}

fn load_version() -> HummockVersion {
    use prost::Message;
    let mut file = File::open("/Users/z28wang/Desktop/dev_data_hummock_checkpoint_0").unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    let ckpt: PbHummockVersionCheckpoint =
        PbHummockVersionCheckpoint::decode(Bytes::from(buf)).unwrap();
    HummockVersion::from_persisted_protobuf(&ckpt.version.unwrap())
}

fn main() {
    let v = load_version();
    generate_ssts(v);
}

#[cfg(test)]
mod tests {
    use crate::next_key_padding;

    #[test]
    fn test_next_key_padding() {
        assert_eq!(next_key_padding(b"2\xff"), b"3\x00");
    }
}
