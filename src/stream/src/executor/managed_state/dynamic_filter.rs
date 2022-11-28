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

use std::collections::btree_map::Range as BTreeMapRange;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{self, *};
use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::anyhow;
use futures::{pin_mut, stream, StreamExt};
use itertools::Itertools;
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::{CompactedRow, Row, Row2};
use risingwave_common::types::{ScalarImpl, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;

use crate::common::table::state_table::{prefix_range_to_memcomparable, StateTable};
use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

type ScalarRange = (Bound<ScalarImpl>, Bound<ScalarImpl>);

// pub struct ManagedRangeCache {
//     inner: RangeCache,
//     // Maps the last accessed epoch to a given partition of the logical range
//     ranges_by_last_accessed: HashMap<u64, ScalarRange>,
//     pub(super) watermark_epoch: Arc<AtomicU64>,
// }

// Proving that the last-accessed 
// Epoch 1: range (50, 100]
// Epoch 2: 

// Notes:
// 1. RangeCache needs to become epoch aware
// 2. Drawing the picture is easy but writing the algorithm is a pain in the ass. But,
//    that is what I'm being paid to do.
// 3. Maybe we need to easily identify the overlapping sub ranges. So some sort of index would 
//    be nice instead of naively scanning the entire `last_accessed` map. What we have here is 
//    essentially a discretization problem (similar to my RW interview leetcode problem). Basically,
//    what we could do is to store the range to epoch mapping in a BTreeMap. (Not sure if we should
//    use the serialized values).
//
//    To state the problem more generally, we need a data structure with two operations. Firstly,
//    we want to be able to identify a range corresponding to a u64 number. Secondly, we want to be able 
//    to identify all the numbers associated (overlapping) with a given range, so we can easily
//    update those values in the main data structure when a new range arrives.
//
//    We can use a BTreeMap to do so. But how to identify the first range (containing Unbounded)? 
//    Simple, it can just be an `Option` field in a wrapper struct indicating "first range". 
//
//    How do we update this "index" structure?
//    Simple, when we access a new range, we will delete any of the elements in the BTreeMap that it contains.
//    Then, we will add the new range's range bounds into the BTreeMap.
//
//    An optimization here is that we can introduce a level of granularity.
// 4. Maybe add some more simple unit tests to test the functionality of the helper functions...
// 5. Dealing with all the corner cases is actually insane...

// impl ManagedRangeCache {
// }


// TODO: What are our assumptions again?

// Represents the 
struct LeftBoundLastAccessed {
    inclusive: bool, // Whether or not this left bound is inclusive of the key
    epoch: Option<u64>, // If `None`, the range to the left is not included in the bound (i.e. this is an upper bound)

    // TODO: fix logic by using `u64::MAX` for `None` instead?
}

#[derive(Default)]
struct RangeLRU {
    // We should be aware of the existing range since 
    existing_range: Option<ScalarRange>,

    // Due to the path of range changes being continuous
    // Every continous range of epochs is associated with a continuous range. 
    last_accessed_at: BTreeMap<u64, ScalarRange>,
    // The secondary index allows us to update `last_accessed_at` in amortized constant time. 
    // It allows us to search for epochs whose associated range overlaps with that of this epoch.
    // `false`/`true` in the bool value of the key indicates `Inclusive`/`Exclusive` respectively.
    // If the value is `None`, it indicates that the range bounded on the left by the key is not 
    // contained in the `RangeLRU`.
    secondary_index: BTreeMap<(ScalarImpl, bool), Option<u64>>,
    // Since we cannot represent the lowest range (Unbounded, x) in our `BTreeMap`,
    // we represent its last-accessed epoch with this field.
    lower_unbounded_epoch: Option<u64>,
    last_accessed_epoch: u64,
}

fn merge_continuous_ranges(r1: ScalarRange, r2: ScalarRange) -> ScalarRange {
    // We take the extreme values
    let lower = if matches!(r1.0, Unbounded) | matches!(r2.0, Unbounded) {
        Unbounded
    } else {
        match (r1.0, r2.0) {
            (Excluded(x), Excluded(y)) => if x < y { Excluded(x) } else { Excluded(y) },
            (Included(x), Included(y)) => if x < y { Included(x) } else { Included(y) },
            (Included(x), Excluded(y)) => if x <= y { Included(x) } else { Excluded(y) },
            (Excluded(x), Included(y)) => if y <= x { Included(x) } else { Excluded(y) },
            _ => unreachable!(),
        } 
    };

    let upper = if matches!(r1.1, Unbounded) | matches!(r2.1, Unbounded) {
        Unbounded
    } else {
        match (r1.1, r2.1) {
            (Excluded(x), Excluded(y)) => if x > y { Excluded(x) } else { Excluded(y) },
            (Included(x), Included(y)) => if x > y { Included(x) } else { Included(y) },
            (Included(x), Excluded(y)) => if x >= y { Included(x) } else { Excluded(y) },
            (Excluded(x), Included(y)) => if y >= x { Included(x) } else { Excluded(y) },
            _ => unreachable!(),
        } 
    };
    (lower, upper)
}

fn translate_lower_bound(bound: Bound<ScalarImpl>) -> Bound<(ScalarImpl, bool)> {
    match bound {
        Included(x) => Included((x, false)),
        Excluded(x) => Included((x, true)),
        Unbounded => Unbounded
    }
}

fn translate_upper_bound(bound: Bound<ScalarImpl>) -> Bound<(ScalarImpl, bool)> {
    match bound {
        Excluded(x) => Excluded((x, false)), // Everything before (x, false)
        Included(x) => Included((x, false)), // Everything including (x, false)
        Unbounded => Unbounded
    }
}

fn invert_bound(bound: Bound<ScalarImpl>) -> Bound<ScalarImpl> {
    match bound {
        Unbounded => Unbounded,
        Included(x) => Excluded(x),
        Excluded(x) => Included(x),
    }
}

impl RangeLRU {
    /// Evicts ranges that are last accessed prior to the given epoch
    /// Due to continuity in the range changes, this range is always contiguous.
    /// 
    /// returns: If some range is evicted, Some((the range to evict, the new `existing_range`)), 
    /// else `None`
    fn evict_before(&mut self, epoch: u64) -> Option<(ScalarRange, Option<ScalarRange>)> {
        let mut range = None;
        let new = self.last_accessed_at.split_off(&epoch);
        let old = std::mem::replace(&mut self.last_accessed_at, new);

        for r in old.values() {
            if let Some(r2) = range {
                range = Some(merge_continuous_ranges(r.clone(), r2));
            } else {
                range = Some(r.clone())
            }
        }
        if let Some(range) = range {
            // If there is a range, there is an existing range
            let existing_range = self.existing_range.as_ref().unwrap();
            let mut iter = old.iter().rev().peekable(); 
            let (from_bottom, new_existing_range) = if range.0 == existing_range.0 {
                // Evict from the bottom
                let new_existing_range = match range.1.clone() {
                    Unbounded => None,
                    Included(x) => Some((Excluded(x), existing_range.1.clone())),
                    Excluded(x) => Some((Included(x), existing_range.1.clone())),
                };
                self.existing_range = new_existing_range.clone();
                (true, new_existing_range)
            } else {
                // Evict from the top
                let new_existing_range = match range.0.clone() {
                    Unbounded => None,
                    Included(x) => Some((existing_range.0.clone(), Excluded(x))),
                    Excluded(x) => Some((existing_range.0.clone(), Included(x))),
                };
                self.existing_range = new_existing_range.clone();
                (false, new_existing_range)
            };
    
            while let Some((k, v)) = iter.next() {
                match translate_lower_bound(v.0.clone()) {
                    Included(x) => {
                        if from_bottom || iter.peek().is_some() {
                            self.secondary_index.remove(&x).unwrap(); // TODO assert the epochs are the same?
                        } else {
                            // From top and last element, we should simply convert it to `None` instead of removing
                            let old = self.secondary_index.get_mut(&x).unwrap();
                            assert_eq!(old.unwrap(), *k);
                            *old = None;
                        }
                    },
                    Unbounded => assert_eq!(self.lower_unbounded_epoch.take().unwrap(), *k),
                    _ => unreachable!(),
                }
            }
            Some((range, new_existing_range))
        } else {
            None
        }

    }

    // Sadly, this is a total mess right now.
    fn access(&mut self, range: ScalarRange, epoch: u64) {
        assert_ne!(epoch, 0);
        assert!(self.last_accessed_epoch < epoch); 
        self.last_accessed_epoch = epoch;
        let new_existing_range = self.existing_range
            .take()
            .map_or(
                Some(range.clone()), 
                |er| Some(merge_continuous_ranges(range.clone(), er))
            );
        self.existing_range = new_existing_range;
        // TODO: sanity check that

        let mut last_deleted_epoch = None;
        let mut remove_keys = vec![];
        {
            let encoded_range = (
                translate_lower_bound(range.0.clone()), 
                translate_upper_bound(range.1.clone())
            );
            let mut iter = self.secondary_index.range(encoded_range).peekable();
            
            while let Some((k, v)) = iter.next() {
                remove_keys.push(k.clone());
                // Delete all of these, replace it with our range. Keep track of the last deleted.
                // Exceptions: 
                if iter.peek().is_some() {
                    // Not the last, so the epoch must be `Some`
                    self.last_accessed_at.remove(v.as_ref().unwrap());
                } else {
                    if let Some(e) = v {
                        // Our secondary index contains this epoch, so it must be `Some`
                        let split_range = self.last_accessed_at.get_mut(&e).unwrap();
                        split_range.0 = range.1.clone();
                    }
                    last_deleted_epoch = v.clone();
                }
            }
            if !matches!(range.0, Unbounded) {
                let upper = match translate_lower_bound(range.0.clone()) {
                    Included(x) => Excluded(x),
                    _ => unreachable!(),
                };
                let lower_range = (Unbounded, upper);
                let mut rev_iter = self.secondary_index.range(lower_range).rev();
    
                let e = rev_iter.next().map_or(self.lower_unbounded_epoch, |(_, &x)| x);
                if let Some(e) = e {
                    let split_range = self.last_accessed_at.get_mut(&e).unwrap();
                    split_range.1 = match range.0.clone() {
                        Excluded(x) => Included(x),
                        Included(x) => Excluded(x),
                        _ => unreachable!()
                    };
                }
            }
        }
        for key in remove_keys {
            println!("REMOVING KEY: {key:?}");
            self.secondary_index.remove(&key).unwrap();
        }
        self.last_accessed_at.insert(epoch, range.clone());

        // Insert lower bound of our range into secondary index
        if matches!(range.0, Unbounded) {
            self.lower_unbounded_epoch = Some(epoch);
        } else {
            let (k, v) = match translate_lower_bound(range.0) {
                Included(k) => (k, Some(epoch)),
                _ => unreachable!()
            };
            self.secondary_index.insert(k, v);
        }

        // Insert upper bound of our range into secondary index
        if matches!(range.1, Unbounded) { /* do nothing */ } else {
            let k = match range.1 {
                Included(x) => (x, true),
                Excluded(x) => (x, false),
                _ => unreachable!()
            };
            self.secondary_index.insert(k, last_deleted_epoch);
        }

        // self.

    }

    // Private function
    // fn 
}

#[cfg(test)]
mod range_lru_test {
    use super::*;
    #[test]
    fn test_range_lru_access() {
        let mut range_lru = RangeLRU::default();
        range_lru.access((Unbounded, Included(ScalarImpl::Int64(100))), 1);
        println!("LAST ACCESSED: {:?}", range_lru.last_accessed_at);
        println!("S-INDEX: {:?}, {:?}", range_lru.lower_unbounded_epoch, range_lru.secondary_index);
        range_lru.access((Excluded(ScalarImpl::Int64(50)), Excluded(ScalarImpl::Int64(150))), 2);

        println!("LAST ACCESSED: {:?}", range_lru.last_accessed_at);
        println!("S-INDEX: {:?}, {:?}", range_lru.lower_unbounded_epoch, range_lru.secondary_index);
        assert_eq!(*range_lru.last_accessed_at.get(&1).unwrap(), (Unbounded, Included(ScalarImpl::Int64(50))));
        assert_eq!(*range_lru.last_accessed_at.get(&2).unwrap(), (Excluded(ScalarImpl::Int64(50)), Excluded(ScalarImpl::Int64(150))));

        range_lru.access((Included(ScalarImpl::Int64(150)), Excluded(ScalarImpl::Int64(151))), 3);
        assert_eq!(*range_lru.last_accessed_at.get(&3).unwrap(), (Included(ScalarImpl::Int64(150)), Excluded(ScalarImpl::Int64(151))));

        range_lru.access((Included(ScalarImpl::Int64(5)), Excluded(ScalarImpl::Int64(50))), 4);
        println!("LAST ACCESSED: {:?}", range_lru.last_accessed_at);
        println!("S-INDEX: {:?}, {:?}", range_lru.lower_unbounded_epoch, range_lru.secondary_index);

        // TODO: test more behaviours?
    }

    #[test]
    fn test_range_lru_merge_continuous_range() {}

    #[test]
    fn test_range_lru_access_and_evict() {
        let mut range_lru = RangeLRU::default();
        range_lru.access((Unbounded, Included(ScalarImpl::Int64(100))), 1);
        println!("LAST ACCESSED: {:?}", range_lru.last_accessed_at);
        println!("S-INDEX: {:?}, {:?}", range_lru.lower_unbounded_epoch, range_lru.secondary_index);
        range_lru.access((Excluded(ScalarImpl::Int64(50)), Excluded(ScalarImpl::Int64(150))), 2);

        println!("LAST ACCESSED: {:?}", range_lru.last_accessed_at);
        println!("S-INDEX: {:?}, {:?}", range_lru.lower_unbounded_epoch, range_lru.secondary_index);
        assert_eq!(*range_lru.last_accessed_at.get(&1).unwrap(), (Unbounded, Included(ScalarImpl::Int64(50))));
        assert_eq!(*range_lru.last_accessed_at.get(&2).unwrap(), (Excluded(ScalarImpl::Int64(50)), Excluded(ScalarImpl::Int64(150))));

        range_lru.access((Included(ScalarImpl::Int64(150)), Excluded(ScalarImpl::Int64(151))), 3);
        assert_eq!(*range_lru.last_accessed_at.get(&3).unwrap(), (Included(ScalarImpl::Int64(150)), Excluded(ScalarImpl::Int64(151))));

        range_lru.access((Included(ScalarImpl::Int64(5)), Excluded(ScalarImpl::Int64(50))), 4);
        println!("LAST ACCESSED: {:?}", range_lru.last_accessed_at);
        println!("S-INDEX: {:?}, {:?}", range_lru.lower_unbounded_epoch, range_lru.secondary_index);

        assert_eq!(
            range_lru.evict_before(2).unwrap(), 
            (
                (Unbounded, Excluded(ScalarImpl::Int64(5))), 
                Some((Included(ScalarImpl::Int64(5)), Excluded(ScalarImpl::Int64(151))))
            )
        );
        assert_eq!(
            range_lru.evict_before(3).unwrap(), 
            (
                (Excluded(ScalarImpl::Int64(50)), Excluded(ScalarImpl::Int64(150))), 
                Some((Included(ScalarImpl::Int64(5)), Included(ScalarImpl::Int64(50))))
            )
        );
    }
}

/// The `RangeCache` caches a given range of `ScalarImpl` keys and corresponding rows.
/// It will evict keys from memory if it is above capacity and shrink its range.
/// Values not in range will have to be retrieved from storage.
/// 
/// TODO: `RangeCache` may be very coupled to `DynamicFilter`. But it is probably fine
/// since it doesn't seem likely candidate for reuse.
pub struct RangeCache<S: StateStore> {
    /// {vnode -> {memcomparable_pk -> row}}
    cache: HashMap<u8, BTreeMap<Vec<u8>, CompactedRow>>,
    pub(crate) state_table: StateTable<S>,
    /// The current range stored in the cache.
    /// Any request for a set of values outside of this range will result in a scan
    /// from storage
    range: Option<ScalarRange>,

    #[expect(dead_code)]
    num_rows_stored: usize,
    #[expect(dead_code)]
    capacity: usize,

    prev_direction_is_up: Option<bool>,

    vnodes: Arc<Bitmap>,
}


impl<S: StateStore> RangeCache<S> {
    /// Create a [`RangeCache`] with given capacity and epoch
    pub fn new(state_table: StateTable<S>, capacity: usize, vnodes: Arc<Bitmap>) -> Self {
        Self {
            cache: HashMap::new(),
            state_table,
            range: None,
            num_rows_stored: 0,
            capacity,
            vnodes,
            prev_direction_is_up: None,
        }
    }

    pub fn init(&mut self, epoch: EpochPair) {
        self.state_table.init_epoch(epoch);
    }

    /// Insert a row and corresponding scalar value key into cache (if within range) and
    /// `StateTable`.
    pub fn insert(&mut self, k: &ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if let Some(r) = &self.range && r.contains(k) {
            let vnode = self.state_table.compute_vnode(&v);
            let vnode_entry = self.cache.entry(vnode).or_insert_with(BTreeMap::new);
            let pk = v.extract_memcomparable_by_indices(self.state_table.pk_serde(), self.state_table.pk_indices());
            vnode_entry.insert(pk, (&v).into());
        }
        self.state_table.insert(v);
        Ok(())
    }

    /// Delete a row and corresponding scalar value key from cache (if within range) and
    /// `StateTable`.
    // FIXME: panic instead of returning Err
    pub fn delete(&mut self, k: &ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if let Some(r) = &self.range && r.contains(k) {
            let vnode = self.state_table.compute_vnode(&v);
            let pk = v.extract_memcomparable_by_indices(self.state_table.pk_serde(), self.state_table.pk_indices());

            self.cache.get_mut(&vnode)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?
                .remove(&pk)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?;
        }
        self.state_table.delete(v);
        Ok(())
    }

    fn to_row_bound(bound: Bound<ScalarImpl>) -> Bound<Row> {
        match bound {
            Unbounded => Unbounded,
            Included(s) => Included(Row::new(vec![Some(s)])),
            Excluded(s) => Excluded(Row::new(vec![Some(s)])),
        }
    }

    // The `DynamicFilter` is defined by a pointer that either moves up or down. Thus,
    // There are only 2 situations when it comes to fetching ranges (even if one side of
    // existing range is `Unbounded`):
    // 1. Extension: The direction of movement stays the same
    // 2. Inflexion: The direction of movement changes 
    // We thus check in debug mode whether the ranges agree at the point at which they are connected.
    #[cfg(debug_assertions)]
    pub fn check_consistency(&self, direction_is_up: bool, range: &ScalarRange) {
        // if let Some(existing_range) = &self.range {
        //     fn is_inversion(b1: &Bound<ScalarImpl>, b2: &Bound<ScalarImpl>) -> bool {
        //         match b1 {
        //             Unbounded => b2.as_ref() == Unbounded,
        //             Included(x) => b2.as_ref() == Excluded(x),
        //             Excluded(x) => b2.as_ref() == Included(x),
        //         }
        //     }
        //     // 
        //     if self.prev_direction_is_up.unwrap() == direction_is_up {
        //         // We have an extension
        //         if direction_is_up {
        //             // Check the lower bound of new range is inversion of
        //             // upper bound of old range
        //             assert!(is_inversion(&range.0, &existing_range.1));
        //         } else {
        //             assert!(is_inversion(&range.1, &existing_range.0));
        //         }
        //     } else {
        //         // We have an inflexion
        //         if direction_is_up {
        //             // Check lower bound of new range is equal to lower
        //             // bound of old range
        //             assert_eq!(range.0, existing_range.0)
        //         } else {
        //             assert_eq!(range.1, existing_range.1)
        //         }
        //     }
        // }
    }

    /// Return an iterator over sets of rows that satisfy the given range. 
    /// 
    /// (no longer correct): Evicts entries if
    /// exceeding capacity based on whether the latest RHS value is the lower or upper bound of
    /// the range.
    pub async fn range(
        &mut self,
        range: ScalarRange,
        direction_is_up: bool,
    ) -> StreamExecutorResult<UnorderedRangeCacheIter<'_>> {
        // What we want: At the end of every epoch we will try to read
        // ranges based on the new value. The values in the range may not all be cached.
        //
        // If the new range is overlapping with the current range, we will keep the
        // current range. We will then evict to capacity after the cache has been populated
        // with the new range.
        //
        // If the new range is non-overlapping, we will delete the old range, and store
        // all elements from the new range.
        //
        // (TODO): We will always prefer to cache values that are closer to the latest value.
        //
        // If this requested range is too large, it will cause OOM. The `StateStore`
        // layer already buffers the entire output of a range scan in `Vec`, so there is
        // currently no workarond for OOM due to large range scans.

        #[cfg(debug_assertions)]
        self.check_consistency(direction_is_up, &range);
        self.prev_direction_is_up = Some(direction_is_up);

        let missing_ranges = if let Some(existing_range) = &self.range {
            let (ranges_to_fetch, new_range, delete_old) =
                get_missing_ranges(existing_range.clone(), range.clone());
            self.range = Some(new_range);
            if delete_old {
                self.cache = HashMap::new();
            }
            ranges_to_fetch
        } else {
            self.range = Some(range.clone());
            vec![range.clone()]
        };

        let missing_ranges = missing_ranges.iter().map(|(r0, r1)| {
            (
                Self::to_row_bound(r0.clone()),
                Self::to_row_bound(r1.clone()),
            )
        });

        for pk_range in missing_ranges {
            let init_maps = self
                .vnodes
                .ones()
                .map(|vnode| {
                    self.cache
                        .get_mut(&(vnode as VirtualNode))
                        .map(std::mem::take)
                        .unwrap_or_default()
                })
                .collect_vec();
            let futures = self
                .vnodes
                .ones()
                .zip_eq(init_maps.into_iter())
                .map(|(vnode, init_map)| self.fetch_vnode_range(vnode, &pk_range, init_map));
            let results: Vec<_> = stream::iter(futures).buffer_unordered(10).collect().await;
            for result in results {
                let (vnode, map) = result?;
                self.cache.insert(vnode, map);
            }
        }

        let range = (Self::to_row_bound(range.0), Self::to_row_bound(range.1));
        let memcomparable_range =
            prefix_range_to_memcomparable(self.state_table.pk_serde(), &range);
        Ok(UnorderedRangeCacheIter::new(
            &self.cache,
            memcomparable_range,
            self.vnodes.clone(),
        ))
    }

    async fn fetch_vnode_range(
        &self,
        vnode: usize,
        pk_range: &(Bound<impl Row2>, Bound<impl Row2>),
        initial_map: BTreeMap<Vec<u8>, CompactedRow>,
    ) -> StreamExecutorResult<(VirtualNode, BTreeMap<Vec<u8>, CompactedRow>)> {
        let vnode = vnode.try_into().unwrap();
        let row_stream = self
            .state_table
            .iter_key_and_val_with_pk_range(pk_range, vnode)
            .await?;
        pin_mut!(row_stream);

        let mut map = initial_map;
        // row stream output is sorted by its pk, aka left key (and then original pk)
        while let Some(res) = row_stream.next().await {
            let (key_bytes, row) = res?;

            map.insert(
                key_bytes[VIRTUAL_NODE_SIZE..].to_vec(),
                (row.as_ref()).into(),
            );
        }

        Ok((vnode, map))
    }

    /// Updates the vnodes for `RangeCache`, purging the rows of the vnodes that are no longer
    /// owned.
    pub async fn update_vnodes(
        &mut self,
        new_vnodes: Arc<Bitmap>,
    ) -> StreamExecutorResult<Arc<Bitmap>> {
        let old_vnodes = self.state_table.update_vnode_bitmap(new_vnodes.clone());
        for (vnode, (old, new)) in old_vnodes.iter().zip_eq(new_vnodes.iter()).enumerate() {
            if old && !new {
                let vnode = vnode.try_into().unwrap();
                self.cache.remove(&vnode);
            }
        }
        if let Some(ref self_range) = self.range {
            let current_range = (
                Self::to_row_bound(self_range.0.clone()),
                Self::to_row_bound(self_range.1.clone()),
            );
            let newly_owned_vnodes = Bitmap::bit_saturate_subtract(&new_vnodes, &old_vnodes);

            let futures = newly_owned_vnodes
                .ones()
                .map(|vnode| self.fetch_vnode_range(vnode, &current_range, BTreeMap::new()));
            let results: Vec<_> = stream::iter(futures).buffer_unordered(10).collect().await;
            for result in results {
                let (vnode, map) = result?;
                self.cache.insert(vnode, map);
            }
        }
        self.vnodes = new_vnodes;
        Ok(old_vnodes)
    }

    /// Flush writes to the `StateTable` from the in-memory buffer.
    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}

pub struct UnorderedRangeCacheIter<'a> {
    cache: &'a HashMap<u8, BTreeMap<Vec<u8>, CompactedRow>>,
    current_map: Option<&'a BTreeMap<Vec<u8>, CompactedRow>>,
    current_iter: Option<BTreeMapRange<'a, Vec<u8>, CompactedRow>>,
    vnodes: Arc<Bitmap>,
    next_vnode: u8,
    completed: bool,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl<'a> UnorderedRangeCacheIter<'a> {
    fn new(
        cache: &'a HashMap<u8, BTreeMap<Vec<u8>, CompactedRow>>,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        vnodes: Arc<Bitmap>,
    ) -> Self {
        let mut new = Self {
            cache,
            current_map: None,
            current_iter: None,
            next_vnode: 0,
            vnodes,
            range,
            completed: false,
        };
        new.refill_iterator();
        new
    }

    fn refill_iterator(&mut self) {
        loop {
            if self.vnodes.is_set(self.next_vnode as usize) && let Some(vnode_range) = self.cache.get(&self.next_vnode) {
                self.current_map = Some(vnode_range);
                self.current_iter = self.current_map.map(|m| m.range(self.range.clone()));
                return;
            } else if self.next_vnode == u8::MAX {
                // The iterator cannot be refilled further.
                self.completed = true;
                return;
            } else {
                self.next_vnode += 1;
            }
        }
    }
}

impl<'a> std::iter::Iterator for UnorderedRangeCacheIter<'a> {
    type Item = &'a CompactedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.completed {
            None
        } else if let Some(iter) = &mut self.current_iter {
            let res = iter.next();
            if res.is_none() {
                if self.next_vnode == u8::MAX {
                    // The iterator cannot be refilled further.
                    self.completed = true;
                    None
                } else {
                    // Try to refill the iterator.
                    self.next_vnode += 1;
                    self.refill_iterator();
                    self.next()
                }
            } else {
                res.map(|r| r.1)
            }
        } else {
            panic!("Not completed but no iterator");
        }
    }
}

// This function returns three objects.
// 1. The ranges required to be fetched from cache.
// 2. The new range
// 3. Whether to delete the existing range.
//
// TODO: Actually, we will never need to fetch more than one range from cache.
// So lets panic or thrown an error.
fn get_missing_ranges(
    existing_range: ScalarRange,
    required_range: ScalarRange,
) -> (Vec<ScalarRange>, ScalarRange, bool) {
    let (existing_contains_lower, existing_contains_upper) =
        range_contains_lower_upper(&existing_range, &required_range.0, &required_range.1);

    if existing_contains_lower && existing_contains_upper {
        (vec![], existing_range.clone(), false)
    } else if existing_contains_lower {
        let lower = match existing_range.1 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![(lower, required_range.1.clone())],
            (existing_range.0, required_range.1),
            false,
        )
    } else if existing_contains_upper {
        let upper = match existing_range.0 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![(required_range.0.clone(), upper)],
            (required_range.0, existing_range.1),
            false,
        )
    } else if range_contains_lower_upper(&required_range, &existing_range.0, &existing_range.1)
        == (true, true)
    {   
        panic!("This scenario is inconceivable");
        let lower = match existing_range.0 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        let upper = match existing_range.1 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![
                (required_range.0.clone(), lower),
                (upper, required_range.1.clone()),
            ],
            required_range,
            false,
        )
    } else {
        // The ranges are non-overlapping. So we delete the old range.
        (vec![required_range.clone()], required_range, true)
    }
}

// Returns whether the given range contains the lower and upper bounds respectively of another
// range.
fn range_contains_lower_upper(
    range: &ScalarRange,
    lower: &Bound<ScalarImpl>,
    upper: &Bound<ScalarImpl>,
) -> (bool, bool) {
    let contains_lower = match &lower {
        Excluded(s) => {
            let modified_lower = if let Excluded(x) = &range.0 {
                Included(x.clone())
            } else {
                range.0.clone()
            };
            let modified_upper = if let Included(x) = &range.1 {
                Excluded(x.clone())
            } else {
                range.1.clone()
            };
            (modified_lower, modified_upper).contains(s)
        }
        Included(s) => range.contains(s),
        Unbounded => matches!(range.0, Unbounded),
    };

    let contains_upper = match &upper {
        Excluded(s) => {
            let modified_lower = if let Included(x) = &range.0 {
                Excluded(x.clone())
            } else {
                range.0.clone()
            };
            let modified_upper = if let Excluded(x) = &range.1 {
                Included(x.clone())
            } else {
                range.1.clone()
            };
            (modified_lower, modified_upper).contains(s)
        }
        Included(s) => range.contains(s),
        Unbounded => matches!(range.1, Unbounded),
    };

    (contains_lower, contains_upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_missing_range() {
        // Overlapping ranges
        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(100))
                ),
                (
                    Included(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                ),
            ),
            (
                vec![(
                    Excluded(ScalarImpl::Int64(100)),
                    Included(ScalarImpl::Int64(150))
                )],
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                false
            )
        );

        // Non-overlapping ranges
        assert_eq!(
            range_contains_lower_upper(
                &(
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(50))
                ),
                &Excluded(ScalarImpl::Int64(50)),
                &Included(ScalarImpl::Int64(150)),
            ),
            (false, false)
        );

        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(50))
                ),
                (
                    Excluded(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                ),
            ),
            (
                vec![(
                    Excluded(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                )],
                (
                    Excluded(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                ),
                true
            )
        );

        // Required contains existing
        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(25)),
                    Excluded(ScalarImpl::Int64(50))
                ),
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
            ),
            (
                vec![
                    (
                        Included(ScalarImpl::Int64(0)),
                        Excluded(ScalarImpl::Int64(25))
                    ),
                    (
                        Included(ScalarImpl::Int64(50)),
                        Included(ScalarImpl::Int64(150))
                    )
                ],
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                false,
            )
        );

        // Existing contains required
        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                (
                    Included(ScalarImpl::Int64(25)),
                    Excluded(ScalarImpl::Int64(50))
                ),
            ),
            (
                vec![],
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                false,
            )
        );
    }

    #[test]
    fn test_dynamic_filter_range_cache_unordered_range_iter() {
        let cache = (0..=u8::MAX)
            .map(|x| {
                (
                    x,
                    vec![(vec![x], CompactedRow { row: vec![x] })]
                        .into_iter()
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let range = (Unbounded, Unbounded);
        let vnodes = Arc::new(Bitmap::from_bytes(bytes::Bytes::from_static(
            &[u8::MAX; 32],
        ))); // set all the bits
        let mut iter = UnorderedRangeCacheIter::new(&cache, range, vnodes);
        for i in 0..=u8::MAX {
            assert_eq!(Some(&CompactedRow { row: vec![i] }), iter.next());
        }
        assert!(iter.next().is_none());
    }
}
