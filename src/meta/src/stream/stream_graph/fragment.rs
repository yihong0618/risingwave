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

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::LazyLock;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{generate_internal_table_name_with_type, TableId};
use risingwave_common::hash::ParallelUnitMapping;
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatcherType, StreamActor, StreamFragmentGraph as StreamFragmentGraphProto,
};

use super::{visit_fragment, visit_internal_tables};
use crate::manager::{IdGeneratorManagerRef, StreamingJob};
use crate::model::FragmentId;
use crate::storage::MetaStore;
use crate::stream::stream_graph::id::{GlobalFragmentId, GlobalFragmentIdGen, GlobalTableIdGen};
use crate::stream::stream_graph::schedule::Distribution;
use crate::MetaResult;

/// The fragment in the building phase, including the [`StreamFragment`] from the frontend and
/// several additional helper fields.
#[derive(Debug, Clone)]
pub struct BuildingFragment {
    /// The fragment structure from the frontend, with the global fragment ID.
    inner: StreamFragment,

    /// A clone of the internal tables in this fragment.
    internal_tables: Vec<Table>,

    /// The ID of the job if it's materialized in this fragment.
    table_id: Option<u32>,
}

impl BuildingFragment {
    /// Create a new [`BuildingFragment`] from a [`StreamFragment`]. The global fragment ID and
    /// global table IDs will be correctly filled with the given `id` and `table_id_gen`.
    fn new(
        id: GlobalFragmentId,
        fragment: StreamFragment,
        job: &StreamingJob,
        table_id_gen: GlobalTableIdGen,
    ) -> Self {
        let mut fragment = StreamFragment {
            fragment_id: id.as_global_id(),
            ..fragment
        };
        let internal_tables = Self::fill_internal_tables(&mut fragment, job, table_id_gen);
        let table_id = Self::fill_job(&mut fragment, job).then(|| job.id());

        Self {
            inner: fragment,
            internal_tables,
            table_id,
        }
    }

    /// Fill the information of the internal tables in the fragment.
    fn fill_internal_tables(
        fragment: &mut StreamFragment,
        job: &StreamingJob,
        table_id_gen: GlobalTableIdGen,
    ) -> Vec<Table> {
        let fragment_id = fragment.fragment_id;
        let mut internal_tables = Vec::new();

        visit_internal_tables(fragment, |table, table_type_name| {
            table.id = table_id_gen.to_global_id(table.id).as_global_id();
            table.schema_id = job.schema_id();
            table.database_id = job.database_id();
            table.name = generate_internal_table_name_with_type(
                &job.name(),
                fragment_id,
                table.id,
                table_type_name,
            );
            table.fragment_id = fragment_id;

            // Record the internal table.
            internal_tables.push(table.clone());
        });

        internal_tables
    }

    /// Fill the information of the job in the fragment.
    fn fill_job(fragment: &mut StreamFragment, job: &StreamingJob) -> bool {
        let table_id = job.id();
        let fragment_id = fragment.fragment_id;
        let mut has_table = false;

        visit_fragment(fragment, |node_body| match node_body {
            NodeBody::Materialize(materialize_node) => {
                materialize_node.table_id = table_id;

                // Fill the ID of the `Table`.
                let table = materialize_node.table.as_mut().unwrap();
                table.id = table_id;
                table.database_id = job.database_id();
                table.schema_id = job.schema_id();
                table.fragment_id = fragment_id;

                has_table = true;
            }
            NodeBody::Sink(sink_node) => {
                sink_node.table_id = table_id;

                has_table = true;
            }
            NodeBody::Dml(dml_node) => {
                dml_node.table_id = table_id;
            }
            _ => {}
        });

        has_table
    }
}

impl Deref for BuildingFragment {
    type Target = StreamFragment;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// In-memory representation of a **Fragment** Graph, built from the [`StreamFragmentGraphProto`]
/// from the frontend.
#[derive(Default)]
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<GlobalFragmentId, BuildingFragment>,

    /// stores edges between fragments: upstream => downstream.
    downstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// stores edges between fragments: downstream -> upstream.
    upstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// Dependent relations of this job.
    dependent_relations: HashSet<TableId>,
}

impl StreamFragmentGraph {
    /// Create a new [`StreamFragmentGraph`] from the given [`StreamFragmentGraphProto`], with all
    /// global IDs correctly filled.
    pub async fn new<S: MetaStore>(
        proto: StreamFragmentGraphProto,
        id_gen: IdGeneratorManagerRef<S>,
        job: &StreamingJob,
    ) -> MetaResult<Self> {
        let fragment_id_gen =
            GlobalFragmentIdGen::new(&id_gen, proto.fragments.len() as u64).await?;
        let table_id_gen = GlobalTableIdGen::new(&id_gen, proto.table_ids_cnt as u64).await?;

        // Create nodes.
        let fragments: HashMap<_, _> = proto
            .fragments
            .into_iter()
            .map(|(id, fragment)| {
                let id = fragment_id_gen.to_global_id(id);
                let fragment = BuildingFragment::new(id, fragment, job, table_id_gen);
                (id, fragment)
            })
            .collect();

        assert_eq!(
            fragments
                .values()
                .map(|f| f.internal_tables.len() as u32)
                .sum::<u32>(),
            proto.table_ids_cnt
        );

        // Create edges.
        let mut downstreams = HashMap::new();
        let mut upstreams = HashMap::new();

        for edge in proto.edges {
            let upstream_id = fragment_id_gen.to_global_id(edge.upstream_id);
            let downstream_id = fragment_id_gen.to_global_id(edge.downstream_id);

            upstreams
                .entry(downstream_id)
                .or_insert_with(HashMap::new)
                .try_insert(
                    upstream_id,
                    StreamFragmentEdge {
                        upstream_id: upstream_id.as_global_id(),
                        downstream_id: downstream_id.as_global_id(),
                        ..edge.clone()
                    },
                )
                .unwrap();
            downstreams
                .entry(upstream_id)
                .or_insert_with(HashMap::new)
                .try_insert(
                    downstream_id,
                    StreamFragmentEdge {
                        upstream_id: upstream_id.as_global_id(),
                        downstream_id: downstream_id.as_global_id(),
                        ..edge
                    },
                )
                .unwrap();
        }

        // Note: Here we directly use the field `dependent_table_ids` in the proto (resolved in
        // frontend), instead of visiting the graph ourselves. Note that for creating table with a
        // connector, the source itself is NOT INCLUDED in this list.
        let dependent_relations = proto
            .dependent_table_ids
            .iter()
            .map(TableId::from)
            .collect();

        Ok(Self {
            fragments,
            downstreams,
            upstreams,
            dependent_relations,
        })
    }

    /// Retrieve the internal tables map of the whole graph.
    pub fn internal_tables(&self) -> HashMap<u32, Table> {
        let mut tables = HashMap::new();
        for fragment in self.fragments.values() {
            for table in &fragment.internal_tables {
                tables
                    .try_insert(table.id, table.clone())
                    .unwrap_or_else(|_| panic!("duplicated table id `{}`", table.id));
            }
        }
        tables
    }

    /// Returns the fragment id where the table is materialized.
    pub fn table_fragment_id(&self) -> FragmentId {
        self.fragments
            .values()
            .filter(|b| b.table_id.is_some())
            .map(|b| b.fragment_id)
            .exactly_one()
            .expect("require exactly 1 materialize/sink node when creating the streaming job")
    }

    pub fn dependent_relations(&self) -> &HashSet<TableId> {
        &self.dependent_relations
    }

    /// Generate topological order of the fragments in this graph. Returns error if the graph is not
    /// a DAG and topological sort can not be done.
    ///
    /// The first fragment popped out from the heap will be the top-most node, or the
    /// `Sink`/`Materialize` in stream graph.
    pub(super) fn topo_order(&self) -> MetaResult<Vec<GlobalFragmentId>> {
        let mut topo = Vec::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all fragments
        for fragment_id in self.fragments.keys() {
            // Count how many downstreams we have for a given fragment
            let downstream_cnt = self.get_downstreams(*fragment_id).len();
            if downstream_cnt == 0 {
                topo.push(*fragment_id);
            } else {
                downstream_cnts.insert(*fragment_id, downstream_cnt);
            }
        }

        let mut i = 0;
        while let Some(&fragment_id) = topo.get(i) {
            i += 1;
            // Find if we can process more fragments
            for upstream_id in self.get_upstreams(fragment_id).keys() {
                let downstream_cnt = downstream_cnts.get_mut(upstream_id).unwrap();
                *downstream_cnt -= 1;
                if *downstream_cnt == 0 {
                    downstream_cnts.remove(upstream_id);
                    topo.push(*upstream_id);
                }
            }
        }

        if !downstream_cnts.is_empty() {
            // There are fragments that are not processed yet.
            bail!("graph is not a DAG");
        }

        Ok(topo)
    }

    /// Seal a [`StreamFragment`] from the graph into a [`Fragment`], which will be further used to
    /// build actors, schedule, and persist into meta store.
    pub(super) fn seal_fragment(&self, id: GlobalFragmentId, actors: Vec<StreamActor>) -> Fragment {
        let BuildingFragment {
            inner,
            internal_tables,
            table_id,
        } = self.fragments.get(&id).unwrap().to_owned();

        let distribution_type = if inner.is_singleton {
            FragmentDistributionType::Single
        } else {
            FragmentDistributionType::Hash
        } as i32;

        let state_table_ids = internal_tables
            .iter()
            .map(|t| t.id)
            .chain(table_id)
            .collect();

        let upstream_fragment_ids = self
            .get_upstreams(id)
            .keys()
            .map(|id| id.as_global_id())
            .collect();

        Fragment {
            fragment_id: inner.fragment_id,
            fragment_type_mask: inner.fragment_type_mask,
            distribution_type,
            actors,
            // Will be filled in `Scheduler::schedule` later.
            vnode_mapping: None,
            state_table_ids,
            upstream_fragment_ids,
        }
    }

    pub(super) fn fragments(&self) -> &HashMap<GlobalFragmentId, BuildingFragment> {
        &self.fragments
    }

    pub(super) fn get_fragment(&self, fragment_id: GlobalFragmentId) -> Option<&StreamFragment> {
        self.fragments.get(&fragment_id).map(|b| b.deref())
    }

    pub(super) fn get_downstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        self.downstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    pub(super) fn get_upstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        self.upstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    pub(super) fn edges(
        &self,
    ) -> impl Iterator<Item = (GlobalFragmentId, GlobalFragmentId, &StreamFragmentEdge)> {
        self.downstreams
            .iter()
            .flat_map(|(&from, tos)| tos.iter().map(move |(&to, edge)| (from, to, edge)))
    }
}

static EMPTY_HASHMAP: LazyLock<HashMap<GlobalFragmentId, StreamFragmentEdge>> =
    LazyLock::new(HashMap::new);

/// A wrapper of [`StreamFragmentGraph`] that contains the information of existing fragments, which
/// is connected to the graph's top-most or bottom-most fragments.
///
/// For example, if we're going to build a mview on an existing mview, the upstream fragment
/// containing the `Materialize` node will be included in this structure.
pub struct CompleteStreamFragmentGraph {
    /// The fragment graph of this streaming job.
    graph: StreamFragmentGraph,

    /// The required information of existing fragments.
    existing_fragments: HashMap<GlobalFragmentId, Fragment>,

    /// Extra edges between existing fragments and the building fragments.
    downstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, DispatcherType>>,

    /// Extra edges between existing fragments and the building fragments.
    #[expect(dead_code)]
    upstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, DispatcherType>>,
}

impl CompleteStreamFragmentGraph {
    /// Create a new [`CompleteStreamFragmentGraph`] with empty existing fragments, i.e., there's no
    /// upstream mviews.
    #[cfg(test)]
    pub fn for_test(graph: StreamFragmentGraph) -> Self {
        Self {
            graph,
            existing_fragments: Default::default(),
            downstreams: Default::default(),
            upstreams: Default::default(),
        }
    }

    /// Create a new [`CompleteStreamFragmentGraph`] for MV-on-MV. Returns an error if the upstream
    /// `Materialize` is failed to resolve.
    pub fn new(
        graph: StreamFragmentGraph,
        upstream_mview_fragments: HashMap<TableId, Fragment>,
    ) -> MetaResult<Self> {
        let mut downstreams = HashMap::new();
        let mut upstreams = HashMap::new();

        for (&id, fragment) in &graph.fragments {
            for &upstream_table_id in &fragment.upstream_table_ids {
                let mview_fragment = upstream_mview_fragments
                    .get(&TableId::new(upstream_table_id))
                    .context("upstream materialized view fragment not found")?;
                let mview_id = GlobalFragmentId::new(mview_fragment.fragment_id);

                // We always use `NoShuffle` for the exchange between the upstream `Materialize` and
                // the downstream `Chain` of the new materialized view.
                let dt = DispatcherType::NoShuffle;

                downstreams
                    .entry(mview_id)
                    .or_insert_with(HashMap::new)
                    .try_insert(id, dt)
                    .unwrap();
                upstreams
                    .entry(id)
                    .or_insert_with(HashMap::new)
                    .try_insert(mview_id, dt)
                    .unwrap();
            }
        }

        let existing_fragments = upstream_mview_fragments
            .into_values()
            .map(|f| (GlobalFragmentId::new(f.fragment_id), f))
            .collect();

        Ok(Self {
            graph,
            existing_fragments,
            downstreams,
            upstreams,
        })
    }

    /// Consumes this complete graph and returns the inner graph.
    // TODO: remove this after scheduler refactoring.
    pub fn into_inner(self) -> StreamFragmentGraph {
        self.graph
    }

    /// Returns the dispatcher types of all edges in the complete graph.
    pub(super) fn dispatch_edges(
        &self,
    ) -> impl Iterator<Item = (GlobalFragmentId, GlobalFragmentId, DispatcherType)> + '_ {
        self.graph
            .edges()
            .map(|(from, to, edge)| {
                let dt = edge.get_dispatch_strategy().unwrap().get_type().unwrap();
                (from, to, dt)
            })
            .chain(
                self.downstreams
                    .iter()
                    .flat_map(|(&from, tos)| tos.iter().map(move |(&to, &dt)| (from, to, dt))),
            )
    }

    /// Returns the distribution of the existing fragments.
    pub(super) fn existing_distribution(&self) -> HashMap<GlobalFragmentId, Distribution> {
        self.existing_fragments
            .iter()
            .map(|(&id, f)| {
                let dist = match f.get_distribution_type().unwrap() {
                    FragmentDistributionType::Unspecified => unreachable!(),
                    FragmentDistributionType::Single => Distribution::Singleton,
                    FragmentDistributionType::Hash => {
                        let mapping =
                            ParallelUnitMapping::from_protobuf(f.get_vnode_mapping().unwrap());
                        Distribution::Hash(mapping)
                    }
                };
                (id, dist)
            })
            .collect()
    }

    /// Returns the building fragments.
    pub(super) fn building_fragments(&self) -> &HashMap<GlobalFragmentId, BuildingFragment> {
        self.graph.fragments()
    }
}
