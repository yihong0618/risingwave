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

use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_pb::common::ParallelUnit;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragmentEdge;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, Dispatcher, DispatcherType, MergeNode, StreamActor, StreamNode,
};

use crate::error::MetaResult;
use crate::manager::IdGeneratorManagerRef;
use crate::model::FragmentId;
use crate::storage::meta_store::MetaStore;
use crate::stream::stream_graph::fragment::{CompleteStreamFragmentGraph, StreamFragmentGraph};
use crate::stream::stream_graph::id::{GlobalActorId, GlobalActorIdGen, GlobalFragmentId};
use crate::stream::stream_graph::schedule;
use crate::stream::stream_graph::schedule::Distribution;
use crate::stream::stream_manager::CreateStreamingJobContext;

/// A list of actors with order.
#[derive(Debug, Clone)]
struct OrderedActorLink(pub Vec<GlobalActorId>);

impl OrderedActorLink {
    pub fn as_global_ids(&self) -> Vec<u32> {
        self.0.iter().map(|x| x.as_global_id()).collect()
    }
}

struct StreamActorDownstream {
    dispatch_strategy: DispatchStrategy,
    dispatcher_id: u64,

    /// Downstream actors.
    actors: OrderedActorLink,

    /// Whether to place the downstream actors on the same node
    // TODO: remove this field after scheduler refactoring
    #[expect(dead_code)]
    same_worker_node: bool,
}

struct StreamActorUpstream {
    /// Upstream actors
    actors: OrderedActorLink,
    /// associate fragment id
    fragment_id: GlobalFragmentId,
    /// Whether to place the upstream actors on the same node
    same_worker_node: bool,
}

/// [`StreamActorBuilder`] builds a stream actor in a stream DAG.
struct StreamActorBuilder {
    /// actor id field
    actor_id: GlobalActorId,

    /// associated fragment id
    fragment_id: GlobalFragmentId,

    /// associated stream node
    nodes: Arc<StreamNode>,

    /// downstream dispatchers (dispatcher, downstream actor, hash mapping)
    downstreams: Vec<StreamActorDownstream>,

    /// upstreams, exchange node operator_id -> upstream actor ids
    upstreams: HashMap<u64, StreamActorUpstream>,

    /// Whether to place this actors on the same node as chain's upstream MVs.
    chain_same_worker_node: bool,
}

impl StreamActorBuilder {
    fn is_chain_same_worker_node(stream_node: &StreamNode) -> bool {
        fn visit(stream_node: &StreamNode) -> bool {
            if let Some(NodeBody::Chain(ref chain)) = stream_node.node_body {
                return chain.same_worker_node;
            }
            stream_node.input.iter().any(visit)
        }
        visit(stream_node)
    }

    pub fn new(
        actor_id: GlobalActorId,
        fragment_id: GlobalFragmentId,
        node: Arc<StreamNode>,
    ) -> Self {
        Self {
            actor_id,
            fragment_id,
            chain_same_worker_node: Self::is_chain_same_worker_node(&node),
            nodes: node,
            downstreams: vec![],
            upstreams: HashMap::new(),
        }
    }

    pub fn fragment_id(&self) -> GlobalFragmentId {
        self.fragment_id
    }

    /// Add a dispatcher to this actor.
    pub fn add_dispatcher(
        &mut self,
        dispatch_strategy: DispatchStrategy,
        dispatcher_id: u64,
        downstream_actors: OrderedActorLink,
        same_worker_node: bool,
    ) {
        self.downstreams.push(StreamActorDownstream {
            dispatch_strategy,
            dispatcher_id,
            actors: downstream_actors,
            same_worker_node,
        });
    }

    /// Build an actor after seal.
    pub fn build(&self) -> StreamActor {
        let dispatcher = self
            .downstreams
            .iter()
            .map(
                |StreamActorDownstream {
                     dispatch_strategy,
                     dispatcher_id,
                     actors,
                     same_worker_node: _,
                 }| Dispatcher {
                    downstream_actor_id: actors.as_global_ids(),
                    r#type: dispatch_strategy.r#type,
                    column_indices: dispatch_strategy.column_indices.clone(),
                    // will be filled later by stream manager
                    hash_mapping: None,
                    dispatcher_id: *dispatcher_id,
                },
            )
            .collect_vec();

        StreamActor {
            actor_id: self.actor_id.as_global_id(),
            fragment_id: self.fragment_id.as_global_id(),
            nodes: Some(self.nodes.deref().clone()),
            dispatcher,
            upstream_actor_id: self
                .upstreams
                .iter()
                .flat_map(|(_, StreamActorUpstream { actors, .. })| actors.0.iter().copied())
                .map(|x| x.as_global_id())
                .collect(), // TODO: store each upstream separately
            same_worker_node_as_upstream: self.chain_same_worker_node
                || self.upstreams.values().any(|u| u.same_worker_node),
            vnode_bitmap: None,
            // To be filled by `StreamGraphBuilder::build`
            mview_definition: "".to_owned(),
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It injects some information to achieve
/// dependencies. See `build_inner` for more details.
#[derive(Default)]
struct StreamGraphBuilder {
    actor_builders: BTreeMap<GlobalActorId, StreamActorBuilder>,
}

impl StreamGraphBuilder {
    /// Insert new generated actor.
    pub fn add_actor(
        &mut self,
        actor_id: GlobalActorId,
        fragment_id: GlobalFragmentId,
        node: Arc<StreamNode>,
    ) {
        self.actor_builders.insert(
            actor_id,
            StreamActorBuilder::new(actor_id, fragment_id, node),
        );
    }

    /// Add dependency between two connected node in the graph.
    pub fn add_link(
        &mut self,
        upstream_fragment_id: GlobalFragmentId,
        upstream_actor_ids: &[GlobalActorId],
        downstream_actor_ids: &[GlobalActorId],
        edge: StreamFragmentEdge,
    ) {
        let exchange_operator_id = edge.link_id;
        let same_worker_node = edge.same_worker_node;
        let dispatch_strategy = edge.dispatch_strategy.unwrap();
        // We can't use the exchange operator id directly as the dispatch id, because an exchange
        // could belong to more than one downstream in DAG.
        // We can use downstream fragment id as an unique id for dispatcher.
        // In this way we can ensure the dispatchers of `StreamActor` would have different id,
        // even though they come from the same exchange operator.
        let dispatch_id = edge.downstream_id as u64;

        if dispatch_strategy.get_type().unwrap() == DispatcherType::NoShuffle {
            assert_eq!(
                upstream_actor_ids.len(),
                downstream_actor_ids.len(),
                "mismatched length when processing no-shuffle exchange: {:?} -> {:?} on exchange {}",
                upstream_actor_ids,
                downstream_actor_ids,
                exchange_operator_id
            );

            // update 1v1 relationship
            upstream_actor_ids
                .iter()
                .zip_eq(downstream_actor_ids.iter())
                .for_each(|(upstream_id, downstream_id)| {
                    self.actor_builders
                        .get_mut(upstream_id)
                        .unwrap()
                        .add_dispatcher(
                            dispatch_strategy.clone(),
                            dispatch_id,
                            OrderedActorLink(vec![*downstream_id]),
                            same_worker_node,
                        );

                    self.actor_builders
                        .get_mut(downstream_id)
                        .unwrap()
                        .upstreams
                        .try_insert(
                            exchange_operator_id,
                            StreamActorUpstream {
                                actors: OrderedActorLink(vec![*upstream_id]),
                                fragment_id: upstream_fragment_id,
                                same_worker_node,
                            },
                        )
                        .unwrap_or_else(|_| {
                            panic!(
                                "duplicated exchange input {} for no-shuffle actors {:?} -> {:?}",
                                exchange_operator_id, upstream_id, downstream_id
                            )
                        });
                });

            return;
        }

        // otherwise, make m * n links between actors.

        assert!(
            !same_worker_node,
            "same_worker_node only applies to 1v1 dispatchers."
        );

        // update actors to have dispatchers, link upstream -> downstream.
        upstream_actor_ids.iter().for_each(|upstream_id| {
            self.actor_builders
                .get_mut(upstream_id)
                .unwrap()
                .add_dispatcher(
                    dispatch_strategy.clone(),
                    dispatch_id,
                    OrderedActorLink(downstream_actor_ids.to_vec()),
                    same_worker_node,
                );
        });

        // update actors to have upstreams, link downstream <- upstream.
        downstream_actor_ids.iter().for_each(|downstream_id| {
            self.actor_builders
                .get_mut(downstream_id)
                .unwrap()
                .upstreams
                .try_insert(
                    exchange_operator_id,
                    StreamActorUpstream {
                        actors: OrderedActorLink(upstream_actor_ids.to_vec()),
                        fragment_id: upstream_fragment_id,
                        same_worker_node,
                    },
                )
                .unwrap_or_else(|_| {
                    panic!(
                        "duplicated exchange input {} for actors {:?} -> {:?}",
                        exchange_operator_id, upstream_actor_ids, downstream_actor_ids
                    )
                });
        });
    }

    /// Build final stream DAG with dependencies with current actor builders.
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
        ctx: &CreateStreamingJobContext,
    ) -> MetaResult<HashMap<GlobalFragmentId, Vec<StreamActor>>> {
        let mut graph: HashMap<GlobalFragmentId, Vec<StreamActor>> = HashMap::new();

        for builder in self.actor_builders.values() {
            let mut actor = builder.build();
            let upstream_actors = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { actors, .. })| (*id, actors.clone()))
                .collect();
            let upstream_fragments = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { fragment_id, .. })| (*id, *fragment_id))
                .collect();
            let stream_node =
                self.build_inner(actor.get_nodes()?, &upstream_actors, &upstream_fragments)?;

            actor.nodes = Some(stream_node);
            actor.mview_definition = ctx.streaming_definition.clone();

            graph.entry(builder.fragment_id()).or_default().push(actor);
        }
        Ok(graph)
    }

    /// Build stream actor inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is `ExchangeNode`, and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's `ExchangeNode`.
    /// 3. replace node's `ExchangeNode` input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a `ChainNode`.
    fn build_inner(
        &self,
        stream_node: &StreamNode,
        upstream_actors: &HashMap<u64, OrderedActorLink>,
        upstream_fragments: &HashMap<u64, GlobalFragmentId>,
    ) -> MetaResult<StreamNode> {
        match stream_node.get_node_body()? {
            NodeBody::Exchange(_) => {
                panic!("ExchangeNode should be eliminated from the top of the plan node when converting fragments to actors: {:#?}", stream_node)
            }
            NodeBody::Chain(_) => Ok(self.resolve_chain_node(stream_node)?),
            _ => {
                let mut new_stream_node = stream_node.clone();

                for (input, new_input) in stream_node
                    .input
                    .iter()
                    .zip_eq(new_stream_node.input.iter_mut())
                {
                    *new_input = match input.get_node_body()? {
                        NodeBody::Exchange(e) => {
                            assert!(!input.get_fields().is_empty());
                            StreamNode {
                                input: vec![],
                                stream_key: input.stream_key.clone(),
                                node_body: Some(NodeBody::Merge(MergeNode {
                                    upstream_actor_id: upstream_actors
                                        .get(&input.get_operator_id())
                                        .expect("failed to find upstream actor id for given exchange node").as_global_ids(),
                                    upstream_fragment_id: upstream_fragments.get(&input.get_operator_id()).unwrap().as_global_id(),
                                    upstream_dispatcher_type: e.get_strategy()?.r#type,
                                    fields: input.get_fields().clone(),
                                })),
                                fields: input.get_fields().clone(),
                                operator_id: input.operator_id,
                                identity: "MergeExecutor".to_string(),
                                append_only: input.append_only,
                            }
                        }
                        _ => self.build_inner(input, upstream_actors, upstream_fragments)?,
                    }
                }
                Ok(new_stream_node)
            }
        }
    }

    /// Resolve the chain node, only rewrite the schema of input `MergeNode`.
    fn resolve_chain_node(&self, stream_node: &StreamNode) -> MetaResult<StreamNode> {
        let NodeBody::Chain(chain_node) = stream_node.get_node_body().unwrap() else {
            unreachable!()
        };
        let input = stream_node.get_input();
        assert_eq!(input.len(), 2);

        let merge_node = &input[0];
        assert_matches!(merge_node.node_body, Some(NodeBody::Merge(_)));
        let batch_plan_node = &input[1];
        assert_matches!(batch_plan_node.node_body, Some(NodeBody::BatchPlan(_)));

        let chain_input = vec![
            StreamNode {
                input: vec![],
                stream_key: merge_node.stream_key.clone(),
                node_body: Some(NodeBody::Merge(MergeNode {
                    upstream_actor_id: vec![],
                    upstream_fragment_id: 0,
                    upstream_dispatcher_type: DispatcherType::NoShuffle as _,
                    fields: chain_node.upstream_fields.clone(),
                })),
                fields: chain_node.upstream_fields.clone(),
                operator_id: merge_node.operator_id,
                identity: "MergeExecutor".to_string(),
                append_only: stream_node.append_only,
            },
            batch_plan_node.clone(),
        ];

        Ok(StreamNode {
            input: chain_input,
            stream_key: stream_node.stream_key.clone(),
            node_body: Some(NodeBody::Chain(chain_node.clone())),
            operator_id: stream_node.operator_id,
            identity: "ChainExecutor".to_string(),
            fields: chain_node.upstream_fields.clone(),
            append_only: stream_node.append_only,
        })
    }
}

/// The mutable state when building actor graph.
struct BuildActorGraphState {
    /// The stream graph builder, to build streaming DAG.
    stream_graph_builder: StreamGraphBuilder,

    /// When converting fragment graph to actor graph, we need to know which actors belong to a
    /// fragment.
    fragment_actors: HashMap<GlobalFragmentId, Vec<GlobalActorId>>,

    /// The next local actor id to use.
    next_local_id: u32,

    /// The global actor id generator.
    actor_id_gen: GlobalActorIdGen,
}

impl BuildActorGraphState {
    /// Create an empty state with the given id generator.
    fn new(actor_id_gen: GlobalActorIdGen) -> Self {
        Self {
            stream_graph_builder: Default::default(),
            fragment_actors: Default::default(),
            next_local_id: 0,
            actor_id_gen,
        }
    }

    /// Get the next global actor id.
    fn next_actor_id(&mut self) -> GlobalActorId {
        let local_id = self.next_local_id;
        self.next_local_id += 1;

        self.actor_id_gen.to_global_id(local_id)
    }

    /// Finish the build and return the inner stream graph builder.
    fn finish(self) -> StreamGraphBuilder {
        assert_eq!(self.actor_id_gen.len(), self.next_local_id);
        self.stream_graph_builder
    }
}

/// [`ActorGraphBuilder`] generates the proto for interconnected actors for a streaming pipeline.
pub struct ActorGraphBuilder {
    /// The pre-scheduled distribution for each fragment.
    // TODO: this is fake now and only the parallelism is used, we should also use the physical
    // distribution itself after scheduler refactoring.
    distributions: HashMap<GlobalFragmentId, Distribution>,

    /// The stream fragment graph.
    fragment_graph: StreamFragmentGraph,
}

impl ActorGraphBuilder {
    /// Create a new actor graph builder with the given "complete" graph.
    pub fn new(complete_graph: CompleteStreamFragmentGraph, default_parallelism: u32) -> Self {
        // TODO: use the real parallel units to generate real distribution.
        let fake_parallel_units = (0..default_parallelism).map(|id| ParallelUnit {
            id,
            worker_node_id: 0,
        });
        let distributions =
            schedule::Scheduler::new(fake_parallel_units, default_parallelism as usize)
                .unwrap()
                .schedule(&complete_graph)
                .unwrap();

        // TODO: directly use the complete graph when building so that we can generalize the
        // processing logic for `Chain`s.
        let fragment_graph = complete_graph.into_inner();

        Self {
            distributions,
            fragment_graph,
        }
    }

    /// Build a stream graph by duplicating each fragment as parallel actors.
    pub async fn generate_graph<S>(
        &self,
        id_gen_manager: IdGeneratorManagerRef<S>,
        ctx: &mut CreateStreamingJobContext,
    ) -> MetaResult<BTreeMap<FragmentId, Fragment>>
    where
        S: MetaStore,
    {
        let actor_len = self
            .distributions
            .values()
            .map(|d| d.parallelism())
            .sum::<usize>() as u64;
        let id_gen = GlobalActorIdGen::new(&id_gen_manager, actor_len).await?;

        // Generate actors of the streaming plan
        let stream_graph = self.build_actor_graph(id_gen, ctx)?.finish().build(&*ctx)?;

        // Serialize the graph
        let stream_graph = stream_graph
            .into_iter()
            .map(|(fragment_id, actors)| {
                let fragment = self.fragment_graph.seal_fragment(fragment_id, actors);
                let fragment_id = fragment_id.as_global_id();
                (fragment_id, fragment)
            })
            .collect();

        Ok(stream_graph)
    }

    /// Build actor graph from fragment graph using topological sort. Setup dispatcher in actor and
    /// generate actors by their parallelism.
    fn build_actor_graph(
        &self,
        id_gen: GlobalActorIdGen,
        ctx: &mut CreateStreamingJobContext,
    ) -> MetaResult<BuildActorGraphState> {
        let mut state = BuildActorGraphState::new(id_gen);

        // Use topological sort to build the graph from downstream to upstream. (The first fragment
        // popped out from the heap will be the top-most node in plan, or the sink in stream graph.)
        for fragment_id in self.fragment_graph.topo_order()? {
            // Build the actors corresponding to the fragment
            self.build_actor_graph_fragment(fragment_id, &mut state, ctx)?;
        }

        Ok(state)
    }

    fn build_actor_graph_fragment(
        &self,
        fragment_id: GlobalFragmentId,
        state: &mut BuildActorGraphState,
        ctx: &mut CreateStreamingJobContext,
    ) -> MetaResult<()> {
        let current_fragment = self
            .fragment_graph
            .get_fragment(fragment_id)
            .unwrap()
            .clone();

        let upstream_table_id = current_fragment
            .upstream_table_ids
            .iter()
            .at_most_one()
            .unwrap()
            .map(TableId::from);
        if let Some(upstream_table_id) = upstream_table_id {
            ctx.chain_fragment_upstream_table_map
                .insert(fragment_id.as_global_id(), upstream_table_id);
        }

        let distribution = &self.distributions[&fragment_id];
        let parallel_degree = distribution.parallelism() as u32;

        let node = Arc::new(current_fragment.node.unwrap());

        let actor_ids = (0..parallel_degree)
            .map(|_| state.next_actor_id())
            .collect_vec();

        for &actor_id in &actor_ids {
            state
                .stream_graph_builder
                .add_actor(actor_id, fragment_id, node.clone());
        }

        for (downstream_fragment_id, dispatch_edge) in
            self.fragment_graph.get_downstreams(fragment_id)
        {
            let downstream_actors = state
                .fragment_actors
                .get(downstream_fragment_id)
                .expect("downstream fragment not processed yet");

            state.stream_graph_builder.add_link(
                fragment_id,
                &actor_ids,
                downstream_actors,
                dispatch_edge.clone(),
            );
        }

        state
            .fragment_actors
            .try_insert(fragment_id, actor_ids)
            .unwrap_or_else(|_| panic!("fragment {:?} is already processed", fragment_id));

        Ok(())
    }
}
