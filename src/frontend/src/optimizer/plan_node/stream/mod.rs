macro_rules! impl_node {
  ($base:ident, $($t:ident),*) => {
      #[derive(Debug, Clone)]
      pub enum Node {
          $($t(Box<$t>),)*
      }
      $(
      impl From<$t> for Node {
          fn from(o: $t) -> Node {
              Node::$t(Box::new(o))
          }
      }
      )*
      pub type PlanOwned = ($base, Node);
      pub type PlanRef = std::rc::Rc<PlanOwned>;
  };
}

/// Implements [`super::LogicalJoin`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone)]
pub struct DeltaJoin {
    pub logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,
}

#[derive(Clone, Debug)]
pub struct DynamicFilter {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    pub predicate: Condition,
    // dist_key_l: Distribution,
    pub left_index: usize,
    pub left: PlanRef,
    pub right: PlanRef,
}

#[derive(Debug, Clone)]
pub struct Exchange {
    pub input: PlanRef,
}

#[derive(Debug, Clone)]
pub struct Expand {
    pub logical: LogicalExpand,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub logical: LogicalFilter,
}

#[derive(Debug, Clone)]
pub struct GlobalSimpleAgg {
    pub logical: LogicalAgg,
}

#[derive(Debug, Clone)]
pub struct GroupTopN {
    pub logical: LogicalTopN,
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct HashAgg {
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
    pub logical: LogicalAgg,
}

/// Implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct HashJoin {
    pub logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    pub is_append_only: bool,
}

#[derive(Debug, Clone)]
pub struct HopWindow {
    logical: LogicalHopWindow,
}

impl_node!(
    PlanBase,
    Exchange,
    DynamicFilter,
    DeltaJoin,
    Expand,
    Filter,
    GlobalSimpleAgg,
    GroupTopN,
    HashAgg,
    HashJoin,
    HopWindow,
);
