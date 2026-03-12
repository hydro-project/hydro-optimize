use hydro_lang::{
    live_collections::stream::NoOrder,
    location::MemberId,
    prelude::{Bounded, Cluster, Singleton, Stream, Unbounded},
};
use serde::{Deserialize, Serialize};

use super::cas_like::{CASLike, CASState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DistributedCASNode {}

pub struct DistributedCAS<'a, 'b> {
    pub cluster: &'b Cluster<'a, DistributedCASNode>,
}

/// Distributed CAS implementation with single round Paxos.
///
/// Write path:
///
/// Read path:
/// 1. Broadcast
/// 2. Wait for quorum. Pick the value with the highest ballot.
impl<'a, 'b, S, Sender> CASLike<'a, S, Sender> for DistributedCAS<'a, 'b>
where
    S: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
{
    fn build(
        self,
        writes: Stream<CASState<S>, Cluster<'a, Sender>, Unbounded, NoOrder>,
        subscribe: Singleton<MemberId<Sender>, Cluster<'a, Sender>, Bounded>,
        sender: &Cluster<'a, Sender>,
    ) -> Stream<CASState<S>, Cluster<'a, Sender>, Unbounded, NoOrder> {
        todo!()
    }
}
