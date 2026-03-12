use std::hash::Hash;
use hydro_lang::{
    live_collections::{boundedness::Boundedness, stream::Ordering},
    location::MemberId,
    prelude::{Cluster, KeyedStream, Stream},
};
use serde::{Deserialize, Serialize};

use crate::chain_replication::cas_like::CASOutput;

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
impl<'a, 'b, State, RequestId, Sender> CASLike<'a, State, RequestId, Sender> for DistributedCAS<'a, 'b>
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    RequestId: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
{
    fn build(
        self,
        writes: KeyedStream<RequestId, CASState<State>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        reads: Stream<RequestId, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        subscribe: Stream<MemberId<Sender>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        sender: &Cluster<'a, Sender>,
    ) -> CASOutput<'a, State, RequestId, Sender> {
        todo!()
    }
}
