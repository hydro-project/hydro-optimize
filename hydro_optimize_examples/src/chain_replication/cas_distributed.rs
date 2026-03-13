use hydro_lang::{
    live_collections::{
        boundedness::Boundedness,
        sliced::sliced,
        stream::{NoOrder, Ordering},
    },
    location::{MemberId, cluster::CLUSTER_SELF_ID},
    nondet::nondet,
    prelude::{Cluster, KeyedStream, Stream, TCP, Unbounded},
};
use serde::{Deserialize, Serialize};
use stageleft::q;
use std::hash::Hash;

use crate::chain_replication::cas_like::CASOutput;

use super::cas_like::{CASLike, CASState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DistributedCASNode {}

pub struct DistributedCAS<'a, 'b> {
    pub cluster: &'b Cluster<'a, DistributedCASNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ballot {
    num: u64,
    node: MemberId<DistributedCASNode>,
}

/// Distributed CAS implementation with single round Paxos.
///
/// Write path:
/// 1. Election (in sequence).
/// 2. If not elected, retry.
/// 3. If not all agree, replicate the max read value (one-by-one), retrying on failure with a potentially new max read value, until successful.
/// 4. Check if the write value is valid against the max value. If not, abort.
/// 5. Replicate the new write value.
/// 6. On success, ACK. Otherwise, retry from step 1.
///
/// Read path:
/// 1. Election (in parallel).
/// 2. If all replicas agree, return the read value. Election failure is fine.
/// 3. Otherwise, replicate the max read value (one-by-one), retrying on failure with a potentially new max read value, until successful. Add to queue of requests waiting for a committed value (but don't send a new write for each read, otherwise we risk preempting ourselves).
/// 4. Return the max read value.
impl<'a, 'b, State, RequestId, Sender> CASLike<'a, State, RequestId, Sender>
    for DistributedCAS<'a, 'b>
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    RequestId: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
{
    fn build(
        self,
        writes: KeyedStream<
            RequestId,
            CASState<State>,
            Cluster<'a, Sender>,
            impl Boundedness,
            impl Ordering,
        >,
        reads: Stream<RequestId, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        subscribe: Stream<MemberId<Sender>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        sender: &Cluster<'a, Sender>,
    ) -> CASOutput<'a, State, RequestId, Sender> {
        // let nondet_ballot = nondet!(/** The ballot used depends on the time of message arrival */);
        // sliced! {
        //     let writes = use(writes.entries(), nondet_ballot);
        //     let mut ballot = use::state(|l| l.singleton(q!(Ballot { num: 0, node: CLUSTER_SELF_ID })));
        // };

        todo!()
    }
}
