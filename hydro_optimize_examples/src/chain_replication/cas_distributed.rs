use hydro_lang::{
    live_collections::{
        boundedness::Boundedness,
        sliced::sliced,
        stream::{AtLeastOnce, ExactlyOnce, NoOrder, Ordering, TotalOrder},
    },
    location::{Location, MemberId, cluster::CLUSTER_SELF_ID},
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Optional, Stream, TCP, Tick, Unbounded},
};
use hydro_std::membership::track_membership;
use serde::{Deserialize, Serialize};
use stageleft::q;
use std::hash::Hash;

use crate::chain_replication::cas_like::CASOutput;

use super::cas_like::{CASLike, CASState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DistributedCASNode {}

/// - `benchmark_mode`: If true, only sends operations to the leader (ID = 0).
pub struct DistributedCAS<'a, 'b> {
    pub cluster: &'b Cluster<'a, DistributedCASNode>,
    pub f: usize,
    pub benchmark_mode: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Ballot {
    num: u64,
    node: MemberId<DistributedCASNode>,
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.num
            .cmp(&other.num)
            .then_with(|| self.node.get_raw_id().cmp(&other.node.get_raw_id()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ElectionReadReq<RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ElectionReadResp<State, RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
    state: CASState<State>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct WriteReq<State, RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
    state: CASState<State>,
}

impl<State: Eq, RequestId: Eq, Sender: Eq> PartialOrd for WriteReq<State, RequestId, Sender> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<State: Eq, RequestId: Eq, Sender: Eq> Ord for WriteReq<State, RequestId, Sender> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ballot
            .cmp(&other.ballot)
            .then_with(|| self.state.version.cmp(&other.state.version))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WriteResp<State, RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
    state: CASState<State>,
}

impl<'a, 'b> DistributedCAS<'a, 'b> {
    /// Route incoming messages to the leader (ID = 0) or random, depending on benchmark mode.
    fn send_to_cas<Payload, Sender>(
        &self,
        stream: Stream<Payload, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
    ) -> KeyedStream<
        MemberId<Sender>,
        Payload,
        Cluster<'a, DistributedCASNode>,
        impl Boundedness,
        impl Ordering,
    >
    where
        Payload: Serialize + for<'de> Deserialize<'de> + 'a,
        Sender: 'a,
    {
        let f = self.f as u32;
        if self.benchmark_mode {
            stream
                .map(q!(|payload| (
                    MemberId::<DistributedCASNode>::from_raw_id(0),
                    payload
                )))
                .demux(self.cluster, TCP.fail_stop().bincode())
        } else {
            stream
                .map(q!(move |payload| (
                    MemberId::<DistributedCASNode>::from_raw_id(rand::random::<u32>() % f as u32),
                    payload
                )))
                .demux(self.cluster, TCP.fail_stop().bincode())
        }
    }

    /// Don't send to self
    fn broadcast_to_everyone_else<Payload>(
        &self,
        stream: Stream<Payload, Cluster<'a, DistributedCASNode>, impl Boundedness, impl Ordering>,
    ) -> KeyedStream<
        MemberId<DistributedCASNode>,
        Payload,
        Cluster<'a, DistributedCASNode>,
        Unbounded,
        NoOrder,
    >
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
    {
        let members = self.cluster.source_cluster_members(self.cluster);
        let curr_replicas = track_membership(members);

        let nondet_membership = nondet!(/** Membership is static */);
        let stream_with_dest = sliced! {
            let stream = use(stream, nondet_membership);
            let curr_replicas = use(curr_replicas, nondet_membership);

            curr_replicas
                .into_keyed_stream()
                .entries()
                .filter_map(q!(move |(member_id, present)| (present && member_id != CLUSTER_SELF_ID).then_some(member_id)))
                .cross_product(stream)
        };
        stream_with_dest.demux(self.cluster, TCP.fail_stop().bincode())
    }
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
/// 1. Election (in parallel, no ballot).
/// 2. If all replicas agree, return the read value.
/// 3. Otherwise, replicate the max read value on the write path, retrying on failure with a potentially new max read value, until successful. Add to queue of requests waiting for a committed value (but don't send a new write for each read, otherwise we risk preempting ourselves).
/// 4. Return the max read value.
impl<'a, 'b, State, RequestId, Sender> CASLike<'a, State, RequestId, Sender>
    for DistributedCAS<'a, 'b>
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    RequestId: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
    Sender: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
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
        // -------------------------------------------------------------
        // Route incoming messages.
        // Note that if subscribes are routed to a non-leader, it will not receive all writes.
        // -------------------------------------------------------------
        let incoming_writes = self.send_to_cas(writes.entries());
        let incoming_reads = self.send_to_cas(reads);
        let incoming_subscribes = self.send_to_cas(subscribe);

        let (incoming_ballots_complete, incoming_ballots) = self.cluster.forward_ref::<Stream<
            Ballot,
            Cluster<'a, DistributedCASNode>,
            Unbounded,
            NoOrder,
            AtLeastOnce,
        >>();
        let max_ballot = incoming_ballots.max();

        let (incoming_election_reads_complete, incoming_election_reads) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<DistributedCASNode>,
                ElectionReadReq<RequestId, Sender>,
                Cluster<'a, DistributedCASNode>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        let (incoming_election_writes_complete, incoming_election_writes) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<DistributedCASNode>,
                WriteReq<State, RequestId, Sender>,
                Cluster<'a, DistributedCASNode>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        let (incoming_leader_writes_complete, incoming_leader_writes) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<Sender>,
                WriteReq<State, RequestId, Sender>,
                Cluster<'a, DistributedCASNode>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // -------------------------------------------------------------
        // Election and state
        // -------------------------------------------------------------
        let nondet_ballot = nondet!(/** The ballot used depends on the time of message arrival */);
        let (reads_for_election,) = sliced! {
            let incoming_reads = use(incoming_reads.entries(), nondet_ballot);
            let incoming_leader_writes = use(incoming_leader_writes.entries(), nondet_ballot);
            let max_ballot = use(max_ballot, nondet_ballot);
            let mut state = use::state_null::<Optional<CASState<State>, Tick<_>, Bounded>>();

            let ballot = max_ballot
                .into_singleton()
                .map(q!(move |max_ballot| max_ballot.unwrap_or_else(|| Ballot {
                    num: 0,
                    node: CLUSTER_SELF_ID.clone(),
            })));

            let winning_write = incoming_leader_writes
                .sort()
                .last();

            let next_state = winning_write
                .zip(ballot.clone())
                .filter_map(q!(|((_, write_req), ballot)| (write_req.ballot >= ballot).then_some(write_req.state)))
                .or(state);

            let reads_for_election = incoming_reads
                .cross_singleton(ballot);

            state = next_state;

            (reads_for_election, )
        };

        let sent_reads = self.broadcast_to_everyone_else(reads_for_election.map(q!(|(
            (client_id, request_id),
            ballot,
        )| {
            ElectionReadReq {
                request_id,
                client_id,
                ballot,
            }
        })));
        incoming_election_reads_complete.complete(sent_reads);

        todo!()
    }
}
