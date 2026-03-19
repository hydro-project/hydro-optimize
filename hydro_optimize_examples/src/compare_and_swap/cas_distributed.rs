use hydro_lang::{
    __manual_proof__ as manual_proof,
    live_collections::{
        boundedness::Boundedness,
        sliced::sliced,
        stream::{AtLeastOnce, ExactlyOnce, NoOrder, Ordering, TotalOrder},
    },
    location::{Location, MemberId, cluster::CLUSTER_SELF_ID},
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Optional, Stream, TCP, Tick, Unbounded},
};
use hydro_std::{membership::track_membership, quorum::collect_quorum_with_response};
use serde::{Deserialize, Serialize};
use stageleft::q;
use std::hash::Hash;

use crate::compare_and_swap::cas_like::CASOutput;

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
struct ReadRequest<RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct WriteRequest<State, RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
    state: CASState<State>,
}

impl<State: Eq, RequestId: Eq, Sender: Eq> PartialOrd for WriteRequest<State, RequestId, Sender> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<State: Eq, RequestId: Eq, Sender: Eq> Ord for WriteRequest<State, RequestId, Sender> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ballot
            .cmp(&other.ballot)
            .then_with(|| self.state.version.cmp(&other.state.version))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Response<State, RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
    max_ballot: Ballot,
    state: Option<CASState<State>>,
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

    /// Returns the state associated with the respondent with the highest ballot, and whether all agree
    /// Returns only 1 response per request ID, on the tick the quorum is first reached.
    #[expect(clippy::type_complexity, reason = "Stream type")]
    fn check_all_agree<State, RequestId, Sender>(
        &self,
        responses: KeyedStream<
            RequestId,
            Response<State, RequestId, Sender>,
            Cluster<'a, DistributedCASNode>,
            Unbounded,
            NoOrder,
        >,
    ) -> KeyedStream<
        RequestId,
        (Response<State, RequestId, Sender>, bool),
        Cluster<'a, DistributedCASNode>,
        Unbounded,
        NoOrder,
    >
    where
        State: Clone + Eq,
        RequestId: Clone + Eq + Hash,
        Sender: Clone,
    {
        let f = self.f;
        sliced! {
            let responses = use(responses.entries(), nondet!(/** The order in which inputs are collected at the quorum affects what is outputted */));
            let mut reached_quorum = use::state_null::<Stream<RequestId, Tick<_>, Bounded, NoOrder>>();
            let mut prev_responses = use::state_null::<Stream<(RequestId, Response<State, RequestId, Sender>), Tick<_>, Bounded, NoOrder>>();

            let current_responses = prev_responses.clone().chain(responses).into_keyed();
            let count_per_key = current_responses.clone().value_counts();

            let (no_quorum, quorum) = count_per_key
                .entries()
                .partition(q!(move |(_request_id, count)| *count < f));
            let (just_quorum, all_quorum) = quorum
                .partition(q!(move |(_request_id, count)| *count < 2 * f));

            let just_reached_quorum = current_responses
                .clone()
                .filter_key_not_in(no_quorum.into_keyed().keys())
                .filter_key_not_in(reached_quorum);
            let result = just_reached_quorum
                .map(q!(|response| (response, true)))
                .reduce(q!(|(prev_response, all_agree), (response, _)| {
                    if prev_response.state != response.state {
                        *all_agree = false;
                    }
                    if prev_response.ballot < response.ballot {
                        *prev_response = response;
                    }
                }, commutative = manual_proof!(/** Max is order independent. No two writes will use the same ballot. */)));    
            // TODO: Increment ballot on each write
            
            reached_quorum = just_quorum.into_keyed().keys();
            prev_responses = current_responses.filter_key_not_in(all_quorum.into_keyed().keys()).entries();

            result.entries()
        }.into_keyed()
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
        let client_writes = self.send_to_cas(writes.entries());
        let client_reads = self.send_to_cas(reads);
        let client_subscribes = self.send_to_cas(subscribe);

        // These are ballots that we don't need to immediately take into account (is ok to delay)
        let (incoming_preemption_ballots_complete, incoming_preemption_ballots) =
            self.cluster.forward_ref::<Stream<
                Ballot,
                Cluster<'a, DistributedCASNode>,
                Unbounded,
                NoOrder,
                AtLeastOnce,
            >>();
        let max_preemption_ballot =
            incoming_preemption_ballots
                .max()
                .map(q!(move |ballot| Ballot {
                    num: ballot.num + 1,
                    node: CLUSTER_SELF_ID.clone()
                }));

        let (incoming_read_requests_complete, incoming_read_requests) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<DistributedCASNode>,
                ReadRequest<RequestId, Sender>,
                Cluster<'a, DistributedCASNode>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        let (incoming_write_requests_complete, incoming_write_requests) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<DistributedCASNode>,
                WriteRequest<State, RequestId, Sender>,
                Cluster<'a, DistributedCASNode>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // -------------------------------------------------------------
        // Election and state
        // -------------------------------------------------------------
        let nondet_ballot = nondet!(/** The ballot used depends on the time of message arrival */);
        let (reads_with_ballot, election_read_responses) = sliced! {
            let client_reads = use(client_reads.entries(), nondet_ballot);
            let incoming_read_requests = use(incoming_read_requests.entries(), nondet_ballot);
            let incoming_write_requests = use(incoming_write_requests.entries(), nondet_ballot);
            let max_preemption_ballot = use(max_preemption_ballot, nondet_ballot);
            let mut state = use::state_null::<Optional<CASState<State>, Tick<_>, Bounded>>();

            // let max_leader_ballot = incoming_election_reads.clone().map(q!(|(_sender, request)| request.ballot))
            //     .chain(incoming_election_writes
            let ballot = max_preemption_ballot
                .into_singleton()
                .map(q!(move |max_ballot| max_ballot.unwrap_or_else(|| Ballot {
                    num: 0,
                    node: CLUSTER_SELF_ID.clone(),
            })));

            let winning_write = incoming_write_requests
                .sort()
                .last();

            let next_state = winning_write
                .zip(ballot.clone())
                .filter_map(q!(|((_, write_req), ballot)| (write_req.ballot >= ballot).then_some(write_req.state)))
                .or(state);

            let reads_with_ballot = client_reads
                .cross_singleton(ballot.clone());
            let election_read_responses = incoming_read_requests
                .cross_singleton(ballot)
                .cross_singleton(next_state.clone().into_singleton());

            state = next_state;

            (reads_with_ballot, election_read_responses)
        };

        let sent_read_requests = self.broadcast_to_everyone_else(reads_with_ballot.map(q!(
            |((client_id, request_id), ballot)| {
                ReadRequest {
                    request_id,
                    client_id,
                    ballot,
                }
            }
        )));
        incoming_read_requests_complete.complete(sent_read_requests);

        let read_responses = election_read_responses
            .map(q!(|(((sender, request), max_ballot), state)| {
                (
                    sender,
                    Response {
                        request_id: request.request_id,
                        client_id: request.client_id,
                        ballot: request.ballot,
                        max_ballot,
                        state,
                    },
                )
            }))
            .demux(self.cluster, TCP.fail_stop().bincode())
            .values()
            .map(q!(|response| (response.request_id.clone(), response)))
            .into_keyed();

        // See if all reads agree
        let read_responses_agree = self.check_all_agree(read_responses).entries();
        let (read_success, read_needs_commit) = read_responses_agree.partition(q!(|(_request_id, (_response, all_agree))| *all_agree));

        let read_result = read_success
            .map(q!(|(request_id, (response, _all_agree))| (response.client_id, (request_id, response.state))))
            .demux(sender, TCP.fail_stop().bincode())
            .values()
            .into_keyed();

        CASOutput {
            read_result,
            write_processed: todo!(),
            subscribe_updates: todo!(),
        }
    }
}
