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
use hydro_std::membership::track_membership;
use serde::{Deserialize, Serialize};
use stageleft::q;
use std::hash::Hash;

use crate::compare_and_swap::cas_like::CASOutput;

use super::cas_like::{CASLike, CASState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Replica {}

/// - `benchmark_mode`: If true, only sends operations to the leader (ID = 0).
pub struct DistributedCAS<'a, 'b> {
    pub cluster: &'b Cluster<'a, Replica>,
    pub f: usize,
    pub benchmark_mode: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Ballot {
    num: u64,
    node: MemberId<Replica>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Request<State, RequestId, Sender> {
    request_id: RequestId,
    client_id: MemberId<Sender>,
    ballot: Ballot,
    state: Option<CASState<State>>,
}

impl<State: Eq, RequestId: Eq, Sender: Eq> PartialOrd for Request<State, RequestId, Sender> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<State: Eq, RequestId: Eq, Sender: Eq> Ord for Request<State, RequestId, Sender> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ballot
            .cmp(&other.ballot)
            .then_with(|| match (&self.state, &other.state) {
                (Some(s), Some(o)) => s.version.cmp(&o.version),
                _ => std::cmp::Ordering::Equal, // Uncomparable
            })
    }
}

/// - `max_ballot`: The max ballot seen by the respondent, which the leader can use to determine if it has been preempted.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Response<State, RequestId, Sender> {
    request: Request<State, RequestId, Sender>,
    max_ballot: Ballot,
    state: Option<CASState<State>>,
}

/// A type that we can call `generate` on to get a new unique ID.
/// We will need to generate unique IDs to commit uncommitted writes (and count the number of votes in the response).
trait Uuid {
    fn generate() -> Self;
}

impl<'a, 'b> DistributedCAS<'a, 'b> {
    /// Route incoming messages to the leader (ID = 0) or random, depending on benchmark mode.
    fn send_to_cas<Payload, Sender>(
        &self,
        stream: Stream<Payload, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
    ) -> KeyedStream<MemberId<Sender>, Payload, Cluster<'a, Replica>, Unbounded, impl Ordering>
    where
        Payload: Serialize + for<'de> Deserialize<'de> + 'a,
        Sender: 'a,
    {
        let f = self.f as u32;
        if self.benchmark_mode {
            stream
                .map(q!(|payload| (MemberId::<Replica>::from_raw_id(0), payload)))
                .demux(self.cluster, TCP.fail_stop().bincode())
        } else {
            stream
                .map(q!(move |payload| (
                    MemberId::<Replica>::from_raw_id(rand::random::<u32>() % f as u32),
                    payload
                )))
                .demux(self.cluster, TCP.fail_stop().bincode())
        }
    }

    /// Send the message over the network to all other replicas, but just pass the message onwards for ourselves.
    fn smart_broadcast<Payload>(
        &self,
        stream: Stream<Payload, Cluster<'a, Replica>, Unbounded, impl Ordering>,
    ) -> KeyedStream<MemberId<Replica>, Payload, Cluster<'a, Replica>, Unbounded, NoOrder>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
    {
        let members = self.cluster.source_cluster_members(self.cluster);
        let curr_replicas = track_membership(members);

        let nondet_membership = nondet!(/** Membership is static */);
        let stream_with_dest = sliced! {
            let stream = use(stream.clone(), nondet_membership);
            let curr_replicas = use(curr_replicas, nondet_membership);

            curr_replicas
                .into_keyed_stream()
                .entries()
                .filter_map(q!(move |(member_id, present)| (present && member_id != CLUSTER_SELF_ID).then_some(member_id)))
                .cross_product(stream)
        };
        let sent_stream = stream_with_dest.demux(self.cluster, TCP.fail_stop().bincode());

        // Add a stream to ourselves but not over the network
        let stream_to_self = stream
            .map(q!(move |payload| (CLUSTER_SELF_ID.clone(), payload)))
            .into_keyed();
        sent_stream.interleave(stream_to_self)
    }

    /// Similar to `smart_broadcast`, but for sending to a single destination.
    fn smart_demux<Payload>(
        &self,
        stream: Stream<
            (MemberId<Replica>, Payload),
            Cluster<'a, Replica>,
            Unbounded,
            impl Ordering,
        >,
    ) -> KeyedStream<MemberId<Replica>, Payload, Cluster<'a, Replica>, Unbounded, NoOrder>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
    {
        let (should_pass, should_demux) =
            stream.partition(q!(
                move |(member_id, _payload)| *member_id == CLUSTER_SELF_ID
            ));
        let demuxed = should_demux.demux(self.cluster, TCP.fail_stop().bincode());
        should_pass.into_keyed().interleave(demuxed)
    }

    /// Returns the state associated with the respondent with the highest ballot, and whether all agree
    /// Returns only 1 response per request ID, on the tick the quorum is first reached.
    #[expect(clippy::type_complexity, reason = "Stream type")]
    fn check_all_agree<State, RequestId, Sender>(
        &self,
        responses: KeyedStream<
            RequestId,
            Response<State, RequestId, Sender>,
            Cluster<'a, Replica>,
            Unbounded,
            NoOrder,
        >,
    ) -> KeyedStream<
        RequestId,
        (Response<State, RequestId, Sender>, bool),
        Cluster<'a, Replica>,
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
                    if prev_response.max_ballot < response.max_ballot {
                        *prev_response = response;
                    }
                }, commutative = manual_proof!(/** Max is order independent. No two writes will use the same ballot. */)));
            // TODO: Remember to increment ballot on each write

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
/// 1. Election (in parallel).
/// 2. If all replicas agree, return the read value.
/// 3. Otherwise, replicate the max read value on the write path, retrying on failure with a potentially new max read value, until successful. Add to queue of requests waiting for a committed value (but don't send a new write for each read, otherwise we risk preempting ourselves).
/// 4. Return the max read value.
///
/// # Assumptions:
/// 1. No 2 operations share the same `RequestId`.
/// 2. Each client subscribes at most once. (Doesn't break correctness but will duplicate messages).
impl<'a, 'b, State, RequestId, Sender> CASLike<'a, State, RequestId, Sender>
    for DistributedCAS<'a, 'b>
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    RequestId: Uuid + Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
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
        let (incoming_preemption_ballots_complete, incoming_preemption_ballots) = self
            .cluster
            .forward_ref::<Stream<
            Ballot,
            Cluster<'a, Replica>,
            Unbounded,
            NoOrder,
            AtLeastOnce,
        >>();
        let max_preemption_ballot = incoming_preemption_ballots.max();

        let (incoming_read_requests_complete, incoming_read_requests) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<Replica>,
                Request<State, RequestId, Sender>,
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        let (incoming_write_requests_complete, incoming_write_requests) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<Replica>,
                Request<State, RequestId, Sender>,
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // Quorum responses to read requests.
        let (incoming_read_uncommitted_responses_complete, incoming_read_uncommitted_responses) =
            self.cluster.forward_ref::<Stream<
                (RequestId, Response<State, RequestId, Sender>),
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // Quorum responses to write requests where not all respondents agree.
        let (incoming_write_uncommitted_responses_complete, incoming_write_uncommitted_responses) =
            self.cluster.forward_ref::<Stream<
                (RequestId, Response<State, RequestId, Sender>),
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // Quorum responses to write requests where all respondents agree, but our write failed.
        let (incoming_write_preempted_responses_complete, incoming_write_preempted_responses) =
            self.cluster.forward_ref::<Stream<
                RequestId,
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // Quorum responses to write requests where all respondents agree and our write succeeded.
        let (incoming_write_success_responses_complete, incoming_write_success_responses) =
            self.cluster.forward_ref::<Stream<
                RequestId,
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // -------------------------------------------------------------
        // Election and state
        //
        // # Invariants
        // Note that write refers to both reconciling uncommitted writes and client writes.
        // 1. At any point, on any replica, there is at most one outgoing write that has not reached a quorum.
        // 2. No client writes will be broadcast unless a committed state was observed (all in the read quorum agree on the same state).
        // 3. No writes reuse the same ballot.
        // -------------------------------------------------------------
        let nondet_ballot = nondet!(/** The ballot used depends on the time of message arrival */);
        let (read_requests, read_responses, write_requests, write_responses, unblocked_reads,) = sliced! {
            let client_reads = use(client_reads.entries(), nondet_ballot);
            let client_writes = use(client_writes.entries(), nondet_ballot);
            let incoming_read_requests = use(incoming_read_requests.entries(), nondet_ballot);
            let incoming_write_requests = use(incoming_write_requests.entries(), nondet_ballot);
            let max_preemption_ballot = use(max_preemption_ballot, nondet_ballot);
            // TODO: We might be able to decide which write to send & the ballot to attach to it in different slices.
            let incoming_read_uncommitted_responses = use(incoming_read_uncommitted_responses, nondet_ballot);
            let incoming_write_uncommitted_responses = use(incoming_write_uncommitted_responses, nondet_ballot);
            let incoming_write_preempted_responses = use(incoming_write_preempted_responses, nondet_ballot);
            let incoming_write_success_responses = use(incoming_write_success_responses, nondet_ballot);
            let mut state = use::state_null::<Optional<CASState<State>, Tick<_>, Bounded>>();
            // The current outgoing write to reconcile uncommitted state, if any.
            // If we find new uncommitted state, we take the max based on ballot.
            let mut current_uncommitted_state = use::state_null::<Optional<(RequestId, Ballot, CASState<State>), Tick<_>, Bounded>>();
            // The current client write we wish to commit, if any. This write may be outgoing (if there is no uncommitted state) or blocking on that state to become committed.
            let mut current_client_write = use::state_null::<Optional<(RequestId, MemberId<Sender>, CASState<State>), Tick<_>, Bounded>>();
            // Writes waiting to be replicated in order. A write can only proceed if there is no outgoing write
            let mut write_queue = use::state_null::<Stream<(RequestId, MemberId<Sender>, CASState<State>), Tick<_>, Bounded, TotalOrder>>();
            // Reads that are waiting for the latest write to complete, before returning with the value of that write.
            let mut reads_blocked_on_outgoing_write = use::state_null::<Stream<(RequestId, MemberId<Sender>), Tick<_>, Bounded, NoOrder>>();
            // Reads that were invoked after the latest write began and in order to preserve causality, cannot return until a new write starts and completes.
            let mut reads_blocked_on_next_write = use::state_null::<Stream<(RequestId, MemberId<Sender>), Tick<_>, Bounded, NoOrder>>();

            // let max_leader_ballot = incoming_election_reads.clone().map(q!(|(_sender, request)| request.ballot))
            //     .chain(incoming_election_writes
            // TODO: Increment if we are about to send a new write
            let ballot = max_preemption_ballot
                .into_singleton()
                .map(q!(move |max_ballot| max_ballot.unwrap_or_else(|| Ballot {
                    num: 0,
                    node: CLUSTER_SELF_ID.clone(),
            })));

            // Queue blocked reads based on whether a write was successful.
            let successful_write_id = incoming_write_success_responses
                .clone()
                .assume_ordering::<TotalOrder>(nondet!(/** There is at most one outgoing write, thus at most one success signal at a time */))
                .first();
            let successful_reconciled_write = successful_write_id
                .clone()
                .zip(current_uncommitted_state.clone())
                .filter_map(q!(|(request_id, (uncommitted_request_id, _ballot, uncommitted_state))| (request_id == uncommitted_request_id).then_some(uncommitted_state)));
            let successful_client_write = successful_write_id
                .zip(current_client_write.clone())
                .filter_map(q!(|(request_id, (client_request_id, _client, client_write))| (request_id == client_request_id).then_some(client_write)));
            let successful_write = successful_reconciled_write.or(successful_client_write);
            let (unblocked_reads, reads_still_blocked_on_outgoing) = reads_blocked_on_outgoing_write
                .clone()
                .cross_singleton(successful_write.clone().into_singleton())
                .partition(q!(|((_request_id, _client), written)| written.is_some()));
            let (reads_now_blocked_on_outgoing, reads_still_blocked_on_next) = reads_blocked_on_next_write
                .clone()
                .cross_singleton(successful_write.into_singleton())
                .partition(q!(|((_request_id, _client), written)| written.is_some()));
            let new_reads_blocked_on_outgoing = reads_still_blocked_on_outgoing
                .chain(reads_now_blocked_on_outgoing)
                .map(q!(|((request_id, client), _)| (request_id, client)));
            let new_reads_blocked_on_next = reads_still_blocked_on_next
                .map(q!(|((request_id, client), _)| (request_id, client)))
                .chain(incoming_read_uncommitted_responses.clone().map(q!(|(request_id, response)| (request_id, response.request.client_id))));
            // TODO: Incoming reads should block on outgoing if there is no current outgoing write

            // Determine what the next uncommitted state is
            // If there is a current_uncommitted_state, then it is inflight and we shouldn't overwrite it (its responses will tell itself it needs to be updated).
            let uncommitted_state_responses = incoming_read_uncommitted_responses
                .chain(incoming_write_uncommitted_responses)
                .filter_map(q!(|(_request_id, response)|
                    response.state.map(|state| (response.max_ballot, state))))
                .max()
                .map(q!(|(ballot, uncommitted_state)| (RequestId::generate(), ballot, uncommitted_state)));
            let next_uncommitted_state = current_uncommitted_state.or(uncommitted_state_responses);


            // Determine which write to process next

            // Determine local state based on incoming write requests
            let winning_write = incoming_write_requests
                .clone()
                .sort()
                .last();
            let next_state = winning_write
                .zip(ballot.clone())
                .filter_map(q!(|((_, write_req), ballot)| (write_req.ballot >= ballot).then_some(write_req.state.unwrap())))
                .or(state);

            // Attach ballots to requests and responses
            let read_requests = client_reads
                .cross_singleton(ballot.clone());
            // TODO: Queue requests, do one at a time, and increment our ballot. Don't attempt to write if there is a reconciling write
            let write_requests = client_writes
                .cross_singleton(ballot.clone());
            let read_responses = incoming_read_requests
                .cross_singleton(ballot.clone())
                .cross_singleton(next_state.clone().into_singleton());
            let write_responses = incoming_write_requests
                .cross_singleton(ballot)
                .cross_singleton(next_state.clone().into_singleton());

            state = next_state;
            reads_blocked_on_outgoing_write = new_reads_blocked_on_outgoing;
            reads_blocked_on_next_write = new_reads_blocked_on_next;
            current_uncommitted_state = next_uncommitted_state;

            (read_requests, read_responses, write_requests, write_responses, unblocked_reads,)
        };

        // -------------------------------------------------------------
        // Broadcast read requests.
        // -------------------------------------------------------------
        let sent_read_requests =
            self.smart_broadcast(read_requests.map(q!(|((client_id, request_id), ballot)| {
                Request {
                    request_id,
                    client_id,
                    ballot,
                    state: None,
                }
            })));
        incoming_read_requests_complete.complete(sent_read_requests);

        // -------------------------------------------------------------
        // Process read responses.
        // -------------------------------------------------------------
        let sent_read_responses = self
            .smart_demux(
                read_responses.map(q!(|(((sender, request), max_ballot), state)| {
                    (
                        sender,
                        Response {
                            request,
                            max_ballot,
                            state,
                        },
                    )
                })),
            )
            .values()
            .map(q!(|response| (
                response.request.request_id.clone(),
                response
            )))
            .into_keyed();
        // See if all reads agree
        let read_responses_agree = self.check_all_agree(sent_read_responses).entries();
        let (read_success, read_uncommitted) = read_responses_agree
            .clone()
            .partition(q!(|(_request_id, (_response, all_agree))| *all_agree));
        incoming_read_uncommitted_responses_complete.complete(read_uncommitted.map(q!(
            |(request_id, (response, _all_agree))| (request_id, response)
        )));
        let read_result = read_success
            .map(q!(|(request_id, (response, _all_agree))| (
                response.request.client_id,
                (request_id, response.state)
            )))
            .interleave(unblocked_reads.map(q!(|((request_id, client_id), state)| (client_id, (request_id, state)))))
            .demux(sender, TCP.fail_stop().bincode())
            .values()
            .into_keyed();

        // -------------------------------------------------------------
        // Broadcast write requests.
        // -------------------------------------------------------------
        let sent_write_requests =
            self.smart_broadcast(write_requests.map(q!(|(
                (sender, (request_id, write)),
                ballot,
            )| Request {
                request_id,
                client_id: sender,
                ballot,
                state: Some(write),
            })));
        incoming_write_requests_complete.complete(sent_write_requests);

        // -------------------------------------------------------------
        // Process write responses.
        // -------------------------------------------------------------
        let sent_write_responses = self
            .smart_demux(
                write_responses.map(q!(|(((sender, request), max_ballot), state)| {
                    (
                        sender,
                        Response {
                            request,
                            max_ballot,
                            state,
                        },
                    )
                })),
            )
            .values()
            .map(q!(|response| (
                response.request.request_id.clone(),
                response
            )))
            .into_keyed();
        // See if our writes were successful
        let write_responses_agree = self.check_all_agree(sent_write_responses).entries();
        let (write_all_agree, write_disagree) =
            write_responses_agree.clone().partition(q!(|(_request_id, (_response, all_agree))| *all_agree));
        let (write_success, write_preempted) =
            write_all_agree.partition(q!(|(_request_id, (response, _all_agree))| response
                .request
                .ballot
                == response.max_ballot));
        incoming_write_uncommitted_responses_complete.complete(write_disagree.map(q!(|(
            request_id,
            (response, _all_agree),
        )| (
            request_id, response
        ))));
        incoming_write_preempted_responses_complete.complete(write_preempted.map(q!(|(
            _request_id,
            (response, _all_agree),
        )| {
            response.request.request_id
        })));
        incoming_write_success_responses_complete.complete(write_success.clone().map(q!(
            |(_request_id, (response, _all_agree))| response.request.request_id
        )));
        let write_processed = write_success
            .clone()
            .map(q!(|(request_id, (response, _all_agree))| (
                response.request.client_id,
                request_id
            )))
            .demux(sender, TCP.fail_stop().bincode())
            .values();

        // -------------------------------------------------------------
        // Broadcast successful writes to current subscribers.
        // -------------------------------------------------------------
        let nondet_subscribe =
            nondet!(/** The "current" set of subscribers depends on message arrival order */);
        let writes_to_subscribers = sliced! {
            let new_subscribers = use(client_subscribes.values(), nondet_subscribe);
            let write_success = use(write_success, nondet_subscribe);
            let mut prev_subscribers = use::state_null::<Stream<MemberId<Sender>, Tick<_>, Bounded, NoOrder>>();

            let current_subscribers = prev_subscribers.chain(new_subscribers);
            let writes_to_subscribers = current_subscribers
                .clone()
                .cross_product(write_success)
                .filter_map(q!(|(subscriber, (_request_id, (response, _all_agree)))| response.state.map(|state| (subscriber, state))));

            prev_subscribers = current_subscribers;
            writes_to_subscribers
        };
        let subscribe_updates = writes_to_subscribers
            .demux(sender, TCP.fail_stop().bincode())
            .values();

        // -------------------------------------------------------------
        // Gather all ballots received as a response.
        // -------------------------------------------------------------
        let received_ballots = read_responses_agree
            .map(q!(
                |(_request_id, (response, _all_agree))| response.max_ballot
            ))
            .interleave(
                write_responses_agree.map(q!(
                    |(_request_id, (response, _all_agree))| response.max_ballot
                )),
            );
        incoming_preemption_ballots_complete.complete(received_ballots);

        CASOutput {
            read_result,
            write_processed,
            subscribe_updates,
        }
    }
}
