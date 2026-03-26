use hydro_lang::{
    __manual_proof__ as manual_proof,
    live_collections::{
        boundedness::Boundedness,
        sliced::sliced,
        stream::{AtLeastOnce, ExactlyOnce, NoOrder, Ordering, TotalOrder},
    },
    location::{Location, MemberId, cluster::CLUSTER_SELF_ID},
    nondet::nondet,
    prelude::{Bounded, Cluster, KeyedStream, Optional, Singleton, Stream, TCP, Tick, Unbounded},
};
use serde::{Deserialize, Serialize};
use stageleft::q;
use std::{fmt::Debug, hash::Hash, time::Duration};

use super::cas_like::{CASLike, CASOutput, CASState, UniqueRequestId};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Replica {}

/// - `benchmark_mode`: If true, only sends operations to the leader (ID = 0).
pub struct DistributedCAS<'a, 'b> {
    pub cluster: &'b Cluster<'a, Replica>,
    pub f: usize,
    pub benchmark_mode: bool,
    pub retry_timeout: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Ballot {
    pub num: u64,
    pub node: MemberId<Replica>,
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

/// No `client_id`: Election request
/// No `ballot`: Read request
/// No `state`: Read and election request
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request<State, Sender> {
    pub id: UniqueRequestId,
    pub client_id: Option<MemberId<Sender>>,
    pub ballot: Option<Ballot>,
    pub state: Option<CASState<State>>,
}

impl<State: Eq, Sender: Eq> PartialOrd for Request<State, Sender> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<State: Eq, Sender: Eq> Ord for Request<State, Sender> {
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
pub struct Response<State, Sender> {
    pub request: Request<State, Sender>,
    pub max_ballot: Ballot,
    pub state: Option<CASState<State>>,
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
        stream: Stream<Payload, Cluster<'a, Replica>, Unbounded, NoOrder>,
    ) -> KeyedStream<MemberId<Replica>, Payload, Cluster<'a, Replica>, Unbounded, NoOrder>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
    {
        let nondet_membership = nondet!(/** Membership is static */);
        stream.broadcast(self.cluster, TCP.fail_stop().bincode(), nondet_membership)
        // TODO: Revert once I figure out how to send to self without creating a negative cycle
        // let members = self.cluster.source_cluster_members(self.cluster);
        // let curr_replicas = track_membership(members);
        //
        // let stream_with_dest = sliced! {
        //     let stream = use(stream.clone(), nondet_membership);
        //     let curr_replicas = use(curr_replicas, nondet_membership);
        //
        //     curr_replicas
        //         .into_keyed_stream()
        //         .entries()
        //         .filter_map(q!(move |(member_id, present)| (present && member_id != CLUSTER_SELF_ID).then_some(member_id)))
        //         .cross_product(stream)
        // };
        // let sent_stream = stream_with_dest.demux(self.cluster, TCP.fail_stop().bincode());
        //
        // // Add a stream to ourselves but not over the network
        // let stream_to_self = stream
        //     .map(q!(move |payload| (CLUSTER_SELF_ID.clone(), payload)))
        //     .into_keyed();
        // sent_stream.merge_unordered(stream_to_self)
    }

    /// Similar to `smart_broadcast`, but for sending to a single destination.
    fn smart_demux<Payload>(
        &self,
        stream: Stream<(MemberId<Replica>, Payload), Cluster<'a, Replica>, Unbounded, NoOrder>,
    ) -> KeyedStream<MemberId<Replica>, Payload, Cluster<'a, Replica>, Unbounded, NoOrder>
    where
        Payload: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
    {
        stream.demux(self.cluster, TCP.fail_stop().bincode())
        // TODO: Revert once I figure out how to send to self without creating a negative cycle
        // let (should_pass, should_demux) =
        //     stream.partition(q!(
        //         move |(member_id, _payload)| *member_id == CLUSTER_SELF_ID
        //     ));
        // let demuxed = should_demux.demux(self.cluster, TCP.fail_stop().bincode());
        // should_pass.into_keyed().merge_unordered(demuxed)
    }

    /// Returns the state associated with the respondent with the highest ballot, and whether all agree
    /// Returns only 1 response per request ID, on the tick the quorum is first reached.
    #[expect(clippy::type_complexity, reason = "Stream type")]
    fn check_all_agree<State, Sender>(
        &self,
        responses: KeyedStream<
            UniqueRequestId,
            Response<State, Sender>,
            Cluster<'a, Replica>,
            Unbounded,
            NoOrder,
        >,
    ) -> Stream<(Response<State, Sender>, bool), Cluster<'a, Replica>, Unbounded, NoOrder>
    where
        State: Clone + Debug + Eq,
        Sender: Clone + Debug,
    {
        let f = self.f;
        sliced! {
            let responses = use(responses.entries(), nondet!(/** The order in which inputs are collected at the quorum affects what is outputted */));
            let mut reached_quorum = use::state_null::<Stream<UniqueRequestId, Tick<_>, Bounded, NoOrder>>();
            let mut prev_responses = use::state_null::<Stream<(UniqueRequestId, Response<State, Sender>), Tick<_>, Bounded, NoOrder>>();

            let current_responses = prev_responses.clone().chain(responses).into_keyed();
            let count_per_key = current_responses.clone().value_counts();

            let (no_quorum, quorum) = count_per_key
                .entries()
                .partition(q!(move |(_request_id, count)| *count < f + 1));
            let (just_quorum, all_quorum) = quorum
                .partition(q!(move |(_request_id, count)| *count < 2 * f + 1));

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

            reached_quorum = just_quorum.into_keyed().keys();
            prev_responses = current_responses.filter_key_not_in(all_quorum.into_keyed().keys()).entries();

            result.values()
        }
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
/// 1. No 2 operations share the same `UniqueRequestId`.
/// 2. Each client subscribes at most once. (Doesn't break correctness but will duplicate messages).
impl<'a, 'b, State, Sender> CASLike<'a, State, Sender>
    for DistributedCAS<'a, 'b>
where
    State: Clone + Debug + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    Sender: Clone + Debug + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
{
    fn build(
        self,
        writes: KeyedStream<
            UniqueRequestId,
            CASState<State>,
            Cluster<'a, Sender>,
            impl Boundedness,
            impl Ordering,
        >,
        reads: Stream<UniqueRequestId, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        subscribe: Stream<MemberId<Sender>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        sender: &Cluster<'a, Sender>,
    ) -> CASOutput<'a, State, Sender> {
        // -------------------------------------------------------------
        // Route incoming messages.
        // Note that if subscribes are routed to a non-leader, it will not receive all writes.
        // -------------------------------------------------------------
        let client_writes = self.send_to_cas(writes.entries());
        let client_reads = self.send_to_cas(reads);
        let client_subscribes = self.send_to_cas(subscribe);

        // These are ballots that we don't need to immediately take into account (is ok to delay)
        let (incoming_response_ballots_complete, incoming_response_ballots) = self
            .cluster
            .forward_ref::<Stream<Ballot, Cluster<'a, Replica>, Unbounded, NoOrder, AtLeastOnce>>();
        let max_response_ballot = incoming_response_ballots.max();

        let (incoming_requests_complete, incoming_requests) =
            self.cluster.forward_ref::<KeyedStream<
                MemberId<Replica>,
                Request<State, Sender>,
                Cluster<'a, Replica>,
                Unbounded,
                NoOrder,
                ExactlyOnce,
            >>();

        // Quorum responses. bool = whether all agree on the highest ballot value
        let (incoming_responses_complete, incoming_responses) = self.cluster.forward_ref::<Stream<
            (Response<State, Sender>, bool),
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
        let retry_timeout = self.retry_timeout;
        let election_timeout = self.cluster.source_interval_delayed(
            q!(Duration::from_secs(CLUSTER_SELF_ID.get_raw_id() as u64)),
            q!(Duration::from_millis(retry_timeout)),
            nondet!(/** Randomly decide when to retry election */),
        );
        let nondet_ballot = nondet!(/** The ballot used depends on the time of message arrival */);
        let (write_requests, election_requests, responses, unblocked_reads, invalid_writes) = sliced! {
            let client_writes = use(client_writes.entries(), nondet_ballot);
            let incoming_requests = use(incoming_requests, nondet_ballot);
            let max_response_ballot = use(max_response_ballot, nondet_ballot);
            let incoming_responses = use(incoming_responses, nondet_ballot);
            let election_timeout = use(election_timeout, nondet_ballot);
            let mut retry_election_count = use::state::<Singleton<usize, _, Bounded>>(|l| l.singleton(q!(0)));
            let mut write_ballot = use::state::<Singleton<Ballot, _, Bounded>>(|l| l.singleton(q!(Ballot {
                num: 0,
                node: CLUSTER_SELF_ID.clone(),
            })));
            let mut state = use::state_null::<Optional<CASState<State>, _, Bounded>>();
            // Last state where we know a majority of replicas agreed.
            let mut committed_state = use::state_null::<Optional<(Ballot, Option<CASState<State>>), _, Bounded>>();
            // Writes blocking in reconcilation or election
            let mut write_queue = use::state_null::<KeyedStream<UniqueRequestId, (MemberId<Sender>, CASState<State>), _, Bounded, NoOrder>>();
            // Reads blocking on reconciliation
            let mut blocked_reads = use::state_null::<KeyedStream<UniqueRequestId, Response<State, Sender>, _, Bounded, NoOrder>>();

            let request_ballots = incoming_requests.clone().values().filter_map(q!(|request| request.ballot));
            let max_ballot_no_default = request_ballots
                .chain(max_response_ballot.into_stream())
                .max()
                .into_singleton();
            let max_ballot = max_ballot_no_default
                .clone()
                .map(q!(move |max_ballot| max_ballot.unwrap_or_else(|| Ballot {
                    num: 0,
                    node: CLUSTER_SELF_ID.clone(),
            })));

            // Incoming responses will contain N blocked_reads and either 1 election or write response, if any.
            let (read_responses, election_or_write_responses) = incoming_responses
                .clone()
                .partition(q!(|(response, _all_agree)| response.request.ballot.is_none()));
            let (election_responses, write_responses) = election_or_write_responses
                .partition(q!(|(response, _all_agree)| response.request.state.is_none()));
            let (write_successes, write_failures) = write_responses
                .partition(q!(|(response, _all_agree)| response.request.ballot.clone().expect("write response missing ballot") == response.max_ballot));

            // 1. Reads responses
            // Unblock reads if we observe a committed state that is higher than the ballot they observed
            let all_blocked_reads = read_responses
                .map(q!(|(response, _all_agree)| (response.request.id, response)))
                .into_keyed()
                .chain(blocked_reads.clone());
            let unblocked_reads = all_blocked_reads
                .clone()
                .cross_singleton(committed_state.clone())
                .filter_map(q!(|(response, (ballot, state))|
                    (ballot >= response.max_ballot)
                        .then(|| (response.request.client_id.expect("read response missing client_id"), (response.request.id, state.clone())))
                ));
            let new_blocked_reads = all_blocked_reads.filter_key_not_in(unblocked_reads.clone().keys());

            // 2. Election responses
            let (won_election, lost_election) = election_responses
                .clone()
                .partition(q!(move |(response, _all_agree)| response.max_ballot.node == CLUSTER_SELF_ID));
            let (won_election_write_complete, won_election_write_uncommitted) = won_election
                .partition(q!(|(_response, all_agree)| *all_agree));
            // Propose reconciling write if some replicas disagree
            let proposed_reconciling_write = won_election_write_uncommitted
                .map(q!(|(response, _all_agree)| (response.request.id, (None, response.state.expect("uncommitted election response missing state")))));
            let new_committed_state = won_election_write_complete
                .clone()
                .map(q!(|(response, _all_agree)| (response.max_ballot, response.state)))
                .chain(committed_state.into_stream())
                .max();
            // Propose a valid client write if the election is succesful and all agree
            let proposed_client_write = write_queue
                .clone()
                .entries()
                .cross_product(won_election_write_complete)
                .filter_map(q!(|((request_id, (client_id, state)), (response, _all_agree))|
                    // Allow write if version is prev+1 or the writer is the same and the version hasn't changed
                    if response.state.is_none_or(|curr_state| curr_state.version + 1 == state.version ||
                        (curr_state.writer == state.writer && curr_state.version == state.version)) {
                        Some((request_id, (Some(client_id), state)))
                    }
                    else {
                        println!("Rejecting invalid write: {:?}, state: {:?}", request_id, state);
                        None
                    }
                ))
                .assume_ordering::<TotalOrder>(nondet!(/** The order of input arrival determines which write we send first */))
                .first();
            // Invalidate writes with lower versions
            let invalid_writes = write_queue
                .clone()
                .entries()
                .cross_singleton(new_committed_state.clone())
                .filter_map(q!(|((request_id, (client_id, state)), (_ballot, committed_state))|
                    committed_state.is_none_or(|committed| committed.version > state.version).then_some((client_id, (request_id, false)))));
            let invalid_write_ids = invalid_writes
                .clone()
                .map(q!(|(_client_id, (request_id, _successful))| request_id));

            // 3. Write responses
            let successful_write = write_successes
                .clone()
                .map(q!(|(response, _all_agree)| response.request.id));

            // Increment new ballot when:
            // - Beginning election (no previous writes/reads)
            // - Consecutive election (previous write completed)
            // - Retrying election
            let new_write_queue = write_queue
                .clone()
                .filter_key_not_in(successful_write.chain(invalid_write_ids))
                .chain(client_writes.clone().map(q!(|(client_id, (request_id, state))| (request_id, (client_id, state)))).into_keyed());
            let had_no_writes_or_uncommitted_reads = write_queue
                .entries()
                .is_empty()
                .and(blocked_reads.entries().is_empty());
            let will_have_no_writes_or_uncommitted_reads = new_write_queue
                .clone()
                .entries()
                .is_empty()
                .and(new_blocked_reads.clone().entries().is_empty());
            let ready_for_new_election = had_no_writes_or_uncommitted_reads
                .and(!will_have_no_writes_or_uncommitted_reads.clone());
            let ready_for_consecutive_election = !(write_successes.is_empty().or(will_have_no_writes_or_uncommitted_reads));
            let ready_to_retry_election = retry_election_count
                .clone()
                .into_stream()
                .defer_tick()
                .first()
                .into_singleton()
                .map(q!(|count| count.is_some_and(|c| c == 1)))
                .and(retry_election_count.clone().map(q!(|count| count == 0)));
            // If we lost the election, or if our writes were preempted, retry after election_timeout
            retry_election_count = lost_election
                .chain(write_failures)
                .map(q!(|_| 2)) // Only allow election after 2 more timeouts (2 in case the timeout immediately goes off)
                .assume_ordering::<TotalOrder>(nondet!(/** Only 1 outgoing election at a time*/))
                .first()
                .unwrap_or(retry_election_count)
                .zip(election_timeout.first().into_singleton())
                .map(q!(|(count, signal)| if signal.is_some() && count > 0 { count - 1 } else { count }));
            let new_ballot = max_ballot
                .clone()
                .map(q!(move |ballot| Ballot { num: ballot.num + 1, node: CLUSTER_SELF_ID.clone() }))
                .filter_if(ready_to_retry_election.clone().or(ready_for_new_election.clone()));
            write_ballot = new_ballot.unwrap_or(write_ballot);

            // Determine local state based on incoming write requests
            let winning_write = incoming_requests
                .clone()
                .values()
                .sort()
                .last()
                .filter_map(q!(|request| request.ballot.zip(request.state)));
            let next_state = winning_write
                .zip(max_ballot.clone())
                .filter_map(q!(|((write_ballot, state), ballot)| (write_ballot >= ballot).then_some(state)))
                .or(state);

            // Attach ballots to requests and responses
            let write_requests = proposed_client_write
                .into_stream()
                .chain(proposed_reconciling_write)
                .cross_singleton(write_ballot.clone());
            let election_requests = write_ballot
                .clone()
                .filter_if(ready_to_retry_election.or(ready_for_new_election).or(ready_for_consecutive_election))
                .into_stream();
            let responses = incoming_requests
                .cross_singleton(max_ballot)
                .cross_singleton(next_state.clone().into_singleton());

            state = next_state;
            blocked_reads = new_blocked_reads;
            committed_state = new_committed_state;
            write_queue = new_write_queue;

            (write_requests, election_requests, responses, unblocked_reads.values(), invalid_writes)
        };

        // -------------------------------------------------------------
        // Broadcast requests.
        // -------------------------------------------------------------
        let sent_requests = self.smart_broadcast(
            client_reads
                .entries()
                .map(q!(|(client_id, request_id)| Request {
                    id: request_id,
                    client_id: Some(client_id),
                    ballot: None,
                    state: None,
                }))
                .merge_unordered(election_requests.map(q!(|ballot| Request {
                    id: UniqueRequestId::generate(),
                    client_id: None,
                    ballot: Some(ballot),
                    state: None,
                })))
                .merge_unordered(write_requests.map(q!(|(
                    (request_id, (client_id, write)),
                    ballot,
                )| Request {
                    id: request_id,
                    client_id,
                    ballot: Some(ballot),
                    state: Some(write),
                }))),
        );
        incoming_requests_complete.complete(sent_requests);

        // -------------------------------------------------------------
        // Reply to requests with responses.
        // -------------------------------------------------------------
        let sent_responses =
            self.smart_demux(responses.entries().map(q!(|(
                sender,
                ((request, max_ballot), state),
            )| {
                (
                    sender,
                    Response {
                        request,
                        max_ballot,
                        state,
                    },
                )
            })))
            .values()
            .map(q!(|response| (response.request.id, response)))
            .into_keyed();

        // -------------------------------------------------------------
        // Process responses.
        // -------------------------------------------------------------
        let responses_agree = self.check_all_agree(sent_responses);
        incoming_responses_complete.complete(responses_agree.clone());
        // Reads are responses without a ballot
        let read_result = responses_agree
            .clone()
            .filter_map(q!(|(response, all_agree)| (response
                .request
                .ballot
                .is_none() // If this is a read
                && response.request.state.is_none_or(|_state| all_agree)) // And either all agree or there's no state
            .then(|| (
                response.request.client_id.expect("read result missing client_id"),
                (response.request.id, response.state.clone())
            ))))
            .merge_unordered(unblocked_reads)
            .demux(sender, TCP.fail_stop().bincode())
            .values()
            .into_keyed();
        let write_success = responses_agree
            .clone()
            .filter_map(q!(|(response, _all_agree)| (response
                .request
                .ballot
                .is_some_and(|ballot| ballot == response.max_ballot) // If we weren't preempted
                && response.request.state.is_some() // And this is a write (not election)
                && response.request.client_id.is_some()) // And this is not a reconciling write
            .then(|| (
                response.request.client_id.expect("write success missing client_id"),
                response.request.id,
                response.state.clone().expect("write success missing state")
            ))));
        let write_processed = write_success
            .clone()
            .map(q!(|(client_id, request_id, _state)| (
                client_id, (request_id, true)
            )))
            .merge_unordered(invalid_writes)
            .demux(sender, TCP.fail_stop().bincode())
            .values()
            .into_keyed();

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
                .map(q!(|(subscriber, (_client_id, _request_id, state))| (subscriber, state)));

            prev_subscribers = current_subscribers;
            writes_to_subscribers
        };
        let subscribe_updates = writes_to_subscribers
            .demux(sender, TCP.fail_stop().bincode())
            .values();

        // -------------------------------------------------------------
        // Gather all ballots received as a response.
        // -------------------------------------------------------------
        let received_ballots =
            responses_agree.map(q!(|(response, _all_agree)| response.max_ballot));
        incoming_response_ballots_complete.complete(received_ballots);

        CASOutput {
            read_result,
            write_processed,
            subscribe_updates,
        }
    }
}
