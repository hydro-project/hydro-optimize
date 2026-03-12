pub mod cas;
pub mod cas_distributed;
pub mod cas_like;
pub mod chain_replication_bench;

use hydro_lang::live_collections::stream::TotalOrder;
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use hydro_lang::{live_collections::stream::NoOrder, location::Location};
use hydro_std::membership::track_membership;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::Instant;

use crate::black_hole;
use crate::chain_replication::cas_like::{CASLike, CASState};

// ---------------------------------------------------------------------------
// Role and Configuration
// ---------------------------------------------------------------------------

/// Role assigned to each Replica based on sorted cluster member ID position.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    /// First in sorted member list — sequences client payloads.
    Acceptor,
    /// Second in sorted member list — commits and propagates to standbys.
    Committer,
    /// Remaining members — receive committed entries and propagate down chain.
    Standby,
}

/// Cluster configuration stored in CAS.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Configuration<Replica> {
    /// Role assignment: member_id → role.
    pub roles: HashMap<MemberId<Replica>, Role>,
    /// Watermark slot — entries below this can be garbage collected.
    pub watermark: u64,
}

impl<Replica: PartialEq> PartialOrd for Configuration<Replica> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.watermark.cmp(&other.watermark))
    }
}

impl<Replica: Eq> Ord for Configuration<Replica> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.watermark.cmp(&other.watermark)
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Payload with assigned slot.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencedPayloadMsg<Payload> {
    /// The client payload.
    pub payload: Payload,
    /// Contiguous slot number assigned by the Acceptor.
    pub slot: u64,
    /// Configuration version at send time.
    pub config_version: u64,
}

impl<Payload: PartialEq + Ord> PartialOrd for SequencedPayloadMsg<Payload> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Payload: Eq + Ord> Ord for SequencedPayloadMsg<Payload> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.slot.cmp(&other.slot)
    }
}

/// Periodic heartbeat exchanged between all nodes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatMsg {
    /// Highest committed slot. Acceptors & Committers don't need to send this, since the standbys don't actually read it anyway
    pub committed: Option<u64>,
    /// Configuration version.
    pub config_version: u64,
    /// Whether we've entered reconfiguration for this version.
    pub reconfig_accepted: bool,
    /// Whether we're requesting a reconfiguration for this version.
    pub reconfig_requested: bool,
}

impl PartialOrd for HeartbeatMsg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeartbeatMsg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.config_version
            .cmp(&other.config_version)
            .then_with(|| self.committed.cmp(&other.committed))
            .then_with(|| self.reconfig_requested.cmp(&other.reconfig_requested))
            .then_with(|| self.reconfig_accepted.cmp(&other.reconfig_accepted))
    }
}

fn get_my_role<'a, Replica: 'a>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
) -> Optional<Role, Tick<Cluster<'a, Replica>>, Bounded> {
    config.filter_map(q!(move |(conf, _version)| conf
        .roles
        .get(&CLUSTER_SELF_ID)
        .cloned()))
}

fn next_member<'a, Replica: 'a>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
) -> Optional<(MemberId<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded> {
    config.filter_map(q!(move |(conf, version)| {
        #[expect(
            clippy::disallowed_methods,
            reason = "sorting after keys() makes the result deterministic"
        )]
        let mut members: Vec<_> = conf.roles.keys().cloned().collect();
        members.sort();
        let my_pos = members.iter().position(|m| *m == CLUSTER_SELF_ID)?;
        members.get(my_pos + 1).cloned().map(|next| (next, version))
    }))
}

fn i_am_acceptor<'a, Replica>(
    my_role: Optional<Role, Tick<Cluster<'a, Replica>>, Bounded>,
) -> Optional<(), Tick<Cluster<'a, Replica>>, Bounded> {
    my_role.filter_map(q!(|role| (role == Role::Acceptor).then_some(())))
}

fn i_am_committer<'a, Replica>(
    my_role: Optional<Role, Tick<Cluster<'a, Replica>>, Bounded>,
) -> Optional<(), Tick<Cluster<'a, Replica>>, Bounded> {
    my_role.filter_map(q!(|role| (role == Role::Committer).then_some(())))
}

fn i_am_standby<'a, Replica>(
    my_role: Optional<Role, Tick<Cluster<'a, Replica>>, Bounded>,
) -> Optional<(), Tick<Cluster<'a, Replica>>, Bounded> {
    my_role.filter_map(q!(|role| (role == Role::Standby).then_some(())))
}

/// Given the latest Heartbeat entries from peers, find a standby that has caught up
/// (i.e., `reconfig_accepted && committed == slot`). Returns the caught-up member
/// and the updated `highest_proposed_new_config_version` state.
#[expect(clippy::type_complexity, reason = "internal function")]
fn propose_new_config<'a, Replica: 'a + Clone + Debug>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
    heartbeat_entries: Stream<
        (MemberId<Replica>, (Instant, HeartbeatMsg)),
        Tick<Cluster<'a, Replica>>,
        Bounded,
        NoOrder,
    >,
    highest_proposed_new_config_version: Singleton<u64, Tick<Cluster<'a, Replica>>, Bounded>,
    slot: Singleton<u64, Tick<Cluster<'a, Replica>>, Bounded>,
) -> (
    Stream<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded, TotalOrder>,
    Singleton<u64, Tick<Cluster<'a, Replica>>, Bounded>,
) {
    let version = version(config.clone());
    let already_proposed_config = highest_proposed_new_config_version
        .clone()
        .zip(version.clone())
        .filter_map(q!(
            |(proposed_v, version)| (proposed_v == version).then_some(())
        ));
    let caught_up = heartbeat_entries
        .filter_if_none(already_proposed_config)
        .cross_singleton(slot.clone())
        .filter_map(q!(|((member_id, (_time, heartbeat)), slot)| (heartbeat
            .reconfig_accepted
            && heartbeat.committed.expect(
                "Received Heartbeat without commited slot number during reconfiguration"
            ) == slot)
            .then_some(member_id)))
        .inspect(q!(|member_id| println!(
            "Standby ready for reconfiguration: {:?}",
            member_id
        )))
        .assume_ordering::<TotalOrder>(
            nondet!(/** If multiple Standbys are ready, pick one at random */),
        )
        .first();
    let new_highest = version
        .filter_if_some(caught_up.clone())
        .unwrap_or(highest_proposed_new_config_version);
    #[expect(
        clippy::disallowed_methods,
        reason = "find_map result is independent of iteration order -- the predicate matches at most one member"
    )]
    let new_config = config
        .zip(caught_up)
        .zip(slot)
        .map(q!(move |(((conf, version), new_member), slot)| {
            let my_role = conf.roles.get(&CLUSTER_SELF_ID).unwrap();
            let role_to_replace = if *my_role == Role::Acceptor {
                Role::Committer
            } else {
                Role::Acceptor
            };
            let old_member = conf
                .roles
                .iter()
                .find_map(|(member_id, role)| (*role == role_to_replace).then_some(member_id))
                .unwrap();
            let mut new_config = conf.clone();
            new_config.roles.insert(new_member, role_to_replace.clone());
            new_config.roles.insert(old_member.clone(), Role::Standby);
            new_config.watermark = slot;
            (new_config, version + 1)
        }))
        .into_stream()
        .inspect(q!(|c| println!("Calculated new config: {:?}", c)));
    (new_config, new_highest)
}

fn everyone_but_me<'a, Replica: 'a>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
) -> Stream<MemberId<Replica>, Tick<Cluster<'a, Replica>>, Bounded, NoOrder> {
    config
        .flat_map_unordered(q!(|(conf, _version)| conf.roles.into_keys()))
        .filter(q!(move |member_id| *member_id != CLUSTER_SELF_ID))
}

fn version<'a, Replica>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
) -> Singleton<u64, Tick<Cluster<'a, Replica>>, Bounded> {
    config.map(q!(|(_conf, version)| version))
}

fn watermark<'a, Replica>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
) -> Singleton<u64, Tick<Cluster<'a, Replica>>, Bounded> {
    config.map(q!(|(conf, _version)| conf.watermark))
}

fn version_stopped<'a, Replica>(
    config: Singleton<(Configuration<Replica>, u64), Tick<Cluster<'a, Replica>>, Bounded>,
    highest_stopped_version: Optional<u64, Tick<Cluster<'a, Replica>>, Bounded>,
) -> Singleton<bool, Tick<Cluster<'a, Replica>>, Bounded>
where
    Replica: Clone,
{
    version(config.clone())
        .zip(highest_stopped_version.into_singleton())
        .map(q!(
            |(v, stopped_version)| stopped_version.is_some_and(|stopped_v| stopped_v == v)
        ))
}

/// Chain replication replicates a log across a cluster using three roles:
/// - **Acceptor**: receives client payloads, assigns contiguous slots, sends Replicate to Committer
/// - **Committer**: confirms storage, propagates StoreCommitted to Standby chain
/// - **Standby**: receives committed entries, propagates down the chain
///
/// Configuration is managed through a CAS interface (atomic register).
/// NOTE: Messages that arrive during reconfiguration will either be lost (because they arrived at a dying node) or never acknowledged (because the committer is not present to acknowledge it)
///
/// # Notable Parameters
/// `heartbeat_interval_millis`: How often to output Heartbeat
/// `heartbeat_check_millis`: How often to check if someone has not heartbeated Heartbeat in a while
/// `heartbeat_expired_millis`: After this much time without a Heartbeat heartbeat, consider a node dead
/// # Returns
/// - A stream containing the ID of the latest acceptor.
/// - A stream of `(slot, payload)` committed entries visible to the client.
#[expect(clippy::type_complexity, reason = "protocol")]
pub fn chain_replication<'a, CAS, Payload, Replica>(
    replicas: &Cluster<'a, Replica>,
    num_replicas: usize,
    client_payloads: Stream<Payload, Cluster<'a, Replica>, Unbounded, NoOrder>,
    heartbeat_output_millis: u64,
    heartbeat_check_millis: u64,
    heartbeat_expired_millis: u64,
    cas: CAS,
) -> (
    Stream<(), Cluster<'a, Replica>, Unbounded, TotalOrder>,
    Stream<(u64, Payload), Cluster<'a, Replica>, Unbounded, NoOrder>,
)
where
    CAS: CASLike<'a, Configuration<Replica>, MemberId<Replica>, Replica>,
    Payload: PartialEq + Eq + Ord + Debug + Clone + Serialize + DeserializeOwned,
    Replica: 'a + Clone + Serialize + DeserializeOwned + Debug + Eq,
{
    assert!(
        num_replicas >= 2,
        "Chain replication must have at least 1 acceptor and 1 committer"
    );
    assert!(
        heartbeat_check_millis > heartbeat_output_millis,
        "Heartbeat check timeout must be higher than output frequency for liveness"
    );
    assert!(
        heartbeat_expired_millis > heartbeat_output_millis,
        "Heartbeat timeout must be higher than output frequency for liveness"
    );

    // Send writes to CAS, subscribe to latest config from CAS
    let (cas_writes_complete, cas_writes) = replicas.forward_ref();
    // Empty stream for reads since we just subscribe
    let cas_no_reads = replicas.source_iter(q!([]));
    let cas_subscribe = replicas.singleton(q!(CLUSTER_SELF_ID)).into_stream();
    let cas_output = cas.build(cas_writes, cas_no_reads, cas_subscribe, replicas);
    // TODO: Throw away unneeded outputs. Remove once Hydro supports dangling HydroNodes.
    black_hole(cas_output.write_processed);
    black_hole(cas_output.read_result.entries());
    let cas_config = cas_output.subscribe_updates;

    // Calculate initial config without CAS
    let replica_members = replicas.source_cluster_members(replicas);
    let curr_replicas = track_membership(replica_members);
    let initial_config_raw = sliced! {
        let curr_replicas = use(curr_replicas, nondet!(/** Waiting until all replicas are live, assuming no member changes */));
        let present_replicas = curr_replicas
            .filter(q!(|r| *r)) // Only consider replicas that are still here
            .keys();
        let all_replicas_present = present_replicas
            .clone()
            .count()
            .filter(q!(move |num| *num == num_replicas));
        present_replicas
            .filter_if_some(all_replicas_present)
            .sort()
    };
    let initial_config = initial_config_raw
        .unique() // The stream will keep outputting every tick
        .enumerate()
        .map(q!(|(index, member_id)| {
            let role = match index {
                0 => Role::Acceptor,
                1 => Role::Committer,
                _ => Role::Standby,
            };
            (member_id, role)
        }))
        .fold(
            q!(|| (
                Configuration {
                    roles: HashMap::new(),
                    watermark: 0,
                },
                0
            )),
            q!(|(conf, _version), (member_id, role)| {
                conf.roles.insert(member_id, role);
            }),
        );
    let config = cas_config
        .max()
        .map(q!(|cas_write| (cas_write.state, cas_write.version)))
        .unwrap_or(initial_config);

    let (sequenced_in_complete, sequenced_in) =
        replicas.forward_ref::<Stream<SequencedPayloadMsg<Payload>, _, Unbounded, NoOrder>>();
    let (log_in_complete, log_in) = replicas.forward_ref::<KeyedStream<
        u64,
        SequencedPayloadMsg<Payload>,
        Cluster<'a, Replica>,
        Unbounded,
        NoOrder,
    >>();
    let (heartbeat_in_complete, heartbeat_in) =
        replicas
            .forward_ref::<KeyedStream<MemberId<Replica>, HeartbeatMsg, _, Unbounded, NoOrder>>();
    let (detected_death_complete, detected_death) =
        replicas
            .forward_ref::<Stream<MemberId<Replica>, Cluster<'a, Replica>, Unbounded, NoOrder>>();

    // -------------------------------------------------------------
    // Forward message to the next member + Logging + Preventing acceptor logging after death detected
    // -------------------------------------------------------------
    let nondet_latest_config = nondet!(/** Messages are forwarded or rejected based on the config at the time of arrival */);
    let (
        unsequenced_to_next,
        sequenced_to_next,
        committed_to_client,
        log,
        new_log_values,
        stopped_version,
    ) = sliced! {
        // Sequencing
        let config = use(config.clone(), nondet_latest_config);
        let client_payloads = use(client_payloads, nondet_latest_config);
        let sequenced_in = use(sequenced_in.clone(), nondet_latest_config);
        // Log entries
        let log_in = use(log_in, nondet_latest_config);
        // Preventing acceptor logging
        let detected_death = use(detected_death, nondet_latest_config);

        let next_member = next_member(config.clone());
        let i_am_acceptor = i_am_acceptor(get_my_role(config.clone()));
        let i_am_committer = i_am_committer(get_my_role(config.clone()));
        let i_am_standby = i_am_standby(get_my_role(config.clone()));

        let version = version(config.clone());
        let first_death = detected_death
            .filter_if_none(i_am_standby)
            .assume_ordering::<TotalOrder>(nondet!(/** Treated as signal */))
            .first();
        let highest_stopped_version = version
            .clone()
            .filter_if_some(first_death)
            .into_stream()
            .across_ticks(|v| v.max());
        let stopped = version_stopped(config.clone(), highest_stopped_version.clone())
            .map(q!(|signal| signal.then_some(())))
            .into_optional();

        let unsequenced_to_next = client_payloads
            .filter_if_some(i_am_acceptor.clone())
            .filter_if_none(stopped) // Don't sequence during reconfiguration
            .cross_singleton(next_member.clone());
        let sequenced_to_next = sequenced_in
            .clone()
            .cross_singleton(next_member);
        let committed_to_client = sequenced_in
            .filter_if_some(i_am_committer);

        let watermark = watermark(config);
        let new_log_values = log_in
            .cross_singleton(version)
            .filter_map(q!(|(payload, version)|
                (payload.config_version >= version).then_some(payload)));
        let log = new_log_values
            .clone()
            .across_ticks(|s|
                s.reduce_watermark(watermark, q!(|curr, next| {
                    if curr.config_version < next.config_version {
                        *curr = next;
                    }
                }, commutative = ManualProof(/* Highest version wins, payloads with the same version guaranteed to be identical */))))
            .entries();

        (unsequenced_to_next, sequenced_to_next, committed_to_client, log, new_log_values, highest_stopped_version.into_stream())
    };

    let highest_stopped_version = stopped_version.max();

    // -------------------------------------------------------------
    // Acceptor's Heartbeat. Triggers reconfig if necessary.
    // Also, just_became_acceptor (to alert the client of a new destination).
    // NOTE: Should be with the sliced block above, but sliced only allows 5 uses right now.
    // -------------------------------------------------------------
    let nondet_heartbeat_acceptor = nondet!(/** Depending on when the timeout is triggered, the acceptor's Heartbeat may or may not trigger reconfiguration */);
    let acceptor_heartbeat_interval = replicas.source_interval(
        q!(Duration::from_millis(heartbeat_output_millis)),
        nondet_heartbeat_acceptor,
    );
    let (acceptor_heartbeat_out, just_became_acceptor) = sliced! {
        let acceptor_heartbeat_interval = use(acceptor_heartbeat_interval, nondet_heartbeat_acceptor);
        let highest_stopped_version = use(highest_stopped_version.clone(), nondet_heartbeat_acceptor);
        let config = use(config.clone(), nondet_heartbeat_acceptor);

        let i_am_acceptor = i_am_acceptor(get_my_role(config.clone()));
        let everyone_but_me = everyone_but_me(config.clone());
        let stopped = version_stopped(config.clone(), highest_stopped_version);

        let unaddressed_heartbeat_out = acceptor_heartbeat_interval
            .filter_if_some(i_am_acceptor.clone())
            .cross_singleton(config)
            .cross_singleton(stopped)
            .map(q!(|((_trigger, (_conf, version)), stopped)| HeartbeatMsg {
                committed: None,
                config_version: version,
                reconfig_accepted: false,
                reconfig_requested: stopped,
            }));
        let heartbeat_out = everyone_but_me.cross_product(unaddressed_heartbeat_out);

        let i_was_acceptor = i_am_acceptor.clone().defer_tick();
        let just_became_acceptor = i_am_acceptor
            .filter_if_none(i_was_acceptor)
            .into_stream();

        (heartbeat_out, just_became_acceptor)
    };

    // Committer tells the client about committed messages
    let output = committed_to_client.map(q!(|sequenced| (sequenced.slot, sequenced.payload)));

    let latest_heartbeat_received = heartbeat_in
        .reduce(q!(
            |max, next| {
                if next > *max {
                    *max = next;
                }
            },
            commutative = ManualProof(/* Max is commutative */)
        ))
        .map(q!(|heartbeat| (Instant::now(), heartbeat)));

    // -------------------------------------------------------------
    // Anything on the acceptor that requires the most up-to-date slot number
    // 1. Sequencing
    // 2. Checking if a standby has caught up during reconfiguration
    // -------------------------------------------------------------
    let nondet_sequence = nondet!(/** Payloads are non-deterministically assigned slot numbers */);
    let (sequenced_to_committer, acceptor_new_config) = sliced! {
        let latest_heartbeat_received = use(latest_heartbeat_received.clone(), nondet_sequence);
        let mut highest_proposed_new_config_version = use::state(|l| l.singleton(q!(0u64)));
        let config = use(config.clone(), nondet_sequence);
        let unsequenced_to_next = use(unsequenced_to_next, nondet_sequence);
        let mut slot = use::state(|l| l.singleton(q!(0u64)));

        // 1. Sequencing
        let curr_slot = slot
            .clone()
            .zip(watermark(config.clone()))
            .map(q!(|(slot, post_reconfig_slot)| std::cmp::max(slot, post_reconfig_slot)));
        let num_payloads = unsequenced_to_next.clone().count();
        let next_slot = curr_slot
            .clone()
            .zip(num_payloads)
            .map(q!(|(slot, num_payloads)| slot + num_payloads as u64));
        slot = next_slot.clone();
        let sequenced_to_committers = unsequenced_to_next
            .assume_ordering::<TotalOrder>(nondet_sequence)
            .enumerate()
            .cross_singleton(curr_slot);

        // 2. Check if a standby has caught up
        let (new_config, new_highest) = propose_new_config(
            config,
            latest_heartbeat_received.entries(),
            highest_proposed_new_config_version,
            next_slot,
        );
        highest_proposed_new_config_version = new_highest;

        (sequenced_to_committers, new_config)
    };

    // ---------------------------------------------------------------------------
    // Catchup during reconfiguration
    // ---------------------------------------------------------------------------
    let nondet_catchup = nondet!(/** Send current state of log, whatever it is when the Heartbeat message arrives */);
    let catchup_log = sliced! {
        let config = use(config.clone(), nondet_catchup);
        let latest_heartbeat_received = use(latest_heartbeat_received.clone(), nondet_catchup);
        let log_in = use(log, nondet_catchup);

        let i_am_standby = i_am_standby(get_my_role(config.clone()));
        let version = version(config);
        let catchup_member_slot = latest_heartbeat_received
            .entries()
            .cross_singleton(version)
            .filter_map(q!(|((member_id, (_time, heartbeat)), version)| {
                (heartbeat.reconfig_accepted && heartbeat.config_version == version)
                    .then(|| (member_id, heartbeat.committed.expect("Only standbys can send reconfig_accepted, and standbys must have heartbeat.committed")))
            }));

        // If we receive a Heartbeat message, and we are not the standby, then send them all missing entries from our log
        catchup_member_slot
            .filter_if_none(i_am_standby)
            .inspect(q!(|(member_id, highest_slot)|
                println!("Received Heartbeat from Standby {:?} during reconfiguration with slot {}", member_id, highest_slot)))
            .cross_product(log_in)
            .filter_map(q!(|((member_id, highest_slot), (slot, payload))|
                (slot > highest_slot).then_some((member_id, payload))
            ))
    };

    let sequenced_to_committers_send_ready = sequenced_to_committer.map(q!(|(
        (zero_index, (payload, (next_member, config_version))),
        curr_slot,
    )| (
        next_member,
        SequencedPayloadMsg {
            payload,
            slot: zero_index as u64 + curr_slot,
            config_version,
        }
    )));
    let sequenced_pass_along = sequenced_to_next.map(q!(|(
        payload,
        (next_member, _config_version),
    )| (next_member, payload)));
    let sequenced_out = sequenced_to_committers_send_ready
        .clone()
        .interleave(sequenced_pass_along)
        .interleave(catchup_log)
        .demux(replicas, TCP.fail_stop().bincode())
        .values();
    sequenced_in_complete.complete(sequenced_out);

    // Gather all payloads with sequence numbers
    let log_input = sequenced_to_committers_send_ready
        .map(q!(|(_next_member, payload)| (payload.slot, payload)))
        .interleave(sequenced_in.map(q!(|payload| (payload.slot, payload))))
        .into_keyed();
    log_in_complete.complete(log_input);

    // -------------------------------------------------------------
    // Anything on committers and standbys that requires reading the latest slot number
    // 1. Heartbeat
    // 2. Watermarking CAS
    // 3. (Committer) Checking if a standby has caught up during reconfiguration
    // -------------------------------------------------------------
    let nondet_heartbeat_log =
        nondet!(/** Heartbeat will query the log's heartbeat non-deterministically */);
    let heartbeat_trigger = replicas.source_interval(
        q!(Duration::from_millis(heartbeat_output_millis)),
        nondet_heartbeat_log,
    );
    let (non_acceptor_heartbeat_out, watermark_to_cas, committer_new_config) = sliced! {
        let new_slots = use(new_log_values.keys(), nondet_heartbeat_log);
        let latest_heartbeat_received = use(latest_heartbeat_received.clone(), nondet_heartbeat_log);
        let config = use(config.clone(), nondet_heartbeat_log);
        let highest_stopped_version = use(highest_stopped_version, nondet_heartbeat_log);
        let heartbeat_trigger = use(heartbeat_trigger, nondet_heartbeat_log);
        let mut highest_proposed_new_config_version = use::state(|l| l.singleton(q!(0u64)));
        let mut log_holes = use::state_null::<Stream<u64, Tick<_>, Bounded, NoOrder>>();

        let should_output_heartbeat = heartbeat_trigger.clone().first();

        // If this is the acceptor, the committed slot is not determined from the log but from Heartbeat messages
        let my_role = get_my_role(config.clone());
        let i_am_acceptor = i_am_acceptor(my_role.clone());
        let i_am_not_last_standby = next_member(config.clone());
        let everyone_but_me = everyone_but_me(config.clone());

        // If this is the committer, set reconfig_requested to true if highest_stopped_version is this version
        let i_am_committer = i_am_committer(my_role.clone());
        let committer_stopped = version_stopped(config.clone(), highest_stopped_version)
            .zip(i_am_committer.into_singleton())
            .map(q!(|(stopped, am_committer)| stopped && am_committer.is_some()));

        // If this is a standby, set reconfig_accepted to true if latest_heartbeat_received contains a message with reconfig_requested == true and version = this version
        let i_am_standby = i_am_standby(my_role);
        let highest_heartbeat_stopped_version = latest_heartbeat_received
            .clone()
            .entries()
            .filter_map(q!(|(_member_id, (_time, heartbeat))|
                (heartbeat.reconfig_requested).then_some(heartbeat.config_version)))
            .across_ticks(|s| s.max());
        let standby_stopped = version_stopped(config.clone(), highest_heartbeat_stopped_version)
            .zip(i_am_standby.into_singleton())
            .map(q!(|(stopped, am_standby)| stopped && am_standby.is_some()));

        // Min log hole = max contiguous slot
        let max_contiguous_slot = log_holes.clone().min().unwrap_or_default();

        // Calculate the new log holes
        let max_log_hole = log_holes.clone().max().unwrap_or_default();
        let max_new_slot = new_slots.clone().max();
        // max_new_slot+2 because we want the next hole to be the largest slot + 1. The other +1 is because the range is exclusive
        let new_potential_holes = max_log_hole
            .zip(max_new_slot)
            .flat_map_unordered(q!(|(max_hole, max_new_slot)| max_hole+1..max_new_slot+2));
        let new_holes = new_potential_holes.chain(log_holes.clone())
            .filter_not_in(new_slots);
        log_holes = new_holes;

        // 1. Heartbeat
        let unaddressed_heartbeat_out = max_contiguous_slot
            .clone()
            .filter_if_none(i_am_acceptor)
            .filter_if_some(should_output_heartbeat.clone())
            .zip(config.clone())
            .zip(committer_stopped)
            .zip(standby_stopped)
            .map(q!(|(((committed, (_conf, version)), committer_stopped), standby_stopped)|
                HeartbeatMsg {
                    committed: Some(committed),
                    config_version: version,
                    reconfig_accepted: standby_stopped,
                    reconfig_requested: committer_stopped,
                }));
        let heartbeat_out = everyone_but_me
            .cross_singleton(unaddressed_heartbeat_out);

        // 2. Watermark
        let watermark_out = max_contiguous_slot
            .clone()
            .filter_if_some(should_output_heartbeat)
            .filter_if_none(i_am_not_last_standby)
            // .filter(q!(|slot| *slot != 0)) // Don't spam watermarks if the log is empty
            .zip(config.clone())
            .map(q!(|(watermark, (mut conf, version))| {
                conf.watermark = watermark;
                CASState {
                    version, // NOTE: Don't increment the version for watermarks. Otherwise payloads with older versions will get rejected erroneously
                    state: conf,
                }
            }))
            .into_stream();

        // 3. Check if a standby has caught up
        let (new_config, new_highest) = propose_new_config(
            config,
            latest_heartbeat_received.entries(),
            highest_proposed_new_config_version,
            max_contiguous_slot,
        );
        highest_proposed_new_config_version = new_highest;

        (heartbeat_out, watermark_out, new_config)
    };

    let heartbeat_out_sent = non_acceptor_heartbeat_out
        .interleave(acceptor_heartbeat_out)
        .demux(replicas, TCP.fail_stop().bincode());
    heartbeat_in_complete.complete(heartbeat_out_sent);

    // -------------------------------------------------------------
    // Timeout
    // Note: This only runs on the acceptor/committer, and will only attempt to replace the other acceptor/committer.
    // -------------------------------------------------------------
    let nondet_heartbeat_check =
        nondet!(/** Periodically check when the latest Heartbeat message has been received */);
    // Source interval to check heartbeat
    let heartbeat_check_interval = replicas.source_interval_delayed(
        q!(Duration::from_millis(heartbeat_check_millis)),
        q!(Duration::from_millis(heartbeat_check_millis)),
        nondet_heartbeat_check,
    );
    let dead_acceptor_committer = sliced! {
        let latest_heartbeat_received = use(latest_heartbeat_received, nondet_heartbeat_check);
        let heartbeat_check_interval = use(heartbeat_check_interval, nondet_heartbeat_check);
        let config = use(config, nondet_heartbeat_check);

        let latest_heartbeat_check = heartbeat_check_interval.first();
        let my_role = get_my_role(config.clone());
        let i_am_standby = i_am_standby(my_role);
        #[expect(clippy::disallowed_methods, reason = "find_map result is independent of iteration order -- the predicate matches at most one member")]
        let other_acceptor_committer = config
            .clone()
            .filter_if_none(i_am_standby)
            .filter_if_some(latest_heartbeat_check)
            .filter_map(q!(move |(conf, _version)| conf.roles
                .iter()
                .find_map(|(member_id, role)|
                    (*member_id != CLUSTER_SELF_ID && *role != Role::Standby)
                        .then(|| member_id.clone()))
            ));

        let other_no_message = other_acceptor_committer
            .clone()
            .into_stream()
            .filter_not_in(latest_heartbeat_received.clone().keys())
            .inspect(q!(|member_id| println!("Have not received Heartbeat from {:?}", member_id)));

        let other_expired = latest_heartbeat_received
            .entries()
            .cross_singleton(other_acceptor_committer)
            .filter_map(q!(move |((member_id, (latest_time, _heartbeat)), other)| {
                (member_id == other &&
                    Instant::now().duration_since(latest_time) >
                    Duration::from_millis(heartbeat_expired_millis))
                    .then_some(member_id)
            }));

       other_no_message
            .chain(other_expired)
            .inspect(q!(|member_id| println!("Detected death of {:?}", member_id)))
    };
    detected_death_complete.complete(dead_acceptor_committer);

    // Send reconfiguration messages to CAS
    let cas_write_out = acceptor_new_config
        .interleave(committer_new_config)
        .map(q!(|(conf, version)| CASState {
            version,
            state: conf
        }))
        .interleave(watermark_to_cas)
        .map(q!(move |cas_state| (CLUSTER_SELF_ID.clone(), cas_state)))
        .into_keyed();
    cas_writes_complete.complete(cas_write_out);

    (just_became_acceptor, output)
}
