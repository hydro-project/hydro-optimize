use hydro_lang::{
    live_collections::{sliced::sliced, stream::NoOrder},
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{KeyedStream, Process, Stream, Unbounded},
};
use stageleft::q;

pub struct Server {}

/// Lock server implementation as described in https://dl.acm.org/doi/pdf/10.1145/3341301.3359651, with the difference being that each server can hold multiple locks.
/// * `acquires`: Stream of (virt_client_id, lock_id), requesting a lock from the server. If the server currently holds the lock, it returns (virt_client_id, lock_id, true). Otherwise, it returns (virt_client_id, lock_id, false).
/// * `releases`: Stream of (virt_client_id, lock_id), releasing a lock back to the server. Returns (virt_client_id, lock_id).
///
/// Assumptions:
/// - No client will send a release message before it knows it has acquired the lock.
/// - Clients block on ACKs for outgoing messages; no client has 2 in-flight messages.
///
#[expect(clippy::type_complexity, reason = "internal Lock Server code // TODO")]
pub fn lock_server<'a, Client>(
    server: &Process<'a, Server>,
    acquires: KeyedStream<MemberId<Client>, (u32, u32), Process<'a, Server>, Unbounded, NoOrder>,
    releases: KeyedStream<MemberId<Client>, (u32, u32), Process<'a, Server>, Unbounded, NoOrder>,
) -> (
    KeyedStream<MemberId<Client>, (u32, u32, bool), Process<'a, Server>, Unbounded, NoOrder>,
    KeyedStream<MemberId<Client>, (u32, u32), Process<'a, Server>, Unbounded, NoOrder>,
) {
    let server_tick = server.tick();
    let mapped_acquires = acquires
        .clone()
        .entries()
        .map(q!(|(client_id, (virtual_client_id, lock_id))| (
            lock_id,
            (client_id, virtual_client_id)
        )))
        .into_keyed();
    let mapped_releases = releases
        .clone()
        .entries()
        .map(q!(|(client_id, (virtual_client_id, lock_id))| (
            lock_id,
            (client_id, virtual_client_id)
        )))
        .into_keyed();

    sliced! {
        let mapped_acquires = use(mapped_acquires, nondet!(/** For each lock, pick a random acquire as the winner */));
        let mapped_releases = use(mapped_releases, nondet!(/** For each lock, only one release should succeed (the owner of the lock) */));

        // lock_id
        let mut free_locks = use::state_null::<Stream<u32, _, _, _>>();
        // (lock_id, (client_id, virtual_client_id))
        let mut acquired_locks = use::state_null::<Stream<(u32, (MemberId<Client>, u32)), _, _, _>>();
        let keyed_acquired_locks = acquired_locks.into_keyed().first();
        // For each lock, pick a random acquire as the winner

        // Always process releases first
        let new_free_locks = keyed_acquired_locks
            .clone()
            .get_many_if_present(mapped_releases)
            .filter(q!(|((holder_client_id, holder_virtual_client_id), (client_id, virtual_client_id))|
                holder_client_id == client_id && holder_virtual_client_id == virtual_client_id
            ))
            .keys()
            .chain(free_locks);

        let winning_acquires = mapped_acquires.assume_ordering(nondet!(/** Randomly pick one acquire to be the winner */)).first();

        // Process acquires for locks that exist
        // TODO: Requires join_stream

        // Create new locks if it's not currently free or acquired
        let created_locks = winning_acquires
            .filter_key_not_in(new_free_locks)
            .filter_key_not_in(keyed_acquired_locks.keys());
    };
}

/// Lock server implementation as described in https://dl.acm.org/doi/pdf/10.1145/3341301.3359651, with the difference being that each server can hold multiple locks.
/// * `acquires`: Stream of (virt_client_id, lock_id), requesting a lock from the server. If the server currently holds the lock, it returns (virt_client_id, lock_id, true). Otherwise, it returns (virt_client_id, lock_id, false).
/// * `releases`: Stream of (virt_client_id, lock_id), releasing a lock back to the server. Returns (virt_client_id, lock_id).
///
/// Assumptions:
/// - No client will send a release message before it knows it has acquired the lock.
/// - Clients block on ACKs for outgoing messages; no client has 2 in-flight messages.
///
/// This version iterates through each request in order, as one might on a single-threaded server.
#[expect(clippy::type_complexity, reason = "internal Lock Server code // TODO")]
pub fn assume_order_lock_server<'a, Client>(
    server: &Process<'a, Server>,
    acquires: KeyedStream<MemberId<Client>, (u32, u32), Process<'a, Server>, Unbounded, NoOrder>,
    releases: KeyedStream<MemberId<Client>, (u32, u32), Process<'a, Server>, Unbounded, NoOrder>,
) -> (
    KeyedStream<MemberId<Client>, (u32, u32, bool), Process<'a, Server>, Unbounded, NoOrder>,
    KeyedStream<MemberId<Client>, (u32, u32), Process<'a, Server>, Unbounded, NoOrder>,
) {
    let server_tick = server.tick();
    let atomic_acquires = acquires
        .clone()
        .entries()
        .map(q!(|(client_id, (virtual_client_id, lock_id))| (
            lock_id,
            (client_id, virtual_client_id, true)
        )))
        .atomic(&server_tick);
    let atomic_releases = releases
        .clone()
        .entries()
        .map(q!(|(client_id, (virtual_client_id, lock_id))| (
            lock_id,
            (client_id, virtual_client_id, false)
        )))
        .atomic(&server_tick);
    let payloads = atomic_acquires.interleave(atomic_releases).into_keyed();

    let lock_state = payloads
        .clone()
        .assume_ordering(nondet!(/** Process in arrival order */))
        .fold(
            q!(|| None),
            q!(|curr_holder, (client_id, virtual_client_id, acquire)| {
                if acquire {
                    // If the lock is currently held by the server, give the client the lock
                    if curr_holder.is_none() {
                        *curr_holder = Some((client_id.clone(), virtual_client_id.clone()));
                    }
                } else {
                    // If the client is releasing the lock and it holds it, give the lock back to the server
                    if let Some((holder_client_id, holder_virtual_client_id)) = curr_holder.as_ref()
                    {
                        if *holder_client_id == client_id
                            && *holder_virtual_client_id == virtual_client_id
                        {
                            *curr_holder = None;
                        }
                    }
                }
            }),
        );

    // TODO
    let acquire_results = sliced! {
        let payloads = use::atomic(payloads.clone(), nondet!(/** Payloads at this tick */));
        let locks_snapshot = use::atomic(lock_state, nondet!(/** Snapshot of lock state at this tick */));

        let acquires = payloads.filter(q!(|(_, _, acquire)| *acquire));
        locks_snapshot.get_many_if_present(acquires)
    }
    .map(q!(|(curr_holder, (client_id, virtual_client_id, _acquire))| {
        let lock_acquired = curr_holder.is_some_and(|(holder_client_id, holder_virtual_client_id)| {
            holder_client_id == client_id && holder_virtual_client_id == virtual_client_id
        });
        (client_id, virtual_client_id, lock_acquired)
    }))
    .entries()
    .map(q!(|(lock_id, (client_id, virtual_client_id, acquired))| (client_id, (virtual_client_id, lock_id, acquired))))
    .into_keyed();

    let release_results = payloads
        .end_atomic()
        .entries()
        .filter_map(q!(|(lock_id, (client_id, virtual_client_id, acquire))| {
            if !acquire {
                Some((client_id, (virtual_client_id, lock_id)))
            } else {
                None
            }
        }))
        .into_keyed();
    (acquire_results, release_results)
}
