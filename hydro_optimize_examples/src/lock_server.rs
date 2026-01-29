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
    let atomic_releases = releases.atomic(&server_tick);
    let mapped_releases = atomic_releases
        .clone()
        .entries()
        .map(q!(|(_client_id, (_virtual_client_id, lock_id))| lock_id));

    let acquire_results = sliced! {
        let mapped_acquires = use(mapped_acquires, nondet!(/** For each lock, pick a random acquire as the winner */));
        let mapped_releases = use::atomic(mapped_releases, nondet!(/** For each lock, only one release should succeed (the owner of the lock) */));

        // lock_id
        // TODO: Replace with KeyedSingleton when support is added
        // (lock_id, (client_id, virtual_client_id))
        let mut acquired_locks = use::state_null::<Stream<(u32, (MemberId<Client>, u32)), _, _, NoOrder>>();
        let keyed_acquired_locks = acquired_locks.into_keyed().assume_ordering(nondet!(/** Actually KeyedSingleton */)).first();

        // Always process releases first. Find out which locks still can't be acquired
        let curr_acquired_locks = keyed_acquired_locks
            .clone()
            .filter_key_not_in(mapped_releases); // Only correct if non-lock holders don't release locks they don't own

        // For each lock, pick a random acquire as the winner
        let winning_acquires = mapped_acquires.clone().assume_ordering(nondet!(/** Randomly pick one acquire to be the winner */)).first();

        // Acquires win for all non-acquired locks, even ones that don't exist yet
        let newly_acquired_locks = winning_acquires.filter_key_not_in(curr_acquired_locks.clone().keys());
        acquired_locks = curr_acquired_locks.entries().chain(newly_acquired_locks.clone().entries());

        let acquire_requests = mapped_acquires.entries().map(q!(|(lock_id, (client_id, virtual_client_id))| ((client_id, virtual_client_id), lock_id))).into_keyed();
        acquire_requests.lookup_keyed_singleton(newly_acquired_locks)
    }
    .entries()
    .map(q!(|((client_id, virtual_client_id), (lock_id, curr_holder))| {
        let lock_acquired = curr_holder.is_some_and(|(holder_client_id, holder_virtual_client_id)| {
            holder_client_id == client_id && holder_virtual_client_id == virtual_client_id
        });
        (client_id, (virtual_client_id, lock_id, lock_acquired))
    }))
    .into_keyed();

    (acquire_results, atomic_releases.end_atomic())
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
    let atomic_acquires = acquires.atomic(&server_tick);
    let lock_id_acquires = atomic_acquires
        .entries()
        .map(q!(|(client_id, (virtual_client_id, lock_id))| (
            lock_id,
            (client_id, virtual_client_id, true)
        )))
        .into_keyed();
    let atomic_releases = releases.atomic(&server_tick);
    let lock_id_releases = atomic_releases
        .clone()
        .entries()
        .map(q!(|(client_id, (virtual_client_id, lock_id))| (
            lock_id,
            (client_id, virtual_client_id, false)
        )))
        .into_keyed();

    let lock_state = lock_id_acquires
        .clone()
        .interleave(lock_id_releases)
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

    // Check if acquires succeeded
    let acquire_results = sliced! {
        let acquires = use::atomic(lock_id_acquires, nondet!(/** Payloads at this tick */));
        let locks_snapshot = use::atomic(lock_state, nondet!(/** Snapshot of lock state at this tick */));

        // Note: join_keyed_singleton is guaranteed to not miss, since an acquire/release must've been processed first
        acquires.join_keyed_singleton(locks_snapshot)
    }
    .entries()
    .map(q!(|(lock_id, ((client_id, virtual_client_id, _acquire), curr_holder))| {
        let lock_acquired = curr_holder.is_some_and(|(holder_client_id, holder_virtual_client_id)| {
            holder_client_id == client_id && holder_virtual_client_id == virtual_client_id
        });
        (client_id, (virtual_client_id, lock_id, lock_acquired))
    }))
    .into_keyed();

    (acquire_results, atomic_releases.end_atomic())
}
