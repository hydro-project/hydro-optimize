use hydro_lang::{
    live_collections::stream::NoOrder,
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{KeyedStream, Process, Unbounded},
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
    let keyed_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, lock_id, acquire))| (
            lock_id,
            (client_id, virt_client_id, acquire)
        )))
        .into_keyed();

    let batched_payloads = keyed_payloads
        .assume_ordering(nondet!(/** For each key, the first to acquire the lock wins */));
    let lock_state = batched_payloads.clone().across_ticks(|stream| {
        stream.reduce(q!(
            |(curr_client_id, curr_virt_client_id, is_held_by_client),
             (client_id, virt_client_id, acquire)| {
                if acquire {
                    // If the lock is currently held by the server, give the client the lock
                    if !*is_held_by_client {
                        *curr_client_id = client_id;
                        *curr_virt_client_id = virt_client_id;
                        *is_held_by_client = true;
                    }
                } else {
                    // If the client is releasing the lock and it holds it, give the lock back to the server
                    if *is_held_by_client
                        && *curr_virt_client_id == virt_client_id
                        && *curr_client_id == client_id
                    {
                        *is_held_by_client = false;
                    }
                }
            }
        ))
    });
    let results = batched_payloads
        .cross_singleton(lock_state)
        .all_ticks()
        .map(q!(|(
            lock_id,
            (
                (client_id, virt_client_id, acquire),
                (curr_client_id, curr_virt_client_id, is_held_by_client),
            ),
        )| {
            if acquire {
                let acquired = is_held_by_client
                    && curr_client_id == client_id
                    && curr_virt_client_id == virt_client_id;
                (client_id, (virt_client_id, lock_id, acquired))
            } else {
                // Releasing always succeeds
                (client_id, (virt_client_id, lock_id, true))
            }
        }));
    results
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
        let payloads = use::atomic(payloads, nondet!(/** Payloads at this tick */));
        let locks_snapshot = use::atomic(lock_state, nondet!(/** Snapshot of lock state at this tick */));

        payloads.clone()
            .filter(q!(|(_, (_, _, acquire))| acquire))
            .cross_singleton(locks_snapshot)
    };

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
