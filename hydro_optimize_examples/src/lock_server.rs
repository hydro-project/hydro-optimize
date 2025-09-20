use hydro_lang::{
    live_collections::stream::NoOrder,
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{Process, Stream, Unbounded},
};
use stageleft::q;

pub struct Server {}

/// Lock server implementation as described in https://dl.acm.org/doi/pdf/10.1145/3341301.3359651, with the difference being that each server can hold multiple locks.
/// Clients send (virt_client_id, server_id, acquire) requesting a lock from the server.
///
/// If acquire = true, then:
/// - If the server currently holds the lock, it returns (virt_client_id, server_id, true).
/// - Otherwise, it returns (virt_client_id, server_id, false).
///
/// If acquire = false, then the client wants to release its lock. Return (virt_client_id, server_id, true).
#[expect(clippy::type_complexity, reason = "internal Lock Server code // TODO")]
pub fn lock_server<'a, Client>(
    server: &Process<'a, Server>,
    payloads: Stream<(MemberId<Client>, (u32, u32, bool)), Process<'a, Server>, Unbounded, NoOrder>,
) -> Stream<(MemberId<Client>, (u32, u32, bool)), Process<'a, Server>, Unbounded, NoOrder> {
    let server_tick = server.tick();
    let keyed_payloads = payloads.map(q!(|(client_id, (virt_client_id, server_id, acquire))| (
        server_id,
        (client_id, virt_client_id, acquire)
    )));

    let batched_payloads = keyed_payloads
        .batch(
            &server_tick,
            nondet!(/** Need to check who currently owns the lock */),
        )
        .assume_ordering(nondet!(/** First to acquire the lock wins */));
    let lock_state = batched_payloads
        .clone()
        .persist()
        .into_keyed()
        .reduce(q!(
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
        .entries();
    let results = batched_payloads.join(lock_state).all_ticks().map(q!(|(
        server_id,
        (
            (client_id, virt_client_id, acquire),
            (curr_client_id, curr_virt_client_id, is_held_by_client),
        ),
    )| {
        if acquire {
            let acquired = is_held_by_client
                && curr_client_id == client_id
                && curr_virt_client_id == virt_client_id;
            (client_id, (virt_client_id, server_id, acquired))
        } else {
            // Releasing always succeeds
            (client_id, (virt_client_id, server_id, true))
        }
    }));
    results
}
