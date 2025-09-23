use hydro_lang::{
    live_collections::stream::NoOrder,
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{Cluster, KeyedStream, Unbounded},
};
use sha2::{Digest, Sha256};
use stageleft::q;

pub struct Client {}
pub struct Server {}

pub trait GraphFunction<'a>:
    Fn(
    &Cluster<'a, Server>,
    KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>
{
}

impl<'a, F> GraphFunction<'a> for F where
    F: Fn(
        &Cluster<'a, Server>,
        KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
    )
        -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>
{
}

fn sha256(n: u32) -> u32 {
    let start_time = std::time::Instant::now();
    let mut sha_input = n;

    loop {
        let mut sha = Sha256::new();
        sha.update(sha_input.to_be_bytes());
        let sha_output = sha.finalize();
        sha_input = sha_output[0].into();
        if start_time.elapsed().as_micros() >= n.into() {
            break;
        }
    }

    sha_input
}

// Note: H = high load, L = low load

pub fn get_graph_function<'a>(name: &str) -> impl GraphFunction<'a> {
    match name {
        "map_h_map_h_map_h" => map_h_map_h_map_h,
        "map_h_map_h_map_l" => map_h_map_h_map_l,
        "map_h_map_l_map_h" => map_h_map_l_map_h,
        "map_l_map_h_map_h" => map_l_map_h_map_h,
        "map_h_map_l_map_l" => map_h_map_l_map_l,
        "map_l_map_h_map_l" => map_l_map_h_map_l,
        "map_l_map_l_map_h" => map_l_map_l_map_h,
        "map_l_map_l_map_l" => map_l_map_l_map_l,
        "map_l_first_map_l_second_union" => map_l_first_map_l_second_union,
        "map_l_first_map_h_second_union" => map_l_first_map_h_second_union,
        "map_h_first_map_l_second_union" => map_h_first_map_l_second_union,
        "map_h_first_map_h_second_union" => map_h_first_map_h_second_union,
        "map_l_map_l_first_payload_second_union" => map_l_map_l_first_payload_second_union,
        "map_l_map_h_first_payload_second_union" => map_l_map_h_first_payload_second_union,
        "map_h_map_l_first_payload_second_union" => map_h_map_l_first_payload_second_union,
        "map_h_map_h_first_payload_second_union" => map_h_map_h_first_payload_second_union,
        "map_l_first_payload_second_union_map_l" => map_l_first_payload_second_union_map_l,
        "map_l_first_payload_second_union_map_h" => map_l_first_payload_second_union_map_h,
        "map_h_first_payload_second_union_map_l" => map_h_first_payload_second_union_map_l,
        "map_h_first_payload_second_union_map_h" => map_h_first_payload_second_union_map_h,
        "map_l_first_map_l_second_anti_join" => map_l_first_map_l_second_anti_join,
        "map_l_first_map_h_second_anti_join" => map_l_first_map_h_second_anti_join,
        "map_h_first_map_l_second_anti_join" => map_h_first_map_l_second_anti_join,
        "map_h_first_map_h_second_anti_join" => map_h_first_map_h_second_anti_join,
        "map_l_map_l_first_payload_second_anti_join" => map_l_map_l_first_payload_second_anti_join,
        "map_l_map_h_first_payload_second_anti_join" => map_l_map_h_first_payload_second_anti_join,
        "map_h_map_l_first_payload_second_anti_join" => map_h_map_l_first_payload_second_anti_join,
        "map_h_map_h_first_payload_second_anti_join" => map_h_map_h_first_payload_second_anti_join,
        "map_l_first_payload_second_anti_join_map_l" => map_l_first_payload_second_anti_join_map_l,
        "map_l_first_payload_second_anti_join_map_h" => map_l_first_payload_second_anti_join_map_h,
        "map_h_first_payload_second_anti_join_map_l" => map_h_first_payload_second_anti_join_map_l,
        "map_h_first_payload_second_anti_join_map_h" => map_h_first_payload_second_anti_join_map_h,
        _ => unimplemented!(),
    }
}

pub fn map_h_map_h_map_h<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
}

pub fn map_h_map_h_map_l<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
}

pub fn map_h_map_l_map_h<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
}

pub fn map_l_map_h_map_h<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
}

pub fn map_h_map_l_map_l<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
}

pub fn map_l_map_h_map_l<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
}

pub fn map_l_map_l_map_h<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(100 + n % 2)
        )))
}

pub fn map_l_map_l_map_l<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(virt_client_id, n)| (
            virt_client_id,
            self::sha256(n % 2 + 1)
        )))
}

pub fn map_l_first_map_l_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let map_l1 = payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(n % 2 + 1))));
    let map_l2 = payloads.map(q!(|(virt_client_id, n)| (
        Some(virt_client_id),
        self::sha256(n % 2 + 1)
    )));
    map_l1
        .interleave(map_l2)
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_l_first_map_h_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let map_l1 = payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(n % 2 + 1))));
    let map_h2 = payloads.map(q!(|(virt_client_id, n)| (
        Some(virt_client_id),
        self::sha256(100 + n % 2)
    )));
    map_l1
        .interleave(map_h2)
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_h_first_map_l_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let map_h1 = payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(100 + n % 2))));
    let map_l2 = payloads.map(q!(|(virt_client_id, n)| (
        Some(virt_client_id),
        self::sha256(n % 2 + 1)
    )));
    map_h1
        .interleave(map_l2)
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_h_first_map_h_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let map_h1 = payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(100 + n % 2))));
    let map_h2 = payloads.map(q!(|(virt_client_id, n)| (
        Some(virt_client_id),
        self::sha256(100 + n % 2)
    )));
    map_h1
        .interleave(map_h2)
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_l_map_l_first_payload_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(n % 2 + 1))))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_l_map_h_first_payload_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(n % 2 + 1)
        )))
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(100 + n % 2))))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_h_map_l_first_payload_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(n % 2 + 1))))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_h_map_h_first_payload_second_union<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(100 + n % 2)
        )))
        .map(q!(|(_virt_client_id, n)| (None, self::sha256(100 + n % 2))))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, n))
            } else {
                None
            }
        }))
}

pub fn map_l_first_payload_second_union_map_l<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(n % 2 + 1)
        )))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            let sha = self::sha256(n % 2 + 1);
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, sha))
            } else {
                None
            }
        }))
}

pub fn map_l_first_payload_second_union_map_h<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(n % 2 + 1)
        )))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            let sha = self::sha256(100 + n % 2);
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, sha))
            } else {
                None
            }
        }))
}

pub fn map_h_first_payload_second_union_map_l<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(100 + n % 2)
        )))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            let sha = self::sha256(n % 2 + 1);
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, sha))
            } else {
                None
            }
        }))
}

pub fn map_h_first_payload_second_union_map_h<'a>(
    _server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    payloads
        .clone()
        .map(q!(|(_virt_client_id, n)| (
            None::<u32>,
            self::sha256(100 + n % 2)
        )))
        .interleave(payloads.map(q!(|(virt_client_id, n)| (Some(virt_client_id), n))))
        .filter_map(q!(|(virt_client_id_opt, n)| {
            let sha = self::sha256(100 + n % 2);
            // Since we cloned payloads, delete half the payloads so 1 input = 1 output
            if let Some(virt_client_id) = virt_client_id_opt {
                Some((virt_client_id, sha))
            } else {
                None
            }
        }))
}

pub fn map_l_first_map_l_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let true_payloads = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            true,
            n
        )));
    let map_l1 = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(n % 2 + 1)
        )))
        .interleave(true_payloads) // The actual payloads that will pass the anti_join
        .batch(&tick, nondet);
    let map_l2 = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(n % 2 + 1)
        )))
        .batch(&tick, nondet);
    map_l1
        .filter_not_in(map_l2)
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, keep, n)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_l_first_map_h_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let true_payloads = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            true,
            n
        )));
    let map_l1 = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(n % 2 + 1)
        )))
        .interleave(true_payloads) // The actual payloads that will pass the anti_join
        .batch(&tick, nondet);
    let map_h2 = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(100 + n % 2)
        )))
        .batch(&tick, nondet);
    map_l1
        .filter_not_in(map_h2)
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, keep, n)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_h_first_map_l_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let true_payloads = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            true,
            n
        )));
    let map_h1 = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(100 + n % 2)
        )))
        .interleave(true_payloads) // The actual payloads that will pass the anti_join
        .batch(&tick, nondet);
    let map_l2 = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(n % 2 + 1)
        )))
        .batch(&tick, nondet);
    map_h1
        .filter_not_in(map_l2)
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, keep, n)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_h_first_map_h_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let true_payloads = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            true,
            n
        )));
    let map_h1 = payloads
        .clone()
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(100 + n % 2)
        )))
        .interleave(true_payloads) // The actual payloads that will pass the anti_join
        .batch(&tick, nondet);
    let map_h2 = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            false,
            self::sha256(100 + n % 2)
        )))
        .batch(&tick, nondet);
    map_h1
        .filter_not_in(map_h2)
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, keep, n)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_l_map_l_first_payload_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));
    false_payloads
        .clone()
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(n % 2 + 1),
            true
        )))
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(n % 2 + 1),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, n, keep)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_l_map_h_first_payload_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));
    false_payloads
        .clone()
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(n % 2 + 1),
            true
        )))
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(100 + n % 2),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, n, keep)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_h_map_l_first_payload_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));
    false_payloads
        .clone()
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(100 + n % 2),
            true
        )))
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(n % 2 + 1),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, n, keep)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_h_map_h_first_payload_second_anti_join<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));
    false_payloads
        .clone()
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(100 + n % 2),
            true
        )))
        .map(q!(|(client_id, virt_client_id, n, _keep)| (
            client_id,
            virt_client_id,
            self::sha256(100 + n % 2),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client_id, virt_client_id, n, keep)| {
            if keep {
                Some((client_id, (virt_client_id, n)))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_l_first_payload_second_anti_join_map_l<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));

    false_payloads
        .clone()
        .map(q!(|(client, virt_client_id, n, _keep)| (
            client,
            virt_client_id,
            self::sha256(n % 2 + 1),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.clone().batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client, virt_client_id, n, keep)| {
            if keep {
                Some((client, (virt_client_id, self::sha256(n % 2 + 1))))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_l_first_payload_second_anti_join_map_h<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));

    false_payloads
        .clone()
        .map(q!(|(client, virt_client_id, n, _keep)| (
            client,
            virt_client_id,
            self::sha256(n % 2 + 1),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.clone().batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client, virt_client_id, n, keep)| {
            if keep {
                Some((client, (virt_client_id, self::sha256(100 + n % 2))))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_h_first_payload_second_anti_join_map_l<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));

    false_payloads
        .clone()
        .map(q!(|(client, virt_client_id, n, _keep)| (
            client,
            virt_client_id,
            self::sha256(100 + n % 2),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.clone().batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client, virt_client_id, n, keep)| {
            if keep {
                Some((client, (virt_client_id, self::sha256(n % 2 + 1))))
            } else {
                None
            }
        }))
        .into_keyed()
}

pub fn map_h_first_payload_second_anti_join_map_h<'a>(
    server: &Cluster<'a, Server>,
    payloads: KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder>,
) -> KeyedStream<MemberId<Client>, (u32, u32), Cluster<'a, Server>, Unbounded, NoOrder> {
    let tick = server.tick();
    let nondet = nondet!(/** Test */);

    let false_payloads = payloads
        .entries()
        .map(q!(|(client_id, (virt_client_id, n))| (
            client_id,
            virt_client_id,
            n,
            false
        )));

    false_payloads
        .clone()
        .map(q!(|(client, virt_client_id, n, _keep)| (
            client,
            virt_client_id,
            self::sha256(100 + n % 2),
            true
        )))
        .interleave(false_payloads.clone())
        .batch(&tick, nondet)
        .filter_not_in(false_payloads.clone().batch(&tick, nondet))
        .all_ticks()
        .filter_map(q!(|(client, virt_client_id, n, keep)| {
            if keep {
                Some((client, (virt_client_id, self::sha256(100 + n % 2))))
            } else {
                None
            }
        }))
        .into_keyed()
}
