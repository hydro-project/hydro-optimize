use crate::flink_queries::flink_queries::{Auction, Bid, Person};
use hydro_lang::{
    live_collections::stream::NoOrder,
    prelude::{Cluster, KeyedStream, Unbounded},
};
use rand;

use stageleft::q;

pub fn bid_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<u32, Option<Bid>, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: (b.auction + 1) % 50,
                bidder: b.bidder + 1,
                price: b.price + 10,
                date_time: b.date_time + 1,
                extra: b.extra,
                channel: b.channel,
                url: b.url,
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 100,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

// Usable for queries_bench_dual for queries with an auction input stream
pub fn auction_workload_generator<'a, Client>(
    ids: KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Auction, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.map_with_key(q!(|(id, _)| {
        Auction {
            id: (id % 4) as i64, // foreign key with Bid.auction
            item_name: "".to_string(),
            description: "".to_string(),
            initial_bid: rand::random_range(0..100),
            reserve: 0,
            date_time: id as i64,
            expires: 0,
            seller: (id % 4) as i64,             // foreign key with Person.id
            category: rand::random_range(9..11), // q3 & q20 filters on = 10
            extra: "".to_string(),
        }
    }))
}

// Usable for queries_bench_dual for queries with an auction input stream
pub fn bid_workload_generator_no_prev<'a, Client>(
    ids: KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.map_with_key(q!(|(id, _)| {
        Bid {
            auction: (id % 4) as i64, // foreign key with Auction.id
            bidder: (id + 1 % 4) as i64,
            price: rand::random_range(5000..10000),
            date_time: (id + 7) as i64,
            extra: "".to_string(),
            channel: format!("Channel {}", id).to_string(),
            url: format!("{}/{}/{}/{}/{}/{}/{}", id, id, id, id, id, id, id).to_string(),
        }
    }))
}

// Usable for queries_bench_dual for queries with a person input stream
pub fn person_workload_generator<'a, Client>(
    ids: KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Person, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.map_with_key(q!(|(id, _)| {
        Person {
            id: (id % 4) as i64, // foreign key with Auction.seller
            name: format!("John {}", id).to_string(),
            email: "john.doe@berkeley.edu".to_string(),
            credit_card: "0123456789".to_string(),
            city: ["Oakland", "Berkeley", "Seattle", "Fort Lauderdale"][rand::random_range(0..4)]
                .to_string(),
            state: ["OR", "CA", "ID", "KT"][rand::random_range(0..4)].to_string(), // q3 filters on in [OR, CA, ID]
            date_time: rand::random_range(100..1000),
            extra: "".to_string(),
        }
    }))
}

pub fn query2_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<u32, Option<(i64, i64)>, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: 123,
                bidder: b.0 + 10,
                price: b.1 + 11,
                date_time: b.0 + 3,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 0,
                bidder: 100,
                price: 11,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query11_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i32, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Output corresponds to (bidder, aggregation count, datetim start, datetime end)
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: 1,
                price: b.0 + 15,
                date_time: b.2 + 7,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 1,
                price: 100,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query14_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, f64, String, i64, String, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: b.1 + 10,
                price: b.2 as i64 + 10,
                date_time: (b.4 + 3) % 24,
                extra: b.5,
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 40000000,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query17_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, i64, i64, i64, i32, i64, i64, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // (auction, datetime, ... [aggregation data])
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: (b.0 + 1) % 3,
                bidder: b.0 + 1,
                price: b.4 + 10000,
                date_time: (b.1 + 1) % 3,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 9000,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query18_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        // (auction, bidder, datetime, price)
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: b.1 + 10,
                price: b.3 as i64 + 15,
                date_time: (b.2 + 3) % 125,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 100,
                date_time: 0,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        }
    }))
}

pub fn query19_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(usize, i64, i64)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.1,
                bidder: b.0 as i64 + 11,
                price: b.2 + 10,
                date_time: (b.2 + 5) % 125,
                extra: "".to_string(),
                channel: "".to_string(),
                url: "".to_string(),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 100,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a".to_string(),
            }
        }
    }))
}

pub fn query22_workload_generator<'a, Client>(
    ids_and_prev: KeyedStream<
        u32,
        Option<(i64, i64, i64, String, String, String, String)>,
        Cluster<'a, Client>,
        Unbounded,
        NoOrder,
    >,
) -> KeyedStream<u32, Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids_and_prev.map(q!(|payload| {
        if let Some(b) = payload {
            Bid {
                auction: b.0 + 1,
                bidder: b.1 + 10,
                price: b.2 + 15,
                date_time: b.0 + 123,
                extra: "".to_string(),
                channel: "".to_string(),
                url: format!("{}a/{}b/{}c/{}d/{}e/{}f/", b.4, b.5, b.6, b.4, b.5, b.6),
            }
        } else {
            Bid {
                auction: 1,
                bidder: 100,
                price: 150,
                date_time: 0,
                extra: "a".to_string(),
                channel: "test".to_string(),
                url: "a/b/c/d/e/f".to_string(),
            }
        }
    }))
}
