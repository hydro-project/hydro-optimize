use crate::flink_queries::flink_queries::{Auction, Bid, Person};
use hydro_lang::{
    live_collections::stream::{NoOrder, TotalOrder},
    nondet::nondet,
    prelude::{Cluster, Stream, Unbounded},
};
use rand::RngExt;

use stageleft::q;

// Empty workload generators for queries who don't need that input

pub fn auction_workload_generator_empty<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Auction, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.map(q!(|_| {
        Auction {
            id: 0,
            item_name: "".to_string(),
            description: "".to_string(),
            initial_bid: 0,
            reserve: 0,
            date_time: 0,
            expires: 0,
            seller: 0,
            category: 0,
            extra: "".to_string(),
        }
    }))
}

pub fn bid_workload_generator_empty<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.map(q!(|_| {
        Bid {
            auction: 0,
            bidder: 0,
            price: 0,
            date_time: 0,
            extra: "".to_string(),
            channel: "".to_string(),
            url: "".to_string(),
        }
    }))
}

pub fn person_workload_generator_empty<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Person, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.map(q!(|_| {
        Person {
            id: 0,
            name: "".to_string(),
            email: "".to_string(),
            credit_card: "".to_string(),
            city: "".to_string(),
            state: "".to_string(),
            date_time: 0,
            extra: "".to_string(),
        }
    }))
}

// Query-specific generators

pub fn q1_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Maps price
    ids.map(q!(|_| {
        Bid {
            auction: 0,
            bidder: 0,
            price: rand::rng().random_range(0..1000),
            date_time: 0,
            extra: "".to_string(),
            channel: "".to_string(),
            url: "".to_string(),
        }
    }))
}

pub fn q2_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on auction % 123 == 0
    ids.map(q!(|_| {
        Bid {
            auction: rand::rng().random_range(121..=123),
            bidder: 0,
            price: rand::rng().random_range(0..1000),
            date_time: 0,
            extra: "".to_string(),
            channel: "".to_string(),
            url: "".to_string(),
        }
    }))
}

pub fn q3_auction_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Auction, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on category = 10 and joins seller with person.id
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Auction {
                    id: *acc as i64,
                    item_name: "".to_string(),
                    description: "".to_string(),
                    initial_bid: rand::random_range(0..100),
                    reserve: 0,
                    date_time: *acc as i64,
                    expires: 0,
                    seller: (*acc % 3) as i64, // foreign key with Person.id
                    category: rand::random_range(9..=10),
                    extra: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q3_person_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Person, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on state in ["OR", "CA", "ID"] and joins id with auction.seller
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Person {
                    id: (*acc % 3) as i64,
                    name: format!("John Doe {}", acc).to_string(),
                    email: "".to_string(),
                    credit_card: "".to_string(),
                    city: format!("City {}", acc).to_string(),
                    state: ["OR", "CA", "ID", "KT"][*acc % 4].to_string(),
                    date_time: 0,
                    extra: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q11_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Tracks # bids per bidder in a timed session
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Bid {
                    auction: rand::rng().random_range(121..=123),
                    bidder: *acc / 5 % 3, // Try to maintain same bidder in a row but also ensure 3 bidders exist
                    price: rand::rng().random_range(0..1000),
                    date_time: *acc + 3,
                    extra: "".to_string(),
                    channel: "".to_string(),
                    url: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q14_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on price and transforms based on datetime
    ids.map(q!(|_| {
        let price = if rand::rng().random_bool(0.8) {
            rand::rng().random_range(1200000..10000000)
        } else {
            rand::rng().random_range(0..1000000)
        };
        let date_time = match rand::rng().random_range(0..3) {
            0 => rand::rng().random_range(8..=18),
            1 => rand::rng().random_range(0..=6),
            _ => rand::rng().random_range(19..=20),
        };
        let extra = if rand::rng().random_bool(0.5) {
            "abcabc".to_string()
        } else {
            "aaaaaa".to_string()
        };
        Bid {
            auction: rand::rng().random_range(0..10),
            bidder: rand::rng().random_range(0..10),
            price: price,
            date_time: date_time,
            extra: extra,
            channel: "".to_string(),
            url: "".to_string(),
        }
    }))
}

pub fn q17_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // aggregation on key (auction, datetime)
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Bid {
                    auction: *acc % 5,
                    bidder: *acc / 5 % 3, // Try to maintain same bidder in a row but also ensure 3 bidders exist
                    price: match rand::rng().random_range(0..3) {
                        0 => rand::rng().random_range(0..10000),
                        1 => rand::rng().random_range(10000..1000000),
                        _ => rand::rng().random_range(1000000..5000000),
                    },
                    date_time: (*acc / 5) % 5,
                    extra: "".to_string(),
                    channel: "".to_string(),
                    url: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q18_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // What's a's last bid for bidder to auction?
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Bid {
                    auction: *acc % 5,
                    bidder: *acc / 5 % 5,
                    price: rand::rng().random_range(0..10000),
                    date_time: *acc,
                    extra: "".to_string(),
                    channel: "".to_string(),
                    url: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q19_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Determine highest price
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Bid {
                    auction: *acc % 5,
                    bidder: *acc / 5 % 5,
                    price: *acc * 1000 + rand::rng().random_range(0..5000),
                    date_time: *acc,
                    extra: "".to_string(),
                    channel: "".to_string(),
                    url: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q20_auction_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Auction, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on category = 10 and joins bid with bid.auction
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Auction {
                    id: *acc % 10 as i64,
                    item_name: "".to_string(),
                    description: "".to_string(),
                    initial_bid: rand::random_range(0..100),
                    reserve: 0,
                    date_time: *acc as i64,
                    expires: 0,
                    seller: (*acc % 3) as i64,
                    category: rand::random_range(9..=10),
                    extra: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q20_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Joins with auction on .auction = auction.id
    ids.map(q!(|_| {
        Bid {
            auction: rand::rng().random_range(0..10),
            bidder: rand::rng().random_range(0..10),
            price: rand::rng().random_range(0..10),
            date_time: rand::rng().random_range(0..10),
            extra: "".to_string(),
            channel: "".to_string(),
            url: "".to_string(),
        }
    }))
}

pub fn q22_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Splits url based on "/" and accesses up to [5]
    ids.map(q!(|_| {
        let dirs = ["a", "b", "c", "d"];
        let d1 = dirs[rand::rng().random_range(0..4)];
        let d2 = dirs[rand::rng().random_range(0..4)];
        let d3 = dirs[rand::rng().random_range(0..4)];
        Bid {
            auction: rand::rng().random_range(0..10),
            bidder: rand::rng().random_range(0..10),
            price: rand::rng().random_range(0..10),
            date_time: rand::rng().random_range(0..10),
            extra: "".to_string(),
            channel: "".to_string(),
            url: format!("http://site.com/{}/{}/{}", d1, d2, d3).to_string(),
        }
    }))
}

pub fn q23_auction_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Auction, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on category = 10 and joins bid with bid.auction
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Auction {
                    id: (*acc + 2) % 5 as i64,
                    item_name: "".to_string(),
                    description: "".to_string(),
                    initial_bid: rand::random_range(0..100),
                    reserve: 0,
                    date_time: *acc as i64,
                    expires: 0,
                    seller: (*acc % 5) as i64,
                    category: rand::random_range(9..=10),
                    extra: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q23_bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on category = 10 and joins bid with bid.auction
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Bid {
                    auction: rand::rng().random_range(0..10),
                    bidder: *acc % 5,
                    price: rand::rng().random_range(0..10),
                    date_time: rand::rng().random_range(0..10),
                    extra: "".to_string(),
                    channel: "".to_string(),
                    url: "".to_string(),
                })
            }),
        )
        .into()
}

pub fn q23_person_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Person, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Filters on category = 10 and joins bid with bid.auction
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(|acc, _| {
                *acc += 1;
                Some(Person {
                    id: *acc % 5,
                    name: format!("John Doe {}", acc).to_string(),
                    email: "".to_string(),
                    credit_card: "".to_string(),
                    city: format!("City {}", acc).to_string(),
                    state: "OR".to_string(),
                    date_time: 0,
                    extra: "".to_string(),
                })
            }),
        )
        .into()
}
