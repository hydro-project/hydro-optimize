use crate::flink_queries::flink_queries::{Auction, Bid, Person};
use hydro_lang::{
    live_collections::stream::{NoOrder, TotalOrder},
    nondet::nondet,
    prelude::{Cluster, Stream, Unbounded},
};
use rand::RngExt;

use stageleft::q;

// Consts from https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java#L36
const FIRST_AUCTION_ID: i64 = 1000;
const FIRST_PERSON_ID: i64 = 1000;
const FIRST_CATEGORY_ID: i64 = 10;

// Used to restrain ID domain to ensure some joins succeed
const NUM_PERSONS: i64 = 500;
const NUM_AUCTIONS: i64 = 1000;

// Empty workload generators for queries who don't need its input

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

// Randomly-generating generators

pub fn auction_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Auction, Cluster<'a, Client>, Unbounded, NoOrder> {
    // Const from https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/AuctionGenerator.java#L32
    const NUM_CATEGORIES: i64 = 5;

    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(move |acc, _| {
                *acc += 1;
                let initial = self::price_generator();
                Some(Auction {
                    id: FIRST_AUCTION_ID + *acc,
                    item_name: self::string_generator(20, ' '),
                    description: self::string_generator(100, ' '),
                    initial_bid: initial,
                    reserve: initial + self::price_generator(),
                    date_time: *acc,
                    expires: *acc + rand::rng().random_range(0..100),
                    seller: FIRST_PERSON_ID + *acc % NUM_PERSONS,
                    category: FIRST_CATEGORY_ID + rand::rng().random_range(0..NUM_CATEGORIES),
                    extra: self::string_generator(100, '_'),
                })
            }),
        )
        .into()
}

pub fn bid_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Bid, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(move |acc, _| {
                *acc += 1;

                Some(Bid {
                    auction: FIRST_AUCTION_ID + *acc % NUM_AUCTIONS,
                    bidder: FIRST_PERSON_ID + *acc % NUM_PERSONS,
                    price: self::price_generator(),
                    date_time: *acc + 7,
                    extra: self::string_generator(32, ' '),
                    channel: self::string_generator(7, ' '),
                    url: self::url_generator(),
                })
            }),
        )
        .into()
}

pub fn person_workload_generator<'a, Client>(
    ids: Stream<u64, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> Stream<Person, Cluster<'a, Client>, Unbounded, NoOrder> {
    ids.assume_ordering::<TotalOrder>(nondet!(/** Processing order doesn't matter **/))
        .scan(
            q!(|| 0),
            q!(move |acc, _| {
                *acc += 1;

                // Consts from https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PersonGenerator.java#L40
                let states: Vec<&str> = "AZ,CA,ID,OR,WA,WY".split(",").collect();
                let cities: Vec<&str> =
        "Phoenix,Los Angeles,San Francisco,Boise,Portland,Bend,Redmond,Seattle,Kent,Cheyenne"
            .split(",")
            .collect();
                Some(Person {
                    id: FIRST_PERSON_ID + *acc % NUM_PERSONS,
                    name: format!(
                        "{} {}",
                        self::string_generator(7, '-'),
                        self::string_generator(7, '-')
                    ),
                    email: format!(
                        "{}@{}.com",
                        self::string_generator(7, '_'),
                        self::string_generator(5, '_')
                    ),
                    credit_card: self::cc_generator(),
                    city: cities[rand::rng().random_range(0..cities.len())].to_string(),
                    state: states[rand::rng().random_range(0..states.len())].to_string(),
                    date_time: *acc,
                    extra: self::string_generator(100, '_'),
                })
            }),
        )
        .into()
}

// Field-specific generators

pub fn string_generator(max_length: i64, special: char) -> String {
    // Const from https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/StringsGenerator.java#L26
    const MIN_STRING_LENGTH: i64 = 3;
    let len = MIN_STRING_LENGTH + rand::rng().random_range(0..(max_length - MIN_STRING_LENGTH));
    let mut sb = String::with_capacity(len as usize);

    for _ in 0..len {
        if rand::rng().random_range(0..13) == 0 {
            sb.push(special);
        } else {
            sb.push(('a' as u8 + rand::rng().random_range(0..26)) as char);
        }
    }
    sb.trim().to_string()
}

pub fn cc_generator() -> String {
    let v: Vec<u32> = (0..4)
        .map(|_| rand::rng().random_range(1000..=9999))
        .collect();
    format!("{}-{}-{}-{}", v[0], v[1], v[2], v[3])
}

pub fn url_generator() -> String {
    format!(
        "https://www.nexmark.com/{}/{}/{}/item.htm?query=1",
        string_generator(5, '_'),
        string_generator(5, '_'),
        string_generator(5, '_'),
    )
    .to_string()
}

pub fn price_generator() -> i64 {
    // From https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PriceGenerator.java
    let x: f64 = rand::rng().random();
    (10.0f64.powf(x * 6.0) * 100.0).round() as i64
}
