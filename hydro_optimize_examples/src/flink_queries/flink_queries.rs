use hydro_lang::{live_collections::stream::NoOrder, prelude::*};
use serde::{Deserialize, Serialize};

pub struct Queries;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Auction {
    pub id: i64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: i64,
    pub reserve: i64,
    pub date_time: i64,
    pub expires: i64,
    pub seller: i64,
    pub category: i64,
    pub extra: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Person {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: i64,
    pub extra: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Bid {
    pub auction: i64,
    pub bidder: i64,
    pub price: i64,
    pub channel: String,
    pub url: String,
    pub date_time: i64,
    pub extra: String,
}

/*
    Queries from https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries
*/

pub fn q1<'a, PerformanceKey>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>
where
    PerformanceKey: Clone,
{
    /*
        Convert each bid value from dollars to euros. Illustrates a simple transformation.
    */
    bid_stream.map(q!(|mut bid_obj| {
        bid_obj.price = (bid_obj.price as f64 * 0.908) as i64;
        bid_obj
    }))
}

pub fn q2<'a, PerformanceKey>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        Find bids with specific auction ids and show their bid price.
    */
    bid_stream.filter_map(q!(|bid_obj| {
        if bid_obj.auction % 123 == 0 {
            Some((bid_obj.auction, bid_obj.price))
        } else {
            None
        }
    }))
}

pub fn q3<'a, PerformanceKey>(
    auction_stream: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    person_stream: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    (String, String, String, i64),
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        Who is selling in OR, ID or CA in category 10, and for what auction ids?
        Illustrates an incremental join (using per-key state and timer) and filter.
    */
    let cat10_auctions = auction_stream.entries().filter_map(q!(|(pk, a)| {
        if a.category == 10 {
            Some((a.seller, (a, pk)))
        } else {
            None
        }
    }));
    let or_id_ca_persons = person_stream.entries().filter_map(q!(|(pk, p)| {
        if p.state == "OR" || p.state == "CA" || p.state == "ID" {
            Some((p.id, (p, pk)))
        } else {
            None
        }
    }));

    or_id_ca_persons
        .join(cat10_auctions)
        .map(q!(|p_a| {
            let (p_pk, a_pk) = p_a.1;
            (p_pk.1, (p_pk.0.name, p_pk.0.city, p_pk.0.state, a_pk.0.id))
        }))
        .into_keyed()
}

pub fn q11<'a, PerformanceKey: Ord + Clone>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (i64, i32, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        How many bids did a user make in each session they were active? Illustrates session windows.
        Group bids by the same user into sessions with max session gap.
        Emit the number of bids per session.
    */
    let tick = bid_stream.location().tick();
    let bid_stream_reformat = bid_stream.entries();

    bid_stream_reformat
        .map(q!(|(pk, bid)| (bid.bidder, (bid.date_time, pk))))
        .into_keyed()
        .batch(&tick, nondet!(/** test */))
        .sort()
        .across_ticks(|s| {
            s.fold(
                q!(|| (None, vec![])),
                q!(|state, (current_time, pk)| {
                    let (current_session, completed) = state;

                    match current_session {
                        None => {
                            *current_session = Some((current_time, current_time, 1, pk.clone()));
                        }
                        Some((start, last, count, pk_prev)) => {
                            let gap = current_time - *last;

                            if gap <= 10 {
                                *last = current_time;
                                *count += 1;
                            } else {
                                completed.push((*start, *last, *count, pk_prev.clone()));
                                *current_session =
                                    Some((current_time, current_time, 1, pk_prev.clone()));
                            }
                        }
                    }
                }),
            )
        })
        .entries()
        .all_ticks()
        .flat_map_unordered(q!(|(bidder, (_current, sessions))| {
            let mut out = vec![];
            for (start, end, count, pk) in sessions {
                out.push((pk, (bidder, count, start, end)));
            }
            out
        }))
        .into_keyed()
}

pub fn q14<'a, PerformanceKey>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    (i64, i64, f64, String, i64, String, i64),
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        Convert bid timestamp into types and find bids with specific price.
        Illustrates duplicate expressions and usage of user-defined-functions.
    */
    bid_stream.filter_map(q!(|bid| {
        if bid.price as f64 * 0.908 > 1000000.0 && bid.price as f64 * 0.908 < 50000000.0 {
            let mut bid_time_type = "otherTime";
            let bid_hour = bid.date_time; // Using just seconds for testing
            if bid_hour >= 8 && bid_hour <= 18 {
                bid_time_type = "dayTime";
            } else if bid_hour <= 6 || bid_hour >= 20 {
                bid_time_type = "nightTime";
            }

            Some((
                bid.auction,
                bid.bidder,
                0.908 * bid.price as f64,
                bid_time_type.to_string(),
                bid.date_time,
                bid.extra.clone(),
                bid.extra.chars().filter(|c| *c == 'c').count() as i64,
            ))
        } else {
            None
        }
    }))
}

pub fn q17<'a, PerformanceKey>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    (i64, i64, i64, i64, i64, i32, i64, i64, i64, i64),
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        How many bids on an auction made a day and what is the price?
        Illustrates an unbounded group aggregation.
    */
    let tick = bid_stream.location().tick();

    let grouped_auction_day = bid_stream
        .entries()
        .map(q!(|(pk, bid)| (
            (bid.auction, bid.date_time),
            (bid.price, pk)
        )))
        .into_keyed();
    let aggregation = grouped_auction_day.fold(
        q!(|| (0, 0, 0, 0, i64::MAX, i64::MIN, 0, 0, None)),
        q!(
            |acc, (bid_price, pk)| {
                acc.0 += 1;
                acc.1 += if bid_price < 10000 { 1 } else { 0 };
                acc.2 += if bid_price >= 10000 && bid_price < 1000000 {
                    1
                } else {
                    0
                };
                acc.3 += if bid_price >= 1000000 { 1 } else { 0 };
                acc.4 = if bid_price < acc.4 { bid_price } else { acc.4 };
                acc.5 = if bid_price > acc.5 { bid_price } else { acc.5 };
                acc.6 += bid_price;
                acc.7 += bid_price;
                acc.8 = Some(pk)
            },
            commutative = manual_proof!(/** comparison + counting is commutative **/)
        ),
    );

    let select = aggregation.map_with_key(q!(|(key, data)| {
        (
            data.8.unwrap(),
            (
                key.0,
                key.1,
                data.0,
                data.1,
                data.2,
                data.3,
                data.4,
                data.5,
                data.6 / data.0,
                data.7,
            ),
        )
    }));

    select
        .snapshot(&tick, nondet!(/** Test **/))
        .values()
        .into_keyed()
        .all_ticks()
}

pub fn q18<'a, PerformanceKey>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (i64, i64, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        What's a's last bid for bidder to auction?
    */
    let tick = bid_stream.location().tick();
    let prepared_bids = bid_stream
        .entries()
        .map(q!(|(pk, b)| (
            (b.bidder, b.auction),
            (b.auction, b.bidder, b.date_time, b.price, pk)
        )))
        .into_keyed();

    // fold instead of reduce due to simulation testing limitations
    let last_bid_to_auctions = prepared_bids.fold(
        q!(|| (None, (0, 0, 0, 0))),
        q!(
            |best, new| {
                // Find last datetime
                if new.2 > best.1.2 {
                    best.1.0 = new.0;
                    best.1.1 = new.1;
                    best.1.2 = new.2;
                    best.1.3 = new.3;
                    best.0 = Some(new.4);
                }
            },
            commutative = manual_proof!(/** max is commutative */)
        ),
    );

    last_bid_to_auctions
        .snapshot(&tick, nondet!(/** test **/))
        .values()
        .filter_map(q!(|elem| {
            match elem.0 {
                None => None,
                Some(pk) => Some((pk, elem.1)),
            }
        }))
        .into_keyed()
        .all_ticks()
}

pub fn q19<'a, PerformanceKey: Ord + Clone>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (usize, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        What's the top price 10 bids of an auction?
        Illustrates a TOP-N query.
    */
    let tick = bid_stream.location().tick();

    let sorted_price = bid_stream
        .entries()
        .map(q!(|(pk, bid)| (bid.auction, (bid.price, pk))))
        .into_keyed()
        .batch(&tick, nondet!(/** test **/))
        .across_ticks(|stream| {
            stream.fold(
                q!(|| vec![]),
                q!(
                    |acc, x| {
                        if acc.len() < 10 {
                            acc.push(x);
                            acc.sort();
                        } else {
                            let last = acc.first().unwrap();
                            if *last < x {
                                acc.remove(0);
                                acc.push(x);
                                acc.sort();
                            }
                        }
                    },
                    commutative = manual_proof!(/** top ten is commutative **/)
                ),
            )
        })
        .entries()
        .all_ticks()
        .flat_map_unordered(q!(|(auction, top_ten)| {
            let mut output = vec![];
            for (i, elem) in top_ten.iter().enumerate() {
                // make k-v pair
                output.push((elem.1.clone(), (top_ten.len() - i - 1, auction, elem.0)));
            }
            output
        }))
        .into_keyed();

    sorted_price
}

pub fn q20<'a, PerformanceKey>(
    auction_stream: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    (i64, i64, String, i64, i64, i64),
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        Get bids with the corresponding auction information where category is 10.
        Illustrates a filter join.
    */
    let cat_10_auctions = auction_stream.entries().filter_map(q!(|(pk, a)| {
        if a.category == 10 {
            Some((a.id, (a, pk)))
        } else {
            None
        }
    }));

    let formatted_bids = bid_stream.entries().map(q!(|(pk, b)| (b.auction, (b, pk))));
    let join = cat_10_auctions.join(formatted_bids);
    let select = join
        .map(q!(|elem| {
            let ((a, pk1), (b, _)) = elem.1;
            (
                pk1,
                (
                    b.auction,
                    b.date_time,
                    a.item_name,
                    a.date_time,
                    a.seller,
                    a.category,
                ),
            )
        }))
        .into_keyed();

    select
}

pub fn q22<'a, PerformanceKey>(
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    (i64, i64, i64, String, String, String, String),
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        What is the directory structure of the URL?
        Illustrates a SPLIT_INDEX SQL.
    */
    bid_stream.map(q!(|bid_obj| {
        let split_url = bid_obj.url.split("/").collect::<Vec<&str>>();
        (
            bid_obj.auction,
            bid_obj.bidder,
            bid_obj.price,
            bid_obj.channel,
            split_url[3].to_string(),
            split_url[4].to_string(),
            split_url[5].to_string(),
        )
    }))
}

pub fn q23<'a, PerformanceKey>(
    auction_stream: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    person_stream: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (Auction, Bid, Person), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        Find all bids made by a person who has also listed an item for auction
        Illustrates a multi-way join.
    */
    let prepared_persons = person_stream.entries().map(q!(|(pk, p)| (p.id, (p, pk))));
    let prepared_bidders = bid_stream.entries().map(q!(|(pk, b)| (b.bidder, (b, pk))));
    let prepared_auctions = auction_stream
        .entries()
        .map(q!(|(pk, a)| (a.seller, (a, pk))));

    let joined_persons_bidders =
        prepared_persons
            .join(prepared_bidders)
            .map(q!(|(_, ((p, pk), (b, _)))| (b.bidder, (p, b, pk))));
    let join = joined_persons_bidders.join(prepared_auctions);
    let select = join.map(q!(|(_, ((p, b, pk), (a, _)))| (pk, (a, b, p))));
    select.into_keyed()
}
