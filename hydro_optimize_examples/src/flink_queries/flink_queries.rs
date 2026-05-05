use hydro_lang::{
    live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};

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
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
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
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, Option<(i64, i64)>, Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        Find bids with specific auction ids and show their bid price.
    */
    bid_stream.map(q!(|bid_obj| {
        if bid_obj.auction % 123 == 0 {
            Some((bid_obj.auction, bid_obj.price))
        } else {
            None
        }
    }))
}

pub fn q3<'a, PerformanceKey: Eq + std::hash::Hash + Clone>(
    auction_stream: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    person_stream: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    Option<(String, String, String, Vec<i64>)>,
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        Who is selling in OR, ID or CA in category 10, and for what auction ids?
        Illustrates an incremental join (using per-key state and timer) and filter.
    */
    let (cat10_auctions, filtered_out) = auction_stream
        .entries()
        .partition(q!(|(_, a)| a.category == 10));

    let formatted_cat10s = cat10_auctions
        .clone()
        .map(q!(|(pk, a)| (a.seller, (a, pk))));
    let cat10_filtered_out = filtered_out.map(q!(|(pk, _)| (pk, None))).into_keyed();

    let (or_id_ca_persons, filtered_out) = person_stream
        .entries()
        .partition(q!(|(_, p)| ["OR", "CA", "ID"].contains(&p.state.as_str())));

    let formatted_states = or_id_ca_persons.clone().map(q!(|(pk, p)| (p.id, (p, pk))));
    let state_filtered_out = filtered_out.map(q!(|(pk, _)| (pk, None))).into_keyed();

    // Acks to ensure unmatched inputs that passed the filter receive an output so benchmark can see progress
    let cat10_input_acks = cat10_auctions.map(q!(|(pk, _a)| (pk, None))).into_keyed();
    let or_state_input_acks = or_id_ca_persons.map(q!(|(pk, _p)| (pk, None))).into_keyed();

    let joined = formatted_states
        .join(formatted_cat10s)
        .map(q!(|(_id, ((p, p_pk), (a, a_pk)))| {
            // Track PKs to know who to send result to later
            let mut pks = HashSet::new();
            pks.insert(a_pk);
            pks.insert(p_pk);

            (p.id, ((p.name, p.city, p.state), a.id, pks))
        }))
        .into_keyed();

    // Condense each Person -> Vec[Auction] and track PKs to later send results to
    let aggregated = joined.fold(
        q!(|| (None, Vec::new(), HashSet::new())),
        q!(
            |acc, (person_info, auction_id, pks)| {
                if acc.0.is_none() {
                    acc.0 = Some(person_info);
                }

                acc.1.push(auction_id);
                acc.2.extend(pks);
            },
            commutative = manual_proof!(/** Order doesn't matter */)
        ),
    );

    let tick = aggregated.location().tick();
    // Prepare for re-keying on PK
    let aggregated = aggregated.map(q!(|(p_opt, auctions, pks)| {
        let (name, city, state) = p_opt.unwrap();
        (pks, Some((name, city, state, auctions)))
    }));

    // Expand stored PKs who contributed an input and send them the result
    let select = aggregated
        .snapshot(&tick, nondet!(/** test */))
        .entries()
        .all_ticks()
        .flat_map_unordered(q!(|(_, (pks, value))| {
            pks.into_iter().map(move |pk| (pk, value.clone()))
        }))
        .into_keyed();

    // Combine outputs
    let cat10_f = cat10_filtered_out.entries();
    let state_f = state_filtered_out.entries();
    let c10_ack = cat10_input_acks.entries();
    let or_ack = or_state_input_acks.entries();
    let sel = select.entries();

    cat10_f
        .merge_unordered(
            state_f.merge_unordered(c10_ack.merge_unordered(or_ack.merge_unordered(sel))),
        )
        .into_keyed()
}

pub fn q11<'a, PerformanceKey: Ord + Clone + Eq + std::hash::Hash>(
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (i64, i32, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        How many bids did a user make in each session they were active? Illustrates session windows.
        Group bids by the same user into sessions with max session gap.
        Emit the number of bids per session.
    */
    let tick = bid_stream.location().tick();

    bid_stream
        .entries()
        // Aggregate by bidder
        .map(q!(|(pk, bid)| (bid.bidder, (bid.bidder, bid.date_time, pk))))
        .into_keyed()
        .batch(&tick, nondet!(/** Need to bound for sorting */))
        // Sort by date_time to ensure sessions are grouped correctly
        .sort()
        .across_ticks(|s| {
            s.assume_retries::<ExactlyOnce>(nondet!(/** one pass per element per key in batch */))
                .assume_ordering::<TotalOrder>(nondet!(/** sorted batch */))
                .generator(
                    q!(|| None::<(i64, i64, i32)>),
                    q!(|open, (bidder, current_time, pk)| {
                        match open {
                            // First session for the bidder
                            None => {
                                *open = Some((current_time, current_time, 1));
                                hydro_lang::live_collections::keyed_stream::Generate::Yield((
                                    pk.clone(),
                                    (bidder, 1, current_time, current_time),
                                ))
                            }
                            Some((start, last, count)) => {
                                let gap = current_time - *last;
                                // gap bound from https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/resources/queries/q11.sql#L23
                                if gap <= 10 {
                                    *last = current_time;
                                    *count += 1;
                                    hydro_lang::live_collections::keyed_stream::Generate::Yield((
                                        pk.clone(),
                                        (bidder, *count, *start, *last),
                                    ))
                                } else {
                                    *open = Some((current_time, current_time, 1));
                                    hydro_lang::live_collections::keyed_stream::Generate::Yield((
                                        pk.clone(),
                                        (bidder, 1, current_time, current_time),
                                    ))
                                }
                            }
                        }
                    }),
                )
                .entries()
        })
        .all_ticks()
        .map(q!(|(_bidder, (pk, row))| (pk, row)))
        .into_keyed()
        .weaken_ordering::<NoOrder>()
}

pub fn q14<'a, PerformanceKey>(
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    Option<(i64, i64, f64, String, i64, String, i64)>,
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        Convert bid timestamp into types and find bids with specific price.
        Illustrates duplicate expressions and usage of user-defined-functions.
    */
    bid_stream.map(q!(|bid| {
        if bid.price as f64 * 0.908 > 1000000.0 && bid.price as f64 * 0.908 < 50000000.0 {
            let mut bid_time_type = "otherTime";
            let bid_hour = bid.date_time % 24;
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

pub fn q17<'a, PerformanceKey: Clone + Eq + std::hash::Hash>(
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
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
    let grouped_auction_day = bid_stream
        .entries()
        .map(q!(|(pk, bid)| (
            (bid.auction, bid.date_time),
            (bid.auction, bid.date_time, bid.price, pk)
        )))
        .into_keyed();

    grouped_auction_day
        .assume_retries::<ExactlyOnce>(nondet!(/** one pass per element per key */))
        .assume_ordering::<TotalOrder>(nondet!(/** result order not concerning */))
        .generator(
            q!(|| (0, 0, 0, 0, i64::MAX, i64::MIN, 0, 0, Vec::new())),
            q!(|acc, (_auction, _date_time, bid_price, pk)| {
                acc.0 += 1;
                acc.1 += if bid_price < 10000 { 1 } else { 0 };
                acc.2 += if bid_price >= 10000 && bid_price < 1000000 {
                    1
                } else {
                    0
                };
                acc.3 += if bid_price >= 1000000 { 1 } else { 0 };
                acc.4 = acc.4.min(bid_price);
                acc.5 = acc.5.max(bid_price);
                acc.6 += bid_price;
                acc.7 += bid_price;
                acc.8.push(pk.clone());

                let row = (
                    _auction,
                    _date_time,
                    acc.0,
                    acc.1,
                    acc.2,
                    acc.3 as i32,
                    acc.4,
                    acc.5,
                    if acc.0 > 0 { acc.6 / acc.0 } else { 0 },
                    acc.7,
                );
                hydro_lang::live_collections::keyed_stream::Generate::Yield((pk.clone(), row))
            }),
        )
        .entries()
        .map(q!(|(_key, (pk, row))| (pk, row)))
        .into_keyed()
        .weaken_ordering::<NoOrder>()
}

pub fn q18<'a, PerformanceKey: Clone + Eq + std::hash::Hash>(
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (i64, i64, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        What's a's last bid for bidder to auction?
    */
    let prepared_bids = bid_stream
        .entries()
        .map(q!(|(pk, b)| (
            (b.bidder, b.auction),
            (b.auction, b.bidder, b.date_time, b.price, pk)
        )))
        // Group by bidder and auction to later find the last bid
        .into_keyed();

    prepared_bids
        .assume_retries::<ExactlyOnce>(nondet!(/** one pass per element per key */))
        .assume_ordering::<TotalOrder>(nondet!(/** need for ordering */))
        .generator(
            q!(|| (0, 0, 0, 0)),
            q!(|best, new| {
                if new.2 > best.2 {
                    *best = (new.0, new.1, new.2, new.3);
                }
                hydro_lang::live_collections::keyed_stream::Generate::Yield((new.4.clone(), *best))
            }),
        )
        .entries()
        .map(q!(|(_group, (pk, row))| (pk, row)))
        .into_keyed()
        .weaken_ordering::<NoOrder>()
}

pub fn q19<'a, PerformanceKey: Ord + Clone + std::hash::Hash>(
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, Vec<(usize, i64, i64)>, Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        What's the top price 10 bids of an auction?
        Illustrates a TOP-N query.
    */
    let tick = bid_stream.location().tick();

    let sorted_price = bid_stream
        .entries()
        .map(q!(|(pk, bid)| (bid.auction, (bid.price, pk.clone()))))
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
        });

    // Flatten the top ten bids per auction
    let per_pk = sorted_price
        .entries()
        .flat_map_unordered(q!(|(auction, top_ten)| {
            let mut out = vec![];

            for (i, (price, pk)) in top_ten.iter().enumerate() {
                let rank = top_ten.len() - i - 1;
                out.push((pk.clone(), (rank, auction, *price)));
            }

            out
        }))
        .into_keyed();

    let aggregated = per_pk.fold(
        q!(|| Vec::new()),
        q!(
            |acc, val| {
                acc.push(val);
            },
            commutative = manual_proof!(/** append */)
        ),
    );

    aggregated.entries().into_keyed().all_ticks()
}

pub fn q20<'a, PerformanceKey: Eq + std::hash::Hash + Clone>(
    auction_stream: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<
    PerformanceKey,
    Option<Vec<(i64, i64, String, i64, i64, i64)>>,
    Process<'a, Queries>,
    Unbounded,
    NoOrder,
> {
    /*
        Get bids with the corresponding auction information where category is 10.
        Illustrates a filter join.
    */
    let tick = bid_stream.location().tick();

    let (cat10_auctions, filtered_out) = auction_stream
        .entries()
        .partition(q!(|(_, a)| a.category == 10));

    let formatted_cat10s = cat10_auctions
        .clone()
        .map(q!(|(pk, a)| (a.id, (a, pk))));
    let cat10_filtered_out = filtered_out.map(q!(|(pk, _)| (pk, None))).into_keyed();

    // Acks so inputs get a reply even when category != 10 or join doesn't match
    let cat10_input_acks = cat10_auctions
        .map(q!(|(pk, _a)| (pk, Option::<Vec<(i64, i64, String, i64, i64, i64)>>::None)))
        .into_keyed();

    let bid_entries = bid_stream.entries();
    let bid_input_acks = bid_entries
        .clone()
        .map(q!(|(pk, _b)| (pk, Option::<Vec<(i64, i64, String, i64, i64, i64)>>::None)))
        .into_keyed();
    
    let formatted_bids = bid_entries.map(q!(|(pk, b)| (b.auction, (b, pk))));
    let join = formatted_cat10s.join(formatted_bids);

    // Format the joined output to prepare for keying on b.auction
    let select = join
        .map(q!(|elem| {
            let ((a, pk1), (b, _)) = elem.1;
            (
                b.auction,
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
                ),
            )
        }))
        .into_keyed();

    // Condense join outputs into vec per auction to many bids
    let agg = select
        .batch(&tick, nondet!(/** test **/))
        .across_ticks(|stream| {
            stream.fold(
                q!(|| Vec::new()),
                q!(
                    |acc, (pk, val)| {
                        acc.push((pk, val));
                    },
                    commutative = manual_proof!(/** order doesn't matter */)
                ),
            )
        })
        // Remove auction key to prepare keying to (PK, v)
        .entries()
        .map(q!(|(_, vec)| vec))
        .flatten_ordered()
        .into_keyed();

    // Condense outputs per PK
    let result = agg
        .across_ticks(|stream| {
            stream.fold(
                q!(|| Vec::new()),
                q!(
                    |acc, val| {
                        acc.push(val);
                    },
                    commutative = manual_proof!(/** order doesn't matter  */)
                ),
            )
        })
        .entries()
        .map(q!(|(pk, vec)| (pk, Some(vec))))
        .into_keyed()
        .all_ticks();

    bid_input_acks.merge_unordered(
        cat10_filtered_out.merge_unordered(cat10_input_acks.merge_unordered(result)),
    )
}

pub fn q22<'a, PerformanceKey>(
    _: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    _: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
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

pub fn q23<'a, PerformanceKey: Eq + std::hash::Hash + Ord + Clone>(
    auction_stream: KeyedStream<PerformanceKey, Auction, Process<'a, Queries>, Unbounded, NoOrder>,
    bid_stream: KeyedStream<PerformanceKey, Bid, Process<'a, Queries>, Unbounded, NoOrder>,
    person_stream: KeyedStream<PerformanceKey, Person, Process<'a, Queries>, Unbounded, NoOrder>,
) -> KeyedStream<PerformanceKey, (Person, Vec<Bid>), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        Find all bids made by a person who has also listed an item for auction
        Illustrates a multi-way join.
    */

    // Prepare for joins and track PKs to later send results to
    let prepared_persons = person_stream.entries().map(q!(|(pk, p)| (
        p.id,
        (p, std::iter::once(pk).collect::<BTreeSet<_>>())
    )));
    let prepared_bidders = bid_stream.entries().map(q!(|(pk, b)| (
        b.bidder,
        (b, std::iter::once(pk).collect::<BTreeSet<_>>())
    )));
    let prepared_auctions = auction_stream.entries().map(q!(|(pk, a)| (
        a.seller,
        std::iter::once(pk).collect::<BTreeSet<_>>()
    )));

    let persons_with_auctions =
        prepared_persons
            .join(prepared_auctions)
            .map(q!(|(_, ((p, pk1), pk2))| {
                let mut pk = pk1;
                pk.extend(pk2);
                (p.id, (p, pk))
            }));

    let join = persons_with_auctions
        .join(prepared_bidders)
        .map(q!(|(_, ((p, pk1), (b, pk2)))| {
            let mut pk = pk1;
            pk.extend(pk2);
            (pk, (b, p))
        }));

    // Re-key by person to condense Person -> Vec[Bids]
    let keyed = join
        .map(q!(|(pk, (b, p))| { (p.id, (p, b, pk)) }))
        .into_keyed();
    let aggregated = keyed
        .fold(
            q!(|| (None, Vec::new(), BTreeSet::new())),
            q!(
                |acc, (p, b, pk)| {
                    acc.0 = Some(p);
                    acc.1.push(b);
                    acc.2.extend(pk);
                },
                commutative = manual_proof!(/** Order doesn't matter */),
            ),
        )
        // Remove Option formatting
        .map(q!(|(p_opt, bids, pks)| { (pks, (p_opt.unwrap(), bids)) }));

    let tick = aggregated.location().tick();
    // Ensure each PK that contributed an input hears about its output
    let select = aggregated
        .snapshot(&tick, nondet!(/** Test */))
        .entries()
        .flat_map_unordered(q!(|(_, (pks, value))| {
            pks.into_iter().map(move |pk| (pk, value.clone()))
        }))
        .into_keyed()
        .all_ticks();

    select
}
