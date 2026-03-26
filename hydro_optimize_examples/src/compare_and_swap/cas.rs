use hydro_lang::live_collections::stream::{Ordering, TotalOrder};
use hydro_lang::live_collections::{boundedness::Boundedness, stream::NoOrder};
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

use super::cas_like::{CASLike, CASOutput, CASState, UniqueRequestId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CASNode {}

pub struct CAS<'a, 'b> {
    pub process: &'b Process<'a, CASNode>,
}

impl<'a, 'b, State, Sender> CASLike<'a, State, Sender> for CAS<'a, 'b>
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    Sender: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
{
    fn build(
        self,
        writes: KeyedStream<
            UniqueRequestId,
            CASState<State>,
            Cluster<'a, Sender>,
            impl Boundedness,
            impl Ordering,
        >,
        reads: Stream<UniqueRequestId, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        subscribe: Stream<MemberId<Sender>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        sender: &Cluster<'a, Sender>,
    ) -> CASOutput<'a, State, Sender> {
        let incoming_writes = writes.send(self.process, TCP.fail_stop().bincode());
        let incoming_reads = reads
            .send(self.process, TCP.fail_stop().bincode())
            .entries();
        let incoming_subscribes = subscribe
            .send(self.process, TCP.fail_stop().bincode())
            .values();

        let nondet_state =
            nondet!(/** State updates and reads are non-deterministic based on arrival order */);
        let (write_outputs, read_outputs) = sliced! {
            let writes = use(incoming_writes, nondet_state);
            let reads = use(incoming_reads, nondet_state);

            let write_outputs = writes
                .entries()
                .assume_ordering::<TotalOrder>(nondet_state)
                .across_ticks(|s| s.scan(q!(|| Option::<CASState<State>>::None),
                    q!(|prev, ((client_id, request_id), write)| {
                        // Allow the same version if it's from the same writer
                        if let Some(prev_write) = prev
                            && write.version != prev_write.version + 1
                                && !(write.version == prev_write.version
                                    && write.writer == prev_write.writer) {
                            println!("Rejecting CASState with version {}, our version is {}",
                                write.version, prev_write.version);
                            Some((client_id, request_id, write, false))
                        }
                        else {
                            *prev = Some(write.clone());
                            Some((client_id, request_id, write, true))
                        }
                    }
                )));
            let curr_state = write_outputs
                .clone()
                .last();

            let read_outputs = reads
                .cross_singleton(curr_state.into_singleton());

            (write_outputs, read_outputs)
        };

        let nondet_subscribe = nondet!(/** Subscribes that arrive after a state change may not hear of thet state change */);
        let subscribe_outputs = sliced! {
            let subscribes = use(incoming_subscribes, nondet_subscribe);
            let write_outputs = use(write_outputs.clone(), nondet_subscribe);
            let mut curr_subscribes = use::state_null::<Stream<MemberId<Sender>, _, _, NoOrder>>();

            let new_subscribes = curr_subscribes.chain(subscribes);
            let subscribe_outputs = new_subscribes
                .clone()
                .cross_product(write_outputs);

            curr_subscribes = new_subscribes;
            subscribe_outputs
        };

        let write_processed = write_outputs
            .map(q!(|(sender, req_id, _write, successful)| (
                sender,
                (req_id, successful)
            )))
            .demux(sender, TCP.fail_stop().bincode())
            .into_keyed()
            .weaken_ordering();
        let read_result = read_outputs
            .map(q!(|((sender, req_id), last_write)| (
                sender,
                (
                    req_id,
                    last_write.map(|(_sender, _request_id, state, _successful)| state)
                )
            )))
            .demux(sender, TCP.fail_stop().bincode())
            .into_keyed();
        let subscribe_updates = subscribe_outputs
            .map(q!(|(
                subscriber,
                (_sender, _req_id, state, _successful),
            )| (subscriber, state)))
            .demux(sender, TCP.fail_stop().bincode());

        CASOutput {
            write_processed,
            read_result,
            subscribe_updates,
        }
    }
}
