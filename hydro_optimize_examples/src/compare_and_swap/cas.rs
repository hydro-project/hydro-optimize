use hydro_lang::live_collections::stream::Ordering;
use hydro_lang::live_collections::{boundedness::Boundedness, stream::NoOrder};
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

use crate::compare_and_swap::cas_like::CASOutput;

use super::cas_like::{CASLike, CASState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CASNode {}

pub struct CAS<'a, 'b> {
    pub process: &'b Process<'a, CASNode>,
}

impl<'a, 'b, State, RequestId, Sender> CASLike<'a, State, RequestId, Sender> for CAS<'a, 'b>
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
    RequestId: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + 'a,
{
    fn build(
        self,
        writes: KeyedStream<
            RequestId,
            CASState<State>,
            Cluster<'a, Sender>,
            impl Boundedness,
            impl Ordering,
        >,
        reads: Stream<RequestId, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        subscribe: Stream<MemberId<Sender>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        sender: &Cluster<'a, Sender>,
    ) -> CASOutput<'a, State, RequestId, Sender> {
        let incoming_writes = writes.send(self.process, TCP.fail_stop().bincode());
        let incoming_reads = reads
            .send(self.process, TCP.fail_stop().bincode())
            .entries();
        let incoming_subscribes = subscribe
            .send(self.process, TCP.fail_stop().bincode())
            .values();

        let tick = self.process.tick();
        let atomic_writes = incoming_writes.atomic(&tick);
        let nondet_state =
            nondet!(/** State updates and reads are non-deterministic based on arrival order */);
        let (state_updates, read_outputs) = sliced! {
            let mut last_write = use::state_null::<Optional<CASState<State>, _, _>>();
            let writes = use::atomic(atomic_writes.clone(), nondet_state);
            let reads = use(incoming_reads, nondet_state);

            let winning_write = writes
                .clone()
                .values()
                .max()
                .zip(last_write.clone().into_singleton())
                .filter_map(q!(|(write, prev)| {
                    // Allow the same version if it's from the same writer
                    if let Some(prev_write) = prev
                        && write.version != prev_write.version + 1
                            && !(write.version == prev_write.version
                                && write.writer == prev_write.writer) {
                        println!("Rejecting CASState with version {}, our version is {}",
                            write.version, prev_write.version);
                        return None;
                    }
                    Some(write)
                }));
            let curr_state = winning_write.or(last_write);
            last_write = curr_state.clone();

            let read_outputs = reads
                .cross_singleton(curr_state.clone().into_singleton());

            (curr_state.into_stream(), read_outputs)
        };

        let write_outputs = atomic_writes.end_atomic().keys();
        let read_outputs =
            read_outputs.map(q!(|((sender, req_id), state)| (sender, (req_id, state))));

        let nondet_subscribe = nondet!(/** Subscribes that arrive after a state change may not hear of thet state change */);
        let subscribe_outputs = sliced! {
            let subscribes = use(incoming_subscribes, nondet_subscribe);
            let state_updates = use(state_updates, nondet_subscribe);
            let mut curr_subscribes = use::state_null::<Stream<MemberId<Sender>, _, _, NoOrder>>();

            let new_subscribes = curr_subscribes.chain(subscribes);
            let subscribe_outputs = new_subscribes
                .clone()
                .cross_product(state_updates);

            curr_subscribes = new_subscribes;
            subscribe_outputs
        };

        let write_processed = write_outputs.demux(sender, TCP.fail_stop().bincode());
        let read_result = read_outputs
            .demux(sender, TCP.fail_stop().bincode())
            .into_keyed();
        let subscribe_updates = subscribe_outputs.demux(sender, TCP.fail_stop().bincode());

        CASOutput {
            write_processed,
            read_result,
            subscribe_updates,
        }
    }
}
