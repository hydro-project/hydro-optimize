use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use hydro_lang::location::MemberId;
use serde::{Deserialize, Serialize};

use super::cas_like::{CASLike, CASState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockCASNode {}

pub struct MockCAS<'a, 'b> {
    pub process: &'b Process<'a, MockCASNode>,
}

impl<'a, 'b, S, Sender> CASLike<'a, S, Sender> for MockCAS<'a, 'b>
where
    S: Clone + Serialize + for<'de> Deserialize<'de> + Ord + 'a,
{
    fn build(
        self,
        writes: Stream<CASState<S>, Cluster<'a, Sender>, Unbounded, NoOrder>,
        subscribe: Singleton<MemberId<Sender>, Cluster<'a, Sender>, Bounded>,
        sender: &Cluster<'a, Sender>,
    ) -> Stream<CASState<S>, Cluster<'a, Sender>, Unbounded, NoOrder> {
        let incoming_writes = writes
            .send(self.process, TCP.fail_stop().bincode());
        let incoming_subscribes = subscribe
            .into_stream()
            .send(self.process, TCP.fail_stop().bincode())
            .values();

        let results = sliced! {
            let mut version = use::state(|l| l.singleton(q!(0u64)));
            let mut curr_writer = use::state_null::<Optional<MemberId<Sender>, _, _>>();
            let writes = use(incoming_writes, nondet!(/** Writes are admitted/rejected based on version number at time of arrival */));

            let winning_write = writes
                .clone()
                .entries()
                .reduce(q!(|(max_writer, max_write), (next_writer, next_write)| {
                    if *max_write < next_write {
                        *max_writer = next_writer;
                        *max_write = next_write;
                    }
                }, commutative = manual_proof!(/** max is commutative */)))
                .zip(version.clone())
                .zip(curr_writer.clone().into_singleton())
                .filter_map(q!(|(((writer, write), version), prev_writer)| {
                    // Allow the same version if it's from the same writer
                    if write.version == version + 1
                        || (write.version == version && prev_writer.is_none_or(|prev| prev == writer)) {
                        Some((writer, write))
                    }
                    else {
                        println!("Rejecting CASState with version {}, our version is {}", write.version, version);
                        None
                    }
                }));
            let highest_version = winning_write
                .clone()
                .map(q!(|(_writer, write)| write.version))
                .unwrap_or(version);
            let new_state = winning_write
                .clone()
                .map(q!(|(_writer, write)| write.state));
            let results = highest_version
                .clone()
                .zip(new_state.clone())
                .into_stream();

            version = highest_version;
            curr_writer = winning_write
                .map(q!(|(writer, _write)| writer))
                .or(curr_writer);

            results
        };

        let state_updates = results.map(q!(|(version, state)| CASState {
            version,
            state
        }));

        let nondet_subscribe = nondet!(/** Subscribes that arrive after a state change may not hear of thet state change */);
        let output = sliced! {
            let subscribes = use(incoming_subscribes, nondet_subscribe);
            let state_updates = use(state_updates, nondet_subscribe);
            let mut curr_subscribes = use::state_null::<Stream<MemberId<Sender>, _, _, NoOrder>>();

            let new_subscribes = curr_subscribes.chain(subscribes);
            let output = new_subscribes
                .clone()
                .cross_product(state_updates);

            curr_subscribes = new_subscribes;
            output
        };

        output.demux(sender, TCP.fail_stop().bincode())
    }
}
