use hydro_lang::live_collections::boundedness::Boundedness;
use hydro_lang::live_collections::stream::Ordering;
use hydro_lang::prelude::*;
use hydro_lang::{live_collections::stream::NoOrder, location::MemberId};
use serde::{Deserialize, Serialize};

/// Version-guarded write request. Accepted only if
/// `version = current_version + 1` in the register.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CASState<State> {
    pub version: u64,
    pub state: State,
}

/// `RequestId` is what allows the clients to uniquely identify each request.
/// We assume that there is at most one outgoing request per ID at any time.
///
/// # Fields
/// - `write_processed`: ACK to each processed write. Processed != successfully updated
/// - `read_result`: Result of read request
/// - `subscribe_updates`: Stream of state updates for subscribers
pub struct CASOutput<'a, State, RequestId, Sender> {
    pub write_processed: Stream<RequestId, Cluster<'a, Sender>, Unbounded, NoOrder>,
    pub read_result: KeyedStream<RequestId, CASState<State>, Cluster<'a, Sender>, Unbounded, NoOrder>,
    pub subscribe_updates: Stream<CASState<State>, Cluster<'a, Sender>, Unbounded, NoOrder>,
}

impl<S: PartialOrd> PartialOrd for CASState<S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.version.cmp(&other.version) {
            std::cmp::Ordering::Equal => self.state.partial_cmp(&other.state),
            ord => Some(ord),
        }
    }
}

impl<S: Ord> Ord for CASState<S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.version.cmp(&other.version).then_with(|| self.state.cmp(&other.state))
    }
}

/// Stream-based versioned atomic-register interface.
///
/// The register holds a state of type `S` with a monotonic version.
///
/// The trait is agnostic about the underlying location — implementations
/// choose whether CAS runs on a `Process` or `Cluster`.
pub trait CASLike<'a, State, RequestId, Sender>: Sized
where
    State: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
    RequestId: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
{
    /// Input streams:
    /// - `writes`: version-guarded state writes from sender location.
    /// - `reads`: reads current state.
    /// - `subscribe`: IDs of senders that would like to learn of updates to the configuration.
    /// - `sender`: the location of the sender.
    fn build(
        self,
        writes: KeyedStream<RequestId, CASState<State>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        reads: Stream<RequestId, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        subscribe: Stream<MemberId<Sender>, Cluster<'a, Sender>, impl Boundedness, impl Ordering>,
        sender: &Cluster<'a, Sender>,
    ) -> CASOutput<'a, State, RequestId, Sender>;
}
