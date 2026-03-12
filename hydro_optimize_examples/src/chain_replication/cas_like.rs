use hydro_lang::prelude::*;
use hydro_lang::{live_collections::stream::NoOrder, location::MemberId};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// Version-guarded write request. Accepted only if
/// `version = current_version + 1` in the register.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CASState<S> {
    pub version: u64,
    pub state: S,
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

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Stream-based versioned atomic-register interface.
///
/// The register holds a state of type `S` with a monotonic version.
/// Writes are accepted only when the request's version is >= the stored version.
///
/// The trait is agnostic about the underlying location — implementations
/// choose whether CAS runs on a `Process`, `Cluster`, or anything else.
pub trait CASLike<'a, S, Sender>: Sized
where
    S: Clone + Serialize + for<'de> Deserialize<'de> + 'a,
{
    /// Construct the CAS dataflow.
    ///
    /// Input streams:
    /// - `writes`: version-guarded state writes from sender location.
    /// - `reads`: reads current state.
    /// - `subscribe`: IDs of senders that would like to learn of any updates to the configuration.
    /// - `sender`: the location of the sender.
    ///
    /// Returns a stream containing any state updates.
    fn build(
        self,
        writes: Stream<CASState<S>, Cluster<'a, Sender>, Unbounded, NoOrder>,
        subscribe: Singleton<MemberId<Sender>, Cluster<'a, Sender>, Bounded>,
        sender: &Cluster<'a, Sender>,
    ) -> Stream<CASState<S>, Cluster<'a, Sender>, Unbounded, NoOrder>;
}
