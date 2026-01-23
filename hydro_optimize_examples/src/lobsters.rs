use std::collections::HashSet;

use hydro_lang::{
    live_collections::stream::NoOrder,
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{Process, Stream, Unbounded},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use stageleft::q;
use tokio::time::Instant;

pub struct Server {}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Story {
    pub title: String,
    pub epoch_time: u128,
    pub id: u32,
}

impl PartialOrd for Story {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.epoch_time.cmp(&other.epoch_time))
    }
}

/// Implementation of Lobsters, roughly based on API calls exposed here: https://lobste.rs/s/cqnzl5/lobste_rs_access_pattern_statistics_for#c_2op8by
/// We expose the following APIs:
/// - add_user (takes username, returns api_key. Rejects if the user already exists (returns None))
/// - get_users (returns usernames)
/// - add_story (takes api_key, title, timestamp, returns story_id)
/// - add_comment (takes api_key, story_id, comment, timestamp, returns comment_id)
/// - upvote_story (takes api_key, story_id)
/// - upvote_comment (takes api_key, comment_id)
/// - get_stories (returns the 20 stories with the latest timestamps)
/// - get_comments (returns the 20 comments with the latest timestamps)
/// - get_story_comments (takes story_id, returns the comments for that story)
///
///   Any call with an invalid API key (either it does not exist or does not have the privileges required) will not receive a response.
#[expect(
    clippy::too_many_arguments,
    clippy::type_complexity,
    reason = "internal Lobsters code // TODO"
)]
pub fn lobsters<'a, Client>(
    server: &Process<'a, Server>,
    add_user: KeyedStream<(MemberId<Client>, u32), String, Process<'a, Server>, Unbounded, NoOrder>,
    get_users: Stream<(MemberId<Client>, u32), Process<'a, Server>, Unbounded, NoOrder>,
    add_story: KeyedStream<
        (MemberId<Client>, u32),
        (String, String, Instant),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _add_comment: KeyedStream<
        (MemberId<Client>, u32),
        (String, u32, String, Instant),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _upvote_story: KeyedStream<
        (MemberId<Client>, u32),
        (String, u32),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _upvote_comment: KeyedStream<
        (MemberId<Client>, u32),
        (String, u32),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _get_stories: Stream<(MemberId<Client>, u32), Process<'a, Server>, Unbounded, NoOrder>,
    _get_comments: Stream<(MemberId<Client>, u32), Process<'a, Server>, Unbounded, NoOrder>,
    _get_story_comments: KeyedStream<
        (MemberId<Client>, u32),
        u32,
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
) -> (
    KeyedStream<(MemberId<Client>, u32), Option<String>, Process<'a, Server>, Unbounded, NoOrder>, // add_user response
) {
    let user_auth_tick = server.tick();
    let stories_tick = server.tick();

    // Persisted users
    let curr_users = add_user
        .map(q!(|(_client_id, username)| (
            username,
            self::generate_api_key(username.clone())
        )))
        .into_keyed()
        .assume_ordering(nondet!(/** First user wins */))
        .first();
    // Send response back to client. Only done after the tick to ensure that once the client gets the response, the user has been added
    let add_user_response = sliced! {
        let new_users = use(add_user_with_api_key, nondet!(/** New users requests this tick */));
        let curr_users = use(curr_users, nondet!(/** Current users this tick */));
        new_users
            .map(q!(|(client_id, (username, api_key))| {
                (username, (client_id, api_key))
            }))
            .into_keyed()

    };

    // Get users
    let _get_users_response = get_users
        .batch(
            &user_auth_tick,
            nondet!(/** Snapshot against current users */),
        )
        .cross_singleton(curr_users_hashset)
        .all_ticks();

    // Add story
    let add_story_pre_join = add_story.map(q!(|(client_id, (api_key, title, timestamp))| {
        (api_key, (client_id, title, timestamp))
    }));
    let stories = add_story_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks();
    let curr_stories = stories.batch(&stories_tick, nondet!(/** Snapshot of current stories */)).assume_ordering(nondet!(/** In order to use enumerate to assign a unique ID, we need total ordering. */));
    // Assign each story a unique ID
    let (story_id_complete_cycle, story_id) =
        stories_tick.cycle_with_initial(stories_tick.singleton(q!(0)));
    let _indexed_curr_stories = curr_stories
        .clone()
        .enumerate()
        .cross_singleton(story_id.clone())
        .map(q!(|((index, story), story_id)| (index + story_id, story)));
    let num_curr_stories = curr_stories.clone().count();
    let new_story_id = num_curr_stories
        .zip(story_id)
        .map(q!(|(num_stories, story_id)| num_stories + story_id));
    story_id_complete_cycle.complete_next_tick(new_story_id);

    let _top_stories = curr_stories.clone().persist().fold_commutative_idempotent(
        q!(|| vec![]),
        q!(
            |vec, (_api_key, ((_client_id, title, timestamp), username))| {
                let new_elem = (title, timestamp, username);
                // TODO: Use a binary heap
                // TODO: Create a struct that is ordered by timestamp
                let pos = vec.binary_search(&new_elem).unwrap_or_else(|e| e);
                vec.insert(pos, new_elem);
                vec.truncate(20);
            }
        ),
    );

    (add_user_response,)
}

fn generate_api_key(email: String) -> String {
    let secret = "There is no secret ingredient";
    let mut hasher = Sha256::new();
    hasher.update(email.as_bytes());
    hasher.update(secret.as_bytes());
    let hash = hasher.finalize();
    format!("{:x}", hash)
}
