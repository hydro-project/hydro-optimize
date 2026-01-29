use hydro_lang::{
    live_collections::{
        sliced::sliced,
        stream::{NoOrder, TotalOrder},
    },
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{Bounded, KeyedSingleton, KeyedStream, Process, Singleton, Stream, Unbounded},
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
    add_user: KeyedStream<MemberId<Client>, (u32, String), Process<'a, Server>, Unbounded, NoOrder>,
    get_users: KeyedStream<MemberId<Client>, u32, Process<'a, Server>, Unbounded, NoOrder>,
    add_story: KeyedStream<
        MemberId<Client>,
        (u32, String, String, Instant),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _add_comment: KeyedStream<
        MemberId<Client>,
        (u32, String, u32, String, Instant),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    upvote_story: KeyedStream<
        MemberId<Client>,
        (u32, String, u32),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _upvote_comment: KeyedStream<
        MemberId<Client>,
        (u32, String, u32),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    get_stories: KeyedStream<MemberId<Client>, u32, Process<'a, Server>, Unbounded, NoOrder>,
    _get_comments: KeyedStream<MemberId<Client>, u32, Process<'a, Server>, Unbounded, NoOrder>,
    _get_story_comments: KeyedStream<
        MemberId<Client>,
        (u32, u32),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
) -> (
    KeyedStream<MemberId<Client>, (u32, Option<String>), Process<'a, Server>, Unbounded, NoOrder>, // add_user response
) {
    let user_auth_tick = server.tick();
    let stories_tick = server.tick();

    // Add users
    let user_key_add_user = add_user
        .entries()
        .map(q!(|(client_id, (virtual_client_id, username))| (
            username.clone(),
            (
                self::generate_api_key(username),
                client_id,
                virtual_client_id
            )
        )))
        .into_keyed();
    let atomic_add_user = user_key_add_user.atomic(&user_auth_tick);
    // Add story
    let api_key_add_story = add_story
        .entries()
        .map(q!(|(
            client_id,
            (virtual_client_id, api_key, title, timestamp),
        )| (
            api_key,
            (client_id, virtual_client_id, title, timestamp)
        )))
        .into_keyed();

    // Persisted users
    let curr_users = atomic_add_user
        .assume_ordering(nondet!(/** First user wins */))
        .first();
    // Anything to do with persisted users
    let (add_user_response, get_users_response, add_story_valid_api_key) = sliced! {
        let new_users = use::atomic(atomic_add_user, nondet!(/** New users requests this tick */));
        let get_users = use(get_users, nondet!(/** Order of processing affects which users will be retrieved */));
        let add_story = use(api_key_add_story, nondet!(/** Add story requests sent before user's creation will fail */));

        let curr_users = use::atomic(curr_users, nondet!(/** Current users this tick */));

        let add_users_response = new_users; // TODO
        let get_users_response = get_users.cross_singleton(curr_users.into_singleton());
        let add_story_valid_api_key = add_story; // TODO
        (add_users_response, get_users_response, add_story_valid_api_key)
    };

    let valid_add_story = add_story_valid_api_key.values();
    // Anything to do with persisted stories
    let (add_story_with_id, upvote_story_response, get_stories_response) = sliced! {
        let add_story = use(valid_add_story, nondet!(/** Add new incoming story requests this tick */));
        let get_stories = use(get_stories, nondet!(/** Order of processing affects which stories will be retrieved */));
        // TODO: Replace with KeyedSingleton once that is supported
        // ID, client_id, virtual_client_id, title, timestamp
        let mut stories = use::state_null::<Stream<(u32, MemberId<Client>, u32, String, Instant), _, Bounded, NoOrder>>();
        let mut next_story_id = use::state::<Singleton<u32, _, Bounded>>(|l| l.singleton(q!(0)));

        let new_stories = add_story.clone().assume_ordering(nondet!(/** Stories are assigned an ID in arbitrary order */))
            .enumerate()
            .cross_singleton(next_story_id)
            .map(q!(|((index, (client_id, virtual_client_id, title, timestamp)), curr_id)| (index as u32 + curr_id, client_id, virtual_client_id, title, timestamp)));
        next_story_id = next_story_id.zip(add_story.count()).map(q!(|(curr_id, count)| curr_id + count as u32));

        stories.

        stories = stories.chain(new_stories);

        (new_stories, )
    };

    let _top_stories = curr_stories.clone().persist().fold_commutative_idempotent(
        q!(|| vec![]),
        q!(
            |vec, (_api_key, ((_client_id, _req_id, title, timestamp), username))| {
                let new_elem = (title, timestamp, username);
                // TODO: Use a binary heap
                // TODO: Create a struct that is ordered by timestamp
                let pos = vec.binary_search(&new_elem).unwrap_or_else(|e| e);
                vec.insert(pos, new_elem);
                vec.truncate(20);
            }
        ),
    );

    (
        add_user_response.end_atomic(),
        get_users_response.end_atomic(),
    )
}

fn generate_api_key(email: String) -> String {
    let secret = "There is no secret ingredient";
    let mut hasher = Sha256::new();
    hasher.update(email.as_bytes());
    hasher.update(secret.as_bytes());
    let hash = hasher.finalize();
    format!("{:x}", hash)
}
