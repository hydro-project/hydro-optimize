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
use std::{collections::HashMap, hash::Hash};
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
    add_user: KeyedStream<Client, String, Process<'a, Server>, Unbounded, NoOrder>,
    get_users: Stream<Client, Process<'a, Server>, Unbounded, NoOrder>,
    add_story: KeyedStream<
        Client,
        (String, String, Instant),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    _add_comment: KeyedStream<
        Client,
        (String, u32, String, Instant),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    upvote_story: KeyedStream<Client, (String, u32), Process<'a, Server>, Unbounded, NoOrder>,
    _upvote_comment: KeyedStream<Client, (String, u32), Process<'a, Server>, Unbounded, NoOrder>,
    get_stories: Stream<Client, Process<'a, Server>, Unbounded, NoOrder>,
    _get_comments: Stream<Client, Process<'a, Server>, Unbounded, NoOrder>,
    _get_story_comments: KeyedStream<Client, u32, Process<'a, Server>, Unbounded, NoOrder>,
) -> (
    KeyedStream<Client, Option<String>, Process<'a, Server>, Unbounded, NoOrder>, // add_user response
    KeyedStream<Client, HashMap<String, (String, Client)>, Process<'a, Server>, Unbounded, NoOrder>, // get_users response
)
where
    Client: Eq + Hash + Clone,
{
    let user_auth_tick = server.tick();
    let stories_tick = server.tick();

    // Add users
    let atomic_add_user = add_user.atomic(&user_auth_tick);
    let user_key_add_user = atomic_add_user
        .clone()
        .entries()
        .map(q!(|(client, username)| (
            username.clone(),
            (self::generate_api_key(username), client,)
        )))
        .into_keyed();

    // Add story
    let api_key_add_story = add_story
        .entries()
        .map(q!(|(client, (api_key, title, timestamp))| (
            api_key,
            (client, title, timestamp)
        )))
        .into_keyed();

    // Persisted users
    let curr_users = user_key_add_user
        .assume_ordering(nondet!(/** First user wins */))
        .first();
    // Anything to do with persisted users
    let (add_user_response, get_users_response, add_story_valid_api_key) = sliced! {
        let add_user = use::atomic(atomic_add_user, nondet!(/** New users requests this tick */));
        let get_users = use(get_users, nondet!(/** Order of processing affects which users will be retrieved */));
        let add_story = use(api_key_add_story, nondet!(/** Add story requests sent before user's creation will fail */));

        let curr_users = use::atomic(curr_users, nondet!(/** Current users this tick */));
        let curr_api_keys = curr_users.entries().map(q!(|(_username, (api_key, _client))| (api_key, ()))).into_keyed().assume_ordering(nondet!(/** Actually KeyedSingleton, assuming API key generation is unique */)).first();

        let add_users_response = add_user.lookup_keyed_singleton(curr_users);
        let get_users_response = get_users.cross_singleton(curr_users.into_singleton());
        let add_story_valid_api_key = add_story.join_keyed_singleton(curr_api_keys);
        (add_users_response, get_users_response, add_story_valid_api_key)
    };

    let valid_add_story =
        add_story_valid_api_key
            .entries()
            .map(q!(|(_api_key, ((client, title, timestamp), _))| (
                client, title, timestamp
            )));
    // Anything to do with persisted stories
    let (add_story_with_id, upvote_story_response, get_stories_response) = sliced! {
        let add_story = use(valid_add_story, nondet!(/** Add new incoming story requests this tick */));
        let get_stories = use(get_stories, nondet!(/** Order of processing affects which stories will be retrieved */));
        let upvote_story = use(upvote_story, nondet!(/** If a story didn't exist when the upvote was sent, it will fail */));
        // TODO: Replace with KeyedSingleton once that is supported
        // ID, client, virtual_client, title, timestamp
        let mut stories = use::state_null::<Stream<(u32, Client, String, Instant), _, Bounded, NoOrder>>();
        let mut next_story_id = use::state::<Singleton<u32, _, Bounded>>(|l| l.singleton(q!(0)));

        let new_stories = add_story.clone().assume_ordering(nondet!(/** Stories are assigned an ID in arbitrary order */))
            .enumerate()
            .cross_singleton(next_story_id)
            .map(q!(|((index, (client, title, timestamp)), curr_id)| (index as u32 + curr_id, client, title, timestamp)));
        next_story_id = next_story_id.zip(add_story.count()).map(q!(|(curr_id, count)| curr_id + count as u32));

        stories = stories.chain(new_stories);

        (new_stories, )
    };

    let _top_stories = curr_stories.clone().persist().fold_commutative_idempotent(
        q!(|| vec![]),
        q!(
            |vec, (_api_key, ((_client, _req_id, title, timestamp), username))| {
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
        add_user_response.map_with_key(q!(|(client, (username, user_with_that_name))| {
            let (api_key, client_with_that_name) = user_with_that_name.unwrap();
            if client == client_with_that_name {
                Some(api_key)
            } else {
                None
            }
        })),
        get_users_response.into_keyed(),
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
