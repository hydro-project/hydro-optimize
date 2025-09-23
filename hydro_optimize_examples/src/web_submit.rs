use std::collections::{HashMap, HashSet};

use hydro_lang::{
    live_collections::stream::NoOrder,
    location::{Location, MemberId},
    nondet::nondet,
    prelude::{Process, Stream, Unbounded},
};
use sha2::{Digest, Sha256};
use stageleft::q;

pub struct Server {}

/// Implementation of WebSubmit https://github.com/ms705/websubmit-rs/tree/master.
/// We expose the following APIs:
/// - add_lecture (takes api_key, lecture_id, lecture, only approves if user is admin)
/// - add_question (takes api_key, question, question_id, lecture_id, only approves if user is admin)
/// - add_user (takes user_email, is_admin, hashes user's email + secret, stores API key in table, emails them the key, should only approve if user is admin but it's tautological so just approve everyone)
/// - get_users (takes api_key, only approves if caller is admin, returns user_id, user_email, user_is_admin)
/// - list_lectures (takes api_key, returns lecture_id, lecture)
/// - list_lecture_questions_all (takes api_key & lecture_id, returns question, question_id, optional answer joining on answer_id = question_id, only approves if user is admin)
/// - list_lecture_questions_user (takes api_key & lecture_id, returns question, question_id, optional answer joining on answer_id = question_id if this user wrote the answer)
/// - add_answer (takes api_key, question_id, answer)
///
///   Any call with an invalid API key (either it does not exist or does not have the privileges required) will not receive a response.
#[expect(
    clippy::too_many_arguments,
    clippy::type_complexity,
    reason = "internal Web Submit code // TODO"
)]
pub fn web_submit<'a, Client>(
    server: &Process<'a, Server>,
    add_lecture: Stream<
        (MemberId<Client>, (String, u32, String)),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    add_question: Stream<
        (MemberId<Client>, (String, String, u32, u32)),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    add_user: Stream<(MemberId<Client>, (String, bool)), Process<'a, Server>, Unbounded, NoOrder>,
    get_users: Stream<(MemberId<Client>, String), Process<'a, Server>, Unbounded, NoOrder>,
    list_lectures: Stream<(MemberId<Client>, String), Process<'a, Server>, Unbounded, NoOrder>,
    list_lecture_questions_all: Stream<
        (MemberId<Client>, (String, u32)),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    list_lecture_questions_user: Stream<
        (MemberId<Client>, (String, u32)),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    add_answer: Stream<
        (MemberId<Client>, (String, u32, String)),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
) -> (
    Stream<(MemberId<Client>, ()), Process<'a, Server>, Unbounded, NoOrder>,
    Stream<(MemberId<Client>, ()), Process<'a, Server>, Unbounded, NoOrder>,
    Stream<(MemberId<Client>, ()), Process<'a, Server>, Unbounded, NoOrder>,
    Stream<(MemberId<Client>, HashMap<String, bool>), Process<'a, Server>, Unbounded, NoOrder>,
    Stream<(MemberId<Client>, HashMap<u32, String>), Process<'a, Server>, Unbounded, NoOrder>,
    Stream<
        (MemberId<Client>, HashMap<u32, (String, HashSet<String>)>),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    Stream<
        (MemberId<Client>, HashMap<u32, (String, Option<String>)>),
        Process<'a, Server>,
        Unbounded,
        NoOrder,
    >,
    Stream<(MemberId<Client>, ()), Process<'a, Server>, Unbounded, NoOrder>,
) {
    let user_auth_tick = server.tick();
    let lectures_tick = server.tick();
    let question_answer_tick = server.tick();

    // Add user
    let add_user_with_api_key = add_user.map(q!(|(client_id, (email, is_admin))| {
        let api_key = self::generate_api_key(email.clone());
        (client_id, (email, is_admin, api_key))
    }));
    let users_this_tick_with_api_key = add_user_with_api_key.batch(
        &user_auth_tick,
        nondet!(/** Snapshot current users to approve/deny access */),
    );
    // Persisted users
    let curr_users = users_this_tick_with_api_key
        .clone()
        .map(q!(|(_client_id, (email, is_admin, api_key))| (
            api_key,
            (email, is_admin)
        )))
        .persist();
    let curr_users_hashmap = curr_users.clone().fold_commutative_idempotent(
        q!(|| HashMap::new()),
        q!(|map, (_api_key, (email, is_admin))| {
            map.insert(email, is_admin);
        }),
    );
    // Email the API key. Only done after the tick to ensure that once the client gets the email, the user has been added
    users_this_tick_with_api_key
        .clone()
        .all_ticks()
        .assume_ordering(nondet!(/** Email order doesn't matter */))
        .assume_retries(nondet!(/** At least once delivery is fine */))
        .for_each(q!(|(_client_id, (email, _is_admin, api_key))| {
            self::send_email(api_key, email)
        }));
    // Send response back to client. Only done after the tick to ensure that once the client gets the response, the user has been added
    let add_user_response =
        users_this_tick_with_api_key.all_ticks().map(q!(|(
            client_id,
            (_email, _is_admin, _api_key),
        )| (client_id, ())));

    // Add lecture
    let add_lecture_pre_join =
        add_lecture.map(q!(|(client_id, (api_key, lecture_id, lecture))| {
            (api_key, (client_id, lecture_id, lecture))
        }));
    let lectures = add_lecture_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks()
        .filter(q!(|(
            _api_key,
            ((_client_id, _lecture_id, _lecture), (_email, is_admin)),
        )| *is_admin));
    let curr_lectures =
        lectures.batch(&lectures_tick, nondet!(/** Snapshot of current lectures */));
    let curr_lectures_hashmap = curr_lectures.clone().persist().fold_commutative_idempotent(
        q!(|| HashMap::new()),
        q!(
            |map, (_api_key, ((_client_id, lecture_id, lecture), (_email, _is_admin)))| {
                map.insert(lecture_id, lecture);
            }
        ),
    );
    // Only done after the lectures_tick to ensure that once the client gets the response, the lecture has been added
    let add_lecture_response = curr_lectures.all_ticks().map(q!(|(
        _api_key,
        ((client_id, _lecture_id, _lecture), (_email, _is_admin)),
    )| (client_id, ())));

    // Add question
    let add_question_pre_join = add_question.map(q!(|(
        client_id,
        (api_key, question, question_id, lecture_id),
    )| {
        (api_key, (client_id, question, question_id, lecture_id))
    }));
    let add_question_auth = add_question_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks()
        .filter(q!(|(
            _api_key,
            ((_client_id, _question, _question_id, _lecture_id), (_email, is_admin)),
        )| *is_admin));
    let add_question_this_tick = add_question_auth.batch(
        &question_answer_tick,
        nondet!(/** Snapshot of current questions */),
    );
    let curr_questions = add_question_this_tick
        .clone()
        .map(q!(|(
            _api_key,
            ((_client_id, question, question_id, lecture_id), (_email, _is_admin)),
        )| (lecture_id, (question_id, question))))
        .persist();
    // Only done after the question_answer_tick to ensure that once the client gets the response, the question has been added
    let add_question_response = add_question_this_tick.all_ticks().map(q!(|(
        _api_key,
        ((client_id, _question, _question_id, _lecture_id), (_email, _is_admin)),
    )| (client_id, ())));

    // Get users
    let get_users_pre_join = get_users.map(q!(|(client_id, api_key)| (api_key, client_id)));
    let get_users_response = get_users_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .filter_map(q!(|(_api_key, (client_id, (_email, is_admin)))| {
            if is_admin { Some(client_id) } else { None }
        }))
        .cross_singleton(curr_users_hashmap)
        .all_ticks();

    // List lectures
    let list_lectures_pre_join = list_lectures.map(q!(|(client_id, api_key)| (api_key, client_id)));
    let list_lectures_auth = list_lectures_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks()
        .map(q!(|(_api_key, (client_id, (_email, _is_admin)))| client_id));
    let list_lectures_response = list_lectures_auth
        .batch(
            &lectures_tick,
            nondet!(/** Join with snapshot of current lectures */),
        )
        .cross_singleton(curr_lectures_hashmap)
        .all_ticks();

    // Add answer
    let add_answer_pre_join = add_answer.map(q!(|(client_id, (api_key, question_id, answer))| {
        (api_key, (client_id, question_id, answer))
    }));
    let add_answer_auth = add_answer_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks();
    let add_answer_this_tick = add_answer_auth.batch(
        &question_answer_tick,
        nondet!(/** Snapshot of current answers */),
    );
    let curr_answers = add_answer_this_tick
        .clone()
        .map(q!(|(
            api_key,
            ((_client_id, question_id, answer), (_email, _is_admin)),
        )| ((question_id, api_key), answer)))
        .persist();
    // Only done after the question_answer_tick to ensure that once the client gets the response, the answer has been added
    let add_answer_response = add_answer_this_tick.all_ticks().map(q!(|(
        _api_key,
        ((client_id, _question_id, _answer), (_email, _is_admin)),
    )| (client_id, ())));

    // List lecture questions all
    let list_lecture_questions_all_pre_join =
        list_lecture_questions_all.map(q!(|(client_id, (api_key, lecture_id))| {
            (api_key, (client_id, lecture_id))
        }));
    let list_lecture_questions_all_auth = list_lecture_questions_all_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks()
        .filter_map(q!(|(
            _api_key,
            ((client_id, lecture_id), (_email, is_admin)),
        )| {
            if is_admin {
                Some((lecture_id, client_id))
            } else {
                None
            }
        }));
    // Find all questions with that ID
    let list_lecture_questions_all_question_only = list_lecture_questions_all_auth
        .batch(
            &question_answer_tick,
            nondet!(/** Join with snapshot of current questions */),
        )
        .join(curr_questions.clone())
        .map(q!(|(_lecture_id, (client_id, (question_id, question)))| (
            question_id,
            (client_id, question)
        )));
    // Don't need to join on api_key since we're getting all answers, regardless of who wrote them
    let curr_answers_no_api_key =
        curr_answers
            .clone()
            .map(q!(|((question_id, _api_key), answer)| (
                question_id,
                answer
            )));
    // Find all answers with the question ID
    let list_lecture_questions_all_with_answer = list_lecture_questions_all_question_only
        .clone()
        .join(curr_answers_no_api_key.clone())
        .map(q!(|(question_id, ((client_id, question), answer))| {
            (client_id, (question_id, question, Some(answer)))
        }));
    // Find all questions without answers
    let list_lecture_questions_all_no_answer = list_lecture_questions_all_question_only
        .anti_join(curr_answers_no_api_key.map(q!(|(question_id, _answer)| question_id)))
        .map(q!(|(question_id, (client_id, question))| (
            client_id,
            (question_id, question, None)
        )));
    let list_lecture_questions_all_response = list_lecture_questions_all_with_answer
        .chain(list_lecture_questions_all_no_answer)
        .into_keyed()
        .fold_commutative_idempotent(
            q!(|| HashMap::new()),
            q!(|map, (question_id, question, answer)| {
                let (_question, set_of_answers) =
                    map.entry(question_id).or_insert((question, HashSet::new()));
                if let Some(answer) = answer {
                    set_of_answers.insert(answer);
                }
            }),
        )
        .entries()
        .all_ticks();

    // List lecture questions user
    let list_lecture_questions_user_pre_join =
        list_lecture_questions_user.map(q!(|(client_id, (api_key, lecture_id))| {
            (api_key, (client_id, lecture_id))
        }));
    let list_lecture_questions_user_auth = list_lecture_questions_user_pre_join
        .batch(
            &user_auth_tick,
            nondet!(/** Compare against current users to approve/deny access */),
        )
        .join(curr_users.clone())
        .all_ticks()
        .map(q!(|(
            api_key,
            ((client_id, lecture_id), (_email, _is_admin)),
        )| (lecture_id, (client_id, api_key))));
    let list_lecture_questions_user_question_only = list_lecture_questions_user_auth
        .batch(
            &question_answer_tick,
            nondet!(/** Join with snapshot of current questions */),
        )
        .join(curr_questions)
        .map(q!(|(
            _lecture_id,
            ((client_id, api_key), (question_id, question)),
        )| (
            (question_id, api_key),
            (client_id, question)
        )));
    // Find all answers with the question ID
    let list_lecture_questions_user_with_answer = list_lecture_questions_user_question_only
        .clone()
        .join(curr_answers.clone())
        .map(q!(|(
            (question_id, _api_key),
            ((client_id, question), answer),
        )| {
            (client_id, (question_id, question, Some(answer)))
        }));
    // Find all questions without answers
    let list_lecture_questions_user_no_answer = list_lecture_questions_user_question_only
        .anti_join(curr_answers.map(q!(|(k, _)| k)))
        .map(q!(|((question_id, _api_key), (client_id, question))| (
            client_id,
            (question_id, question, None)
        )));
    let list_lecture_questions_user_response = list_lecture_questions_user_with_answer
        .chain(list_lecture_questions_user_no_answer)
        .into_keyed()
        .fold_commutative_idempotent(
            q!(|| HashMap::new()),
            q!(|map, (question_id, question, answer)| {
                map.insert(question_id, (question, answer));
            }),
        )
        .entries()
        .all_ticks();

    (
        add_lecture_response,
        add_question_response,
        add_user_response,
        get_users_response,
        list_lectures_response,
        list_lecture_questions_all_response,
        list_lecture_questions_user_response,
        add_answer_response,
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

fn send_email(_api_key: String, _email: String) {}
