use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use argon2::{Argon2, PasswordHasher};
use flpc::unit::ServerUnit;
use futures_util::{StreamExt, poll};
use sqlx::{PgPool, types::JsonValue};
use tokio::{net::TcpListener, sync::Semaphore, time::timeout};

use super::*;
use crate::database::{serial_tests::postgres_test, users::UserHandler};

const LIMITS: (usize, usize) = (4, 6);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn request(username: &str, password: &str) -> RemoveSelf {
    RemoveSelf::new(username.into(), password.into())
}

#[derive(Debug, PartialEq)]
struct RemovalSnapshot {
    user: Option<JsonValue>,
    profile: Option<JsonValue>,
    id_available: bool,
}

async fn seed_account(pool: &PgPool, username: &str, password: &str) -> sqlx::Result<()> {
    let hash = Argon2::default()
        .hash_password(password.as_bytes())
        .expect("test password should hash")
        .to_string();
    sqlx::query("DELETE FROM available_user_ids WHERE user_id = 7")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE VALUES (7, $1, $2)",
    )
    .bind(username)
    .bind(hash)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO user_information (user_id, username) VALUES (7, $1)")
        .bind(username)
        .execute(pool)
        .await?;
    Ok(())
}

async fn target_snapshot(pool: &PgPool) -> sqlx::Result<RemovalSnapshot> {
    Ok(RemovalSnapshot {
        user: sqlx::query_scalar::<_, JsonValue>(
            "SELECT to_jsonb(record) FROM (SELECT * FROM users WHERE user_id = 7) AS record",
        )
        .fetch_optional(pool)
        .await?,
        profile: sqlx::query_scalar::<_, JsonValue>(
            "SELECT to_jsonb(record) FROM (
                 SELECT * FROM user_information WHERE user_id = 7
             ) AS record",
        )
        .fetch_optional(pool)
        .await?,
        id_available: sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 7)",
        )
        .fetch_one(pool)
        .await?,
    })
}

async fn run_removal(
    pool: PgPool,
    username: &str,
    password: &str,
    permit: Arc<Semaphore>,
) -> u8 {
    timeout(TEST_TIMEOUT, async {
        let (sink, mut peer) = loopback().await;
        let (user_tx, user_rx) = mpsc::channel(1);
        let worker = tokio::spawn(UserHandler::new(pool, user_rx, permit).run());
        timeout(
            TEST_TIMEOUT,
            handle_removeself(
                request(username, password),
                user_tx.clone(),
                sink,
                LIMITS.0,
                LIMITS.1,
            ),
        )
        .await
        .expect("removal handler should finish");
        let code = timeout(TEST_TIMEOUT, read_result(&mut peer))
            .await
            .expect("peer should receive one frame and EOF");
        drop(user_tx);
        timeout(TEST_TIMEOUT, worker)
            .await
            .expect("worker should stop after the request sender closes")
            .expect("worker task should join");
        code
    })
    .await
    .expect("removal exchange should finish within the timeout")
}

async fn loopback() -> (FlpcSink, Framed<TcpStream, FlpcCodec>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let (sink, _) = Framed::new(server, FlpcCodec).split();
    (sink, Framed::new(peer, FlpcCodec))
}

async fn read_result(peer: &mut Framed<TcpStream, FlpcCodec>) -> u8 {
    let frame = peer.next().await.expect("removal response").expect("valid FLPC frame");
    let FlpcUnit::Server(ServerUnit::ServerRemoveSelf(response)) = frame else {
        panic!("expected ServerRemoveSelf, got {frame:?}");
    };
    assert!(peer.next().await.is_none(), "expected exactly one response frame");
    *response.result()
}

fn try_receive_request(user_rx: &mut mpsc::Receiver<UserRequest>) -> RemoveSelfRequest {
    let UserRequest::RemoveSelfRequest(request) = user_rx
        .try_recv()
        .expect("removal request should be queued")
    else {
        panic!("expected a removal request");
    };
    request
}

fn fill_request_queue(user_tx: &mpsc::Sender<UserRequest>) {
    let (response_tx, _) = tokio::sync::oneshot::channel();
    let request = RemoveSelfRequest::new("occupied".into(), "🔑é".into(), response_tx);
    assert!(user_tx
        .try_send(UserRequest::RemoveSelfRequest(request))
        .is_ok());
}

fn assert_no_queued_request(user_rx: &mut mpsc::Receiver<UserRequest>) {
    assert!(matches!(
        user_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
}

// Alias: Forwards the exact request
// Input: Receive the helper's message and respond with Success(65535)
// Expected: Exactly one request of the correct variant, with both strings unchanged. Caller gets Success(65535).
#[tokio::test]
async fn try_remove_forward() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_remove("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());

    let request = try_receive_request(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "🔑é");
    assert_no_queued_request(&mut user_rx);
    assert!(request
        .try_send(RemoveSelfResponse::Success(u16::MAX))
        .is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should receive the worker reply");
    assert!(matches!(response, RemoveSelfResponse::Success(u16::MAX)));
    assert_no_queued_request(&mut user_rx);
}

// Alias: Handles a closed request channel
// Input: Drop the mpsc receiver before calling
// Expected: HandlerError.
#[tokio::test]
async fn try_remove_no_receiver() {
    let (user_tx, user_rx) = mpsc::channel(1);
    drop(user_rx);
    let response = timeout(TEST_TIMEOUT, try_remove("éé", "🔑é", user_tx))
        .await
        .expect("caller should finish when the receiver is gone");
    assert!(matches!(response, RemoveSelfResponse::HandlerError));
}

// Alias: Handles a dropped one-shot sender
// Input: Receive and drop the request without replying
// Expected: HandlerError.
#[tokio::test]
async fn try_remove_no_reply() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_remove("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = try_receive_request(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "🔑é");
    drop(request);

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish when the reply sender is dropped");
    assert!(matches!(response, RemoveSelfResponse::HandlerError));
}

// Alias: Waits for channel capacity
// Input: Fill the channel with a request for "occupied", poll the call, drain the filler, then respond Success(65535) to the tested request
// Expected: Pending while full, then forwards the tested request once and returns Success(65535).
#[tokio::test]
async fn try_remove_full() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    fill_request_queue(&user_tx);
    let mut caller = pin!(try_remove("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending(), "send waits while full");

    let filler = try_receive_request(&mut user_rx);
    assert_eq!(filler.username(), "occupied");
    assert_eq!(filler.password(), "🔑é");
    assert!(poll!(caller.as_mut()).is_pending(), "caller waits for its reply");

    let request = try_receive_request(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "🔑é");
    assert_no_queued_request(&mut user_rx);
    assert!(request
        .try_send(RemoveSelfResponse::Success(u16::MAX))
        .is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RemoveSelfResponse::Success(u16::MAX)));
    assert_no_queued_request(&mut user_rx);
}

// Alias: Forwards the missing user reply
// Input: RemoveSelfResponse::UserNotFound
// Expected: RemoveSelfResponse::UserNotFound
#[tokio::test]
async fn try_remove_not_found() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_remove("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = try_receive_request(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "🔑é");
    assert!(request.try_send(RemoveSelfResponse::UserNotFound).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RemoveSelfResponse::UserNotFound));
}

// Alias: Forwards the wrong credentials reply
// Input: RemoveSelfResponse::WrongCredentials
// Expected: RemoveSelfResponse::WrongCredentials
#[tokio::test]
async fn try_remove_wrong_credentials() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_remove("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = try_receive_request(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "🔑é");
    assert!(request.try_send(RemoveSelfResponse::WrongCredentials).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RemoveSelfResponse::WrongCredentials));
}

// Alias: Forwards the removal handler error
// Input: RemoveSelfResponse::HandlerError
// Expected: RemoveSelfResponse::HandlerError
#[tokio::test]
async fn try_remove_handler_error() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_remove("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = try_receive_request(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "🔑é");
    assert!(request.try_send(RemoveSelfResponse::HandlerError).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RemoveSelfResponse::HandlerError));
}

// Alias: Removes at exact UTF-8 byte limits
// Input: account(7, "éé", H("🔑é")), ("éé", "🔑é")
// Expected: Code 0. Account 7 and its profile are gone; ID 7 remains claimed.
postgres_test! {
    async fn removeself_ok(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "éé", "🔑é").await?;
        let code = run_removal(
            pool.clone(),
            "éé",
            "🔑é",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 0);
        assert_eq!(
            target_snapshot(&pool).await?,
            RemovalSnapshot {
                user: None,
                profile: None,
                id_available: false,
            }
        );
        Ok(())
    }
}

// Alias: Preserves the account for wrong credentials
// Input: account(7, "user", H("secret")), ("user", "wrong")
// Expected: Code 1. Account 7 and its profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_wrong(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "user", "secret").await?;
        let before = target_snapshot(&pool).await?;
        let code = run_removal(
            pool.clone(),
            "user",
            "wrong",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 1);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an empty username
// Input: account(7, "user", H("secret")), ("", "secret")
// Expected: Code 2. Account 7 and its profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_empty_user(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "user", "secret").await?;
        let before = target_snapshot(&pool).await?;
        let code = run_removal(
            pool.clone(),
            "",
            "secret",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an empty password
// Input: account(7, "user", H("secret")), ("user", "")
// Expected: Code 2. Account 7 and its profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_empty_password(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "user", "secret").await?;
        let before = target_snapshot(&pool).await?;
        let code = run_removal(
            pool.clone(),
            "user",
            "",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an over-limit username before deletion
// Input: account(7, "users", H("secret")), ("users", "secret")
// Expected: Code 2. The matching account and profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_long_user(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "users", "secret").await?;
        let before = target_snapshot(&pool).await?;
        let code = run_removal(
            pool.clone(),
            "users",
            "secret",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an over-limit password before deletion
// Input: account(7, "user", H("secrets")), ("user", "secrets")
// Expected: Code 2. The matching account and profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_long_password(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "user", "secrets").await?;
        let before = target_snapshot(&pool).await?;
        let code = run_removal(
            pool.clone(),
            "user",
            "secrets",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Reports a missing username
// Input: account(7, "user", H("secret")), ("none", "secret")
// Expected: Code 3. Account 7 and its profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_missing(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "user", "secret").await?;
        let before = target_snapshot(&pool).await?;
        let code = run_removal(
            pool.clone(),
            "none",
            "secret",
            Arc::new(Semaphore::new(1)),
        )
        .await;

        assert_eq!(code, 3);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Reports verification failure
// Input: account(7, "user", H("secret")), closed worker verification semaphore, ("user", "secret")
// Expected: Code 4. Account 7 and its profile remain; ID 7 stays claimed.
postgres_test! {
    async fn removeself_worker_error(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, "user", "secret").await?;
        let before = target_snapshot(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        permit.close();
        let code = run_removal(pool.clone(), "user", "secret", permit).await;

        assert_eq!(code, 4);
        assert_eq!(target_snapshot(&pool).await?, before);
        Ok(())
    }
}
