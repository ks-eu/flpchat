use std::time::Duration;

use argon2::{Argon2, PasswordHasher};
use sqlx::PgPool;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

use crate::database::{hash::verify_password, serial_tests::postgres_test};

use super::{
    LoginRequest, LoginResponse, RegRequest, RegResponse, RemoveSelfRequest, RemoveSelfResponse, UserHandler, UserRequest, run,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn set_available_ids(pool: &PgPool, ids: &[i32]) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE NOT (user_id = ANY($1))")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_account(pool: &PgPool, id: i32, username: &str, hash: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE user_id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3)",
    )
    .bind(id)
    .bind(username)
    .bind(hash)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO user_information (user_id, username) VALUES ($1, $2)")
        .bind(id)
        .bind(username)
        .execute(pool)
        .await?;
    Ok(())
}

fn start_worker(pool: &PgPool) -> (mpsc::Sender<UserRequest>, JoinHandle<()>) {
    let (user_tx, user_rx) = mpsc::channel(4);
    let worker =
        tokio::spawn(run(UserHandler::new(user_rx, pool.clone(), 1)));
    (user_tx, worker)
}

fn queue_registration(
    user_tx: &mpsc::Sender<UserRequest>,
    username: &str,
    password: &str,
) -> oneshot::Receiver<RegResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    user_tx
        .try_send(UserRequest::RegRequest(RegRequest::new(
            username.into(),
            password.into(),
            reply_tx,
        )))
        .unwrap();
    reply_rx
}

fn queue_removal(
    user_tx: &mpsc::Sender<UserRequest>,
    username: &str,
    password: &str,
) -> oneshot::Receiver<RemoveSelfResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    user_tx
        .try_send(UserRequest::RemoveSelfRequest(RemoveSelfRequest::new(
            username.into(),
            password.into(),
            reply_tx,
        )))
        .unwrap();
    reply_rx
}

fn queue_login(
    user_tx: &mpsc::Sender<UserRequest>,
    username: &str,
    password: &str,
) -> oneshot::Receiver<LoginResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    user_tx
        .try_send(UserRequest::LoginRequest(LoginRequest::new(
            username.into(),
            password.into(),
            reply_tx,
        )))
        .unwrap();
    reply_rx
}

async fn finish_worker(
    user_tx: mpsc::Sender<UserRequest>,
    worker: JoinHandle<()>,
) -> Result<(), tokio::task::JoinError> {
    drop(user_tx);
    timeout(TEST_TIMEOUT, worker)
        .await
        .expect("worker should exit after draining requests")
}

async fn reply_and_finish<T>(
    user_tx: mpsc::Sender<UserRequest>,
    worker: JoinHandle<()>,
    reply: oneshot::Receiver<T>,
) -> T {
    let result = timeout(TEST_TIMEOUT, reply)
        .await
        .expect("worker reply should arrive")
        .expect("worker should keep the reply sender");
    finish_worker(user_tx, worker)
        .await
        .expect("worker should join");
    result
}

async fn account(pool: &PgPool, username: &str) -> sqlx::Result<Option<(i32, String)>> {
    sqlx::query_as("SELECT user_id, password_hash FROM users WHERE username = $1")
        .bind(username)
        .fetch_optional(pool)
        .await
}

async fn profile_id(pool: &PgPool, username: &str) -> sqlx::Result<Option<i32>> {
    sqlx::query_scalar("SELECT user_id FROM user_information WHERE username = $1")
        .bind(username)
        .fetch_optional(pool)
        .await
}

async fn available_ids(pool: &PgPool) -> sqlx::Result<Vec<i32>> {
    sqlx::query_scalar("SELECT user_id FROM available_user_ids ORDER BY user_id")
        .fetch_all(pool)
        .await
}

// Alias: Registers a new username
// Input: IDs=[7], RegRequest("new-user", "new-password")
// Expected: Success(7). The stored hash verifies "new-password".
postgres_test! {
    async fn worker_reg_new(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "new-user", "new-password");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::Success(7)));
        let (id, hash) = account(&pool, "new-user").await?.unwrap();
        assert_eq!(id, 7);
        assert_eq!(verify_password("new-password", &hash), Ok(true));
        assert_eq!(profile_id(&pool, "new-user").await?, Some(7));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Rejects the same username and password
// Input: account(7, "taken", H("secret")), IDs=[8], RegRequest("taken", "secret")
// Expected: UsernameExists. Account 7 and ID 8 stay unchanged.
postgres_test! {
    async fn worker_reg_same_password(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "taken", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "taken", "secret");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::UsernameExists));
        assert_eq!(account(&pool, "taken").await?, Some((7, hash.clone())));
        assert_eq!(profile_id(&pool, "taken").await?, Some(7));
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Rejects the same username with a different password
// Input: account(7, "taken", "old-hash"), IDs=[8], RegRequest("taken", "different-password")
// Expected: UsernameExists. Stored hash remains "old-hash"; ID 8 stays available.
postgres_test! {
    async fn worker_reg_different_password(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "taken", "different-password");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::UsernameExists));
        assert_eq!(account(&pool, "taken").await?, Some((7, "old-hash".into())));
        assert_eq!(profile_id(&pool, "taken").await?, Some(7));
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Accepts a new username with an existing password
// Input: account(7, "taken", H("secret")), IDs=[8], RegRequest("new-user", "secret")
// Expected: Success(8). The two distinct hashes verify the same password.
postgres_test! {
    async fn worker_reg_reused_password(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "taken", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "new-user", "secret");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::Success(8)));
        assert_eq!(account(&pool, "taken").await?, Some((7, hash.clone())));
        let (id, new_hash) = account(&pool, "new-user").await?.unwrap();
        assert_eq!(id, 8);
        assert_ne!(new_hash, hash);
        assert_eq!(verify_password("secret", &new_hash), Ok(true));
        assert_eq!(profile_id(&pool, "taken").await?, Some(7));
        assert_eq!(profile_id(&pool, "new-user").await?, Some(8));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Reports exhausted IDs
// Input: IDs=[], RegRequest("new-user", "new-password")
// Expected: NotEnoughSpace. No account or profile created.
postgres_test! {
    async fn worker_reg_full(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[]).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "new-user", "new-password");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::NotEnoughSpace));
        assert_eq!(account(&pool, "new-user").await?, None);
        assert_eq!(profile_id(&pool, "new-user").await?, None);
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Removes with correct credentials
// Input: account(7, "target", H("secret")), RemoveSelfRequest("target", "secret")
// Expected: Success(7). Account 7 and its profile are gone.
postgres_test! {
    async fn worker_remove_match(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "target", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_removal(&user_tx, "target", "secret");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RemoveSelfResponse::Success(7)));
        assert_eq!(account(&pool, "target").await?, None);
        assert_eq!(profile_id(&pool, "target").await?, None);
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Rejects a wrong password
// Input: account(7, "target", H("secret")), RemoveSelfRequest("target", "wrong")
// Expected: WrongCredentials. Account 7 and its profile remain.
postgres_test! {
    async fn worker_remove_wrong(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "target", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_removal(&user_tx, "target", "wrong");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RemoveSelfResponse::WrongCredentials));
        assert_eq!(account(&pool, "target").await?, Some((7, hash)));
        assert_eq!(profile_id(&pool, "target").await?, Some(7));
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Reports a missing user
// Input: account(7, "target", H("secret")), RemoveSelfRequest("absent", "secret")
// Expected: UserNotFound. Account 7 stays intact.
postgres_test! {
    async fn worker_remove_missing(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "target", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_removal(&user_tx, "absent", "secret");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RemoveSelfResponse::UserNotFound));
        assert_eq!(account(&pool, "target").await?, Some((7, hash)));
        assert_eq!(profile_id(&pool, "target").await?, Some(7));
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Continues after a dropped reply receiver
// Input: IDs=[7,8]. Queue RegRequest("first", "first-password"), drop its reply receiver, then queue RegRequest("second", "second-password").
// Expected: Both accounts are inserted. The second caller receives Success(8).
postgres_test! {
    async fn worker_reply_dropped(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let (user_tx, user_rx) = mpsc::channel(2);
        let first = queue_registration(&user_tx, "first", "first-password");
        drop(first);
        let second = queue_registration(&user_tx, "second", "second-password");
        let worker = tokio::spawn(
            run(UserHandler::new(user_rx, pool.clone(), 1)),
        );
        let result = reply_and_finish(user_tx, worker, second).await;

        assert!(matches!(result, RegResponse::Success(8)));
        let (id, hash) = account(&pool, "first").await?.unwrap();
        assert_eq!(id, 7);
        assert_eq!(verify_password("first-password", &hash), Ok(true));
        let (id, hash) = account(&pool, "second").await?.unwrap();
        assert_eq!(id, 8);
        assert_eq!(verify_password("second-password", &hash), Ok(true));
        assert_eq!(profile_id(&pool, "first").await?, Some(7));
        assert_eq!(profile_id(&pool, "second").await?, Some(8));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Drains the queue before shutdown
// Input: IDs=[7], queue RegRequest("queued", "queued-password"), then close all request senders before running the worker.
// Expected: The caller receives Success(7) and run returns within the test timeout.
postgres_test! {
    async fn worker_drain(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        let (user_tx, user_rx) = mpsc::channel(1);
        let reply = queue_registration(&user_tx, "queued", "queued-password");
        drop(user_tx);

        let worker = UserHandler::new(user_rx, pool.clone(), 1);
        let result = timeout(TEST_TIMEOUT, async {
            tokio::join!(run(worker), reply)
        })
        .await
        .expect("worker should drain and exit");
        let (_, reply) = result;
        assert!(matches!(reply, Ok(RegResponse::Success(7))));
        let (id, hash) = account(&pool, "queued").await?.unwrap();
        assert_eq!(id, 7);
        assert_eq!(verify_password("queued-password", &hash), Ok(true));
        assert_eq!(profile_id(&pool, "queued").await?, Some(7));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

postgres_test! {
    async fn worker_login_success(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_account(&pool, 65535, "Élodie", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_login(&user_tx, "Élodie", "café🔑");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, LoginResponse::Success(u16::MAX)));
        assert_eq!(account(&pool, "Élodie").await?, Some((65535, hash)));
        assert_eq!(profile_id(&pool, "Élodie").await?, Some(65535));
        assert_eq!(available_ids(&pool).await?, [8]);
        Ok(())
    }
}

postgres_test! {
    async fn worker_login_rejects_credentials(pool: PgPool) -> sqlx::Result<()> {
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_account(&pool, 7, "Élodie", &hash).await?;
        let (user_tx, worker) = start_worker(&pool);
        let wrong = queue_login(&user_tx, "Élodie", "cafe\u{301}🔑");
        let missing = queue_login(&user_tx, "élodie", "café🔑");

        assert!(matches!(timeout(TEST_TIMEOUT, wrong).await, Ok(Ok(LoginResponse::WrongCredentials))));
        let result = reply_and_finish(user_tx, worker, missing).await;
        assert!(matches!(result, LoginResponse::UserNotFound));
        assert_eq!(account(&pool, "Élodie").await?, Some((7, hash)));
        Ok(())
    }
}

postgres_test! {
    async fn worker_login_bad_hash(pool: PgPool) -> sqlx::Result<()> {
        seed_account(&pool, 7, "target", "invalid-phc").await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_login(&user_tx, "target", "secret");
        let result = reply_and_finish(user_tx, worker, reply).await;
        assert!(matches!(result, LoginResponse::HandlerError));
        Ok(())
    }
}

postgres_test! {
    async fn worker_login_closed_semaphore(pool: PgPool) -> sqlx::Result<()> {
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "target", &hash).await?;
        let (user_tx, user_rx) = mpsc::channel(2);
        let handler = UserHandler::new(user_rx, pool, 1);
        handler.permit().close();
        let worker = tokio::spawn(run(handler));
        let existing = queue_login(&user_tx, "target", "secret");
        let missing = queue_login(&user_tx, "absent", "secret");

        assert!(matches!(timeout(TEST_TIMEOUT, existing).await, Ok(Ok(LoginResponse::HandlerError))));
        let result = reply_and_finish(user_tx, worker, missing).await;
        assert!(matches!(result, LoginResponse::UserNotFound));
        Ok(())
    }
}

postgres_test! {
    async fn worker_login_closed_pool(pool: PgPool) -> sqlx::Result<()> {
        pool.close().await;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_login(&user_tx, "target", "secret");
        let result = reply_and_finish(user_tx, worker, reply).await;
        assert!(matches!(result, LoginResponse::HandlerError));
        Ok(())
    }
}

postgres_test! {
    async fn worker_login_dropped_reply(pool: PgPool) -> sqlx::Result<()> {
        let hash = Argon2::default().hash_password(b"secret").unwrap().to_string();
        seed_account(&pool, 7, "target", &hash).await?;
        let (user_tx, user_rx) = mpsc::channel(2);
        drop(queue_login(&user_tx, "target", "secret"));
        let reply = queue_login(&user_tx, "target", "secret");
        let worker = tokio::spawn(
            run(UserHandler::new(user_rx, pool, 1)),
        );

        let result = reply_and_finish(user_tx, worker, reply).await;
        assert!(matches!(result, LoginResponse::Success(7)));
        Ok(())
    }
}
