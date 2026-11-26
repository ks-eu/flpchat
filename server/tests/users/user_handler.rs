use std::{sync::Arc, time::Duration};

use argon2::{Argon2, PasswordHasher};
use sqlx::PgPool;
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

use crate::database::serial_tests::postgres_test;

use super::{
    RegRequest, RegResponse, RemoveSelfRequest, RemoveSelfResponse, UserHandler, UserRequest,
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
        tokio::spawn(UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1))).run());
    (user_tx, worker)
}

fn queue_registration(
    user_tx: &mpsc::Sender<UserRequest>,
    username: &str,
    hash: &str,
) -> oneshot::Receiver<RegResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    user_tx
        .try_send(UserRequest::RegRequest(RegRequest::new(
            username.into(),
            hash.into(),
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
// Input: IDs=[7], RegRequest("new-user", "new-hash")
// Expected: Success(7). Account 7 stores "new-hash".
postgres_test! {
    async fn worker_reg_new(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "new-user", "new-hash");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::Success(7)));
        assert_eq!(account(&pool, "new-user").await?, Some((7, "new-hash".into())));
        assert_eq!(profile_id(&pool, "new-user").await?, Some(7));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Rejects the same username and hash
// Input: account(7, "taken", "old-hash"), IDs=[8], RegRequest("taken", "old-hash")
// Expected: UsernameExists. Account 7 and ID 8 stay unchanged.
postgres_test! {
    async fn worker_reg_same_hash(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "taken", "old-hash");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::UsernameExists));
        assert_eq!(account(&pool, "taken").await?, Some((7, "old-hash".into())));
        assert_eq!(profile_id(&pool, "taken").await?, Some(7));
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Rejects the same username with a different hash
// Input: account(7, "taken", "old-hash"), IDs=[8], RegRequest("taken", "different-hash")
// Expected: UsernameExists. Stored hash remains "old-hash"; ID 8 stays available.
postgres_test! {
    async fn worker_reg_different_hash(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "taken", "different-hash");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::UsernameExists));
        assert_eq!(account(&pool, "taken").await?, Some((7, "old-hash".into())));
        assert_eq!(profile_id(&pool, "taken").await?, Some(7));
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Accepts a new username with an existing hash
// Input: account(7, "taken", "old-hash"), IDs=[8], RegRequest("new-user", "old-hash")
// Expected: Success(8). Both accounts store "old-hash".
postgres_test! {
    async fn worker_reg_reused_hash(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "new-user", "old-hash");
        let result = reply_and_finish(user_tx, worker, reply).await;

        assert!(matches!(result, RegResponse::Success(8)));
        assert_eq!(account(&pool, "taken").await?, Some((7, "old-hash".into())));
        assert_eq!(account(&pool, "new-user").await?, Some((8, "old-hash".into())));
        assert_eq!(profile_id(&pool, "taken").await?, Some(7));
        assert_eq!(profile_id(&pool, "new-user").await?, Some(8));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Reports exhausted IDs
// Input: IDs=[], RegRequest("new-user", "new-hash")
// Expected: NotEnoughSpace. No account or profile created.
postgres_test! {
    async fn worker_reg_full(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[]).await?;
        let (user_tx, worker) = start_worker(&pool);
        let reply = queue_registration(&user_tx, "new-user", "new-hash");
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
// Input: IDs=[7,8]. Queue RegRequest("first", "first-hash"), drop its reply receiver, then queue RegRequest("second", "second-hash").
// Expected: Both accounts are inserted. The second caller receives Success(8).
postgres_test! {
    async fn worker_reply_dropped(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let (user_tx, user_rx) = mpsc::channel(2);
        let first = queue_registration(&user_tx, "first", "first-hash");
        drop(first);
        let second = queue_registration(&user_tx, "second", "second-hash");
        let worker = tokio::spawn(
            UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1))).run(),
        );
        let result = reply_and_finish(user_tx, worker, second).await;

        assert!(matches!(result, RegResponse::Success(8)));
        assert_eq!(account(&pool, "first").await?, Some((7, "first-hash".into())));
        assert_eq!(account(&pool, "second").await?, Some((8, "second-hash".into())));
        assert_eq!(profile_id(&pool, "first").await?, Some(7));
        assert_eq!(profile_id(&pool, "second").await?, Some(8));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Drains the queue before shutdown
// Input: IDs=[7], queue RegRequest("queued", "queued-hash"), then close all request senders before running the worker.
// Expected: The caller receives Success(7) and run returns within the test timeout.
postgres_test! {
    async fn worker_drain(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        let (user_tx, user_rx) = mpsc::channel(1);
        let reply = queue_registration(&user_tx, "queued", "queued-hash");
        drop(user_tx);

        let worker = UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1)));
        let result = timeout(TEST_TIMEOUT, async {
            tokio::join!(worker.run(), reply)
        })
        .await
        .expect("worker should drain and exit");
        let (_, reply) = result;
        assert!(matches!(reply, Ok(RegResponse::Success(7))));
        assert_eq!(account(&pool, "queued").await?, Some((7, "queued-hash".into())));
        assert_eq!(profile_id(&pool, "queued").await?, Some(7));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}
