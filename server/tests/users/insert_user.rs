use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    time::{sleep, timeout},
};

use crate::database::{hash::verify_password, serial_tests::postgres_test};
use crate::users::registration::RegRequest;

use super::{PgPool, RegResponse, UserHandler, UserRequest};

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

fn open_permit() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(1))
}

fn closed_permit() -> Arc<Semaphore> {
    let permit = open_permit();
    permit.close();
    permit
}

fn handler(pool: &PgPool) -> UserHandler {
    UserHandler::new(pool.clone(), mpsc::channel(1).1)
}

async fn insert(
    handler: &UserHandler,
    username: &str,
    password: &str,
    permit: Arc<Semaphore>,
) -> RegResponse {
    let (reply_tx, reply_rx) = oneshot::channel();
    let request = RegRequest::new(username.into(), password.into(), reply_tx);
    handler.insert_user(request, permit).await;
    reply_rx
        .await
        .expect("insert_user should send one response")
}

async fn available_ids(pool: &PgPool) -> sqlx::Result<Vec<i32>> {
    sqlx::query_scalar("SELECT user_id FROM available_user_ids ORDER BY user_id")
        .fetch_all(pool)
        .await
}

// Alias: Inserts account and profile
// Input: IDs=[7,42], insert_user("new-user", "fixture-password")
// Expected: Success(7). Account 7 stores the exact username and a hash that verifies the submitted password; matching profile has admin_status=false, a creation timestamp and last_online=NULL. Remaining IDs=[42].
postgres_test! {
    async fn insert_new(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 42]).await?;

        assert!(matches!(
            insert(&handler(&pool), "new-user", "fixture-password", open_permit()).await,
            RegResponse::Success(7)
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(account.0, 7);
        assert_eq!(account.1, "new-user");
        assert_eq!(verify_password("fixture-password", &account.2), Ok(true));

        let profile = sqlx::query_as::<_, (i32, String, bool, bool, bool)>(
            "SELECT user_id, username, admin_status,
                    user_created IS NOT NULL, last_online IS NULL
             FROM user_information",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(profile, (7, "new-user".into(), false, true, true));
        assert_eq!(available_ids(&pool).await?, [42]);

        Ok(())
    }
}

// Alias: Preserves the existing account and ID pool
// Input: account(7, "taken", "old-hash"), IDs=[8]; profile admin=true, created 2026-01-01T00:00:00Z, last_online 2026-01-02T00:00:00Z; insert_user("taken", "different-password") with a closed semaphore
// Expected: UsernameExists. Account, profile, and IDs stay unchanged.
postgres_test! {
    async fn insert_duplicate(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;
        sqlx::query(
            "UPDATE user_information
             SET admin_status = true,
                 user_created = '2026-01-01 00:00:00+00',
                 last_online = '2026-01-02 00:00:00+00'
             WHERE user_id = 7",
        )
        .execute(&pool)
        .await?;

        assert!(matches!(
            insert(&handler(&pool), "taken", "different-password", closed_permit()).await,
            RegResponse::UsernameExists
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(account, [(7, "taken".into(), "old-hash".into())]);
        let profile = sqlx::query_as::<_, (i32, String, bool, Option<bool>, Option<bool>)>(
            "SELECT user_id, username, admin_status,
                    user_created = '2026-01-01 00:00:00+00',
                    last_online = '2026-01-02 00:00:00+00'
             FROM user_information",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(profile, [(7, "taken".into(), true, Some(true), Some(true))]);
        assert_eq!(available_ids(&pool).await?, [8]);

        Ok(())
    }
}

// Alias: Reports capacity without revealing a duplicate username
// Input: account(7, "taken", "old-hash"), IDs=[], insert_user("taken", "different-password") with a closed semaphore
// Expected: NotEnoughSpace and no writes.
postgres_test! {
    async fn insert_duplicate_full(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;

        assert!(matches!(
            insert(&handler(&pool), "taken", "different-password", closed_permit()).await,
            RegResponse::NotEnoughSpace
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(account, [(7, "taken".into(), "old-hash".into())]);
        let profile = sqlx::query_as::<_, (i32, String, bool, bool, bool)>(
            "SELECT user_id, username, admin_status,
                    user_created IS NULL, last_online IS NULL
             FROM user_information",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(profile, [(7, "taken".into(), false, true, true)]);
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Claims the last usable ID
// Input: IDs=[65535], insert_user("last-user", "fixture-password")
// Expected: Success(65535); matching account and profile exist and IDs=[].
postgres_test! {
    async fn insert_max_id(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[65535]).await?;

        assert!(matches!(
            insert(&handler(&pool), "last-user", "fixture-password", open_permit()).await,
            RegResponse::Success(65535)
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(account.0, 65535);
        assert_eq!(account.1, "last-user");
        assert_eq!(verify_password("fixture-password", &account.2), Ok(true));
        let profile = sqlx::query_as::<_, (i32, String, bool, bool, bool)>(
            "SELECT user_id, username, admin_status,
                    user_created IS NOT NULL, last_online IS NULL
             FROM user_information",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(profile, (65535, "last-user".into(), false, true, true));
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Creates no rows when IDs are exhausted
// Input: IDs=[], closed semaphore, insert_user("new-user", "fixture-password")
// Expected: NotEnoughSpace; no account or profile row for "new-user".
postgres_test! {
    async fn insert_full(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[]).await?;

        assert!(matches!(
            insert(&handler(&pool), "new-user", "fixture-password", closed_permit()).await,
            RegResponse::NotEnoughSpace
        ));
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT NOT EXISTS (SELECT 1 FROM users WHERE username = 'new-user')
                    AND NOT EXISTS (SELECT 1 FROM user_information WHERE username = 'new-user')",
        )
        .fetch_one(&pool)
        .await?);
        assert!(available_ids(&pool).await?.is_empty());

        Ok(())
    }
}

// Alias: Chooses the lowest available ID
// Input: IDs=[7,42,101], insert_user("new-user", "fixture-password")
// Expected: Success(7); remaining IDs=[42,101].
postgres_test! {
    async fn insert_lowest(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 42, 101]).await?;

        assert!(matches!(
            insert(&handler(&pool), "new-user", "fixture-password", open_permit()).await,
            RegResponse::Success(7)
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(account.0, 7);
        assert_eq!(account.1, "new-user");
        assert_eq!(verify_password("fixture-password", &account.2), Ok(true));
        assert_eq!(available_ids(&pool).await?, [42, 101]);

        Ok(())
    }
}

// Alias: Rolls back when profile insertion fails
// Input: IDs=[7], constraint rejects profile username "blocked", insert_user("blocked", "fixture-password")
// Expected: The failed SQL statement preserves ID 7. The handler returns HandlerError before hashing.
postgres_test! {
    async fn insert_profile_error(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        sqlx::query(
            "ALTER TABLE user_information
             ADD CONSTRAINT reject_fixture_username CHECK (username <> 'blocked')",
        )
        .execute(&pool)
        .await?;

        let error = sqlx::query_as::<_, (bool, Option<i32>)>(include_str!(
            "../../src/database/queries/insert_user.sql"
        ))
        .bind("blocked")
        .fetch_one(&pool)
        .await
        .expect_err("the combined query should reject the invalid profile");
        assert!(matches!(error, sqlx::Error::Database(error)
            if error.constraint() == Some("reject_fixture_username")));
        assert_eq!(available_ids(&pool).await?, [7]);

        let response = timeout(
            CLAIM_TIMEOUT,
            insert(&handler(&pool), "blocked", "fixture-password", Arc::new(Semaphore::new(0))),
        )
        .await
        .expect("profile errors should be returned before acquiring a hashing permit");
        assert!(matches!(response, RegResponse::HandlerError));

        let row_counts = sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM user_information)",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(row_counts, (0, 0));
        assert_eq!(available_ids(&pool).await?, [7]);

        Ok(())
    }
}

const CLAIM_TIMEOUT: Duration = Duration::from_secs(5);

async fn wait_for_claim(pool: &PgPool) -> sqlx::Result<()> {
    timeout(CLAIM_TIMEOUT, async {
        loop {
            match sqlx::query(
                "SELECT user_id FROM available_user_ids WHERE user_id = 7 FOR UPDATE NOWAIT",
            )
            .fetch_one(pool)
            .await
            {
                Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => {
                    return Ok(());
                }
                result => {
                    result?;
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("registration should claim ID 7 before hashing")
}

postgres_test! {
    async fn insert_hash_failure_reuses_id(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        let handler = handler(&pool);
        assert!(matches!(
            insert(&handler, "same", "first", closed_permit()).await,
            RegResponse::HandlerError
        ));
        assert_eq!(available_ids(&pool).await?, [7]);
        let row_counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM user_information)",
        ).fetch_one(&pool).await?;
        assert_eq!(row_counts, (0, 0));
        assert!(matches!(
            insert(&handler, "same", "second", open_permit()).await,
            RegResponse::Success(7)
        ));
        let hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'same'")
            .fetch_one(&pool).await?;
        assert_eq!(verify_password("second", &hash), Ok(true));
        Ok(())
    }
}

postgres_test! {
    async fn insert_cancelled_worker_reuses_id(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        let (user_tx, user_rx) = mpsc::channel(1);
        let (reply_tx, reply_rx) = oneshot::channel();
        let worker = tokio::spawn(
            UserHandler::new(pool.clone(), user_rx).run(Arc::new(Semaphore::new(0))),
        );
        assert!(user_tx.try_send(UserRequest::RegRequest(
            RegRequest::new("same".into(), "first".into(), reply_tx),
        )).is_ok());
        wait_for_claim(&pool).await?;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert!(reply_rx.await.is_err());

        let response = timeout(
            CLAIM_TIMEOUT,
            insert(&handler(&pool), "same", "second", open_permit()),
        ).await.expect("retry should acquire the released ID");
        assert!(matches!(response, RegResponse::Success(7)));
        let hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'same'")
            .fetch_one(&pool).await?;
        assert_eq!(verify_password("second", &hash), Ok(true));
        Ok(())
    }
}

async fn wait_for_blocked_insert(pool: &PgPool) -> sqlx::Result<()> {
    timeout(CLAIM_TIMEOUT, async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0
                )",
            )
            .fetch_one(pool)
            .await?;
            if blocked {
                return Ok::<_, sqlx::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("competing insert should wait for the registration transaction")
}

async fn assert_concurrent_duplicate(
    pool: &PgPool,
    ids: &[i32],
    expected: RegResponse,
) -> sqlx::Result<()> {
    set_available_ids(pool, ids).await?;
    let permit = open_permit();
    let held = Arc::clone(&permit).acquire_owned().await.unwrap();
    let first = handler(pool);
    let first = tokio::spawn(async move { insert(&first, "same", "first", permit).await });
    wait_for_claim(pool).await?;

    let second = handler(pool);
    let second =
        tokio::spawn(async move { insert(&second, "same", "second", closed_permit()).await });
    wait_for_blocked_insert(pool).await?;
    drop(held);

    let (first, second) = timeout(CLAIM_TIMEOUT, async { tokio::join!(first, second) })
        .await
        .expect("both registrations should finish");
    assert!(matches!(first.unwrap(), RegResponse::Success(7)));
    assert_eq!(
        std::mem::discriminant(&second.unwrap()),
        std::mem::discriminant(&expected),
    );
    assert_eq!(available_ids(pool).await?, &ids[1..]);
    let hash: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'same'")
            .fetch_one(pool)
            .await?;
    assert_eq!(verify_password("first", &hash), Ok(true));
    Ok(())
}

postgres_test! {
    async fn insert_concurrent_duplicate_last_id(pool: PgPool) -> sqlx::Result<()> {
        assert_concurrent_duplicate(&pool, &[7], RegResponse::NotEnoughSpace).await
    }
}

postgres_test! {
    async fn insert_concurrent_duplicate_spare_id(pool: PgPool) -> sqlx::Result<()> {
        assert_concurrent_duplicate(&pool, &[7, 8], RegResponse::UsernameExists).await
    }
}

postgres_test! {
    async fn insert_reserves_username_before_hashing(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let permit = open_permit();
        let held = Arc::clone(&permit).acquire_owned().await.unwrap();
        let first = handler(&pool);
        let first = tokio::spawn(async move { insert(&first, "same", "secret", permit).await });
        wait_for_claim(&pool).await?;

        let other_pool = pool.clone();
        let competitor = tokio::spawn(async move {
            sqlx::query(
                "INSERT INTO users (user_id, username, password_hash)
                 OVERRIDING SYSTEM VALUE VALUES (8, 'same', 'fixture-hash')",
            )
            .execute(&other_pool)
            .await
        });
        wait_for_blocked_insert(&pool).await?;
        let visible: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM users WHERE username = 'same')")
            .fetch_one(&pool).await?;
        assert!(!visible, "the provisional row must be invisible to other connections");

        drop(held);
        let (first, competitor) = timeout(CLAIM_TIMEOUT, async { tokio::join!(first, competitor) })
            .await.expect("both registrations should finish");
        assert!(matches!(first.unwrap(), RegResponse::Success(7)));
        let error = competitor.unwrap().unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(error)
            if error.is_unique_violation() && error.constraint() == Some("users_username_key")));
        Ok(())
    }
}
