use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};

use crate::database::serial_tests::postgres_test;

use super::transaction_pool::transaction_pool;
use super::{PgPool, RegResponse, UserHandler};

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

fn handler(pool: &PgPool) -> UserHandler {
    UserHandler::new(
        pool.clone(),
        mpsc::channel(1).1,
        Arc::new(Semaphore::new(1)),
    )
}

async fn available_ids(pool: &PgPool) -> sqlx::Result<Vec<i32>> {
    sqlx::query_scalar("SELECT user_id FROM available_user_ids ORDER BY user_id")
        .fetch_all(pool)
        .await
}

// Alias: Inserts account and profile
// Input: IDs=[7,42], insert_user("new-user", "fixture-hash")
// Expected: Success(7). Account 7 stores the exact username and hash; matching profile has admin_status=false, a creation timestamp and last_online=NULL. Remaining IDs=[42].
postgres_test! {
    async fn insert_new(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        set_available_ids(&pool, &[7, 42]).await?;

        assert!(matches!(
            handler(&pool).insert_user("new-user", "fixture-hash").await,
            RegResponse::Success(7)
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(account, (7, "new-user".into(), "fixture-hash".into()));

        let profile = sqlx::query_as::<_, (i32, String, bool, bool, bool)>(
            "SELECT user_id, username, admin_status,
                    user_created IS NOT NULL, last_online IS NULL
             FROM user_information",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(profile, (7, "new-user".into(), false, true, true));
        assert_eq!(available_ids(&pool).await?, [42]);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Preserves the existing account and ID pool
// Input: account(7, "taken", "old-hash"), IDs=[8]; profile admin=true, created 2026-01-01T00:00:00Z, last_online 2026-01-02T00:00:00Z; insert_user("taken", "different-hash")
// Expected: UsernameExists. Account hash, all seeded profile fields, and IDs=[8] remain unchanged.
postgres_test! {
    async fn insert_duplicate(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
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
            handler(&pool).insert_user("taken", "different-hash").await,
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

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Detects duplicate before capacity
// Input: account(7, "taken", "old-hash"), IDs=[], insert_user("taken", "different-hash")
// Expected: UsernameExists and no writes.
postgres_test! {
    async fn insert_duplicate_full(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        set_available_ids(&pool, &[]).await?;
        seed_account(&pool, 7, "taken", "old-hash").await?;

        assert!(matches!(
            handler(&pool).insert_user("taken", "different-hash").await,
            RegResponse::UsernameExists
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

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Claims the last usable ID
// Input: IDs=[65535], insert_user("last-user", "fixture-hash")
// Expected: Success(65535); matching account and profile exist and IDs=[].
postgres_test! {
    async fn insert_max_id(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        set_available_ids(&pool, &[65535]).await?;

        assert!(matches!(
            handler(&pool).insert_user("last-user", "fixture-hash").await,
            RegResponse::Success(65535)
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(account, (65535, "last-user".into(), "fixture-hash".into()));
        let profile = sqlx::query_as::<_, (i32, String, bool, bool, bool)>(
            "SELECT user_id, username, admin_status,
                    user_created IS NOT NULL, last_online IS NULL
             FROM user_information",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(profile, (65535, "last-user".into(), false, true, true));
        assert!(available_ids(&pool).await?.is_empty());

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Creates no rows when IDs are exhausted
// Input: IDs=[], insert_user("new-user", "fixture-hash")
// Expected: NotEnoughSpace; no account or profile row for "new-user".
postgres_test! {
    async fn insert_full(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        set_available_ids(&pool, &[]).await?;

        assert!(matches!(
            handler(&pool).insert_user("new-user", "fixture-hash").await,
            RegResponse::NotEnoughSpace
        ));
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT NOT EXISTS (SELECT 1 FROM users WHERE username = 'new-user')
                    AND NOT EXISTS (SELECT 1 FROM user_information WHERE username = 'new-user')",
        )
        .fetch_one(&pool)
        .await?);
        assert!(available_ids(&pool).await?.is_empty());

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Chooses the lowest available ID
// Input: IDs=[7,42,101], insert_user("new-user", "fixture-hash")
// Expected: Success(7); remaining IDs=[42,101].
postgres_test! {
    async fn insert_lowest(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        set_available_ids(&pool, &[7, 42, 101]).await?;

        assert!(matches!(
            handler(&pool).insert_user("new-user", "fixture-hash").await,
            RegResponse::Success(7)
        ));
        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(account, (7, "new-user".into(), "fixture-hash".into()));
        assert_eq!(available_ids(&pool).await?, [42, 101]);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Rolls back when profile insertion fails
// Input: IDs=[7], constraint rejects profile username "blocked", insert_user("blocked", "fixture-hash")
// Expected: HandlerError. Neither row survives and ID 7 remains available.
postgres_test! {
    async fn insert_profile_error(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7]).await?;
        sqlx::query(
            "ALTER TABLE user_information
             ADD CONSTRAINT reject_fixture_username CHECK (username <> 'blocked')",
        )
        .execute(&pool)
        .await?;

        let response = handler(&pool)
            .insert_user("blocked", "fixture-hash")
            .await;
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
