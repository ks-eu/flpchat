use tokio::sync::mpsc;

use crate::database::serial_tests::postgres_test;

use super::transaction_pool::{delete_completed, track_completed_deletes, transaction_pool};
use super::{PgPool, RegResponse, UserHandler};

postgres_test! {
    async fn insert_user_success_rolls_back(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        let username = "insert_user_test";
        let password_hash = "fixture-hash";
        let RegResponse::Success(user_id) =
            handler.insert_user(username, password_hash).await
        else {
            panic!("expected insert_user to return Success");
        };
        let user_id = i32::from(user_id);

        let account = sqlx::query_as::<_, (String, String)>(
            "SELECT username, password_hash FROM users WHERE user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&tx_pool)
        .await?;
        assert_eq!(account, (username.to_owned(), password_hash.to_owned()));

        let profile = sqlx::query_as::<_, (String, bool, bool, bool)>(
            "SELECT username, admin_status, user_created IS NOT NULL, last_online IS NULL
             FROM user_information WHERE user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&tx_pool)
        .await?;
        assert_eq!(profile, (username.to_owned(), false, true, true));

        let id_available = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = $1)",
        )
        .bind(user_id)
        .fetch_one(&tx_pool)
        .await?;
        assert!(!id_available, "insert_user should claim the ID");

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;

        let state = sqlx::query_as::<_, (bool, bool, bool)>(
            "SELECT EXISTS (SELECT 1 FROM users WHERE user_id = $1),
                    EXISTS (SELECT 1 FROM user_information WHERE user_id = $1),
                    EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = $1)",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(state, (false, false, true), "rollback should restore the ID pool");

        Ok(())
    }
}

postgres_test! {
    async fn insert_user_claims_lowest_available_id(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids WHERE user_id NOT IN (7, 42, 101)")
            .execute(&tx_pool)
            .await?;

        assert!(matches!(
            handler.insert_user("lowest_id", "fixture-hash").await,
            RegResponse::Success(7)
        ));

        let available_ids =
            sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids ORDER BY user_id")
                .fetch_all(&tx_pool)
                .await?;
        assert_eq!(available_ids, [42, 101]);

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_claims_last_usable_id(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids WHERE user_id <> 65535")
            .execute(&tx_pool)
            .await?;

        assert!(matches!(
            handler.insert_user("last_id", "fixture-hash").await,
            RegResponse::Success(65535)
        ));
        assert!(matches!(
            handler.insert_user("next_user", "fixture-hash").await,
            RegResponse::NotEnoughSpace
        ));

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_creates_no_rows_when_id_pool_is_empty(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids")
            .execute(&tx_pool)
            .await?;

        assert!(matches!(
            handler.insert_user("no_id", "fixture-hash").await,
            RegResponse::NotEnoughSpace
        ));

        let row_counts = sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM user_information)",
        )
        .fetch_one(&tx_pool)
        .await?;
        assert_eq!(row_counts, (0, 0));

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_preserves_existing_account_and_available_id(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids WHERE user_id > 2")
            .execute(&tx_pool)
            .await?;

        let username = "duplicate_user";
        assert!(matches!(
            handler.insert_user(username, "original-hash").await,
            RegResponse::Success(1)
        ));

        // Make a profile rewrite visible even if it writes the same username.
        sqlx::query(
            "UPDATE user_information
             SET admin_status = true,
                 user_created = '2026-01-01 00:00:00+00',
                 last_online = '2026-01-02 00:00:00+00'
             WHERE user_id = 1",
        )
        .execute(&tx_pool)
        .await?;

        assert!(matches!(
            handler.insert_user(username, "different-hash").await,
            RegResponse::UsernameExists
        ));

        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_all(&tx_pool)
        .await?;
        assert_eq!(
            account,
            [(1, username.to_owned(), "original-hash".to_owned())]
        );

        let profile = sqlx::query_as::<_, (i32, String, bool, Option<bool>, Option<bool>)>(
            "SELECT user_id, username, admin_status,
                    user_created = '2026-01-01 00:00:00+00',
                    last_online = '2026-01-02 00:00:00+00'
             FROM user_information",
        )
        .fetch_all(&tx_pool)
        .await?;
        assert_eq!(
            profile,
            [(1, username.to_owned(), true, Some(true), Some(true))]
        );

        let available_ids = sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids")
            .fetch_all(&tx_pool)
            .await?;
        assert_eq!(available_ids, [2]);

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_reports_duplicate_before_exhausted_pool(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids WHERE user_id <> 1")
            .execute(&tx_pool)
            .await?;

        let username = "duplicate_when_exhausted";
        assert!(matches!(
            handler.insert_user(username, "original-hash").await,
            RegResponse::Success(1)
        ));
        let available_count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM available_user_ids")
            .fetch_one(&tx_pool)
            .await?;
        assert_eq!(available_count, 0);

        assert!(matches!(
            handler.insert_user(username, "different-hash").await,
            RegResponse::UsernameExists
        ));

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_restores_claimed_id_if_profile_insert_fails(pool: PgPool) -> sqlx::Result<()> {
        let tx_pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(tx_pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids WHERE user_id <> 7")
            .execute(&tx_pool)
            .await?;
        sqlx::query(
            "ALTER TABLE user_information
             ADD CONSTRAINT reject_fixture_username CHECK (username <> 'rejected_profile')",
        )
        .execute(&tx_pool)
        .await?;

        track_completed_deletes(&tx_pool, "available_user_ids").await?;

        sqlx::query("SAVEPOINT insertion_attempt")
            .execute(&tx_pool)
            .await?;
        let response =
            handler.insert_user("rejected_profile", "fixture-hash").await;
        sqlx::query("ROLLBACK TO SAVEPOINT insertion_attempt")
            .execute(&tx_pool)
            .await?;
        assert!(matches!(response, RegResponse::HandlerError));

        assert!(
            !delete_completed(&tx_pool).await?,
            "ID deletion completed before the profile error"
        );

        let row_counts = sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM user_information)",
        )
        .fetch_one(&tx_pool)
        .await?;
        assert_eq!(row_counts, (0, 0));

        let available_ids = sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids")
            .fetch_all(&tx_pool)
            .await?;
        assert_eq!(available_ids, [7]);

        sqlx::query("ROLLBACK").execute(&tx_pool).await?;
        Ok(())
    }
}
