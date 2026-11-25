use crate::database::serial_tests::postgres_test;

use super::{PgPool, RegResponse, UserHandler};

postgres_test! {
    async fn insert_user_success_rolls_back(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        let username = "insert_user_test";
        let password_hash = "fixture-hash";
        let RegResponse::Success(user_id) =
            UserHandler::insert_user(&mut *tx, username, password_hash).await
        else {
            panic!("expected insert_user to return Success");
        };
        let user_id = i32::from(user_id);

        let account = sqlx::query_as::<_, (String, String)>(
            "SELECT username, password_hash FROM users WHERE user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(account, (username.to_owned(), password_hash.to_owned()));

        let profile = sqlx::query_as::<_, (String, bool, bool, bool)>(
            "SELECT username, admin_status, user_created IS NOT NULL, last_online IS NULL
             FROM user_information WHERE user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(profile, (username.to_owned(), false, true, true));

        let id_available = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = $1)",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
        assert!(!id_available, "insert_user should claim the ID");

        tx.rollback().await?;

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
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM available_user_ids WHERE user_id NOT IN (7, 42, 101)")
            .execute(&mut *tx)
            .await?;

        assert!(matches!(
            UserHandler::insert_user(&mut *tx, "lowest_id", "fixture-hash").await,
            RegResponse::Success(7)
        ));

        let available_ids =
            sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids ORDER BY user_id")
                .fetch_all(&mut *tx)
                .await?;
        assert_eq!(available_ids, [42, 101]);

        tx.rollback().await
    }
}

postgres_test! {
    async fn insert_user_claims_last_usable_id(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM available_user_ids WHERE user_id <> 65535")
            .execute(&mut *tx)
            .await?;

        assert!(matches!(
            UserHandler::insert_user(&mut *tx, "last_id", "fixture-hash").await,
            RegResponse::Success(65535)
        ));
        assert!(matches!(
            UserHandler::insert_user(&mut *tx, "next_user", "fixture-hash").await,
            RegResponse::NotEnoughSpace
        ));

        tx.rollback().await
    }
}

postgres_test! {
    async fn insert_user_creates_no_rows_when_id_pool_is_empty(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM available_user_ids")
            .execute(&mut *tx)
            .await?;

        assert!(matches!(
            UserHandler::insert_user(&mut *tx, "no_id", "fixture-hash").await,
            RegResponse::NotEnoughSpace
        ));

        let row_counts = sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM user_information)",
        )
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(row_counts, (0, 0));

        tx.rollback().await
    }
}

postgres_test! {
    async fn insert_user_preserves_existing_account_and_available_id(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM available_user_ids WHERE user_id > 2")
            .execute(&mut *tx)
            .await?;

        let username = "duplicate_user";
        assert!(matches!(
            UserHandler::insert_user(&mut *tx, username, "original-hash").await,
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
        .execute(&mut *tx)
        .await?;

        assert!(matches!(
            UserHandler::insert_user(&mut *tx, username, "different-hash").await,
            RegResponse::UsernameExists
        ));

        let account = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users",
        )
        .fetch_all(&mut *tx)
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
        .fetch_all(&mut *tx)
        .await?;
        assert_eq!(
            profile,
            [(1, username.to_owned(), true, Some(true), Some(true))]
        );

        let available_ids = sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids")
            .fetch_all(&mut *tx)
            .await?;
        assert_eq!(available_ids, [2]);

        tx.rollback().await
    }
}

postgres_test! {
    async fn insert_user_reports_duplicate_before_exhausted_pool(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM available_user_ids WHERE user_id <> 1")
            .execute(&mut *tx)
            .await?;

        let username = "duplicate_when_exhausted";
        assert!(matches!(
            UserHandler::insert_user(&mut *tx, username, "original-hash").await,
            RegResponse::Success(1)
        ));
        let available_count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM available_user_ids")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(available_count, 0);

        assert!(matches!(
            UserHandler::insert_user(&mut *tx, username, "different-hash").await,
            RegResponse::UsernameExists
        ));

        tx.rollback().await
    }
}
