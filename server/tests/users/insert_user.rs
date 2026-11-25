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
