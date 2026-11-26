use crate::database::serial_tests::postgres_test;

use super::transaction_pool::transaction_pool;
use super::{PgPool, RegResponse, RemoveResponse, UserHandler, remove_user};

async fn register_user(pool: &PgPool, username: &str) -> u16 {
    let RegResponse::Success(user_id) =
        UserHandler::insert_user(pool, username, "fixture-hash").await
    else {
        panic!("failed to register fixture user {username}");
    };
    user_id
}

postgres_test! {
    async fn remove_user_deletes_account_and_profile_and_releases_id(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let user_id = register_user(&pool, "remove_user_test").await;

        assert!(matches!(
            remove_user(&pool, user_id).await,
            RemoveResponse::Success
        ));

        let counts = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT (SELECT count(*) FROM users WHERE user_id = $1),
                    (SELECT count(*) FROM user_information WHERE user_id = $1),
                    (SELECT count(*) FROM available_user_ids WHERE user_id = $1)",
        )
        .bind(i32::from(user_id))
        .fetch_one(&pool)
        .await?;
        assert_eq!(counts, (0, 0, 1));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}
