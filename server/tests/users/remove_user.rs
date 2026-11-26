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

postgres_test! {
    async fn remove_user_cascades_only_to_their_messages(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let removed_id = register_user(&pool, "removed_user").await;
        let sender_id = register_user(&pool, "remaining_sender").await;
        let recipient_id = register_user(&pool, "remaining_recipient").await;

        sqlx::query(
            "INSERT INTO messages (message_id, user_id, message)
             VALUES (11, $1, 'removed public message'), (12, $2, 'remaining public message')",
        )
        .bind(i32::from(removed_id))
        .bind(i32::from(sender_id))
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (21, $1, $2, 'removed outgoing message'),
                    (22, $2, $1, 'removed incoming message'),
                    (23, $2, $3, 'remaining private message')",
        )
        .bind(i32::from(removed_id))
        .bind(i32::from(sender_id))
        .bind(i32::from(recipient_id))
        .execute(&pool)
        .await?;

        assert!(matches!(
            remove_user(&pool, removed_id).await,
            RemoveResponse::Success
        ));

        let public_messages = sqlx::query_as::<_, (i32, i32, String)>(
            "SELECT message_id::int, user_id, message FROM messages",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            public_messages,
            [(
                12,
                i32::from(sender_id),
                "remaining public message".to_owned()
            )]
        );

        let private_messages = sqlx::query_as::<_, (i32, i32, i32, String)>(
            "SELECT privatemessage_id::int, user_id, target_id, message FROM private_messages",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            private_messages,
            [(
                23,
                i32::from(sender_id),
                i32::from(recipient_id),
                "remaining private message".to_owned()
            )]
        );

        let remaining_profiles =
            sqlx::query_scalar::<_, String>("SELECT username FROM user_information ORDER BY username")
                .fetch_all(&pool)
                .await?;
        assert_eq!(
            remaining_profiles,
            ["remaining_recipient", "remaining_sender"]
        );

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_reuses_the_lowest_released_id(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let released_id = register_user(&pool, "original_user").await;
        assert_eq!(released_id, 1);
        assert!(matches!(
            remove_user(&pool, released_id).await,
            RemoveResponse::Success
        ));

        let new_username = "replacement_user";
        let new_id = register_user(&pool, new_username).await;
        assert_eq!(new_id, released_id);

        let account_and_profile = sqlx::query_as::<_, (String, String)>(
            "SELECT users.username, user_information.username
             FROM users JOIN user_information USING (user_id) WHERE user_id = $1",
        )
        .bind(i32::from(new_id))
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            account_and_profile,
            (new_username.to_owned(), new_username.to_owned())
        );

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}
