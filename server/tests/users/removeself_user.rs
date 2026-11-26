use tokio::sync::mpsc;

use crate::database::serial_tests::postgres_test;

use super::transaction_pool::{delete_completed, track_completed_deletes, transaction_pool};
use super::{PgPool, RegResponse, RemoveResponse, UserHandler};

const FIXTURE_HASH: &str = "fixture-hash";

async fn register_user(handler: &UserHandler, username: &str, hashed_password: &str) -> u16 {
    let RegResponse::Success(user_id) = handler.insert_user(username, hashed_password).await else {
        panic!("failed to register fixture user {username}");
    };
    user_id
}

postgres_test! {
    async fn removeself_user_deletes_account_and_profile_and_releases_id(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        let username = "remove_user_test";
        let user_id = register_user(&handler, username, FIXTURE_HASH).await;

        assert!(matches!(
            handler.removeself_user(username, FIXTURE_HASH).await,
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
    async fn removeself_user_cascades_only_to_their_messages(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        let username = "removed_user";
        let removed_id = register_user(&handler, username, FIXTURE_HASH).await;
        let sender_id = register_user(&handler, "remaining_sender", FIXTURE_HASH).await;
        let recipient_id = register_user(&handler, "remaining_recipient", FIXTURE_HASH).await;

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
            handler.removeself_user(username, FIXTURE_HASH).await,
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
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        let username = "original_user";
        let released_id = register_user(&handler, username, FIXTURE_HASH).await;
        assert_eq!(released_id, 1);
        assert!(matches!(
            handler.removeself_user(username, FIXTURE_HASH).await,
            RemoveResponse::Success
        ));

        let new_username = "replacement_user";
        let new_id = register_user(&handler, new_username, FIXTURE_HASH).await;
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

postgres_test! {
    async fn removeself_user_reports_missing_and_already_removed_usernames(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        sqlx::query("DELETE FROM available_user_ids WHERE user_id > 2")
            .execute(&pool)
            .await?;
        let username = "known_user";
        let user_id = register_user(&handler, username, FIXTURE_HASH).await;
        assert_eq!(user_id, 1);

        assert!(matches!(
            handler.removeself_user("missing_user", FIXTURE_HASH).await,
            RemoveResponse::UserNotFound
        ));
        let available_ids =
            sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids ORDER BY user_id")
                .fetch_all(&pool)
                .await?;
        assert_eq!(available_ids, [2]);

        assert!(matches!(
            handler.removeself_user(username, FIXTURE_HASH).await,
            RemoveResponse::Success
        ));
        assert!(matches!(
            handler.removeself_user(username, FIXTURE_HASH).await,
            RemoveResponse::UserNotFound
        ));
        let available_ids =
            sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids ORDER BY user_id")
                .fetch_all(&pool)
                .await?;
        assert_eq!(available_ids, [1, 2]);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn removeself_user_rejects_another_users_hash_without_changes(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        let username = "protected_user";
        let other_hash = "other-hash";
        let user_id = i32::from(register_user(&handler, username, FIXTURE_HASH).await);
        let other_user_id = i32::from(register_user(&handler, "other_user", other_hash).await);

        sqlx::query("INSERT INTO messages (message_id, user_id, message) VALUES (11, $1, 'public')")
            .bind(user_id)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (21, $1, $2, 'private')",
        )
        .bind(user_id)
        .bind(other_user_id)
        .execute(&pool)
        .await?;

        assert!(matches!(
            handler.removeself_user(username, other_hash).await,
            RemoveResponse::WrongCredentials
        ));

        let state = sqlx::query_as::<_, (bool, bool, bool, bool, bool, bool)>(
            "SELECT EXISTS (SELECT 1 FROM users WHERE user_id = $1 AND password_hash = $3),
                    EXISTS (SELECT 1 FROM users WHERE user_id = $2),
                    EXISTS (SELECT 1 FROM user_information WHERE user_id = $1),
                    EXISTS (SELECT 1 FROM messages WHERE user_id = $1),
                    EXISTS (SELECT 1 FROM private_messages WHERE user_id = $1 AND target_id = $2),
                    EXISTS (SELECT 1 FROM available_user_ids WHERE user_id IN ($1, $2))",
        )
        .bind(user_id)
        .bind(other_user_id)
        .bind(FIXTURE_HASH)
        .fetch_one(&pool)
        .await?;
        assert_eq!(state, (true, true, true, true, true, false));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn removeself_user_keeps_dependent_rows_if_returning_id_fails(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        let username = "release_conflict";
        let removed_id = register_user(&handler, username, FIXTURE_HASH).await;
        let user_id = i32::from(removed_id);
        let other_user_id = i32::from(register_user(&handler, "other_user", FIXTURE_HASH).await);

        sqlx::query(
            "INSERT INTO messages (message_id, user_id, message) VALUES (11, $1, 'public message')",
        )
        .bind(user_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (21, $1, $2, 'outgoing message'), (22, $2, $1, 'incoming message')",
        )
        .bind(user_id)
        .bind(other_user_id)
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO available_user_ids (user_id) VALUES ($1)")
            .bind(user_id)
            .execute(&pool)
            .await?;

        track_completed_deletes(&pool, "users").await?;
        sqlx::query("SAVEPOINT removal_attempt")
            .execute(&pool)
            .await?;
        let response = handler.removeself_user(username, FIXTURE_HASH).await;
        sqlx::query("ROLLBACK TO SAVEPOINT removal_attempt")
            .execute(&pool)
            .await?;
        assert!(matches!(response, RemoveResponse::HandlerError));
        assert!(
            !delete_completed(&pool).await?,
            "user deletion completed before the ID release error"
        );

        let surviving_rows = sqlx::query_as::<_, (bool, bool, bool, bool, bool)>(
            "SELECT EXISTS (SELECT 1 FROM users WHERE user_id = $1 AND username = 'release_conflict'),
                    EXISTS (SELECT 1 FROM user_information WHERE user_id = $1 AND username = 'release_conflict'),
                    EXISTS (SELECT 1 FROM messages WHERE message_id = 11 AND user_id = $1),
                    EXISTS (SELECT 1 FROM private_messages WHERE privatemessage_id = 21 AND user_id = $1 AND target_id = $2),
                    EXISTS (SELECT 1 FROM private_messages WHERE privatemessage_id = 22 AND user_id = $2 AND target_id = $1)",
        )
        .bind(user_id)
        .bind(other_user_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(surviving_rows, (true, true, true, true, true));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}
