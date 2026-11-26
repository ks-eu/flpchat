use std::sync::Arc;

use argon2::{Argon2, PasswordHasher};
use sqlx::types::JsonValue;
use tokio::sync::{Semaphore, mpsc};

use crate::database::serial_tests::postgres_test;

use super::transaction_pool::{delete_completed, track_completed_deletes, transaction_pool};
use super::{PgPool, RegResponse, RemoveSelfResponse, UserHandler, VerifyError, VerifyResult};

const FIXTURE_HASH: &str = "fixture-hash";

#[derive(Debug, PartialEq)]
struct DatabaseSnapshot {
    users: Vec<JsonValue>,
    profiles: Vec<JsonValue>,
    public_messages: Vec<JsonValue>,
    private_messages: Vec<JsonValue>,
    available_ids: Vec<i32>,
}

async fn seed_removal_fixture(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE user_id <> 10")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE
         VALUES (7, 'target', 'target-hash'),
                (8, 'peer-a', 'peer-a-hash'),
                (9, 'peer-b', 'peer-b-hash')",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO user_information (user_id, username)
         VALUES (7, 'target'), (8, 'peer-a'), (9, 'peer-b')",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn snapshot(pool: &PgPool) -> sqlx::Result<DatabaseSnapshot> {
    Ok(DatabaseSnapshot {
        users: sqlx::query_scalar(
            "SELECT to_jsonb(record) FROM (SELECT * FROM users ORDER BY user_id) AS record",
        )
        .fetch_all(pool)
        .await?,
        profiles: sqlx::query_scalar(
            "SELECT to_jsonb(record) FROM (
                 SELECT * FROM user_information ORDER BY user_id
             ) AS record",
        )
        .fetch_all(pool)
        .await?,
        public_messages: sqlx::query_scalar(
            "SELECT to_jsonb(record) FROM (
                 SELECT * FROM messages ORDER BY message_id
             ) AS record",
        )
        .fetch_all(pool)
        .await?,
        private_messages: sqlx::query_scalar(
            "SELECT to_jsonb(record) FROM (
                 SELECT * FROM private_messages ORDER BY privatemessage_id
             ) AS record",
        )
        .fetch_all(pool)
        .await?,
        available_ids: sqlx::query_scalar(
            "SELECT user_id FROM available_user_ids ORDER BY user_id",
        )
        .fetch_all(pool)
        .await?,
    })
}

async fn register_user(handler: &UserHandler, username: &str, hashed_password: &str) -> u16 {
    let RegResponse::Success(user_id) = handler.insert_user(username, hashed_password).await else {
        panic!("failed to register fixture user {username}");
    };
    user_id
}

postgres_test! {
    async fn get_hashed_password_matches_exact_username(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));
        register_user(&handler, "O'Reilly", FIXTURE_HASH).await;
        register_user(&handler, "o'reilly", "other-hash").await;

        assert_eq!(
            handler.get_hashed_password("O'Reilly").await?.as_deref(),
            Some(FIXTURE_HASH)
        );
        assert_eq!(
            handler.get_hashed_password("o'reilly").await?.as_deref(),
            Some("other-hash")
        );
        assert_eq!(handler.get_hashed_password("O'Reill").await?, None);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn verify_credentials_requires_exact_password_bytes(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));
        let password = "café🔑";
        let hash = Argon2::default().hash_password(password.as_bytes()).unwrap().to_string();
        register_user(&handler, "account", &hash).await;

        assert!(matches!(
            handler.verify_credentials("account", password).await,
            Ok(VerifyResult::Success(stored_hash)) if stored_hash == hash
        ));
        assert_eq!(permit.available_permits(), 1);
        assert!(matches!(
            handler.verify_credentials("account", "cafe\u{301}🔑").await,
            Ok(VerifyResult::WrongCredentials)
        ));
        assert_eq!(permit.available_permits(), 1);
        assert!(matches!(
            handler.verify_credentials("account", &hash).await,
            Ok(VerifyResult::WrongCredentials)
        ));
        assert!(matches!(
            handler.verify_credentials("absent", password).await,
            Ok(VerifyResult::UsernameNotFound)
        ));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn verify_credentials_distinguishes_hash_permit_and_lookup_errors(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));
        let password = "password";
        let hash = Argon2::default().hash_password(password.as_bytes()).unwrap().to_string();
        let unsupported_hash = hash.replacen("$argon2id$", "$scrypt$", 1);
        register_user(&handler, "malformed", FIXTURE_HASH).await;
        register_user(&handler, "unsupported", &unsupported_hash).await;
        register_user(&handler, "account", &hash).await;

        for username in ["malformed", "unsupported"] {
            assert!(
                matches!(
                    handler.verify_credentials(username, password).await,
                    Err(VerifyError::Argon2Error)
                ),
                "expected an Argon2 error for {username}"
            );
            assert_eq!(permit.available_permits(), 1);
        }

        permit.close();
        assert!(matches!(
            handler.verify_credentials("account", password).await,
            Err(VerifyError::PermitError)
        ));
        assert!(matches!(
            handler.verify_credentials("absent", password).await,
            Ok(VerifyResult::UsernameNotFound)
        ));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        pool.close().await;
        assert!(matches!(
            handler.get_hashed_password("account").await,
            Err(sqlx::Error::PoolClosed)
        ));
        assert!(matches!(
            handler.verify_credentials("account", password).await,
            Err(VerifyError::SqlxError)
        ));
        Ok(())
    }
}

// Alias: Deletes the account without recycling its ID
// Input: R without messages, removeself_user("target", "target-hash")
// Expected: Success(7). Account and profile 7 are gone. Peers survive and IDs=[10].
postgres_test! {
    async fn remove_match(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::Success(7)
        ));
        let users = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users ORDER BY user_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            users,
            [
                (8, "peer-a".into(), "peer-a-hash".into()),
                (9, "peer-b".into(), "peer-b-hash".into()),
            ]
        );
        let profiles = sqlx::query_scalar::<_, String>(
            "SELECT username FROM user_information ORDER BY user_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(profiles, ["peer-a", "peer-b"]);
        let available = sqlx::query_scalar::<_, i32>(
            "SELECT user_id FROM available_user_ids ORDER BY user_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(available, [10]);

        Ok(())
    }
}

// Alias: Preserves rows for a different account's hash
// Input: R with all messages, removeself_user("target", "peer-a-hash")
// Expected: WrongCredentials. Accounts, profiles, all messages and IDs are unchanged.
postgres_test! {
    async fn remove_wrong(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        sqlx::raw_sql(
            "INSERT INTO messages (message_id, user_id, message)
             VALUES (11, 7, 'message-11'), (12, 8, 'message-12');
             INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (21, 7, 8, 'message-21'),
                    (22, 8, 7, 'message-22'),
                    (23, 8, 9, 'message-23');",
        )
        .execute(&pool)
        .await?;
        let before = snapshot(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("target", "peer-a-hash").await,
            RemoveSelfResponse::WrongCredentials
        ));
        assert_eq!(snapshot(&pool).await?, before);

        Ok(())
    }
}

// Alias: Reports unknown username
// Input: R without messages, removeself_user("missing", "target-hash")
// Expected: UserNotFound and no writes.
postgres_test! {
    async fn remove_missing(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        let before = snapshot(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("missing", "target-hash").await,
            RemoveSelfResponse::UserNotFound
        ));
        assert_eq!(snapshot(&pool).await?, before);

        Ok(())
    }
}

// Alias: Reports an already removed username
// Input: Remove ("target", "target-hash") successfully from R, then call again with the same values.
// Expected: Second call returns UserNotFound. Peers and IDs=[10] remain unchanged.
postgres_test! {
    async fn remove_twice(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::Success(7)
        ));
        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::UserNotFound
        ));
        let users = sqlx::query_as::<_, (i32, String, String)>(
            "SELECT user_id, username, password_hash FROM users ORDER BY user_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            users,
            [
                (8, "peer-a".into(), "peer-a-hash".into()),
                (9, "peer-b".into(), "peer-b-hash".into()),
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT username FROM user_information ORDER BY user_id",
            )
            .fetch_all(&pool)
            .await?,
            ["peer-a", "peer-b"]
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>(
                "SELECT user_id FROM available_user_ids ORDER BY user_id",
            )
            .fetch_all(&pool)
            .await?,
            [10]
        );

        Ok(())
    }
}

// Alias: Uses the recreated account's current hash and ID
// Input: Remove target 7 from R; recreate account(10, "target", "replacement-hash"); call with "target-hash", then "replacement-hash".
// Expected: First call returns WrongCredentials and leaves account 10 intact. Second returns Success(10) and deletes account 10. Neither ID is recycled.
postgres_test! {
    async fn remove_recreated(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));
        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::Success(7)
        ));
        sqlx::query("DELETE FROM available_user_ids WHERE user_id = 10")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE VALUES (10, 'target', 'replacement-hash')",
        )
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO user_information (user_id, username) VALUES (10, 'target')")
            .execute(&pool)
            .await?;

        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::WrongCredentials
        ));
        assert_eq!(
            sqlx::query_as::<_, (i32, String, String)>(
                "SELECT user_id, username, password_hash FROM users WHERE username = 'target'",
            )
            .fetch_one(&pool)
            .await?,
            (10, "target".into(), "replacement-hash".into())
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>(
                "SELECT user_id FROM user_information WHERE username = 'target'",
            )
            .fetch_one(&pool)
            .await?,
            10
        );
        assert!(matches!(
            handler.removeself_user("target", "replacement-hash").await,
            RemoveSelfResponse::Success(10)
        ));
        let final_state = snapshot(&pool).await?;
        assert!(final_state
            .users
            .iter()
            .all(|user| user["user_id"] != 10));
        assert!(final_state
            .profiles
            .iter()
            .all(|profile| profile["user_id"] != 10));
        assert!(final_state.available_ids.is_empty());

        Ok(())
    }
}

// Alias: Returns the maximum user ID
// Input: account(65535, "last-user", "last-hash"), removeself_user("last-user", "last-hash")
// Expected: Success(65535) and account 65535 is gone.
postgres_test! {
    async fn remove_max_id(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query("DELETE FROM available_user_ids")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE VALUES (65535, 'last-user', 'last-hash')",
        )
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO user_information (user_id, username) VALUES (65535, 'last-user')")
            .execute(&pool)
            .await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("last-user", "last-hash").await,
            RemoveSelfResponse::Success(65535)
        ));
        let final_state = snapshot(&pool).await?;
        assert!(final_state.users.is_empty());
        assert!(final_state.profiles.is_empty());

        Ok(())
    }
}

// Alias: Deletes the target's public messages
// Input: R plus public messages 11 and 12, remove ("target", "target-hash").
// Expected: Success(7); profile 7 and message 11 are gone. Message 12 survives.
postgres_test! {
    async fn remove_public_cascade(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        sqlx::query(
            "INSERT INTO messages (message_id, user_id, message)
             VALUES (11, 7, 'message-11'), (12, 8, 'message-12')",
        )
        .execute(&pool)
        .await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::Success(7)
        ));
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM user_information WHERE user_id = 7)",
        )
        .fetch_one(&pool)
        .await?);
        assert_eq!(
            sqlx::query_as::<_, (i64, i32, String)>(
                "SELECT message_id::bigint, user_id, message FROM messages",
            )
            .fetch_all(&pool)
            .await?,
            [(12, 8, "message-12".into())]
        );

        Ok(())
    }
}

// Alias: Cascades through the sender ID
// Input: R plus private messages 21 (7 to 8) and 23 (8 to 9), remove ("target", "target-hash").
// Expected: Success(7); private message 21 is gone. Message 23 survives.
postgres_test! {
    async fn remove_private_sender(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        sqlx::query(
            "INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (21, 7, 8, 'message-21'), (23, 8, 9, 'message-23')",
        )
        .execute(&pool)
        .await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::Success(7)
        ));
        assert_eq!(
            sqlx::query_as::<_, (i64, i32, i32, String)>(
                "SELECT privatemessage_id::bigint, user_id, target_id, message FROM private_messages",
            )
            .fetch_all(&pool)
            .await?,
            [(23, 8, 9, "message-23".into())]
        );

        Ok(())
    }
}

// Alias: Cascades through the recipient ID
// Input: R plus private messages 22 (8 to 7) and 23 (8 to 9), remove ("target", "target-hash").
// Expected: Success(7); private message 22 is gone. Message 23 survives.
postgres_test! {
    async fn remove_private_recipient(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        sqlx::query(
            "INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (22, 8, 7, 'message-22'), (23, 8, 9, 'message-23')",
        )
        .execute(&pool)
        .await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert!(matches!(
            handler.removeself_user("target", "target-hash").await,
            RemoveSelfResponse::Success(7)
        ));
        assert_eq!(
            sqlx::query_as::<_, (i64, i32, i32, String)>(
                "SELECT privatemessage_id::bigint, user_id, target_id, message FROM private_messages",
            )
            .fetch_all(&pool)
            .await?,
            [(23, 8, 9, "message-23".into())]
        );

        Ok(())
    }
}

// Alias: Preserves dependents when deletion is blocked
// Input: R plus all messages; a non-cascading foreign key references account 7; remove ("target", "target-hash").
// Expected: HandlerError. All accounts, profiles, messages, and IDs survive.
postgres_test! {
    async fn remove_db_error(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_removal_fixture(&pool).await?;
        sqlx::raw_sql(
            "INSERT INTO messages (message_id, user_id, message)
             VALUES (11, 7, 'message-11'), (12, 8, 'message-12');
             INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (21, 7, 8, 'message-21'),
                    (22, 8, 7, 'message-22'),
                    (23, 8, 9, 'message-23');",
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE TABLE removal_guard (user_id INTEGER REFERENCES users (user_id))")
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO removal_guard (user_id) VALUES (7)")
            .execute(&pool)
            .await?;
        let before = snapshot(&pool).await?;
        track_completed_deletes(&pool, "users").await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        sqlx::query("SAVEPOINT removal_attempt").execute(&pool).await?;
        let response = handler.removeself_user("target", "target-hash").await;
        sqlx::query("ROLLBACK TO SAVEPOINT removal_attempt")
            .execute(&pool)
            .await?;
        assert!(matches!(response, RemoveSelfResponse::HandlerError));
        assert!(!delete_completed(&pool).await?);
        assert_eq!(snapshot(&pool).await?, before);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}
