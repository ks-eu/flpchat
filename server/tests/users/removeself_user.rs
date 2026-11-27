use std::{pin::pin, sync::Arc, time::Duration};

use argon2::{Argon2, PasswordHasher};
use futures_util::poll;
use sqlx::types::JsonValue;
use tokio::{
    sync::{Semaphore, mpsc},
    time::timeout,
};

use crate::database::serial_tests::postgres_test;

use super::transaction_pool::transaction_pool;
use super::{PgPool, RemoveSelfResponse, UserHandler, VerifyError, VerifyResult};

const VERIFY_TIMEOUT: Duration = Duration::from_secs(5);

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

async fn seed_verify_account(pool: &PgPool, hash: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE user_id = 7")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE VALUES (7, 'account', $1)",
    )
    .bind(hash)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO user_information (user_id, username) VALUES (7, 'account')")
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_lookup_accounts(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE user_id IN (7, 8)")
        .execute(pool)
        .await?;
    sqlx::raw_sql(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE
         VALUES (7, 'O''Reilly', 'hash-upper'), (8, 'o''reilly', 'hash-lower');
         INSERT INTO user_information (user_id, username)
         VALUES (7, 'O''Reilly'), (8, 'o''reilly');",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_lookup_account(pool: &PgPool, username: &str, hash: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE user_id = 7")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE VALUES (7, $1, $2)",
    )
    .bind(username)
    .bind(hash)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO user_information (user_id, username) VALUES (7, $1)")
        .bind(username)
        .execute(pool)
        .await?;
    Ok(())
}

// Alias: Looks up the exact quoted username
// Input: account(7, "O'Reilly", "hash-upper"), account(8, "o'reilly", "hash-lower"), input "O'Reilly"
// Expected: Ok(Some("hash-upper")).
postgres_test! {
    async fn lookup_exact_upper(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_accounts(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert_eq!(handler.get_hashed_password("O'Reilly").await?.as_deref(), Some("hash-upper"));
        Ok(())
    }
}

// Alias: Distinguishes username case
// Input: Same account fixture, input "o'reilly"
// Expected: Ok(Some("hash-lower")).
postgres_test! {
    async fn lookup_exact_lower(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_accounts(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert_eq!(handler.get_hashed_password("o'reilly").await?.as_deref(), Some("hash-lower"));
        Ok(())
    }
}

// Alias: Returns none for a near match
// Input: Same account fixture, input "O'Reill"
// Expected: Ok(None).
postgres_test! {
    async fn lookup_missing(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_accounts(&pool).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert_eq!(handler.get_hashed_password("O'Reill").await?, None);
        Ok(())
    }
}

// Alias: Returns the stored string unchanged
// Input: account(7, "account", "invalid-phc"), input "account"
// Expected: Ok(Some("invalid-phc")).
postgres_test! {
    async fn lookup_raw(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_account(&pool, "account", "invalid-phc").await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        assert_eq!(handler.get_hashed_password("account").await?.as_deref(), Some("invalid-phc"));
        Ok(())
    }
}

// Alias: Returns the pool error
// Input: Closed pool, input "account"
// Expected: Err(sqlx::Error::PoolClosed).
postgres_test! {
    async fn lookup_closed(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_account(&pool, "account", "invalid-phc").await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));
        pool.close().await;

        assert!(matches!(
            handler.get_hashed_password("account").await,
            Err(sqlx::Error::PoolClosed)
        ));
        Ok(())
    }
}

// Alias: Returns the stored hash on exact match
// Input: V, ("account", "café🔑")
// Expected: Success(original_encoded_hash) byte-for-byte; one permit remains available.
postgres_test! {
    async fn verify_match(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", "café🔑").await,
            Ok(VerifyResult::Success(stored_hash)) if stored_hash == hash
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Rejects different plaintext
// Input: V, ("account", "wrong")
// Expected: WrongCredentials; one permit remains available.
postgres_test! {
    async fn verify_wrong(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", "wrong").await,
            Ok(VerifyResult::WrongCredentials)
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Compares exact password bytes
// Input: V, ("account", "cafe\u{301}🔑") using a combining accent
// Expected: WrongCredentials; one permit remains available.
postgres_test! {
    async fn verify_unicode(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", "cafe\u{301}🔑").await,
            Ok(VerifyResult::WrongCredentials)
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Rejects the encoded hash as plaintext
// Input: V, ("account", original_encoded_hash)
// Expected: WrongCredentials; one permit remains available.
postgres_test! {
    async fn verify_hash_as_password(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", &hash).await,
            Ok(VerifyResult::WrongCredentials)
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Reports a missing username before permit acquisition
// Input: V with a closed semaphore, ("absent", "café🔑")
// Expected: UsernameNotFound; the semaphore remains closed and unacquired.
postgres_test! {
    async fn verify_missing(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        permit.close();
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("absent", "café🔑").await,
            Ok(VerifyResult::UsernameNotFound)
        ));
        assert!(permit.is_closed());
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Reports malformed stored hash
// Input: account(7, "account", "invalid-phc"), ("account", "café🔑")
// Expected: Err(VerifyError::Argon2Error); one permit remains available.
postgres_test! {
    async fn verify_malformed(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        seed_verify_account(&pool, "invalid-phc").await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", "café🔑").await,
            Err(VerifyError::Argon2Error)
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Reports unsupported stored algorithm
// Input: V with "$argon2id$" replaced by "$scrypt$", ("account", "café🔑")
// Expected: Err(VerifyError::Argon2Error); one permit remains available.
postgres_test! {
    async fn verify_unsupported(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default()
            .hash_password("café🔑".as_bytes())
            .unwrap()
            .to_string()
            .replace("$argon2id$", "$scrypt$");
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", "café🔑").await,
            Err(VerifyError::Argon2Error)
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Waits for a permit
// Input: Hold V's only permit, poll ("account", "café🔑") to permit acquisition, then release the held permit.
// Expected: Pending while held. After release, Success(original_encoded_hash) and one permit available.
postgres_test! {
    async fn verify_wait(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));
        let held_permit = permit.clone().acquire_owned().await.unwrap();
        let held_connection = pool.acquire().await?;

        let mut verification = pin!(handler.verify_credentials("account", "café🔑"));
        assert!(poll!(verification.as_mut()).is_pending());
        let mut probe = pin!(pool.acquire());
        assert!(poll!(probe.as_mut()).is_pending());
        drop(held_connection);

        let probe_connection = timeout(VERIFY_TIMEOUT, async {
            tokio::select! {
                biased;
                _ = verification.as_mut() => panic!("verification finished while the permit was held"),
                connection = probe.as_mut() => connection.expect("pool probe should acquire the released connection"),
            }
        })
        .await
        .expect("verification should release the connection and wait for its permit");
        assert!(poll!(verification.as_mut()).is_pending());
        {
            let mut second_waiter = pin!(permit.acquire());
            assert!(poll!(second_waiter.as_mut()).is_pending());
            drop(held_permit);
            assert!(poll!(second_waiter.as_mut()).is_pending());
        }
        drop(probe_connection);

        assert!(matches!(
            timeout(VERIFY_TIMEOUT, verification.as_mut())
                .await
                .expect("verification should finish after permit release"),
            Ok(VerifyResult::Success(stored_hash)) if stored_hash == hash
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Reports a closed semaphore
// Input: V with semaphore closed, ("account", "café🔑")
// Expected: Err(VerifyError::PermitError).
postgres_test! {
    async fn verify_closed(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        permit.close();
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::clone(&permit));

        assert!(matches!(
            handler.verify_credentials("account", "café🔑").await,
            Err(VerifyError::PermitError)
        ));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Reports lookup failure
// Input: Close V's pool, then call ("account", "café🔑")
// Expected: Err(VerifyError::SqlxError).
postgres_test! {
    async fn verify_db_error(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));
        pool.close().await;

        assert!(matches!(
            handler.verify_credentials("account", "café🔑").await,
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
        let handler = UserHandler::new(pool.clone(), mpsc::channel(1).1, Arc::new(Semaphore::new(1)));

        let response = handler.removeself_user("target", "target-hash").await;
        assert!(matches!(response, RemoveSelfResponse::HandlerError));
        assert_eq!(snapshot(&pool).await?, before);

        Ok(())
    }
}
