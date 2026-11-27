use std::{pin::pin, sync::Arc, time::Duration};

use argon2::{Argon2, PasswordHasher};
use futures_util::poll;
use sqlx::types::JsonValue;
use tokio::{
    sync::{Semaphore, oneshot},
    time::timeout,
};

use crate::database::serial_tests::postgres_test;

use super::transaction_pool::transaction_pool;
use super::{PgPool, RemoveSelfRequest, RemoveSelfResponse, VerifyError, VerifyResult, get_credentials, removeself_user, verify_credentials};

const VERIFY_TIMEOUT: Duration = Duration::from_secs(5);
const TARGET_PASSWORD: &str = "timing-password";
const TARGET_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$bGlnaHRjaGF0LWJlbmNobWFyaw$NG0zwI7kM/Cq+dLLB3HMbPwk1I8NW1iSDPpYPr/ImjU";

async fn remove(pool: &PgPool, username: &str, password: &str) -> RemoveSelfResponse {
    let (reply_tx, reply_rx) = oneshot::channel();
    let request = RemoveSelfRequest::new(username.into(), password.into(), reply_tx);
    removeself_user(pool, request, Arc::new(Semaphore::new(1))).await;
    reply_rx.await.expect("removeself_user should send one response")
}

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
         VALUES (7, 'target', $1),
                (8, 'peer-a', 'peer-a-hash'),
                (9, 'peer-b', 'peer-b-hash')",
    )
    .bind(TARGET_HASH)
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
// Expected: Ok(Some((7, "hash-upper"))).
postgres_test! {
    async fn lookup_exact_upper(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_accounts(&pool).await?;

        assert_eq!(get_credentials(&pool, "O'Reilly").await?, Some((7, "hash-upper".into())));
        Ok(())
    }
}

// Alias: Distinguishes username case
// Input: Same account fixture, input "o'reilly"
// Expected: Ok(Some((8, "hash-lower"))).
postgres_test! {
    async fn lookup_exact_lower(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_accounts(&pool).await?;

        assert_eq!(get_credentials(&pool, "o'reilly").await?, Some((8, "hash-lower".into())));
        Ok(())
    }
}

// Alias: Returns none for a near match
// Input: Same account fixture, input "O'Reill"
// Expected: Ok(None).
postgres_test! {
    async fn lookup_missing(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_accounts(&pool).await?;

        assert_eq!(get_credentials(&pool, "O'Reill").await?, None);
        Ok(())
    }
}

// Alias: Returns the stored string unchanged
// Input: account(7, "account", "invalid-phc"), input "account"
// Expected: Ok(Some((7, "invalid-phc"))).
postgres_test! {
    async fn lookup_raw(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_account(&pool, "account", "invalid-phc").await?;

        assert_eq!(get_credentials(&pool, "account").await?, Some((7, "invalid-phc".into())));
        Ok(())
    }
}

// Alias: Returns the pool error
// Input: Closed pool, input "account"
// Expected: Err(sqlx::Error::PoolClosed).
postgres_test! {
    async fn lookup_closed(pool: PgPool) -> sqlx::Result<()> {
        seed_lookup_account(&pool, "account", "invalid-phc").await?;
        pool.close().await;

        assert!(matches!(
            get_credentials(&pool, "account").await,
            Err(sqlx::Error::PoolClosed)
        ));
        Ok(())
    }
}

// Alias: Returns the user ID on exact match
// Input: V, ("account", "café🔑")
// Expected: Success(7); one permit remains available.
postgres_test! {
    async fn verify_match(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;

        assert!(matches!(
            verify_credentials(&pool, "account", "café🔑", Arc::clone(&permit)).await,
            Ok(VerifyResult::Success(7))
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

        assert!(matches!(
            verify_credentials(&pool, "account", "wrong", Arc::clone(&permit)).await,
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

        assert!(matches!(
            verify_credentials(&pool, "account", "cafe\u{301}🔑", Arc::clone(&permit)).await,
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

        assert!(matches!(
            verify_credentials(&pool, "account", &hash, Arc::clone(&permit)).await,
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

        assert!(matches!(
            verify_credentials(&pool, "absent", "café🔑", Arc::clone(&permit)).await,
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

        assert!(matches!(
            verify_credentials(&pool, "account", "café🔑", Arc::clone(&permit)).await,
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

        assert!(matches!(
            verify_credentials(&pool, "account", "café🔑", Arc::clone(&permit)).await,
            Err(VerifyError::Argon2Error)
        ));
        assert_eq!(permit.available_permits(), 1);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

// Alias: Waits for a permit
// Input: Hold V's only permit, poll ("account", "café🔑") to permit acquisition, then release the held permit.
// Expected: Pending while held. After release, Success(7) and one permit available.
postgres_test! {
    async fn verify_wait(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let hash = Argon2::default().hash_password("café🔑".as_bytes()).unwrap().to_string();
        seed_verify_account(&pool, &hash).await?;
        let held_permit = permit.clone().acquire_owned().await.unwrap();
        let held_connection = pool.acquire().await?;

        let mut verification = pin!(verify_credentials(
            &pool,
            "account", "café🔑", Arc::clone(&permit),
        ));
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
            Ok(VerifyResult::Success(7))
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

        assert!(matches!(
            verify_credentials(&pool, "account", "café🔑", Arc::clone(&permit)).await,
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
        pool.close().await;

        assert!(matches!(
            verify_credentials(&pool, "account", "café🔑", Arc::new(Semaphore::new(1))).await,
            Err(VerifyError::SqlxError)
        ));
        Ok(())
    }
}

// Alias: Deletes the account without recycling its ID
// Input: R without messages, RemoveSelfRequest("target", "timing-password")
// Expected: Success(7). Account and profile 7 are gone. Peers survive and IDs=[10].
postgres_test! {
    async fn remove_match(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;

        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
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

// Alias: Preserves rows for a wrong password
// Input: R with all messages, RemoveSelfRequest("target", "wrong")
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

        assert!(matches!(
            remove(&pool, "target", "wrong").await,
            RemoveSelfResponse::WrongCredentials
        ));
        assert_eq!(snapshot(&pool).await?, before);

        Ok(())
    }
}

// Alias: Reports unknown username
// Input: R without messages, RemoveSelfRequest("missing", "timing-password")
// Expected: UserNotFound and no writes.
postgres_test! {
    async fn remove_missing(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        let before = snapshot(&pool).await?;

        assert!(matches!(
            remove(&pool, "missing", TARGET_PASSWORD).await,
            RemoveSelfResponse::UserNotFound
        ));
        assert_eq!(snapshot(&pool).await?, before);

        Ok(())
    }
}

// Alias: Reports an already removed username
// Input: Remove ("target", "timing-password") successfully from R, then call again with the same values.
// Expected: Second call returns UserNotFound. Peers and IDs=[10] remain unchanged.
postgres_test! {
    async fn remove_twice(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;

        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
            RemoveSelfResponse::Success(7)
        ));
        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
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
// Input: Remove target 7 from R; recreate account(10, "target", "replacement-hash"); call with "timing-password", then "replacement-hash".
// Expected: First call returns WrongCredentials and leaves account 10 intact. Second returns Success(10) and deletes account 10. Neither ID is recycled.
postgres_test! {
    async fn remove_recreated(pool: PgPool) -> sqlx::Result<()> {
        seed_removal_fixture(&pool).await?;
        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
            RemoveSelfResponse::Success(7)
        ));
        sqlx::query("DELETE FROM available_user_ids WHERE user_id = 10")
            .execute(&pool)
            .await?;
        let replacement_hash = Argon2::default().hash_password(b"replacement").unwrap().to_string();
        sqlx::query(
            "INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE VALUES (10, 'target', $1)",
        )
        .bind(&replacement_hash)
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO user_information (user_id, username) VALUES (10, 'target')")
            .execute(&pool)
            .await?;

        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
            RemoveSelfResponse::WrongCredentials
        ));
        assert_eq!(
            sqlx::query_as::<_, (i32, String, String)>(
                "SELECT user_id, username, password_hash FROM users WHERE username = 'target'",
            )
            .fetch_one(&pool)
            .await?,
            (10, "target".into(), replacement_hash)
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
            remove(&pool, "target", "replacement").await,
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
// Input: account(65535, "last-user", "last-hash"), RemoveSelfRequest("last-user", "timing-password")
// Expected: Success(65535) and account 65535 is gone.
postgres_test! {
    async fn remove_max_id(pool: PgPool) -> sqlx::Result<()> {
        sqlx::query("DELETE FROM available_user_ids")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE VALUES (65535, 'last-user', $1)",
        )
        .bind(TARGET_HASH)
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO user_information (user_id, username) VALUES (65535, 'last-user')")
            .execute(&pool)
            .await?;

        assert!(matches!(
            remove(&pool, "last-user", TARGET_PASSWORD).await,
            RemoveSelfResponse::Success(65535)
        ));
        let final_state = snapshot(&pool).await?;
        assert!(final_state.users.is_empty());
        assert!(final_state.profiles.is_empty());

        Ok(())
    }
}

// Alias: Deletes the target's public messages
// Input: R plus public messages 11 and 12, remove ("target", "timing-password").
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

        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
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
// Input: R plus private messages 21 (7 to 8) and 23 (8 to 9), remove ("target", "timing-password").
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

        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
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
// Input: R plus private messages 22 (8 to 7) and 23 (8 to 9), remove ("target", "timing-password").
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

        assert!(matches!(
            remove(&pool, "target", TARGET_PASSWORD).await,
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
// Input: R plus all messages; a non-cascading foreign key references account 7; remove ("target", "timing-password").
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

        let response = remove(&pool, "target", TARGET_PASSWORD).await;
        assert!(matches!(response, RemoveSelfResponse::HandlerError));
        assert_eq!(snapshot(&pool).await?, before);

        Ok(())
    }
}

postgres_test! {
    async fn remove_stale_id(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_removal_fixture(&pool).await?;
        let permit = Arc::new(Semaphore::new(1));
        let held_permit = Arc::clone(&permit).acquire_owned().await.unwrap();
        let held_connection = pool.acquire().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = RemoveSelfRequest::new("target".into(), TARGET_PASSWORD.into(), reply_tx);
        let mut removal = pin!(removeself_user(&pool, request, permit));
        assert!(poll!(removal.as_mut()).is_pending());

        let mut acquire = pin!(pool.acquire());
        assert!(poll!(acquire.as_mut()).is_pending());
        drop(held_connection);
        let mut connection = timeout(VERIFY_TIMEOUT, async {
            tokio::select! {
                biased;
                _ = removal.as_mut() => panic!("removal finished while hashing was blocked"),
                connection = acquire.as_mut() => connection,
            }
        })
        .await
        .expect("removal should release the lookup connection")?;

        assert!(poll!(removal.as_mut()).is_pending());
        sqlx::raw_sql(
            "DELETE FROM users WHERE user_id = 7;
             DELETE FROM available_user_ids WHERE user_id = 10;",
        )
        .execute(&mut *connection)
        .await?;
        sqlx::query(
            "INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE VALUES (10, 'target', $1)",
        )
        .bind(TARGET_HASH)
        .execute(&mut *connection)
        .await?;
        sqlx::query("INSERT INTO user_information (user_id, username) VALUES (10, 'target')")
            .execute(&mut *connection)
            .await?;
        drop(connection);

        drop(held_permit);
        timeout(VERIFY_TIMEOUT, removal).await.expect("removal should finish");
        assert!(matches!(reply_rx.await, Ok(RemoveSelfResponse::HandlerError)));
        let remaining: i32 = sqlx::query_scalar(
            "SELECT user_id FROM users JOIN user_information USING (user_id, username)
             WHERE username = 'target'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(remaining, 10);
        assert!(snapshot(&pool).await?.available_ids.is_empty());
        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}
