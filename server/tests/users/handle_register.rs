use std::time::Duration;

use argon2::{Argon2, PasswordHash, PasswordVerifier};
use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register},
};
use futures_util::StreamExt;
use sqlx::{PgPool, postgres::PgPoolOptions, types::JsonValue};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::database::{serial_tests::postgres_test, users::{UserHandler, run}};

use super::handle_register;

const SERVER_TOKEN: &str = "token";
const MAX_USERNAME_BYTES: usize = 4;
const MAX_PASSWORD_BYTES: usize = 6;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq)]
struct RegistrationSnapshot {
    users: Vec<JsonValue>,
    profiles: Vec<JsonValue>,
    available_ids: Vec<i32>,
}

async fn set_available_ids(pool: &PgPool, ids: &[i32]) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE NOT (user_id = ANY($1))")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_existing_user(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM available_user_ids WHERE user_id = 7")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE VALUES (7, 'user', 'old-hash')",
    )
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO user_information (user_id, username) VALUES (7, 'user')")
        .execute(pool)
        .await?;
    Ok(())
}

async fn snapshot(pool: &PgPool) -> sqlx::Result<RegistrationSnapshot> {
    Ok(RegistrationSnapshot {
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
        available_ids: sqlx::query_scalar(
            "SELECT user_id FROM available_user_ids ORDER BY user_id",
        )
        .fetch_all(pool)
        .await?,
    })
}

async fn registration_outcome(
    worker_pool: PgPool,
    request: Register,
    close_permit: bool,
) -> u8 {
    timeout(TEST_TIMEOUT, async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (sink, _) = Framed::new(server, FlpcCodec).split();
        let mut peer = Framed::new(peer, FlpcCodec);
        let (user_tx, user_rx) = mpsc::channel(1);
        let handler = UserHandler::new(user_rx, worker_pool, 1);
        if close_permit {
            handler.permit().close();
        }
        let worker = tokio::spawn(run(handler));
        let handler = handle_register(
            request,
            user_tx.clone(),
            sink,
            SERVER_TOKEN,
            MAX_USERNAME_BYTES,
            MAX_PASSWORD_BYTES,
        );
        let (_, frame) = tokio::join!(handler, peer.next());
        let frame = frame
            .expect("server should send one registration frame")
            .expect("peer should decode the registration frame");
        let FlpcUnit::ServerRegister(response) = frame else {
            panic!("expected ServerRegister, got {frame:?}");
        };
        assert!(
            peer.next().await.is_none(),
            "expected exactly one response frame"
        );
        drop(user_tx);
        timeout(TEST_TIMEOUT, worker)
            .await
            .expect("worker should exit after the request sender closes")
            .expect("worker should join");
        *response.outcome()
    })
    .await
    .expect("registration exchange should finish within the timeout")
}

// Alias: Registers at both byte limits
// Input: ("token", "éé", "🔑é")
// Expected: Code 0. Account 7 and profile exist, and its stored hash verifies "🔑é".
postgres_test! {
    async fn register_ok(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "éé".into(), "🔑é".into()),
            false,
        )
        .await;

        assert_eq!(code, 0);
        let (user_id, hash) = sqlx::query_as::<_, (i32, String)>(
            "SELECT user_id, password_hash FROM users WHERE username = 'éé'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(user_id, 7);
        let parsed_hash = PasswordHash::new(&hash).expect("stored hash should be valid PHC");
        Argon2::default()
            .verify_password("🔑é".as_bytes(), &parsed_hash)
            .expect("stored hash should verify the submitted password");
        assert_eq!(
            sqlx::query_scalar::<_, i32>(
                "SELECT user_id FROM user_information WHERE username = 'éé'",
            )
            .fetch_one(&pool)
            .await?,
            7
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT user_id FROM available_user_ids ORDER BY user_id")
                .fetch_all(&pool)
                .await?,
            [8]
        );
        Ok(())
    }
}

// Alias: Checks token before lengths
// Input: ("wrong", "", "")
// Expected: Code 1. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_token(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new("wrong".into(), "".into(), "".into()),
            false,
        )
        .await;

        assert_eq!(code, 1);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an empty username
// Input: ("token", "", "secret")
// Expected: Code 2. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_empty_user(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "".into(), "secret".into()),
            false,
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an empty password
// Input: ("token", "user", "")
// Expected: Code 2. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_empty_password(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "user".into(), "".into()),
            false,
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an over-limit username
// Input: ("token", "users", "secret")
// Expected: Code 2. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_long_user(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "users".into(), "secret".into()),
            false,
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Rejects an over-limit password
// Input: ("token", "user", "secrets")
// Expected: Code 2. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_long_password(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "user".into(), "secrets".into()),
            false,
        )
        .await;

        assert_eq!(code, 2);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Reports ID exhaustion for an existing username
// Input: Existing "user", IDs=[], ("token", "user", "secret")
// Expected: Code 3. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_full(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[]).await?;
        seed_existing_user(&pool).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "user".into(), "secret".into()),
            false,
        )
        .await;

        assert_eq!(code, 3);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Reports an existing username
// Input: account(7, "user", "old-hash"), ("token", "user", "secret")
// Expected: Code 4. Original account/hash and profile remain; available IDs remain unchanged.
postgres_test! {
    async fn register_duplicate(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[8]).await?;
        seed_existing_user(&pool).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "user".into(), "secret".into()),
            false,
        )
        .await;

        assert_eq!(code, 4);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Reports unavailable hashing
// Input: Closed hashing semaphore, ("token", "user", "secret")
// Expected: Code 5. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_hash_error(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let code = registration_outcome(
            pool.clone(),
            Register::new(SERVER_TOKEN.into(), "user".into(), "secret".into()),
            true,
        )
        .await;

        assert_eq!(code, 5);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}

// Alias: Reports a worker database failure
// Input: Closed worker pool, open hashing semaphore, ("token", "user", "secret")
// Expected: Code 5. Account, profile, and available IDs remain unchanged.
postgres_test! {
    async fn register_worker_error(pool: PgPool) -> sqlx::Result<()> {
        set_available_ids(&pool, &[7, 8]).await?;
        let before = snapshot(&pool).await?;
        let worker_pool = PgPoolOptions::new()
            .connect_with(pool.connect_options().as_ref().clone())
            .await?;
        worker_pool.close().await;
        let code = registration_outcome(
            worker_pool,
            Register::new(SERVER_TOKEN.into(), "user".into(), "secret".into()),
            false,
        )
        .await;

        assert_eq!(code, 5);
        assert_eq!(snapshot(&pool).await?, before);
        Ok(())
    }
}
