use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use argon2::{Argon2, PasswordHasher};
use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register, RemoveSelf, ServerUnit},
};
use futures_util::StreamExt;
use sqlx::{
    Acquire, Arguments, PgPool, Postgres, Transaction, postgres::PgArguments, types::JsonValue,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{RegResponse, UserHandler, VerifyResult};
use crate::database::serial_tests::postgres_test;
use crate::users::{registration::handle_register, remove::handle_removeself};

const SAMPLE_COUNT: usize = 20;
const TIMING_PASSWORD: &str = "timing-password";
const EXPLAIN_INSERT: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/insert_user.sql"),
);
const EXPLAIN_REMOVE: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/remove_user.sql"),
);
const EXPLAIN_REMOVESELF: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/removeself_user.sql"),
);
const VERIFY_REMOVAL_WITH_MESSAGE_HISTORY: &str =
    "SELECT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1)
     AND NOT EXISTS (SELECT 1 FROM users WHERE user_id = 1)
     AND NOT EXISTS (SELECT 1 FROM user_information WHERE user_id = 1)
     AND NOT EXISTS (SELECT 1 FROM messages WHERE user_id = 1)
     AND NOT EXISTS (SELECT 1 FROM private_messages WHERE 1 IN (user_id, target_id))
     AND (SELECT count(*) FROM messages) = 5000
     AND (SELECT count(*) FROM private_messages) = 50";

async fn measure_query(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    explain_sql: &'static str,
    arguments: PgArguments,
    verify_sql: &'static str,
) -> sqlx::Result<()> {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        let mut sample = tx.begin().await?;
        let plan = sqlx::query_scalar_with::<_, JsonValue, _>(explain_sql, arguments.clone())
            .fetch_one(&mut *sample)
            .await?;
        let elapsed_ms = plan[0]["Execution Time"]
            .as_f64()
            .expect("PostgreSQL EXPLAIN should report execution time");
        assert!(elapsed_ms.is_finite() && elapsed_ms >= 0.0);

        if run == 0 {
            let expected_state = sqlx::query_scalar::<_, bool>(verify_sql)
                .fetch_one(&mut *sample)
                .await?;
            assert!(
                expected_state,
                "{name} warm-up should leave the expected database state"
            );
        } else {
            samples.push(elapsed_ms);
        }
        sample.rollback().await?;
    }

    report_samples(name, &mut samples);
    Ok(())
}

fn report_samples(name: &str, samples: &mut [f64]) {
    samples.sort_unstable_by(f64::total_cmp);
    let middle = samples.len() / 2;
    let median = (samples[middle - 1] + samples[middle]) / 2.0;
    println!(
        "{name}: {} samples, min {:.3} ms, median {:.3} ms, max {:.3} ms",
        samples.len(),
        samples[0],
        median,
        samples[samples.len() - 1]
    );
}

async fn loopback() -> (Framed<TcpStream, FlpcCodec>, Framed<TcpStream, FlpcCodec>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (Framed::new(server, FlpcCodec), Framed::new(peer, FlpcCodec))
}

async fn read_response(peer: &mut Framed<TcpStream, FlpcCodec>) -> FlpcUnit {
    timeout(Duration::from_secs(5), peer.next())
        .await
        .expect("handler response should arrive promptly")
        .expect("handler should send a response")
        .expect("peer should decode the response")
}

postgres_test! {
    async fn verify_credentials_execution_time(pool: PgPool) -> sqlx::Result<()> {
        let worker = UserHandler::new(pool, mpsc::channel(1).1, Arc::new(Semaphore::new(1)));
        let username = "timing_verify";
        let hash = Argon2::default().hash_password(TIMING_PASSWORD.as_bytes()).unwrap().to_string();
        assert!(matches!(worker.insert_user(username, &hash).await, RegResponse::Success(_)));

        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for run in 0..=SAMPLE_COUNT {
            let start = Instant::now();
            let result = worker.verify_credentials(username, TIMING_PASSWORD).await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert!(matches!(result, Ok(VerifyResult::Success(verified)) if verified == hash));
            if run != 0 {
                samples.push(elapsed_ms);
            }
        }
        report_samples("verify_credentials", &mut samples);
        Ok(())
    }
}

postgres_test! {
    async fn handle_register_execution_time(pool: PgPool) -> sqlx::Result<()> {
        let (user_tx, user_rx) = mpsc::channel(1);
        let permit = Arc::new(Semaphore::new(1));
        let worker = tokio::spawn(UserHandler::new(pool, user_rx, Arc::clone(&permit)).run());

        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for run in 0..=SAMPLE_COUNT {
            let (server, mut peer) = loopback().await;
            let (sink, _) = server.split();
            let request = Register::new(
                "token".into(),
                format!("timing_register_{run}"),
                TIMING_PASSWORD.into(),
            );
            let start = Instant::now();
            handle_register(request, user_tx.clone(), Arc::clone(&permit), sink, "token", 32, 64)
                .await;
            let response = read_response(&mut peer).await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert!(
                matches!(response, FlpcUnit::Server(ServerUnit::ServerRegister(reply)) if *reply.outcome() == 0),
                "registration sample should succeed",
            );
            if run != 0 {
                samples.push(elapsed_ms);
            }
        }

        drop(user_tx);
        worker.await.expect("user worker should exit");
        report_samples("handle_register", &mut samples);
        Ok(())
    }
}

postgres_test! {
    async fn handle_removeself_execution_time(pool: PgPool) -> sqlx::Result<()> {
        let (user_tx, user_rx) = mpsc::channel(1);
        let worker = UserHandler::new(pool, user_rx, Arc::new(Semaphore::new(1)));
        let hash = Argon2::default().hash_password(TIMING_PASSWORD.as_bytes()).unwrap().to_string();
        let usernames: Vec<_> = (0..=SAMPLE_COUNT)
            .map(|run| format!("timing_removeself_{run}"))
            .collect();
        for username in &usernames {
            assert!(matches!(worker.insert_user(username, &hash).await, RegResponse::Success(_)));
        }
        let worker = tokio::spawn(worker.run());

        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for (run, username) in usernames.into_iter().enumerate() {
            let (server, mut peer) = loopback().await;
            let (sink, _) = server.split();
            let request = RemoveSelf::new(username, TIMING_PASSWORD.into());
            let start = Instant::now();
            handle_removeself(request, user_tx.clone(), sink, 32, 64).await;
            let response = read_response(&mut peer).await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert!(
                matches!(response, FlpcUnit::Server(ServerUnit::ServerRemoveSelf(reply)) if *reply.result() == 0),
                "removal sample should succeed",
            );
            if run != 0 {
                samples.push(elapsed_ms);
            }
        }

        drop(user_tx);
        worker.await.expect("user worker should exit");
        report_samples("handle_removeself", &mut samples);
        Ok(())
    }
}

postgres_test! {
    async fn insert_user_query_execution_time(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        let mut arguments = PgArguments::default();
        arguments.add("timing_insert").map_err(sqlx::Error::Encode)?;
        arguments.add("fixture-hash").map_err(sqlx::Error::Encode)?;
        measure_query(
            &mut tx,
            "insert_user",
            EXPLAIN_INSERT,
            arguments,
            "SELECT EXISTS (
                 SELECT 1 FROM users JOIN user_information USING (user_id, username)
                 WHERE username = 'timing_insert' AND password_hash = 'fixture-hash'
             ) AND NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1)",
        )
        .await?;
        tx.rollback().await
    }
}

postgres_test! {
    async fn remove_user_query_execution_time(pool: PgPool) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(
            "DELETE FROM available_user_ids WHERE user_id IN (1, 2);
             INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE
             VALUES (1, 'timing_remove', 'fixture-hash'), (2, 'timing_peer', 'fixture-hash');
             INSERT INTO user_information (user_id, username) VALUES (1, 'timing_remove');
             INSERT INTO messages (message_id, user_id, message) VALUES (1, 1, 'public');
             INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
             VALUES (1, 1, 2, 'outgoing'), (2, 2, 1, 'incoming');",
        )
        .execute(&mut *tx)
        .await?;

        let mut arguments = PgArguments::default();
        arguments.add(1_i32).map_err(sqlx::Error::Encode)?;
        measure_query(
            &mut tx,
            "remove_user",
            EXPLAIN_REMOVE,
            arguments,
            "SELECT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM users WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM user_information WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM messages)
             AND NOT EXISTS (SELECT 1 FROM private_messages)",
        )
        .await?;
        tx.rollback().await
    }
}

async fn seed_message_history(tx: &mut Transaction<'_, Postgres>) -> sqlx::Result<()> {
    // Each peer exchanges ten messages in each direction with user 1 and sends ten to the next peer.
    sqlx::raw_sql(
        "DELETE FROM available_user_ids WHERE user_id BETWEEN 1 AND 6;
         INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE
         SELECT user_id, 'timing_user_' || user_id, 'fixture-hash'
         FROM generate_series(1, 6) AS fixture(user_id);
         INSERT INTO user_information (user_id, username)
         SELECT user_id, username FROM users;
         INSERT INTO messages (message_id, user_id, message)
         SELECT (user_id - 1) * 1000 + n, user_id, 'public ' || n
         FROM users CROSS JOIN generate_series(1, 1000) AS message(n);
         INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
         SELECT (peer_id - 2) * 10 + n, 1, peer_id, 'outgoing ' || n
         FROM generate_series(2, 6) AS peer(peer_id)
         CROSS JOIN generate_series(1, 10) AS message(n)
         UNION ALL
         SELECT 50 + (peer_id - 2) * 10 + n, peer_id, 1, 'incoming ' || n
         FROM generate_series(2, 6) AS peer(peer_id)
         CROSS JOIN generate_series(1, 10) AS message(n)
         UNION ALL
         SELECT 100 + (peer_id - 2) * 10 + n, peer_id, 2 + (peer_id - 1) % 5, 'peer ' || n
         FROM generate_series(2, 6) AS peer(peer_id)
         CROSS JOIN generate_series(1, 10) AS message(n);",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

postgres_test! {
    async fn remove_user_query_execution_time_with_message_history(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        seed_message_history(&mut tx).await?;

        let mut arguments = PgArguments::default();
        arguments.add(1_i32).map_err(sqlx::Error::Encode)?;
        measure_query(
            &mut tx,
            "remove_user with message history",
            EXPLAIN_REMOVE,
            arguments,
            VERIFY_REMOVAL_WITH_MESSAGE_HISTORY,
        )
        .await?;
        tx.rollback().await
    }
}

postgres_test! {
    async fn removeself_user_query_execution_time_with_message_history(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        seed_message_history(&mut tx).await?;

        let mut arguments = PgArguments::default();
        arguments.add("timing_user_1").map_err(sqlx::Error::Encode)?;
        arguments.add("fixture-hash").map_err(sqlx::Error::Encode)?;
        measure_query(
            &mut tx,
            "removeself_user with message history",
            EXPLAIN_REMOVESELF,
            arguments,
            "SELECT NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM users WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM user_information WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM messages WHERE user_id = 1)
             AND NOT EXISTS (SELECT 1 FROM private_messages WHERE 1 IN (user_id, target_id))
             AND (SELECT count(*) FROM messages) = 5000
             AND (SELECT count(*) FROM private_messages) = 50",
        )
        .await?;
        tx.rollback().await
    }
}

postgres_test! {
    async fn removeself_user_wrong_hash_query_execution_time_with_message_history(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let mut tx = pool.begin().await?;
        seed_message_history(&mut tx).await?;

        let mut arguments = PgArguments::default();
        arguments.add("timing_user_1").map_err(sqlx::Error::Encode)?;
        arguments.add("wrong-hash").map_err(sqlx::Error::Encode)?;
        measure_query(
            &mut tx,
            "removeself_user with wrong hash and message history",
            EXPLAIN_REMOVESELF,
            arguments,
            "SELECT EXISTS (
                 SELECT 1 FROM users JOIN user_information USING (user_id, username)
                 WHERE user_id = 1 AND password_hash = 'fixture-hash'
             ) AND NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1)
             AND (SELECT count(*) FROM messages) = 6000
             AND (SELECT count(*) FROM private_messages) = 150",
        )
        .await?;
        tx.rollback().await
    }
}
