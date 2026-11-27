use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register, RemoveSelf, ServerUnit},
};
use futures_util::StreamExt;
use sqlx::{Arguments, PgPool, postgres::PgArguments, types::JsonValue};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{UserHandler, VerifyResult, transaction_pool::transaction_pool};
use crate::database::serial_tests::postgres_test;
use crate::database::hash::{hash_password, verify_password};
use crate::users::{registration::handle_register, remove::handle_removeself};

const TCP_TIMEOUT: Duration = Duration::from_secs(5);
const SAMPLE_COUNT: usize = 20;
const TIMING_PASSWORD: &str = "timing-password";
const BENCHMARK_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$bGlnaHRjaGF0LWJlbmNobWFyaw$NG0zwI7kM/Cq+dLLB3HMbPwk1I8NW1iSDPpYPr/ImjU";
const EXPLAIN_CLAIM: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/claim_user_id.sql"),
);
const EXPLAIN_INSERT: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/insert_user.sql"),
);
const EXPLAIN_SET_HASH: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/set_password_hash.sql"),
);
const EXPLAIN_REMOVESELF: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/database/queries/removeself_user.sql"),
);
const EXPLAIN_LOOKUP: &str = "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) SELECT password_hash FROM users WHERE username = $1";

async fn seed_accounts(pool: &PgPool, with_history: bool) -> sqlx::Result<()> {
    sqlx::raw_sql(
        "DELETE FROM available_user_ids WHERE user_id BETWEEN 1 AND 1000;
         INSERT INTO users (user_id, username, password_hash)
         OVERRIDING SYSTEM VALUE
         SELECT user_id, 'bench-' || lpad(user_id::text, 4, '0'), 'fixture-hash'
         FROM generate_series(1, 1000) AS fixture(user_id);
         INSERT INTO user_information (user_id, username)
         SELECT user_id, username FROM users ORDER BY user_id;",
    )
    .execute(pool)
    .await?;
    sqlx::query("UPDATE users SET password_hash = $1 WHERE user_id = 1")
        .bind(BENCHMARK_HASH)
        .execute(pool)
        .await?;
    if with_history {
        seed_history(pool).await?;
    }
    Ok(())
}

async fn seed_history(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::raw_sql(
        "INSERT INTO messages (message_id, user_id, message)
         SELECT 100000 + n, 1, 'target public ' || n
         FROM generate_series(1, 1000) AS message(n);
         INSERT INTO messages (message_id, user_id, message)
         VALUES (200000, 2, 'unrelated public');
         INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
         SELECT 300000 + n, 1, 2, 'target outgoing ' || n
         FROM generate_series(1, 50) AS message(n);
         INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
         SELECT 400000 + n, 2, 1, 'target incoming ' || n
         FROM generate_series(1, 50) AS message(n);
         INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
         VALUES (500000, 2, 3, 'unrelated private');",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn sql_insert_state(pool: &PgPool) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM users JOIN user_information USING (user_id, username)
            WHERE user_id = 1001 AND username = 'bench-new' AND password_hash = $1
        ) AND NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1001)
          AND EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1002)",
    )
    .bind(BENCHMARK_HASH)
    .fetch_one(pool)
    .await
}

async fn registered_hash(pool: &PgPool) -> sqlx::Result<String> {
    let hash: String = sqlx::query_scalar(
        "SELECT password_hash FROM users JOIN user_information USING (user_id, username)
         WHERE user_id = 1001 AND username = 'bench-new'
           AND NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1001)
           AND EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1002)",
    )
    .fetch_one(pool)
    .await?;
    Ok(hash)
}

async fn removal_state(pool: &PgPool, removed: bool, with_history: bool) -> sqlx::Result<bool> {
    if removed {
        let state: (bool, bool, bool, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT NOT EXISTS (SELECT 1 FROM users WHERE user_id = 1),
                    NOT EXISTS (SELECT 1 FROM user_information WHERE user_id = 1),
                    NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1),
                    (SELECT count(*) FROM messages WHERE user_id = 1),
                    (SELECT count(*) FROM private_messages WHERE 1 IN (user_id, target_id)),
                    (SELECT count(*) FROM messages),
                    (SELECT count(*) FROM private_messages)",
        )
        .fetch_one(pool)
        .await?;
        let expected = if with_history {
            (true, true, true, 0, 0, 1, 1)
        } else {
            (true, true, true, 0, 0, 0, 0)
        };
        if state != expected {
            return Ok(false);
        }
        if with_history {
            return sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM messages WHERE message_id = 200000 AND user_id = 2 AND message = 'unrelated public')
                  AND EXISTS (SELECT 1 FROM private_messages WHERE privatemessage_id = 500000 AND user_id = 2 AND target_id = 3 AND message = 'unrelated private')",
            )
            .fetch_one(pool)
            .await;
        }
        return Ok(true);
    }

    let state: (bool, bool, bool, String, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM users WHERE user_id = 1 AND username = 'bench-0001' AND password_hash = $1),
                EXISTS (SELECT 1 FROM user_information WHERE user_id = 1 AND username = 'bench-0001'),
                NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1),
                (SELECT password_hash FROM users WHERE user_id = 1),
                (SELECT count(*) FROM messages WHERE user_id = 1),
                (SELECT count(*) FROM private_messages WHERE 1 IN (user_id, target_id)),
                (SELECT count(*) FROM messages),
                (SELECT count(*) FROM private_messages)",
    )
    .bind(BENCHMARK_HASH)
    .fetch_one(pool)
    .await?;
    if state
        != (
            true,
            true,
            true,
            BENCHMARK_HASH.to_owned(),
            1000,
            100,
            1001,
            101,
        )
    {
        return Ok(false);
    }
    sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM messages WHERE user_id = 1 AND message_id BETWEEN 100001 AND 101000) = 1000
          AND (SELECT count(*) FROM private_messages WHERE user_id = 1 AND target_id = 2 AND privatemessage_id BETWEEN 300001 AND 300050) = 50
          AND (SELECT count(*) FROM private_messages WHERE user_id = 2 AND target_id = 1 AND privatemessage_id BETWEEN 400001 AND 400050) = 50
          AND EXISTS (SELECT 1 FROM messages WHERE message_id = 200000 AND user_id = 2 AND message = 'unrelated public')
          AND EXISTS (SELECT 1 FROM private_messages WHERE privatemessage_id = 500000 AND user_id = 2 AND target_id = 3 AND message = 'unrelated private')",
    )
    .fetch_one(pool)
    .await
}

async fn explain_ms(pool: &PgPool, sql: &'static str, args: PgArguments) -> sqlx::Result<f64> {
    let plan = sqlx::query_scalar_with::<_, JsonValue, _>(sql, args)
        .fetch_one(pool)
        .await?;
    let ms = plan[0]["Execution Time"]
        .as_f64()
        .expect("EXPLAIN should report Execution Time in milliseconds");
    assert!(ms.is_finite() && ms >= 0.0);
    Ok(ms)
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
    let peer = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (Framed::new(server, FlpcCodec), Framed::new(peer, FlpcCodec))
}

async fn read_timed_response(
    peer: &mut Framed<TcpStream, FlpcCodec>,
    start: Instant,
) -> (FlpcUnit, f64) {
    let frame = peer.next().await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let response = frame
        .expect("handler should send a response")
        .expect("peer should decode the response");
    (response, elapsed_ms)
}

// Alias: Hashes the timing password
// Input: hash_password("timing-password", one-permit semaphore)
// Expected: The warm-up hash verifies the exact password; report 20 measured samples in milliseconds.
#[tokio::test]
#[ignore = "standard performance benchmark"]
async fn perf_hash() {
    let permit = Arc::new(Semaphore::new(1));
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        let hash_permit = Arc::clone(&permit);
        let start = Instant::now();
        let hashed_password = hash_password(TIMING_PASSWORD, hash_permit).await.unwrap();
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        if run == 0 {
            let parsed_hash = PasswordHash::new(&hashed_password).unwrap();
            Argon2::default()
                .verify_password(TIMING_PASSWORD.as_bytes(), &parsed_hash)
                .unwrap();
        } else {
            samples.push(elapsed_ms);
        }
    }
    report_samples("perf-hash", &mut samples);
}

// Alias: Verifies the timing password
// Input: verify_password("timing-password", precomputed hash)
// Expected: Ok(true) for each call; report 20 measured samples in milliseconds.
#[test]
#[ignore = "standard performance benchmark"]
fn perf_verify_password() {
    let hashed_password = Argon2::default()
        .hash_password(TIMING_PASSWORD.as_bytes())
        .unwrap()
        .to_string();
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        let start = Instant::now();
        let result = verify_password(TIMING_PASSWORD, &hashed_password);
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(result, Ok(true));
        if run != 0 {
            samples.push(elapsed_ms);
        }
    }
    report_samples("perf-verify-password", &mut samples);
}

// Alias: Claims an ID and inserts a new account (PostgreSQL Execution Time)
// Input: "bench-new", the precomputed timing-password fixture hash, and 1,000 existing accounts
// Expected: (false, Some(1001)); account/profile are inserted and ID 1001 is claimed.
postgres_test! {
    async fn perf_insert_sql(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_accounts(&pool, false).await?;
        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for run in 0..=SAMPLE_COUNT {
            sqlx::query("SAVEPOINT benchmark_sample").execute(&pool).await?;
            let raw = sqlx::query_as::<_, (bool, Option<i32>)>(
                include_str!("../../src/database/queries/claim_user_id.sql"),
            ).bind("bench-new").fetch_one(&pool).await?;
            assert_eq!(raw, (false, Some(1001)));
            let inserted: i32 = sqlx::query_scalar(include_str!("../../src/database/queries/insert_user.sql"))
                .bind(1001_i32).bind("bench-new").fetch_one(&pool).await?;
            assert_eq!(inserted, 1001);
            assert!(verify_password(TIMING_PASSWORD, &registered_hash(&pool).await?).is_err());
            let updated: i32 = sqlx::query_scalar(include_str!("../../src/database/queries/set_password_hash.sql"))
                .bind(1001_i32).bind(BENCHMARK_HASH).fetch_one(&pool).await?;
            assert_eq!(updated, 1001);
            assert!(sql_insert_state(&pool).await?);
            sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample").execute(&pool).await?;

            sqlx::query("SAVEPOINT benchmark_sample").execute(&pool).await?;
            let mut claim_args = PgArguments::default();
            claim_args.add("bench-new").map_err(sqlx::Error::Encode)?;
            let claim_ms = explain_ms(&pool, EXPLAIN_CLAIM, claim_args).await?;
            let mut args = PgArguments::default();
            args.add(1001_i32).map_err(sqlx::Error::Encode)?;
            args.add("bench-new").map_err(sqlx::Error::Encode)?;
            let insert_ms = explain_ms(&pool, EXPLAIN_INSERT, args).await?;
            let mut hash_args = PgArguments::default();
            hash_args.add(1001_i32).map_err(sqlx::Error::Encode)?;
            hash_args.add(BENCHMARK_HASH).map_err(sqlx::Error::Encode)?;
            let elapsed_ms = claim_ms + insert_ms + explain_ms(&pool, EXPLAIN_SET_HASH, hash_args).await?;
            assert!(sql_insert_state(&pool).await?);
            sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample").execute(&pool).await?;
            if run > 0 { samples.push(elapsed_ms); }
        }
        report_samples("perf-insert-sql", &mut samples);
        pool.close().await;
        Ok(())
    }
}

// Alias: Looks up the target account hash (PostgreSQL Execution Time)
// Input: Lookup SQL for "bench-0001" in the 1,000-account fixture
// Expected: Exact precomputed timing-password hash; report Execution Time in milliseconds.
postgres_test! {
    async fn perf_lookup_sql(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_accounts(&pool, false).await?;
        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for run in 0..=SAMPLE_COUNT {
            sqlx::query("SAVEPOINT benchmark_sample").execute(&pool).await?;
            let hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE username = $1")
                .bind("bench-0001").fetch_one(&pool).await?;
            assert_eq!(hash, BENCHMARK_HASH);
            sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample").execute(&pool).await?;

            sqlx::query("SAVEPOINT benchmark_sample").execute(&pool).await?;
            let mut args = PgArguments::default();
            args.add("bench-0001").map_err(sqlx::Error::Encode)?;
            let elapsed_ms = explain_ms(&pool, EXPLAIN_LOOKUP, args).await?;
            sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample").execute(&pool).await?;
            if run > 0 { samples.push(elapsed_ms); }
        }
        report_samples("perf-lookup-sql", &mut samples);
        pool.close().await;
        Ok(())
    }
}

async fn measure_remove_sql(
    pool: &PgPool,
    with_history: bool,
    correct_hash: bool,
    case_id: &'static str,
) -> sqlx::Result<()> {
    let hash = if correct_hash {
        BENCHMARK_HASH
    } else {
        "wrong-hash"
    };
    let expected = if correct_hash {
        (Some(true), Some(1))
    } else {
        (Some(false), None)
    };
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        sqlx::query("SAVEPOINT benchmark_sample")
            .execute(pool)
            .await?;
        let result = sqlx::query_as::<_, (Option<bool>, Option<i32>)>(include_str!(
            "../../src/database/queries/removeself_user.sql"
        ))
        .bind("bench-0001")
        .bind(hash)
        .fetch_one(pool)
        .await?;
        assert_eq!(result, expected);
        assert!(removal_state(pool, correct_hash, with_history).await?);
        sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample")
            .execute(pool)
            .await?;

        sqlx::query("SAVEPOINT benchmark_sample")
            .execute(pool)
            .await?;
        let mut args = PgArguments::default();
        args.add("bench-0001").map_err(sqlx::Error::Encode)?;
        args.add(hash).map_err(sqlx::Error::Encode)?;
        let elapsed_ms = explain_ms(pool, EXPLAIN_REMOVESELF, args).await?;
        assert!(removal_state(pool, correct_hash, with_history).await?);
        sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample")
            .execute(pool)
            .await?;
        if run > 0 {
            samples.push(elapsed_ms);
        }
    }
    report_samples(case_id, &mut samples);
    Ok(())
}

// Alias: Removes an account without message history (PostgreSQL Execution Time)
// Input: removeself_user.sql for "bench-0001" with the timing-password fixture hash
// Expected: (Some(true), Some(1)); account/profile are removed and ID 1 stays claimed.
postgres_test! {
    async fn perf_remove_empty_sql(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_accounts(&pool, false).await?;
        measure_remove_sql(&pool, false, true, "perf-remove-empty-sql").await?;
        pool.close().await;
        Ok(())
    }
}

// Alias: Registers a new account through the TCP handler
// Input: handle_register("token", "bench-new", "timing-password", limits 32/64); handler call through decoded TCP reply
// Expected: Peer decodes ServerRegister code 0; account/profile and ID 1001 state are correct.
postgres_test! {
    async fn perf_register_tcp(pool: PgPool) -> sqlx::Result<()> {
        seed_accounts(&pool, false).await?;
        let (user_tx, user_rx) = mpsc::channel(1);
        let worker = tokio::spawn(UserHandler::new(pool.clone(), user_rx).run(Arc::new(Semaphore::new(1))));
        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for run in 0..=SAMPLE_COUNT {
            let (server, mut peer) = loopback().await;
            let (sink, _) = server.split();
            let request = Register::new("token".into(), "bench-new".into(), TIMING_PASSWORD.into());
            let sender = user_tx.clone();
            let ((), (response, elapsed_ms)) = timeout(TCP_TIMEOUT, async {
                let start = Instant::now();
                tokio::join!(
                    handle_register(request, sender, sink, "token", 32, 64),
                    read_timed_response(&mut peer, start),
                )
            })
            .await
            .expect("TCP handler and peer should finish");
            assert!(matches!(response, FlpcUnit::Server(ServerUnit::ServerRegister(reply)) if *reply.outcome() == 0));
            let registered_hash = registered_hash(&pool).await?;
            if run == 0 {
                assert_eq!(verify_password(TIMING_PASSWORD, &registered_hash), Ok(true));
            }
            sqlx::query(
                "WITH removed AS (DELETE FROM users WHERE user_id = 1001 RETURNING user_id)
                 INSERT INTO available_user_ids (user_id) SELECT user_id FROM removed",
            ).execute(&pool).await?;
            if run > 0 { samples.push(elapsed_ms); }
        }
        drop(user_tx);
        timeout(TCP_TIMEOUT, worker)
            .await
            .expect("user worker should exit before timeout")
            .expect("user worker should finish");
        report_samples("perf-register-tcp", &mut samples);
        pool.close().await;
        Ok(())
    }
}

async fn measure_remove_tcp(
    pool: PgPool,
    with_history: bool,
    case_id: &'static str,
) -> sqlx::Result<()> {
    let pool = transaction_pool(&pool).await?;
    seed_accounts(&pool, with_history).await?;
    let (user_tx, user_rx) = mpsc::channel(1);
    let worker =
        tokio::spawn(UserHandler::new(pool.clone(), user_rx).run(Arc::new(Semaphore::new(1))));
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        let (server, mut peer) = loopback().await;
        let (sink, _) = server.split();
        let request = RemoveSelf::new("bench-0001".into(), TIMING_PASSWORD.into());
        let sender = user_tx.clone();
        sqlx::query("SAVEPOINT benchmark_sample")
            .execute(&pool)
            .await?;
        let ((), (response, elapsed_ms)) = timeout(TCP_TIMEOUT, async {
            let start = Instant::now();
            tokio::join!(
                handle_removeself(request, sender, sink, 32, 64),
                read_timed_response(&mut peer, start),
            )
        })
        .await
        .expect("TCP handler and peer should finish");
        assert!(
            matches!(response, FlpcUnit::Server(ServerUnit::ServerRemoveSelf(reply)) if *reply.result() == 0)
        );
        assert!(removal_state(&pool, true, with_history).await?);
        sqlx::raw_sql("ROLLBACK TO SAVEPOINT benchmark_sample; RELEASE SAVEPOINT benchmark_sample")
            .execute(&pool)
            .await?;
        if run > 0 {
            samples.push(elapsed_ms);
        }
    }
    drop(user_tx);
    timeout(TCP_TIMEOUT, worker)
        .await
        .expect("user worker should exit before timeout")
        .expect("user worker should finish");
    report_samples(case_id, &mut samples);
    pool.close().await;
    Ok(())
}

// Alias: Removes an account without message history through the TCP handler
// Input: handle_removeself("bench-0001", "timing-password", limits 32/64); handler call through decoded TCP reply
// Expected: Peer decodes ServerRemoveSelf code 0; account/profile are removed and ID 1 stays claimed.
postgres_test! {
    async fn perf_remove_empty_tcp(pool: PgPool) -> sqlx::Result<()> {
        measure_remove_tcp(pool, false, "perf-remove-empty-tcp").await
    }
}

// Alias: Removes an account with message history (PostgreSQL Execution Time)
// Input: removeself_user.sql for "bench-0001" with the timing-password fixture hash and full history
// Expected: (Some(true), Some(1)); target messages are removed and unrelated account-2/account-3 rows survive.
postgres_test! {
    async fn perf_remove_history_sql(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_accounts(&pool, true).await?;
        measure_remove_sql(&pool, true, true, "perf-remove-history-sql").await?;
        pool.close().await;
        Ok(())
    }
}

// Alias: Removes an account with message history through the TCP handler
// Input: handle_removeself("bench-0001", "timing-password", limits 32/64); handler call through decoded TCP reply
// Expected: Peer decodes code 0; target public/private rows disappear and both unrelated rows survive.
postgres_test! {
    async fn perf_remove_history_tcp(pool: PgPool) -> sqlx::Result<()> {
        measure_remove_tcp(pool, true, "perf-remove-history-tcp").await
    }
}

// Alias: Preserves an account after wrong credentials (PostgreSQL Execution Time)
// Input: removeself_user.sql for "bench-0001" with "wrong-hash" and full history
// Expected: (Some(false), None); account/profile/hash, claimed ID, and every target/unrelated message remain.
postgres_test! {
    async fn perf_remove_wrong_sql(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_accounts(&pool, true).await?;
        measure_remove_sql(&pool, true, false, "perf-remove-wrong-sql").await?;
        pool.close().await;
        Ok(())
    }
}

// Alias: Verifies credentials through UserHandler
// Input: verify_credentials("bench-0001", "timing-password") with the precomputed fixture hash
// Expected: Success with the exact hash for the warm-up and all measured calls; report 20 samples in milliseconds.
postgres_test! {
    async fn perf_verify_credentials(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        seed_accounts(&pool, false).await?;
        let worker = UserHandler::new(pool.clone(), mpsc::channel(1).1);
        let permit = Arc::new(Semaphore::new(1));
        let mut samples = Vec::with_capacity(SAMPLE_COUNT);
        for run in 0..=SAMPLE_COUNT {
            let start = Instant::now();
            let result = worker.verify_credentials("bench-0001", TIMING_PASSWORD, Arc::clone(&permit)).await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert!(matches!(result, Ok(VerifyResult::Success(hash)) if hash == BENCHMARK_HASH));
            if run > 0 { samples.push(elapsed_ms); }
        }
        report_samples("perf-verify-credentials", &mut samples);
        pool.close().await;
        Ok(())
    }
}
