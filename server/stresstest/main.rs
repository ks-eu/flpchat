use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register, RemoveSelf, ServerUnit},
};
use futures_util::StreamExt;
use sqlx::{
    Arguments, PgPool,
    postgres::{PgArguments, PgPoolOptions},
    types::JsonValue,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_util::codec::Framed;

#[path = "../src/database/mod.rs"]
mod database;
#[path = "../src/hashing.rs"]
mod hashing;
#[path = "../src/users/mod.rs"]
mod users;

use database::users::UserHandler;
use users::{registration::handle_register, remove::handle_removeself};

const SAMPLE_COUNT: usize = 20;
const PASSWORD: &str = "timing-password";
const TOKEN: &str = "token";
const FIXTURE_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$bGlnaHRjaGF0LWJlbmNobWFyaw$NG0zwI7kM/Cq+dLLB3HMbPwk1I8NW1iSDPpYPr/ImjU";
const INSERT_SQL: &str = include_str!("../src/database/queries/insert_user.sql");
const REMOVE_SQL: &str = include_str!("../src/database/queries/removeself_user.sql");
const EXPLAIN_INSERT: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../src/database/queries/insert_user.sql")
);
const EXPLAIN_REMOVE: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../src/database/queries/removeself_user.sql")
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() != ["--stress"] {
        eprintln!("Usage: lightchat-regremove-stress --stress");
        std::process::exit(2);
    }
    let database_url = std::env::var("LIGHTCHAT_STRESS_DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    sqlx::raw_sql(include_str!("fixtures.sql"))
        .execute(&pool)
        .await?;
    sqlx::query("BEGIN").execute(&pool).await?;

    run_sql_insert(&pool).await?;
    run_tcp_insert(&pool).await?;
    run_sql_remove(&pool).await?;
    run_tcp_remove(&pool).await?;
    pool.close().await;
    Ok(())
}

async fn savepoint(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("SAVEPOINT stress_sample").execute(pool).await?;
    Ok(())
}

async fn rollback_sample(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::raw_sql("ROLLBACK TO SAVEPOINT stress_sample; RELEASE SAVEPOINT stress_sample")
        .execute(pool)
        .await?;
    Ok(())
}

async fn plan_ms(pool: &PgPool, sql: &'static str, args: PgArguments) -> sqlx::Result<f64> {
    let plan = sqlx::query_scalar_with::<_, JsonValue, _>(sql, args)
        .fetch_one(pool)
        .await?;
    let ms = plan[0]["Execution Time"]
        .as_f64()
        .expect("PostgreSQL plan must report Execution Time in milliseconds");
    assert!(ms.is_finite() && ms >= 0.0);
    Ok(ms)
}

fn report_samples(id: &str, samples: &mut [f64]) {
    samples.sort_unstable_by(f64::total_cmp);
    let middle = samples.len() / 2;
    let median = (samples[middle - 1] + samples[middle]) / 2.0;
    println!(
        "{id}: {} samples, min {:.3} ms, median {:.3} ms, max {:.3} ms",
        samples.len(),
        samples[0],
        median,
        samples[samples.len() - 1]
    );
}

async fn insert_state(pool: &PgPool) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT password_hash FROM users JOIN user_information USING (user_id, username)
         WHERE user_id = 50001 AND username = 'bench-new'
           AND NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 50001)
           AND EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 50002)",
    )
    .fetch_one(pool)
    .await
}

async fn removed_state(pool: &PgPool) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS (SELECT 1 FROM users WHERE user_id = 1)
          AND NOT EXISTS (SELECT 1 FROM user_information WHERE user_id = 1)
          AND NOT EXISTS (SELECT 1 FROM available_user_ids WHERE user_id = 1)
          AND NOT EXISTS (SELECT 1 FROM messages WHERE user_id = 1)
          AND NOT EXISTS (SELECT 1 FROM private_messages WHERE 1 IN (user_id, target_id))
          AND (SELECT count(*) FROM messages) = 990000
          AND (SELECT count(*) FROM private_messages) = 490000
          AND EXISTS (SELECT 1 FROM messages WHERE message_id = 10001 AND user_id = 2)
          AND EXISTS (SELECT 1 FROM private_messages WHERE privatemessage_id = 10001 AND user_id = 2 AND target_id = 3)",
    )
    .fetch_one(pool)
    .await
}

async fn run_sql_insert(pool: &PgPool) -> sqlx::Result<()> {
    savepoint(pool).await?;
    let result = sqlx::query_as::<_, (bool, Option<i32>)>(INSERT_SQL)
        .bind("bench-new")
        .bind(FIXTURE_HASH)
        .fetch_one(pool)
        .await?;
    assert_eq!(result, (false, Some(50001)));
    assert_eq!(insert_state(pool).await?, FIXTURE_HASH);
    rollback_sample(pool).await?;

    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        savepoint(pool).await?;
        let mut args = PgArguments::default();
        args.add("bench-new").map_err(sqlx::Error::Encode)?;
        args.add(FIXTURE_HASH).map_err(sqlx::Error::Encode)?;
        let elapsed_ms = plan_ms(pool, EXPLAIN_INSERT, args).await?;
        assert_eq!(insert_state(pool).await?, FIXTURE_HASH);
        rollback_sample(pool).await?;
        if run > 0 {
            samples.push(elapsed_ms);
        }
    }
    report_samples("stress-reg-sql", &mut samples);
    Ok(())
}

async fn loopback() -> (Framed<TcpStream, FlpcCodec>, Framed<TcpStream, FlpcCodec>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (Framed::new(server, FlpcCodec), Framed::new(peer, FlpcCodec))
}

async fn read_timed(peer: &mut Framed<TcpStream, FlpcCodec>, start: Instant) -> (FlpcUnit, f64) {
    let frame = timeout(Duration::from_secs(30), peer.next())
        .await
        .expect("handler response should arrive");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let response = frame
        .expect("handler should send a response")
        .expect("peer should decode the response");
    (response, elapsed_ms)
}

async fn run_tcp_insert(pool: &PgPool) -> Result<(), Box<dyn Error>> {
    let (user_tx, user_rx) = mpsc::channel(1);
    let permit = Arc::new(Semaphore::new(1));
    let worker = tokio::spawn(UserHandler::new(pool.clone(), user_rx, Arc::clone(&permit)).run());
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        let (server, mut peer) = loopback().await;
        let (sink, _) = server.split();
        let request = Register::new(TOKEN.into(), "bench-new".into(), PASSWORD.into());
        let sender = user_tx.clone();
        let hash_permit = Arc::clone(&permit);
        savepoint(pool).await?;
        let start = Instant::now();
        let ((), (response, elapsed_ms)) = tokio::join!(
            handle_register(request, sender, hash_permit, sink, TOKEN, 32, 64),
            read_timed(&mut peer, start),
        );
        assert!(
            matches!(response, FlpcUnit::Server(ServerUnit::ServerRegister(reply)) if *reply.outcome() == 0)
        );
        let hash = insert_state(pool).await?;
        assert_eq!(hashing::verify_password(PASSWORD, &hash), Ok(true));
        rollback_sample(pool).await?;
        if run > 0 {
            samples.push(elapsed_ms);
        }
    }
    drop(user_tx);
    worker.await?;
    report_samples("stress-reg-tcp", &mut samples);
    Ok(())
}

async fn run_sql_remove(pool: &PgPool) -> sqlx::Result<()> {
    savepoint(pool).await?;
    let result = sqlx::query_as::<_, (Option<bool>, Option<i32>)>(REMOVE_SQL)
        .bind("bench-00001")
        .bind(FIXTURE_HASH)
        .fetch_one(pool)
        .await?;
    assert_eq!(result, (Some(true), Some(1)));
    assert!(removed_state(pool).await?);
    rollback_sample(pool).await?;

    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        savepoint(pool).await?;
        let mut args = PgArguments::default();
        args.add("bench-00001").map_err(sqlx::Error::Encode)?;
        args.add(FIXTURE_HASH).map_err(sqlx::Error::Encode)?;
        let elapsed_ms = plan_ms(pool, EXPLAIN_REMOVE, args).await?;
        assert!(removed_state(pool).await?);
        rollback_sample(pool).await?;
        if run > 0 {
            samples.push(elapsed_ms);
        }
    }
    report_samples("stress-remove-sql", &mut samples);
    Ok(())
}

async fn run_tcp_remove(pool: &PgPool) -> Result<(), Box<dyn Error>> {
    let (user_tx, user_rx) = mpsc::channel(1);
    let worker =
        tokio::spawn(UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1))).run());
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for run in 0..=SAMPLE_COUNT {
        let (server, mut peer) = loopback().await;
        let (sink, _) = server.split();
        let request = RemoveSelf::new("bench-00001".into(), PASSWORD.into());
        let sender = user_tx.clone();
        savepoint(pool).await?;
        let start = Instant::now();
        let ((), (response, elapsed_ms)) = tokio::join!(
            handle_removeself(request, sender, sink, 32, 64),
            read_timed(&mut peer, start),
        );
        assert!(
            matches!(response, FlpcUnit::Server(ServerUnit::ServerRemoveSelf(reply)) if *reply.result() == 0)
        );
        assert!(removed_state(pool).await?);
        rollback_sample(pool).await?;
        if run > 0 {
            samples.push(elapsed_ms);
        }
    }
    drop(user_tx);
    worker.await?;
    report_samples("stress-remove-tcp", &mut samples);
    Ok(())
}
