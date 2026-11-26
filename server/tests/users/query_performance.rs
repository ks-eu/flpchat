use sqlx::{
    Acquire, Arguments, PgPool, Postgres, Transaction, postgres::PgArguments, types::JsonValue,
};

use crate::database::serial_tests::postgres_test;

const SAMPLE_COUNT: usize = 20;
const EXPLAIN_INSERT: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/users/queries/insert_user.sql"),
);
const EXPLAIN_REMOVE: &str = concat!(
    "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) ",
    include_str!("../../src/users/queries/remove_user.sql"),
);

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
            let changed_rows = sqlx::query_scalar::<_, bool>(verify_sql)
                .fetch_one(&mut *sample)
                .await?;
            assert!(
                changed_rows,
                "{name} warm-up should change the intended rows"
            );
        } else {
            samples.push(elapsed_ms);
        }
        sample.rollback().await?;
    }

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
    Ok(())
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
