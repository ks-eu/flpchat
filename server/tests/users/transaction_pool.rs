use sqlx::{PgExecutor, PgPool, postgres::PgPoolOptions};

pub(super) async fn transaction_pool(pool: &PgPool) -> sqlx::Result<PgPool> {
    // Pool-based APIs share one PostgreSQL session and its transaction.
    PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("BEGIN").execute(connection).await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await
}

pub(super) async fn track_completed_deletes<'e>(
    executor: impl PgExecutor<'e>,
    table: &'static str,
) -> sqlx::Result<()> {
    assert!(matches!(table, "available_user_ids" | "users"));
    // A temporary sequence records completed statements across savepoint rollback.
    let sql = format!(
        "CREATE TEMP SEQUENCE completed_deletes;
         CREATE FUNCTION pg_temp.record_completed_delete() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM nextval('pg_temp.completed_deletes');
             RETURN NULL;
         END;
         $$;
         CREATE TRIGGER record_completed_delete
         AFTER DELETE ON {table} FOR EACH STATEMENT
         EXECUTE FUNCTION pg_temp.record_completed_delete();",
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(executor)
        .await?;
    Ok(())
}

pub(super) async fn delete_completed<'e>(executor: impl PgExecutor<'e>) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT is_called FROM pg_temp.completed_deletes")
        .fetch_one(executor)
        .await
}
