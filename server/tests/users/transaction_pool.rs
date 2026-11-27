use sqlx::{PgPool, postgres::PgPoolOptions};

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
