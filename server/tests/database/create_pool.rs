use super::{create_pool, serial_tests};

#[tokio::test]
#[ignore = "requires PostgreSQL via DATABASE_URL"]
// Alias: Connects to the test database
// Input: Runner-provided DATABASE_URL and its ready PostgreSQL instance
// Expected: Ok(pool) and SELECT 1 returns 1. Close the pool.
async fn create_pool_connects_to_postgres() -> sqlx::Result<()> {
    let _guard = serial_tests::lock();
    let pool = create_pool().await?;
    let result = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await;
    pool.close().await;
    let result = result?;
    assert_eq!(result, 1);

    Ok(())
}
