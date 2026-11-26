use super::{create_pool, serial_tests};

#[tokio::test]
#[ignore = "requires PostgreSQL via DATABASE_URL"]
async fn create_pool_connects_to_postgres() -> sqlx::Result<()> {
    let _guard = serial_tests::lock();
    let pool = create_pool().await?;
    let result = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await?;
    assert_eq!(result, 1);

    pool.close().await;
    Ok(())
}
