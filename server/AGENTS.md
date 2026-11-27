Agents must run every test suite through `./tests/test.sh` only.

PostgreSQL tests must run sequentially. Use `postgres_test!` for tests with a
migrated `PgPool`. The macro holds the shared lock through database setup, the
test body, and cleanup. For tests that manage their own pool, acquire the same
lock before connecting and hold it through cleanup, as in
`tests/database/create_pool.rs`.

```rust
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
```
