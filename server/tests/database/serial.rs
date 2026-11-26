use std::sync::{Mutex, MutexGuard, PoisonError};

static POSTGRES_TEST_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn lock() -> MutexGuard<'static, ()> {
    POSTGRES_TEST_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

macro_rules! postgres_test {
    (async fn $name:ident($pool:ident: $pool_type:ident $(,)?) -> sqlx::Result<()> $body:block) => {
        #[test]
        #[ignore = "requires PostgreSQL via DATABASE_URL"]
        #[allow(unnameable_test_items)]
        fn $name() -> sqlx::Result<()> {
            #[sqlx::test(migrations = "./migrations")]
            async fn $name($pool: $pool_type) -> sqlx::Result<()> $body

            // Hold the lock through SQLx database setup, the test, and cleanup.
            let _guard = $crate::database::serial_tests::lock();
            $name()
        }
    };
}

pub(crate) use postgres_test;
