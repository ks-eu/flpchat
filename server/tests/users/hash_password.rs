use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use argon2::{
    Argon2,
    password_hash::{Error, PasswordVerifier, phc::PasswordHash},
};
use futures_util::poll;
use tokio::sync::{AcquireError, Semaphore};
use tokio::time::timeout;

use super::{Register, hash_password};

#[tokio::test]
async fn hash_password_verifies_exact_input_and_uses_fresh_salts() {
    let permit = Arc::new(Semaphore::new(1));
    let argon2 = Argon2::default();
    let hash_and_verify = async |password: &str| {
        let request = Register::new("token".into(), "user".into(), password.into());
        let encoded = hash_password(&request, Arc::clone(&permit)).await.unwrap();
        let hash = PasswordHash::new(&encoded).unwrap();
        argon2.verify_password(password.as_bytes(), &hash).unwrap();
        hash
    };

    let first = hash_and_verify("ordinary-password").await;
    assert!(matches!(
        argon2.verify_password(b"wrong-password", &first),
        Err(Error::PasswordInvalid)
    ));

    let second = hash_and_verify("ordinary-password").await;
    assert_ne!(first.salt.unwrap(), second.salt.unwrap());

    hash_and_verify("").await;
    hash_and_verify("café🔑").await;
}

#[tokio::test]
async fn hash_password_waits_for_and_releases_a_permit() {
    let semaphore = Arc::new(Semaphore::new(1));
    let held_permit = semaphore.try_acquire().unwrap();
    let request = Register::new("token".into(), "user".into(), "password".into());
    let mut hashing = pin!(hash_password(&request, Arc::clone(&semaphore)));

    assert!(poll!(hashing.as_mut()).is_pending());
    drop(held_permit);

    timeout(Duration::from_secs(5), hashing)
        .await
        .expect("hashing should resume when the permit is released")
        .expect("hashing should succeed");
    assert_eq!(semaphore.available_permits(), 1);
}

#[tokio::test]
async fn hash_password_fails_when_the_semaphore_closes() {
    let semaphore = Arc::new(Semaphore::new(1));
    let held_permit = semaphore.try_acquire().unwrap();
    let request = Register::new("token".into(), "user".into(), "password".into());
    let mut waiting = pin!(hash_password(&request, Arc::clone(&semaphore)));

    assert!(poll!(waiting.as_mut()).is_pending());
    semaphore.close();

    timeout(Duration::from_secs(5), async {
        let error = waiting.await.expect_err("waiting hash should fail");
        assert!(error.is::<AcquireError>());

        drop(held_permit);
        let error = hash_password(&request, Arc::clone(&semaphore))
            .await
            .expect_err("new hash should fail");
        assert!(error.is::<AcquireError>());
    })
    .await
    .expect("both hash requests should resolve");
}
