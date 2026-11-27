use std::{pin::pin, sync::Arc, time::Duration};

use argon2::{Argon2, PasswordHash, PasswordVerifier, password_hash::Error};
use futures_util::poll;
use tokio::{
    sync::{AcquireError, Semaphore},
    time::timeout,
};

use crate::database::hash::hash_password;

// Alias: Hashes an ASCII password
// Input: "ordinary-password"
// Expected: Parseable Argon2 hash that verifies the exact password.
#[tokio::test]
async fn hash_ascii() {
    let hash = hash_password("ordinary-password", Arc::new(Semaphore::new(1)))
        .await
        .unwrap();
    let parsed = PasswordHash::new(&hash).expect("hash should be parseable");
    Argon2::default()
        .verify_password(b"ordinary-password", &parsed)
        .expect("hash should verify the exact password");
}

// Alias: Hashes a multibyte password
// Input: "café🔑"
// Expected: Parseable Argon2 hash that verifies the exact password.
#[tokio::test]
async fn hash_unicode() {
    let hash = hash_password("café🔑", Arc::new(Semaphore::new(1)))
        .await
        .unwrap();
    let parsed = PasswordHash::new(&hash).expect("hash should be parseable");
    Argon2::default()
        .verify_password("café🔑".as_bytes(), &parsed)
        .expect("hash should verify the exact password");
}

// Alias: Hashes empty plaintext at the helper boundary
// Input: ""
// Expected: Parseable Argon2 hash that verifies empty plaintext.
#[tokio::test]
async fn hash_empty() {
    let hash = hash_password("", Arc::new(Semaphore::new(1)))
        .await
        .unwrap();
    let parsed = PasswordHash::new(&hash).expect("hash should be parseable");
    Argon2::default()
        .verify_password(b"", &parsed)
        .expect("hash should verify empty plaintext");
}

// Alias: Rejects a different password on verification
// Input: Hash "ordinary-password" and verify with "wrong-password"
// Expected: Argon2 reports PasswordInvalid.
#[tokio::test]
async fn hash_wrong() {
    let hash = hash_password("ordinary-password", Arc::new(Semaphore::new(1)))
        .await
        .unwrap();
    let parsed = PasswordHash::new(&hash).expect("hash should be parseable");
    assert!(matches!(
        Argon2::default().verify_password(b"wrong-password", &parsed),
        Err(Error::PasswordInvalid)
    ));
}

// Alias: Uses a fresh salt
// Input: Hash "ordinary-password" twice
// Expected: Both verify and their encoded salts differ.
#[tokio::test]
async fn hash_salt() {
    let permit = Arc::new(Semaphore::new(1));
    let first = hash_password("ordinary-password", Arc::clone(&permit))
        .await
        .unwrap();
    let second = hash_password("ordinary-password", permit).await.unwrap();
    let first = PasswordHash::new(&first).expect("first hash should be parseable");
    let second = PasswordHash::new(&second).expect("second hash should be parseable");
    let argon2 = Argon2::default();
    argon2
        .verify_password(b"ordinary-password", &first)
        .expect("first hash should verify");
    argon2
        .verify_password(b"ordinary-password", &second)
        .expect("second hash should verify");
    assert_ne!(first.salt.unwrap(), second.salt.unwrap());
}

// Alias: Waits for and returns the permit
// Input: Hold the only permit, poll hash_password("ordinary-password", …), then release the permit
// Expected: The future is pending while held, succeeds after release and leaves one available permit.
#[tokio::test]
async fn hash_wait() {
    let semaphore = Arc::new(Semaphore::new(1));
    let held_permit = semaphore.clone().try_acquire_owned().unwrap();
    let mut hashing = pin!(hash_password("ordinary-password", Arc::clone(&semaphore)));
    assert!(poll!(hashing.as_mut()).is_pending());
    drop(held_permit);

    let hash = timeout(Duration::from_secs(5), hashing)
        .await
        .expect("hash should resume after permit release")
        .expect("hash should succeed");
    let parsed = PasswordHash::new(&hash).expect("hash should be parseable");
    Argon2::default()
        .verify_password(b"ordinary-password", &parsed)
        .expect("hash should verify the exact password");
    assert_eq!(semaphore.available_permits(), 1);
}

// Alias: Rejects a waiter when the semaphore closes
// Input: Hold the only permit and poll hash_password("ordinary-password", …) before closing the semaphore
// Expected: AcquireError.
#[tokio::test]
async fn hash_close_waiter() {
    let semaphore = Arc::new(Semaphore::new(1));
    let held_permit = semaphore.clone().try_acquire_owned().unwrap();
    let mut waiting = pin!(hash_password("ordinary-password", Arc::clone(&semaphore)));
    assert!(poll!(waiting.as_mut()).is_pending());
    semaphore.close();

    let error = timeout(Duration::from_secs(5), waiting)
        .await
        .expect("closed waiter should resolve")
        .expect_err("closing the semaphore should reject the waiter");
    assert!(error.is::<AcquireError>());
    drop(held_permit);
}

// Alias: Rejects a new call on a closed semaphore
// Input: Close the semaphore, then call hash_password("ordinary-password", …)
// Expected: AcquireError.
#[tokio::test]
async fn hash_close_new() {
    let semaphore = Arc::new(Semaphore::new(1));
    semaphore.close();
    let error = hash_password("ordinary-password", semaphore)
        .await
        .expect_err("closed semaphore should reject a new hash");
    assert!(error.is::<AcquireError>());
}
