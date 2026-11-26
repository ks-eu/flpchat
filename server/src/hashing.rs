use std::error::Error;
use std::sync::Arc;

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use tokio::sync::Semaphore;

pub(crate) fn verify_password(
    password: &str,
    hashed_password: &str,
) -> Result<bool, argon2::password_hash::Error> {
    let parsed_hash = PasswordHash::new(hashed_password)?;
    match Argon2::default().verify_password(password.as_bytes(), &parsed_hash) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::PasswordInvalid) => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[path = "../tests/users/verify_password.rs"]
mod verify_password_tests;

/// Hashes a password while holding a semaphore permit.
pub(crate) async fn hash_password(
    password: &str,
    hash_permit: Arc<Semaphore>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let permit = hash_permit.acquire_owned().await?;
    let password = password.to_owned();
    let hash = tokio::task::spawn_blocking(move || {
        // Keep the permit until hashing ends even if the caller is canceled.
        let _permit = permit;
        Argon2::default().hash_password(password.as_bytes())
    })
    .await??;

    Ok(hash.to_string())
}
