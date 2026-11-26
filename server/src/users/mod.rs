use std::error::Error;
use std::sync::Arc;

use argon2::{Argon2, PasswordHasher};
use flpc::unit::Register;
use tokio::sync::Semaphore;

pub(crate) mod registration;
pub(crate) mod remove;

/// Hashes the request's password while holding a semaphore permit.
pub(crate) async fn hash_password(
    request: &Register,
    hash_permit: Arc<Semaphore>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let permit = hash_permit.acquire_owned().await?;
    let password = request.password().clone();
    let hash = tokio::task::spawn_blocking(move || {
        // Keep the permit until hashing ends even if the caller is canceled.
        let _permit = permit;
        Argon2::default().hash_password(password.as_bytes())
    })
    .await??;

    Ok(hash.to_string())
}

#[cfg(test)]
#[path = "../../tests/users/hash_password.rs"]
mod hash_password_tests;
