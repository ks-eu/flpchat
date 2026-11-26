use std::error::Error;
use std::sync::Arc;

use argon2::{Argon2, PasswordHasher};
use tokio::sync::Semaphore;

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
