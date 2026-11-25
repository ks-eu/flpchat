use std::sync::Arc;

use argon2::{
    Argon2,
    password_hash::{Error, PasswordVerifier, phc::PasswordHash},
};
use tokio::sync::Semaphore;

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
