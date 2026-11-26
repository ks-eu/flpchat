use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};

use super::verify_password;

// Alias: Verifies exact plaintext
// Input: password "café🔑", hash H("café🔑")
// Expected: Ok(true)
#[test]
fn password_match() {
    let hashed_password = Argon2::default()
        .hash_password("café🔑".as_bytes())
        .unwrap()
        .to_string();

    assert_eq!(verify_password("café🔑", &hashed_password), Ok(true));
}

// Alias: Rejects different plaintext
// Input: password "wrong", hash H("café🔑")
// Expected: Ok(false)
#[test]
fn password_wrong() {
    let hashed_password = Argon2::default()
        .hash_password("café🔑".as_bytes())
        .unwrap()
        .to_string();

    assert_eq!(verify_password("wrong", &hashed_password), Ok(false));
}

// Alias: Rejects malformed hash encoding
// Input: password "café🔑", hash "invalid-phc"
// Expected: parsing error
#[test]
fn password_malformed() {
    let expected_error =
        argon2::password_hash::Error::from(PasswordHash::new("invalid-phc").unwrap_err());
    assert_eq!(
        verify_password("café🔑", "invalid-phc"),
        Err(expected_error)
    );
}

// Alias: Rejects an unsupported hash algorithm
// Input: password "café🔑", hash H("café🔑") with "argon2id" replaced by "scrypt"
// Expected: verification error
#[test]
fn password_unsupported() {
    let hashed_password = Argon2::default()
        .hash_password("café🔑".as_bytes())
        .unwrap()
        .to_string()
        .replace("$argon2id$", "$scrypt$");
    let parsed_hash = PasswordHash::new(&hashed_password).unwrap();
    let expected_error = Argon2::default()
        .verify_password("café🔑".as_bytes(), &parsed_hash)
        .unwrap_err();

    assert_eq!(
        verify_password("café🔑", &hashed_password),
        Err(expected_error)
    );
}
