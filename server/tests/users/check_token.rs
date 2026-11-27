use super::{Register, check_token};

// Alias: Accepts exact match
// Input: request token "secret", server token "secret"
// Expected: false.
#[test]
fn token_exact() {
    let request = Register::new("secret".into(), "user".into(), "secret".into());
    assert!(!check_token(&request, "secret"));
}

// Alias: Rejects different content
// Input: request token "wrong", server token "secret"
// Expected: true.
#[test]
fn token_different() {
    let request = Register::new("wrong".into(), "user".into(), "secret".into());
    assert!(check_token(&request, "secret"));
}

// Alias: Rejects case difference
// Input: request token "Secret", server token "secret"
// Expected: true.
#[test]
fn token_case() {
    let request = Register::new("Secret".into(), "user".into(), "secret".into());
    assert!(check_token(&request, "secret"));
}

// Alias: Rejects extra whitespace
// Input: request token "secret ", server token "secret"
// Expected: true.
#[test]
fn token_space() {
    let request = Register::new("secret ".into(), "user".into(), "secret".into());
    assert!(check_token(&request, "secret"));
}

// Alias: Rejects empty request token
// Input: request token "", server token "secret"
// Expected: true.
#[test]
fn token_empty_request() {
    let request = Register::new("".into(), "user".into(), "secret".into());
    assert!(check_token(&request, "secret"));
}

// Alias: Rejects a nonmatching configured empty token
// Input: request token "secret", server token ""
// Expected: true.
#[test]
fn token_empty_server() {
    let request = Register::new("secret".into(), "user".into(), "secret".into());
    assert!(check_token(&request, ""));
}

// Alias: Accepts matching empty tokens
// Input: request token "", server token ""
// Expected: false.
#[test]
fn token_both_empty() {
    let request = Register::new("".into(), "user".into(), "secret".into());
    assert!(!check_token(&request, ""));
}
