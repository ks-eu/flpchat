use super::invalid_credential_lengths;

// Alias: Accepts one byte each
// Input: username "a", password "b", limits 4 and 6
// Expected: false
#[test]
fn length_min() {
    assert!(!invalid_credential_lengths("a", "b", 4, 6));
}

// Alias: Accepts exact ASCII limits
// Input: username "user", password "secret", limits 4 and 6
// Expected: false
#[test]
fn length_exact() {
    assert!(!invalid_credential_lengths("user", "secret", 4, 6));
}

// Alias: Rejects empty username
// Input: username "", password "secret", limits 4 and 6
// Expected: true
#[test]
fn length_empty_user() {
    assert!(invalid_credential_lengths("", "secret", 4, 6));
}

// Alias: Rejects empty password
// Input: username "user", password "", limits 4 and 6
// Expected: true
#[test]
fn length_empty_password() {
    assert!(invalid_credential_lengths("user", "", 4, 6));
}

// Alias: Rejects two empty fields
// Input: username "", password "", limits 4 and 6
// Expected: true
#[test]
fn length_both_empty() {
    assert!(invalid_credential_lengths("", "", 4, 6));
}

// Alias: Rejects username one byte over
// Input: username "users", password "secret", limits 4 and 6
// Expected: true
#[test]
fn length_long_user() {
    assert!(invalid_credential_lengths("users", "secret", 4, 6));
}

// Alias: Rejects password one byte over
// Input: username "user", password "secrets", limits 4 and 6
// Expected: true
#[test]
fn length_long_password() {
    assert!(invalid_credential_lengths("user", "secrets", 4, 6));
}

// Alias: Rejects a zero username limit
// Input: username "a", password "b", limits 0 and 6
// Expected: true
#[test]
fn length_zero_user() {
    assert!(invalid_credential_lengths("a", "b", 0, 6));
}

// Alias: Rejects a zero password limit
// Input: username "a", password "b", limits 4 and 0
// Expected: true
#[test]
fn length_zero_password() {
    assert!(invalid_credential_lengths("a", "b", 4, 0));
}

// Alias: Accepts exact UTF-8 byte limits
// Input: username "éé", password "🔑é", limits 4 and 6
// Expected: false
#[test]
fn length_multi_exact() {
    assert!(!invalid_credential_lengths("éé", "🔑é", 4, 6));
}

// Alias: Counts username bytes
// Input: username "éé", password "🔑é", limits 3 and 6
// Expected: true
#[test]
fn length_multi_user() {
    assert!(invalid_credential_lengths("éé", "🔑é", 3, 6));
}

// Alias: Counts password bytes
// Input: username "éé", password "🔑é", limits 4 and 5
// Expected: true
#[test]
fn length_multi_password() {
    assert!(invalid_credential_lengths("éé", "🔑é", 4, 5));
}
