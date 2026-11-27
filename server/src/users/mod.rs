pub(crate) mod registration;
pub(crate) mod remove;
pub(crate) mod login;

pub(crate) fn invalid_credential_lengths(
    username: &str,
    password: &str,
    max_len_username: usize,
    max_len_password: usize,
) -> bool {
    username.is_empty()
        || username.len() > max_len_username
        || password.is_empty()
        || password.len() > max_len_password
}

#[cfg(test)]
#[path = "../../tests/users/check_length.rs"]
mod check_length_tests;
