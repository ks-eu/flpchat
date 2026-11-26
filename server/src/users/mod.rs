use flpc::unit::Register;

pub(crate) mod registration;
pub(crate) mod remove;

fn check_length(
    request: &Register,
    max_len_username: usize,
    max_len_password: usize,
) -> bool {
    if request.username().len() > max_len_username ||
       request.username().len() == 0               ||
       request.password().len() > max_len_password ||
       request.password().len() == 0 {
        true
    } else {
        false
    }
}

#[cfg(test)]
#[path = "../../tests/users/check_length.rs"]
mod check_length_tests;
