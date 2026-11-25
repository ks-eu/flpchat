use super::{Register, check_length};

#[test]
fn check_length_enforces_each_field_byte_limit() {
    // (username, password, username limit, password limit, should_reject)
    let cases = [
        ("", "ok", 3, 5, false),
        ("ab", "ok", 3, 5, false),
        ("abc", "ok", 3, 5, false),
        ("abcd", "ok", 3, 5, true),
        ("ok", "", 3, 5, false),
        ("ok", "1234", 3, 5, false),
        ("ok", "12345", 3, 5, false),
        ("ok", "123456", 3, 5, true),
        ("abc", "12345", 3, 5, false),
        ("abcd", "12345", 3, 5, true),
        ("abc", "123456", 3, 5, true),
        ("abcd", "123456", 3, 5, true),
        ("", "ok", 0, 5, false),
        ("a", "ok", 0, 5, true),
        ("ok", "", 3, 0, false),
        ("ok", "1", 3, 0, true),
        ("éé", "ok", 4, 5, false),
        ("éé", "ok", 3, 5, true),
        ("ok", "éé", 2, 4, false),
        ("ok", "éé", 2, 3, true),
    ];

    for (username, password, username_limit, password_limit, should_reject) in cases {
        let request = Register::new("token".into(), username.into(), password.into());
        assert_eq!(
            check_length(&request, username_limit, password_limit),
            should_reject,
            "username {username:?} (limit {username_limit}), password {password:?} (limit {password_limit})",
        );
    }
}
