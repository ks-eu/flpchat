use super::{Register, check_token};

#[test]
fn check_token_rejects_any_difference() {
    // (request token, server token, should_reject)
    let cases = [
        ("token", "token", false),
        ("clé", "clé", false),
        ("", "", false),
        ("", "token", true),
        ("token", "", true),
        ("xoken", "token", true),
        ("tokex", "token", true),
        ("toke", "token", true),
        ("token", "toke", true),
        ("Token", "token", true),
        (" token", "token", true),
        ("token ", "token", true),
        ("é", "e\u{301}", true),
    ];

    for (request_token, server_token, should_reject) in cases {
        let request = Register::new(request_token.into(), "user".into(), "password".into());
        assert_eq!(
            check_token(&request, server_token.into()),
            should_reject,
            "request token {request_token:?}, server token {server_token:?}",
        );
    }
}
