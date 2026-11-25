use std::mem::discriminant;
use std::pin::pin;
use std::time::Duration;

use futures_util::poll;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::{RegResponse, UserRequest, try_insert};

#[tokio::test]
async fn try_insert_delivers_request_and_returns_success_id() {
    let cases = [
        ("alice", "password-hash", 1),
        ("", "", 1),
        ("用户", "café🔑", u16::MAX),
    ];

    for (username, hashed_password, expected_id) in cases {
        let (user_tx, mut user_rx) = mpsc::channel(1);
        let mut caller = pin!(try_insert(username, hashed_password, user_tx));

        assert!(poll!(caller.as_mut()).is_pending());
        let UserRequest::RegRequest(request) = user_rx.try_recv().expect("registration request");
        assert_eq!(request.username(), username);
        assert_eq!(request.hashed_password(), hashed_password);
        assert!(request.try_send(RegResponse::Success(expected_id)).is_ok());

        let response = timeout(Duration::from_secs(5), caller)
            .await
            .expect("caller should receive the worker reply");
        assert!(matches!(response, RegResponse::Success(id) if id == expected_id));
    }
}

#[tokio::test]
async fn try_insert_returns_the_workers_unsuccessful_reply() {
    let cases = [
        RegResponse::NotEnoughSpace,
        RegResponse::UsernameExists,
        RegResponse::HandlerError,
    ];

    for reply in cases {
        let (user_tx, mut user_rx) = mpsc::channel(1);
        let mut caller = pin!(try_insert("alice", "password-hash", user_tx));

        assert!(poll!(caller.as_mut()).is_pending());
        let UserRequest::RegRequest(request) = user_rx.try_recv().expect("registration request");
        let expected = discriminant(&reply);
        assert!(request.try_send(reply).is_ok());

        let response = timeout(Duration::from_secs(5), caller)
            .await
            .expect("caller should receive the worker reply");
        assert_eq!(discriminant(&response), expected);
    }
}
