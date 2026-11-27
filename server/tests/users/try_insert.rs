use std::{pin::pin, time::Duration};

use futures_util::poll;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

use super::{RegRequest, RegResponse, UserRequest, try_insert};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn receive_registration(user_rx: &mut mpsc::Receiver<UserRequest>) -> RegRequest {
    let UserRequest::RegRequest(request) = user_rx
        .try_recv()
        .expect("registration request should be queued")
    else {
        panic!("expected a registration request");
    };
    request
}

fn assert_no_queued_request(user_rx: &mut mpsc::Receiver<UserRequest>) {
    assert!(matches!(
        user_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
}

fn fill_request_queue(user_tx: &mpsc::Sender<UserRequest>) {
    let (response_tx, _) = oneshot::channel();
    let request = RegRequest::new("occupied".into(), "fixture-password".into(), response_tx);
    assert!(user_tx.try_send(UserRequest::RegRequest(request)).is_ok());
}

// Alias: Forwards the exact request
// Input: Receive the helper's message and respond with Success(65535)
// Expected: Exactly one request of the correct variant, with both strings unchanged. Caller gets Success(65535).
#[tokio::test]
async fn try_insert_forward() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_insert("éé", "fixture-password", user_tx));

    assert!(poll!(caller.as_mut()).is_pending());
    let request = receive_registration(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "fixture-password");
    assert_no_queued_request(&mut user_rx);
    assert!(request.try_send(RegResponse::Success(u16::MAX)).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should receive the worker reply");
    assert!(matches!(response, RegResponse::Success(u16::MAX)));
    assert_no_queued_request(&mut user_rx);
}

// Alias: Handles a closed request channel
// Input: Drop the mpsc receiver before calling
// Expected: HandlerError.
#[tokio::test]
async fn try_insert_no_receiver() {
    let (user_tx, user_rx) = mpsc::channel(1);
    drop(user_rx);

    let response = timeout(TEST_TIMEOUT, try_insert("éé", "fixture-password", user_tx))
        .await
        .expect("caller should finish when the receiver is gone");
    assert!(matches!(response, RegResponse::HandlerError));
}

// Alias: Handles a dropped one-shot sender
// Input: Receive and drop the request without replying
// Expected: HandlerError.
#[tokio::test]
async fn try_insert_no_reply() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_insert("éé", "fixture-password", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());

    let request = receive_registration(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "fixture-password");
    drop(request);

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish when the reply sender is dropped");
    assert!(matches!(response, RegResponse::HandlerError));
}

// Alias: Waits for channel capacity
// Input: Fill the channel with a request for "occupied", poll the call, drain the filler, then respond Success(65535) to the tested request
// Expected: Pending while full, then forwards the tested request once and returns Success(65535).
#[tokio::test]
async fn try_insert_full() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    fill_request_queue(&user_tx);
    let mut caller = pin!(try_insert("éé", "fixture-password", user_tx));
    assert!(
        poll!(caller.as_mut()).is_pending(),
        "send waits while the queue is full"
    );

    let filler = receive_registration(&mut user_rx);
    assert_eq!(filler.username(), "occupied");
    assert_eq!(filler.password(), "fixture-password");
    assert!(
        poll!(caller.as_mut()).is_pending(),
        "caller waits for the tested reply"
    );

    let request = receive_registration(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "fixture-password");
    assert_no_queued_request(&mut user_rx);
    assert!(request.try_send(RegResponse::Success(u16::MAX)).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RegResponse::Success(u16::MAX)));
    assert_no_queued_request(&mut user_rx);
}

// Alias: Forwards the duplicate username reply
// Input: RegResponse::UsernameExists
// Expected: RegResponse::UsernameExists.
#[tokio::test]
async fn try_insert_username_exists() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_insert("éé", "fixture-password", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = receive_registration(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "fixture-password");
    assert!(request.try_send(RegResponse::UsernameExists).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RegResponse::UsernameExists));
}

// Alias: Forwards the capacity reply
// Input: RegResponse::NotEnoughSpace
// Expected: RegResponse::NotEnoughSpace.
#[tokio::test]
async fn try_insert_no_space() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_insert("éé", "fixture-password", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = receive_registration(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "fixture-password");
    assert!(request.try_send(RegResponse::NotEnoughSpace).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RegResponse::NotEnoughSpace));
}

// Alias: Forwards the registration handler error
// Input: RegResponse::HandlerError
// Expected: RegResponse::HandlerError.
#[tokio::test]
async fn try_insert_handler_error() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_insert("éé", "fixture-password", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    let request = receive_registration(&mut user_rx);
    assert_eq!(request.username(), "éé");
    assert_eq!(request.password(), "fixture-password");
    assert!(request.try_send(RegResponse::HandlerError).is_ok());

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish after reply");
    assert!(matches!(response, RegResponse::HandlerError));
}
