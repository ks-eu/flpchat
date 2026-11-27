use std::{pin::pin, time::Duration};

use flpc::unit::ServerUnit;
use futures_util::{StreamExt, poll};
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

use super::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn receive_login(user_rx: &mut mpsc::Receiver<UserRequest>) -> LoginRequest {
    let UserRequest::LoginRequest(request) = user_rx.try_recv().expect("queued login request")
    else {
        panic!("expected a login request");
    };
    request
}

#[tokio::test]
async fn try_verify_forwards_credentials_and_responses() {
    for reply in [
        LoginResponse::Success(u16::MAX),
        LoginResponse::UserNotFound,
        LoginResponse::WrongCredentials,
        LoginResponse::HandlerError,
    ] {
        let expected = std::mem::discriminant(&reply);
        let (user_tx, mut user_rx) = mpsc::channel(1);
        let mut caller = pin!(try_verify("éé", "🔑é", user_tx));
        assert!(poll!(caller.as_mut()).is_pending());

        let request = receive_login(&mut user_rx);
        assert_eq!(request.username(), "éé");
        assert_eq!(request.password(), "🔑é");
        assert!(request.try_send(reply).is_ok());

        let response = timeout(TEST_TIMEOUT, caller).await.expect("login reply");
        assert_eq!(std::mem::discriminant(&response), expected);
        if let LoginResponse::Success(id) = response {
            assert_eq!(id, u16::MAX);
        }
        assert!(matches!(
            user_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}

#[tokio::test]
async fn try_verify_handles_closed_request_channel() {
    let (user_tx, user_rx) = mpsc::channel(1);
    drop(user_rx);

    let response = timeout(TEST_TIMEOUT, try_verify("éé", "🔑é", user_tx))
        .await
        .expect("caller should finish when the receiver is gone");
    assert!(matches!(response, LoginResponse::HandlerError));
}

#[tokio::test]
async fn try_verify_handles_dropped_reply() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let mut caller = pin!(try_verify("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    drop(receive_login(&mut user_rx));

    let response = timeout(TEST_TIMEOUT, caller)
        .await
        .expect("caller should finish when the reply sender is gone");
    assert!(matches!(response, LoginResponse::HandlerError));
}

#[tokio::test]
async fn try_verify_waits_for_request_capacity() {
    let (user_tx, mut user_rx) = mpsc::channel(1);
    let (reply_tx, _reply_rx) = oneshot::channel();
    let filler = LoginRequest::new("occupied".into(), "fixture".into(), reply_tx);
    assert!(user_tx.try_send(UserRequest::LoginRequest(filler)).is_ok());

    let mut caller = pin!(try_verify("éé", "🔑é", user_tx));
    assert!(poll!(caller.as_mut()).is_pending());
    assert_eq!(receive_login(&mut user_rx).username(), "occupied");
    assert!(poll!(caller.as_mut()).is_pending());
    assert!(
        receive_login(&mut user_rx)
            .try_send(LoginResponse::Success(7))
            .is_ok()
    );

    let response = timeout(TEST_TIMEOUT, caller).await.expect("login reply");
    assert!(matches!(response, LoginResponse::Success(7)));
}

async fn loopback() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (server, peer)
}

#[tokio::test]
async fn send_serverlogin_sends_one_frame_with_given_code() {
    timeout(TEST_TIMEOUT, async {
        let (server, peer) = loopback().await;
        let (sink, _) = Framed::new(server, FlpcCodec).split();
        let mut peer = Framed::new(peer, FlpcCodec);

        send_serverlogin(0x5a, sink).await.unwrap();

        let frame = peer.next().await.expect("login frame").unwrap();
        let FlpcUnit::Server(ServerUnit::ServerLogin(response)) = frame else {
            panic!("expected ServerLogin, got {frame:?}");
        };
        assert_eq!(*response.outcome(), 0x5a);
        assert!(peer.next().await.is_none(), "expected exactly one frame");
    })
    .await
    .expect("login frame exchange should finish");
}

#[tokio::test]
async fn send_serverlogin_returns_write_error() {
    timeout(TEST_TIMEOUT, async {
        let (mut server, _peer) = loopback().await;
        server.shutdown().await.unwrap();
        let (sink, _) = Framed::new(server, FlpcCodec).split();

        assert!(matches!(
            send_serverlogin(0, sink).await,
            Err(CodecError::Io(_))
        ));
    })
    .await
    .expect("write should fail after shutdown");
}
