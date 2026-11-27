use std::{pin::pin, time::Duration};

use argon2::{Argon2, PasswordHasher};
use flpc::unit::ServerUnit;
use futures_util::{StreamExt, poll};
use sqlx::PgPool;
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

use crate::database::{serial_tests::postgres_test, users::{UserHandler, run}};

use super::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const USERNAME: &str = "éé";
const PASSWORD: &str = "🔑é";
const USERNAME_LIMIT: usize = 4;
const PASSWORD_LIMIT: usize = 6;

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

async fn login_sink() -> (FlpcSink, Framed<TcpStream, FlpcCodec>) {
    let (server, peer) = loopback().await;
    let (sink, _) = Framed::new(server, FlpcCodec).split();
    (sink, Framed::new(peer, FlpcCodec))
}

async fn assert_login_frame(peer: &mut Framed<TcpStream, FlpcCodec>, expected: u8) {
    let frame = peer
        .next()
        .await
        .expect("login response frame")
        .expect("valid FLPC frame");
    let FlpcUnit::Server(ServerUnit::ServerLogin(response)) = frame else {
        panic!("expected ServerLogin, got {frame:?}");
    };
    assert_eq!(*response.outcome(), expected);
    assert!(peer.next().await.is_none(), "expected exactly one frame");
}

postgres_test! {
    async fn handle_login_verification_returns_id_and_sink(pool: PgPool) -> sqlx::Result<()> {
        let hash = Argon2::default().hash_password(PASSWORD.as_bytes()).unwrap().to_string();
        sqlx::query("DELETE FROM available_user_ids WHERE user_id = 65535")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO users (user_id, username, password_hash)
             OVERRIDING SYSTEM VALUE VALUES (65535, $1, $2)",
        )
        .bind(USERNAME)
        .bind(hash)
        .execute(&pool)
        .await?;
        let (user_tx, user_rx) = mpsc::channel(1);
        let worker = tokio::spawn(run(UserHandler::new(user_rx, pool, 1)));

        timeout(TEST_TIMEOUT, async {
            let (sink, mut peer) = login_sink().await;
            let (id, mut returned_sink) = handle_login_verification(
                Login::new(USERNAME.into(), PASSWORD.into()),
                user_tx,
                sink,
                USERNAME_LIMIT,
                PASSWORD_LIMIT,
            )
            .await
            .expect("login should return an ID and a usable sink");
            assert_eq!(id, u16::MAX);
            returned_sink.send(ServerLogin::new(0x5a).into()).await.unwrap();
            drop(returned_sink);
            assert_login_frame(&mut peer, 0x5a).await;
            worker.await.expect("login worker should exit");
        })
        .await
        .expect("login exchange should finish");
        Ok(())
    }
}

#[tokio::test]
async fn handle_login_verification_invalid_lengths_skip_worker() {
    timeout(TEST_TIMEOUT, async {
        for (username, password) in [
            ("", PASSWORD),
            ("ééa", PASSWORD),
            (USERNAME, ""),
            (USERNAME, "🔑éa"),
        ] {
            let (sink, mut peer) = login_sink().await;
            let (user_tx, mut user_rx) = mpsc::channel(1);
            let (result, ()) = tokio::join!(
                handle_login_verification(
                    Login::new(username.into(), password.into()),
                    user_tx,
                    sink,
                    USERNAME_LIMIT,
                    PASSWORD_LIMIT,
                ),
                async {
                    assert!(
                        user_rx.recv().await.is_none(),
                        "invalid credentials should never enqueue a login"
                    );
                },
            );
            assert!(result.is_none());
            assert_login_frame(&mut peer, 2).await;
        }
    })
    .await
    .expect("validation exchanges should finish");
}

async fn assert_login_rejected(reply: Option<LoginResponse>, expected: u8) {
    timeout(TEST_TIMEOUT, async {
        let (sink, mut peer) = login_sink().await;
        let (user_tx, mut user_rx) = mpsc::channel(1);
        let mut caller = pin!(handle_login_verification(
            Login::new(USERNAME.into(), PASSWORD.into()),
            user_tx,
            sink,
            USERNAME_LIMIT,
            PASSWORD_LIMIT,
        ));
        assert!(poll!(caller.as_mut()).is_pending());
        let request = receive_login(&mut user_rx);
        if let Some(reply) = reply {
            assert!(request.try_send(reply).is_ok());
        } else {
            drop(request);
        }
        assert!(caller.await.is_none());
        assert_login_frame(&mut peer, expected).await;
    })
    .await
    .expect("rejection exchange should finish");
}

#[tokio::test]
async fn handle_login_verification_wrong_credentials() {
    assert_login_rejected(Some(LoginResponse::WrongCredentials), 1).await;
}

#[tokio::test]
async fn handle_login_verification_missing_user() {
    assert_login_rejected(Some(LoginResponse::UserNotFound), 3).await;
}

#[tokio::test]
async fn handle_login_verification_worker_error() {
    assert_login_rejected(Some(LoginResponse::HandlerError), 4).await;
}

#[tokio::test]
async fn handle_login_verification_dropped_reply() {
    assert_login_rejected(None, 4).await;
}

#[tokio::test]
async fn handle_login_verification_closed_worker() {
    timeout(TEST_TIMEOUT, async {
        let (sink, mut peer) = login_sink().await;
        let (user_tx, user_rx) = mpsc::channel(1);
        drop(user_rx);
        let result = handle_login_verification(
            Login::new(USERNAME.into(), PASSWORD.into()),
            user_tx,
            sink,
            USERNAME_LIMIT,
            PASSWORD_LIMIT,
        )
        .await;

        assert!(result.is_none());
        assert_login_frame(&mut peer, 4).await;
    })
    .await
    .expect("closed-worker exchange should finish");
}
