use tokio::sync::{
    oneshot,
    mpsc,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use flpc::unit::{
    FlpcUnit,
    RemoveSelf,
    ServerRemoveSelf,
};
use flpc::tokio::{CodecError, FlpcCodec};

use crate::database::users::UserRequest;
use crate::users::invalid_credential_lengths;


type FlpcSink = SplitSink<Framed<TcpStream, FlpcCodec>, FlpcUnit>;

pub enum RemoveSelfResponse {
    Success(u16),
    UserNotFound,
    WrongCredentials,
    HandlerError,
}

pub struct RemoveSelfRequest {
    username: String,
    password: String,
    response_tx: oneshot::Sender<RemoveSelfResponse>,
}

impl RemoveSelfRequest {
    pub fn new(
        username: String,
        password: String,
        response_tx: oneshot::Sender<RemoveSelfResponse>,
    ) -> Self {
        Self {
            username,
            password,
            response_tx,
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn try_send(
        self,
        response: RemoveSelfResponse,
    ) -> Result<(), RemoveSelfResponse> {
        self.response_tx.send(response)
    }
}


pub async fn handle_removeself(
    request: RemoveSelf,
    user_tx: mpsc::Sender<UserRequest>,
    sink: FlpcSink,
    max_len_username: usize,
    max_len_password: usize,
) {
    let code = if invalid_credential_lengths(
        request.username(),
        request.password(),
        max_len_username,
        max_len_password,
    ) {
        2
    } else {
        match try_remove(request.username(), request.password(), user_tx).await {
            RemoveSelfResponse::Success(_) => 0,
            RemoveSelfResponse::WrongCredentials => 1,
            RemoveSelfResponse::UserNotFound => 3,
            RemoveSelfResponse::HandlerError => 4,
        }
    };
    let _ = send_serverremoveself(code, sink).await;
}

async fn try_remove(
    username: &str,
    password: &str,
    user_tx: mpsc::Sender<UserRequest>,
) -> RemoveSelfResponse {
    let (response_tx, response_rx) = oneshot::channel();
    let request = RemoveSelfRequest::new(username.to_owned(), password.to_owned(), response_tx);
    if user_tx.send(UserRequest::RemoveSelfRequest(request)).await.is_err() {
        return RemoveSelfResponse::HandlerError;
    }

    response_rx.await.unwrap_or(RemoveSelfResponse::HandlerError)
}

async fn send_serverremoveself(
    code: u8,
    mut sink: FlpcSink,
) -> Result<(), CodecError> {
    sink.send(ServerRemoveSelf::new(code).into()).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use flpc::unit::{Register, ServerUnit};
    use futures_util::StreamExt;
    use sqlx::PgPool;
    use tokio::{net::TcpListener, sync::Semaphore, time::timeout};

    use super::*;
    use crate::database::{serial_tests::postgres_test, users::UserHandler};
    use crate::users::registration::handle_register;

    const LIMITS: (usize, usize) = (4, 6);
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn request(username: &str, password: &str) -> RemoveSelf {
        RemoveSelf::new(username.into(), password.into())
    }

    async fn loopback() -> (FlpcSink, Framed<TcpStream, FlpcCodec>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (sink, _) = Framed::new(server, FlpcCodec).split();
        (sink, Framed::new(peer, FlpcCodec))
    }

    async fn read_result(peer: &mut Framed<TcpStream, FlpcCodec>) -> u8 {
        let frame = peer.next().await.expect("removal response").expect("valid FLPC frame");
        let FlpcUnit::Server(ServerUnit::ServerRemoveSelf(response)) = frame else {
            panic!("expected ServerRemoveSelf, got {frame:?}");
        };
        assert!(peer.next().await.is_none(), "expected exactly one response frame");
        *response.result()
    }

    async fn receive_request(user_rx: &mut mpsc::Receiver<UserRequest>) -> RemoveSelfRequest {
        let Some(UserRequest::RemoveSelfRequest(request)) = user_rx.recv().await else {
            panic!("expected a removal request");
        };
        request
    }

    #[tokio::test]
    async fn try_remove_forwards_exact_credentials_and_returns_reply() {
        timeout(TEST_TIMEOUT, async {
            let (user_tx, mut user_rx) = mpsc::channel(1);
            let (response, ()) = tokio::join!(try_remove("éé", "🔑é", user_tx), async {
                let request = receive_request(&mut user_rx).await;
                assert_eq!(request.username(), "éé");
                assert_eq!(request.password(), "🔑é");
                assert!(request.try_send(RemoveSelfResponse::Success(u16::MAX)).is_ok());
            });
            assert!(matches!(response, RemoveSelfResponse::Success(u16::MAX)));
        })
        .await
        .expect("worker exchange should complete");
    }

    #[tokio::test]
    async fn try_remove_reports_closed_request_and_reply_channels() {
        timeout(TEST_TIMEOUT, async {
            let (user_tx, user_rx) = mpsc::channel(1);
            drop(user_rx);
            assert!(matches!(
                try_remove("user", "secret", user_tx).await,
                RemoveSelfResponse::HandlerError
            ));

            let (user_tx, mut user_rx) = mpsc::channel(1);
            let (response, ()) = tokio::join!(try_remove("user", "secret", user_tx), async {
                drop(receive_request(&mut user_rx).await);
            });
            assert!(matches!(response, RemoveSelfResponse::HandlerError));
        })
        .await
        .expect("closed channels should resolve");
    }

    #[tokio::test]
    async fn send_serverremoveself_preserves_result_byte() {
        timeout(TEST_TIMEOUT, async {
            let (sink, mut peer) = loopback().await;
            send_serverremoveself(0xa5, sink).await.unwrap();
            assert_eq!(read_result(&mut peer).await, 0xa5);
        })
        .await
        .expect("response should reach peer");
    }

    #[tokio::test]
    async fn handle_removeself_encodes_worker_outcomes() {
        let cases = [
            (RemoveSelfResponse::Success(u16::MAX), 0),
            (RemoveSelfResponse::WrongCredentials, 1),
            (RemoveSelfResponse::UserNotFound, 3),
            (RemoveSelfResponse::HandlerError, 4),
        ];

        for (response, code) in cases {
            timeout(TEST_TIMEOUT, async {
                let (sink, mut peer) = loopback().await;
                let (user_tx, mut user_rx) = mpsc::channel(1);
                tokio::join!(
                    handle_removeself(request("éé", "🔑é"), user_tx, sink, LIMITS.0, LIMITS.1),
                    async {
                        let request = receive_request(&mut user_rx).await;
                        assert_eq!(request.username(), "éé");
                        assert_eq!(request.password(), "🔑é");
                        assert!(request.try_send(response).is_ok());
                    },
                );
                assert_eq!(read_result(&mut peer).await, code);
            })
            .await
            .expect("valid removal should reach worker and respond");
        }
    }

    #[tokio::test]
    async fn handle_removeself_rejects_invalid_credentials_before_dispatch() {
        let cases = [
            ("", "secret", LIMITS),
            ("user", "", LIMITS),
            ("users", "secret", LIMITS),
            ("user", "secrets", LIMITS),
            ("ééx", "secret", LIMITS),
            ("user", "🔑éx", LIMITS),
            ("user", "secret", (0, 6)),
            ("user", "secret", (4, 0)),
        ];

        for (username, password, (username_limit, password_limit)) in cases {
            timeout(TEST_TIMEOUT, async {
                let (sink, mut peer) = loopback().await;
                let (user_tx, mut user_rx) = mpsc::channel(1);
                handle_removeself(
                    request(username, password),
                    user_tx,
                    sink,
                    username_limit,
                    password_limit,
                )
                .await;
                assert!(user_rx.recv().await.is_none(), "invalid credentials reached worker");
                assert_eq!(read_result(&mut peer).await, 2);
            })
            .await
            .expect("invalid removal should respond without a worker");
        }
    }

    postgres_test! {
        async fn registration_and_removal_use_the_real_worker(pool: PgPool) -> sqlx::Result<()> {
            let (user_tx, user_rx) = mpsc::channel(1);
            let permit = Arc::new(Semaphore::new(1));
            let worker = UserHandler::new(pool.clone(), user_rx, Arc::clone(&permit));

            timeout(Duration::from_secs(15), async {
                let exercise = async move {
                    let (sink, mut peer) = loopback().await;
                    handle_register(
                        Register::new("token".into(), "user".into(), "🔑é".into()),
                        user_tx.clone(),
                        permit,
                        sink,
                        "token",
                        LIMITS.0,
                        LIMITS.1,
                    )
                    .await;
                    let registration = peer
                        .next()
                        .await
                        .expect("registration reply")
                        .expect("valid FLPC frame");
                    let FlpcUnit::Server(ServerUnit::ServerRegister(registration)) = registration else {
                        panic!("expected ServerRegister, got {registration:?}");
                    };
                    assert_eq!(*registration.outcome(), 0);

                    for (password, code, account_exists) in [
                        ("wrong", 1, true),
                        ("🔑é", 0, false),
                        ("🔑é", 3, false),
                    ] {
                        let (sink, mut peer) = loopback().await;
                        handle_removeself(
                            request("user", password),
                            user_tx.clone(),
                            sink,
                            LIMITS.0,
                            LIMITS.1,
                        )
                        .await;
                        assert_eq!(read_result(&mut peer).await, code);
                        let exists: bool = sqlx::query_scalar(
                            "SELECT EXISTS (SELECT 1 FROM users WHERE username = $1)",
                        )
                        .bind("user")
                        .fetch_one(&pool)
                        .await?;
                        assert_eq!(exists, account_exists, "account state after result {code}");
                    }
                    Ok(())
                };
                let ((), result) = tokio::join!(worker.run(), exercise);
                result
            })
            .await
            .expect("registration and removal should complete")
        }
    }
}
