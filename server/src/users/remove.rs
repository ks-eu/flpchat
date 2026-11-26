use std::error::Error;
use std::sync::Arc;

use tokio::sync::{
    oneshot,
    mpsc,
    Semaphore,
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
    todo!();
}

fn check_length(
    request: &RemoveSelf,
    max_len_username: usize,
    max_len_password: usize,
) -> bool {
    todo!("validate removal credential lengths");
}

async fn try_remove(
    username: &str,
    password: &str,
    user_tx: mpsc::Sender<UserRequest>,
) -> RemoveSelfResponse {
    todo!("send removal request and await reply");
}

async fn send_serverremoveself(
    code: u8,
    sink: FlpcSink,
) -> Result<(), CodecError> {
    todo!("send removal result frame");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use flpc::unit::ServerUnit;
    use futures_util::StreamExt;
    use tokio::{net::TcpListener, time::timeout};

    use super::*;

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

    #[test]
    fn check_length_enforces_credential_byte_boundaries() {
        let cases = [
            ("a", "b", 4, 6, false),
            ("user", "secret", 4, 6, false),
            ("users", "secret", 4, 6, true),
            ("user", "secrets", 4, 6, true),
            ("users", "secrets", 4, 6, true),
            ("", "b", 4, 6, true),
            ("a", "", 4, 6, true),
            ("", "", 4, 6, true),
            ("a", "b", 0, 6, true),
            ("a", "b", 4, 0, true),
            ("", "", 0, 0, true),
            ("éé", "🔑é", 4, 6, false),
            ("éé", "🔑é", 3, 6, true),
            ("éé", "🔑é", 4, 5, true),
        ];

        for (username, password, username_limit, password_limit, rejects) in cases {
            assert_eq!(
                check_length(&request(username, password), username_limit, password_limit),
                rejects,
                "credentials {username:?}, {password:?} with limits {username_limit}, {password_limit}",
            );
        }
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
}
