use std::{future::Future, sync::Arc, time::Duration};

use argon2::{
    Argon2,
    password_hash::{PasswordVerifier, phc::PasswordHash},
};
use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register, ServerUnit},
};
use futures_util::StreamExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_util::codec::Framed;

use super::{RegRequest, RegResponse, handle_register};
use crate::users::UserRequest;

const SERVER_TOKEN: &str = "server-token";
const MAX_USERNAME_BYTES: usize = 4;
const MAX_PASSWORD_BYTES: usize = 6;

async fn registration_outcome(
    request: Register,
    user_tx: mpsc::Sender<UserRequest>,
    hash_permit: Arc<Semaphore>,
    worker: impl Future<Output = ()>,
) -> u8 {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (sink, _) = Framed::new(server, FlpcCodec).split();
        let mut peer = Framed::new(peer, FlpcCodec);
        let handler = handle_register(
            request,
            user_tx,
            hash_permit,
            sink,
            SERVER_TOKEN.into(),
            MAX_USERNAME_BYTES,
            MAX_PASSWORD_BYTES,
        );
        tokio::join!(handler, worker);

        let frame = peer
            .next()
            .await
            .expect("server should send a registration frame")
            .expect("peer should decode the registration frame");
        let FlpcUnit::Server(ServerUnit::ServerRegister(response)) = frame else {
            panic!("expected ServerRegister, got {frame:?}");
        };
        *response.outcome()
    })
    .await
    .expect("registration exchange should complete")
}

async fn receive_registration(user_rx: &mut mpsc::Receiver<UserRequest>) -> RegRequest {
    let UserRequest::RegRequest(request) = user_rx
        .recv()
        .await
        .expect("worker should receive a registration request");
    request
}

fn assert_credentials(request: &RegRequest, username: &str, password: &str) {
    assert_eq!(request.username(), username);
    let hash =
        PasswordHash::new(request.hashed_password()).expect("worker should receive a PHC hash");
    Argon2::default()
        .verify_password(password.as_bytes(), &hash)
        .expect("hash should verify against the submitted password");
}

#[tokio::test]
async fn handle_register_encodes_worker_outcomes() {
    let cases = [
        (RegResponse::Success(42), 0),
        (RegResponse::NotEnoughSpace, 3),
        (RegResponse::UsernameExists, 4),
        (RegResponse::HandlerError, 5),
    ];

    for (response, expected_code) in cases {
        let (user_tx, mut user_rx) = mpsc::channel(1);
        let (username, password) = ("Zoë", "pāss");
        let request = Register::new(SERVER_TOKEN.into(), username.into(), password.into());
        let code = registration_outcome(request, user_tx, Arc::new(Semaphore::new(1)), async {
            let request = receive_registration(&mut user_rx).await;
            if expected_code == 0 {
                assert_credentials(&request, username, password);
            }
            assert!(request.try_send(response).is_ok());
        })
        .await;

        assert_eq!(code, expected_code);
    }
}
