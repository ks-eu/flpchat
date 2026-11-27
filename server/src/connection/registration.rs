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
    Register,
    ServerRegister,
};
use flpc::tokio::{CodecError, FlpcCodec};

use crate::database::users::UserRequest;
use crate::connection::invalid_credential_lengths;

type FlpcSink = SplitSink<Framed<TcpStream, FlpcCodec>, FlpcUnit>;

pub enum RegResponse {
    Success(u16),
    NotEnoughSpace,
    UsernameExists,
    HandlerError,
}

pub struct RegRequest {
    username: String,
    password: String,
    response_tx: oneshot::Sender<RegResponse>,
}


impl RegRequest {
    pub fn new(
        username: String,
        password: String,
        response_tx: oneshot::Sender<RegResponse>,
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
        response: RegResponse
    ) -> Result<(), RegResponse> {
        self.response_tx.send(response)
    }
}


pub async fn handle_register(
    request: Register,
    user_tx: mpsc::Sender<UserRequest>,
    sink: FlpcSink,
    server_token: &str,
    max_len_username: usize,
    max_len_password: usize,
) {
    if check_token(&request, server_token) {
        // Wrong Credentials
        let _ = send_serverregister(1, sink).await;
        return
    }

    if invalid_credential_lengths(
        request.username(),
        request.password(),
        max_len_username,
        max_len_password,
    ) {
        // Invalid Username
        let _ = send_serverregister(2, sink).await;
        return
    }

    match try_insert(
        request.username(),
        request.password(),
        user_tx,
    ).await {
        RegResponse::Success(_) => {
            let _ = send_serverregister(0, sink).await;
        }
        RegResponse::NotEnoughSpace => {
            let _ = send_serverregister(3, sink).await;
        }
        RegResponse::UsernameExists => {
            let _ = send_serverregister(4, sink).await;
        }
        RegResponse::HandlerError => {
            let _ = send_serverregister(5, sink).await;
        }
    }
}

fn check_token(
    request: &Register,
    server_token: &str,
) -> bool {
    if *request.token() == server_token {
        false
    } else {
        true
    }
}

/// Sends an insert request and returns a handler error if either channel
/// closes early.
async fn try_insert(
    username: &str,
    password: &str,
    user_tx: mpsc::Sender<UserRequest>,
) -> RegResponse {
    let (response_tx, response_rx) = oneshot::channel();
    let request = RegRequest::new(
        username.to_string(),
        password.to_string(),
        response_tx
    );
    if user_tx.send(UserRequest::RegRequest(request)).await.is_err() {
        return RegResponse::HandlerError;
    }

    response_rx.await.unwrap_or(RegResponse::HandlerError)
}


/// Sends a registration outcome through the FLPC sink.
async fn send_serverregister(
    response_code: u8,
    mut sink: FlpcSink,
) -> Result<(), CodecError> {
    sink.send(ServerRegister::new(response_code).into()).await
}

#[cfg(test)]
#[path = "../../tests/users/check_token.rs"]
mod check_token_tests;

#[cfg(test)]
#[path = "../../tests/users/try_insert.rs"]
mod try_insert_tests;

#[cfg(test)]
#[path = "../../tests/users/handle_register.rs"]
mod handle_register_tests;

