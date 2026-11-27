use tokio::sync::{oneshot, mpsc};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;

use flpc::unit::{
    FlpcUnit,
    Login,
    ServerLogin
};
use flpc::tokio::{CodecError, FlpcCodec};

use crate::database::users::UserRequest;
use crate::users::invalid_credential_lengths;

type FlpcSink = SplitSink<Framed<TcpStream, FlpcCodec>, FlpcUnit>;


pub enum LoginResponse {
    Success(u16),
    UserNotFound,
    WrongCredentials,
    HandlerError,
}

pub struct LoginRequest {
    username: String,
    password: String,
    response_tx: oneshot::Sender<LoginResponse>,
}

impl LoginRequest {
    pub fn new(
        username: String,
        password: String,
        response_tx: oneshot::Sender<LoginResponse>,
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
        response: LoginResponse,
    ) -> Result<(), LoginResponse> {
        self.response_tx.send(response)
    }
}


pub async fn handle_login(
    request: Login,
    user_tx: mpsc::Sender<UserRequest>,
    sink: FlpcSink,
    max_len_username: usize,
    max_len_password: usize,
) {
    todo!();
}

async fn try_verify(
    username: &str,
    password: &str,
    user_tx: mpsc::Sender<UserRequest>,
) -> LoginResponse {
    let (response_tx, response_rx) = oneshot::channel();
    let request = LoginRequest::new(username.to_owned(), password.to_owned(), response_tx);
    if user_tx.send(UserRequest::LoginRequest(request)).await.is_err() {
        return LoginResponse::HandlerError;
    }

    response_rx.await.unwrap_or(LoginResponse::HandlerError)
}

async fn send_serverlogin(
    code: u8,
    mut sink: FlpcSink,
) -> Result<(), CodecError> {
    sink.send(ServerLogin::new(code).into()).await
}

#[cfg(test)]
#[path = "../../tests/users/login.rs"]
mod tests;
