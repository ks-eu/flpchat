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
use crate::connection::invalid_credential_lengths;


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
#[path = "../../tests/users/remove.rs"]
mod tests;
