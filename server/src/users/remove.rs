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
    hashed_password: String,
    response_tx: oneshot::Sender<RemoveSelfResponse>,
}

impl RemoveSelfRequest {
    pub fn new(
        username: String,
        hashed_password: String,
        response_tx: oneshot::Sender<RemoveSelfResponse>,
    ) -> Self {
        Self {
            username,
            hashed_password,
            response_tx,
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn hashed_password(&self) -> &str {
        &self.hashed_password
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
    hash_permit: Arc<Semaphore>,
    sink: FlpcSink,
    max_len_username: usize,
    max_len_password: usize,
) {
    todo!();
}
