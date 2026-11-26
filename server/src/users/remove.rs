use std::error::Error;
use std::sync::Arc;

use argon2::{Argon2, PasswordHasher};
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
    Register,
    ServerRegister,
};
use flpc::tokio::{CodecError, FlpcCodec};

use crate::users::UserRequest;


type FlpcSink = SplitSink<Framed<TcpStream, FlpcCodec>, FlpcUnit>;

pub enum RemoveResponse {
    Success,
    UserNotFound,
    WrongCredentials,
    HandlerError,
}

pub struct RemoveRequest {
    id: u16,
    response_tx: oneshot::Sender<RemoveResponse>,
}

impl RemoveRequest {
    pub fn new(
        id: u16,
        response_tx: oneshot::Sender<RemoveResponse>,
    ) -> Self {
        Self {
            id,
            response_tx,
        }
    }

    pub fn id(&self) -> &u16 {
        &self.id
    }

    pub fn try_send(
        self,
        response: RemoveResponse,
    ) -> Result<(), RemoveResponse> {
        self.response_tx.send(response)
    }
}
