//! routing and conversion for message units.
use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use flpc::unit::{
    Message,
    ServerMessage,
    PrivateMessage,
    ServerPrivateMessage,
};

use crate::orchestration::router::{
    RouterOutput,
};
use crate::limid::LimId;

pub async fn message_router<T>(
    message: Message,
    id: u16,
    limid_tx: mpsc::Sender<oneshot::Receiver<LimId>>,
    message_tx: mpsc::Sender<T>,
    writers: &HashMap<u16, mpsc::Sender<RouterOutput>>,
) {}

pub async fn private_message_router<T>(
    message: PrivateMessage,
    id: u16,
    limid_tx: mpsc::Sender<oneshot::Receiver<LimId>>,
    message_tx: mpsc::Sender<T>,
    writers: &HashMap<u16, mpsc::Sender<RouterOutput>>,
) {}

async fn get_limid(
    limid_tx: mpsc::Sender<oneshot::Receiver<LimId>>
) -> Result<LimId, oneshot::error::RecvError> {
    todo!()
}
    
async fn store_message<T>(
    message: T,
    message_tx: mpsc::Sender<T>
) {}

fn to_server <T, U>(
    message: T,
    id: u16,
    message_id: LimId,
) -> U {
    todo!()
}

fn check_online (
    target_id: u16,
) -> bool {
    todo!()
}
