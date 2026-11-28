//! routing and conversion for message units.
//!
//! extra notes for implementation:
//!
//! order is
//! receive message -> get message_id -> convert -> try_send -> send to database handler
//! if there is failure at these steps (except database handler send), then send ServerMessageFail
//! (needs to be implmeneted)
//!

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

// end to end message in, message to writers or singlesender to sender (need to implement
// servermessagefail in flpc first)
pub async fn message_router<T>(
    message: Message,
    id: u16,
    limid_tx: mpsc::Sender<oneshot::Receiver<LimId>>,
    message_tx: mpsc::Sender<T>,
    writers: &HashMap<u16, mpsc::Sender<RouterOutput>>,
) {}

// end to end private message in, message to writers or singlesender to sender
pub async fn private_message_router<T>(
    message: PrivateMessage,
    id: u16,
    limid_tx: mpsc::Sender<oneshot::Receiver<LimId>>,
    message_tx: mpsc::Sender<T>,
    writers: &HashMap<u16, mpsc::Sender<RouterOutput>>,
) {}

// calls the limid generator, returns the generated limid
async fn get_limid(
    limid_tx: mpsc::Sender<oneshot::Receiver<LimId>>
) -> Result<LimId, oneshot::error::RecvError> {
    todo!()
}

// sends message or private message to the message table database handler (T represents a
// request enum, needs implementing)
async fn store_message<T>(
    message: T,
    message_tx: mpsc::Sender<T>
) {}

// takes message/private message, returns the server version or servermessagefail
fn to_server <T, U>(
    message: T,
    id: u16,
    message_id: LimId,
) -> U {
    todo!()
}

// checks if the targetted recipient is online
fn check_online (
    target_id: u16,
) -> bool {
    todo!()
}

// the sender for successful private message - sends back to sender and to the
// target recipient. do this before adding to database
async fn send_private_message(
    id: u16,
    target_id: u16,
    writers: &HashMap<u16, mpsc::Sender<RouterOutput>>,
) {
    todo!()
}
