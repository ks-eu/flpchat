//! routing and conversion for message units.
//!
//! extra notes for implementation:
//!
//! order is
//! receive message -> get message_id -> convert -> try_send -> send to database handler
//! if there is failure at these steps (except database handler send), then send ServerMessageFail
//! (needs to be implmeneted)
//!

use std::{collections::HashMap, error::Error};

use tokio::sync::{mpsc, oneshot};

use flpc::unit::{
    FlpcUnit,
    Message,
    ServerMessage,
    PrivateMessage,
    ServerPrivateMessage,
};

use crate::limid::LimId;
use crate::connection::message::{
    MessageRequest,
    MessageResponse,
};

// end to end message in, message to writers or singlesender to sender (need to implement
// servermessagefail in flpc first)
pub async fn message_router(
    message: Message,
    id: u16,
    limid_tx: mpsc::Sender<oneshot::Sender<LimId>>,
    message_tx: mpsc::Sender<MessageRequest>,
    writers: &HashMap<u16, mpsc::Sender<FlpcUnit>>,
) {}

// end to end private message in, message to writers or singlesender to sender
pub async fn private_message_router(
    message: PrivateMessage,
    id: u16,
    limid_tx: mpsc::Sender<oneshot::Sender<LimId>>,
    message_tx: mpsc::Sender<MessageRequest>,
    writers: &HashMap<u16, mpsc::Sender<FlpcUnit>>,
) {}

// Requests an ID from the generator.
async fn get_limid(
    limid_tx: &mpsc::Sender<oneshot::Sender<LimId>>,
) -> Result<LimId, Box<dyn Error + Send + Sync>> {
    let (reply, response) = oneshot::channel();
    limid_tx.send(reply).await?;
    response.await.map_err(Into::into)
}

// sends message or private message to the message table database handler
async fn store_message(
    message: impl Into<MessageRequest>,
    message_tx: &mpsc::Sender<MessageRequest>,
) -> Result<(), mpsc::error::SendError<MessageRequest>> {
    message_tx.send(message.into()).await
}

// FLPC's timestamp field carries the packed message ID, including its sequence.
fn to_server_message(message: Message, sender_id: u16, message_id: LimId) -> ServerMessage {
    let message_id = u64::from_be_bytes(message_id.to_be_bytes());
    ServerMessage::new(sender_id, message_id, message.message().clone())
}

fn to_server_private_message(
    message: PrivateMessage,
    sender_id: u16,
    message_id: LimId,
) -> ServerPrivateMessage {
    let message_id = u64::from_be_bytes(message_id.to_be_bytes());
    ServerPrivateMessage::new(sender_id, *message.target(), message_id, message.message().clone())
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
    writers: &HashMap<u16, mpsc::Sender<FlpcUnit>>,
) {
    todo!()
}

#[cfg(test)]
#[path = "../../../tests/orchestration/get_limid.rs"]
mod get_limid_tests;
