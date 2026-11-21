use std::sync::Arc;

use flpc::header::{ClientMessage, Message, UserJoin, UserLeft};
use tokio::sync::RwLock;

use crate::message::event::ServerEvent;
use crate::users::UserState;

pub async fn route_message(
    users: Arc<RwLock<UserState>>,
    message: Message,
    sender_id: u16,
) {
    let writers: Vec<_> = {
        let state = users.read().await;
        state
            .users()
            .iter()
            .filter(|(id, user)| **id != sender_id && user.is_onboarded())
            .filter_map(|(&id, _)| state.sender(id).cloned())
            .collect()
    };

    let Some((last_writer, other_writers)) = writers.split_last() else {
        return;
    };
    let header = ClientMessage::from_message(message, sender_id);
    for writer in other_writers {
        if let Ok(copy) = ClientMessage::new(header.uuid(), header.msg()) {
            let _ = writer.send(ServerEvent::Message(copy)).await;
        }
    }
    let _ = last_writer.send(ServerEvent::Message(header)).await;
}

pub async fn route_connected(users: Arc<RwLock<UserState>>, id: u16) {
    let (username, writers) = {
        let state = users.read().await;
        let Some(username) = state.username(id) else {
            return;
        };
        let writers: Vec<_> = state
            .users()
            .keys()
            .filter_map(|&id| state.sender(id).cloned())
            .collect();
        (username.to_owned(), writers)
    };

    for writer in writers {
        let Ok(header) = UserJoin::new(&username, id) else {
            return;
        };
        let _ = writer.send(ServerEvent::UserJoin(header)).await;
    }
}

pub async fn route_disconnected(users: Arc<RwLock<UserState>>, id: u16) {
    let writers: Vec<_> = {
        let state = users.read().await;
        state
            .users()
            .keys()
            .filter_map(|&id| state.sender(id).cloned())
            .collect()
    };

    for writer in writers {
        let _ = writer.send(ServerEvent::UserLeft(UserLeft::new(id))).await;
    }
}
