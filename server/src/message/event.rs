use flpc::header::{
    Message,
    ClientMessage,
    UserJoin,
    UserLeft,
};
use flpc::tokio::{FLPCMsg};

pub enum ServerEvent {
    Message(ClientMessage),
    UserJoin(UserJoin),
    UserLeft(UserLeft),
}

pub enum ClientEvent {
    Message(Message, u16),
    Connected(u16),
    Disconnected(u16),
}

impl From<ServerEvent> for FLPCMsg {
    fn from(event: ServerEvent) -> Self {
        match event {
            ServerEvent::Message(header) => FLPCMsg::ClientMessage(header),
            ServerEvent::UserJoin(header) => FLPCMsg::UserJoin(header),
            ServerEvent::UserLeft(header) => FLPCMsg::UserLeft(header),
        }
    }
}
