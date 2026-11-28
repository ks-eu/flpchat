use flpc::unit::{ServerMessage, ServerPrivateMessage};

pub enum MessageRequest {
    InsertMessage(ServerMessage),
    InsertPrivateMessage(ServerPrivateMessage),
}

impl From<ServerMessage> for MessageRequest {
    fn from(message: ServerMessage) -> Self {
        Self::InsertMessage(message)
    }
}

impl From<ServerPrivateMessage> for MessageRequest {
    fn from(message: ServerPrivateMessage) -> Self {
        Self::InsertPrivateMessage(message)
    }
}

pub enum MessageResponse {
    Success,
    Fail,
}
