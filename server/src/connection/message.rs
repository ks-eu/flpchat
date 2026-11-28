use flpc::unit::{ServerMessage, ServerPrivateMessage};

pub enum MessageRequest {
    InsertMessage(ServerMessage),
    InsertPrivateMessage(ServerPrivateMessage),
}

pub enum MessageResponse {
    Success,
    Fail,
}
