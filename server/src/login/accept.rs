use std::collections::HashMap;
use std::sync::Arc;
use std::io::{self};

use tokio_util::codec::Framed;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use futures_util::{Sink, SinkExt, StreamExt};

use flpc::tokio::{FLPCodec, FLPCMsg};
use flpc::error::ProtocolError;
use flpc::header::{Join, JoinFail, JoinSuccess, UserJoin};

use crate::users::{
    ConnectedUser,
    UserState,
    try_insert,
    TryInsert,
};


enum CheckJoin {
    Success(Join),
    WrongMsgType,
    TcpError,
}

pub enum JoinOutcome {
    Success(u16),
    ClientFail,
    TcpFail,
}

pub async fn handle_join (
    users: Arc<RwLock<UserState>>,
    stream: &mut Framed<TcpStream, FLPCodec>,
    max_length: u8,
) -> JoinOutcome {
    let username = match check_join(stream, max_length).await {
        CheckJoin::Success(username) => username,
        CheckJoin::WrongMsgType => return JoinOutcome::ClientFail,
        CheckJoin::TcpError => return JoinOutcome::TcpFail,
        _ => unreachable!(),
    };
    
    match try_insert(users, username.username()).await {
        TryInsert::Success(id) => {
            return JoinOutcome::Success(id)
        }
        TryInsert::Fail(_) => {
            return JoinOutcome::ClientFail
        }
        _ => {
            unreachable!()
        }
    }
}

// Will add more detailed return messages
async fn check_join(
    stream: &mut Framed<TcpStream, FLPCodec>,
    max_length: u8,
) -> CheckJoin {
    match stream.next().await {
        Some(Ok(FLPCMsg::Join(username))) => {
            if check_length(&username, max_length) {
                return CheckJoin::Success(username)
            } else {
                return CheckJoin::WrongMsgType
            }
        }
        Some(Ok(_)) => {
            return CheckJoin::WrongMsgType
        }
        Some(Err(ProtocolError::UnknownHeader | ProtocolError::UTF8Error)) => {
            return CheckJoin::WrongMsgType
        }
        Some(Err(ProtocolError::IoError(_))) => {
            return CheckJoin::TcpError
        }
        Some(Err(_)) => {
            return CheckJoin::TcpError
        }
        None => {
            return CheckJoin::TcpError
        }
    }
}

 
fn check_length(
    username: &Join,
    max_length: u8,
) -> bool {
    if username.length() > max_length {
        return false
    } else {
        return true
    }
}


pub async fn send_joinfail<S>(
    sink: &mut S,
    failcode: u8,
) -> io::Result<()>
where
    S: Sink<FLPCMsg, Error = ProtocolError> + Unpin,
{
    sink
        .send(FLPCMsg::JoinFail(JoinFail::new(failcode)))
        .await
        .map_err(|error| match error {
            ProtocolError::IoError(error) => error,
            error => io::Error::other(format!(
                "failed to send JoinFail: {error:?}"
            )),
        })
}


pub async fn send_joinsuccess<S>(sink: &mut S) -> io::Result<()>
where
    S: Sink<FLPCMsg, Error = ProtocolError> + Unpin,
{
    sink
        .send(FLPCMsg::JoinSuccess(JoinSuccess {}))
        .await
        .map_err(|error| match error {
            ProtocolError::IoError(error) => error,
            error => io::Error::other(format!(
                "failed to send JoinSuccess: {error:?}"
            )),
        })
}


pub fn get_userjoin_buffer(
    users: &HashMap<u16, ConnectedUser>,
) -> Result<Vec<UserJoin>, ProtocolError> {
    users
        .iter()
        .filter(|(_, user)| user.is_onboarded())
        .map(|(&id, user)| UserJoin::new(user.username(), id))
        .collect()
}


pub async fn send_userjoin_buffer<S>(
    sink: &mut S,
    joins: Vec<UserJoin>,
) -> Result<(), ProtocolError>
where
    S: Sink<FLPCMsg, Error = ProtocolError> + Unpin,
{
    for join in joins {
        sink.feed(FLPCMsg::UserJoin(join)).await?;
    }

    sink.flush().await
}
