use tokio::sync::{oneshot, mpsc};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use futures_util::stream::{SplitStream, SplitSink};
use futures_util::{StreamExt, SinkExt};

use flpc::unit::{
    FlpcUnit,
    WrongUnit,
    ClientUnit,
};
use flpc::tokio::{CodecError, FlpcCodec};

use crate::connection::login::handle_login_verification;
use crate::connection::registration::handle_register;
use crate::connection::remove::handle_removeself;
use crate::database::users::UserRequest;

type FlpcStream = SplitStream<Framed<TcpStream, FlpcCodec>>;
type FlpcSink = SplitSink<Framed<TcpStream, FlpcCodec>, FlpcUnit>;


pub struct ConnectionHandler {
    listener: TcpListener,
    user_tx: mpsc::Sender<UserRequest>,
    token: String,
    max_len_username: usize,
    max_len_password: usize,
}

impl ConnectionHandler {
    pub fn new(
        listener: TcpListener,
        user_tx: mpsc::Sender<UserRequest>,
        token: &str,
        max_len_username: usize,
        max_len_password: usize,
    ) -> Self {
        Self {
            listener,
            user_tx,
            token: token.to_string(),
            max_len_username,
            max_len_password,
        }
    }

    pub async fn run(&self) {
        loop {
            let (stream, _) = match self.listener.accept().await {
                Ok(stream) => stream,
                Err(_) => break,
            };

            let (mut sink, mut stream) = Framed::new(
                stream,
                FlpcCodec,
            ).split::<FlpcUnit>();

            todo!()
        }
    }
}

async fn route_first(
    unit: FlpcUnit,
    user_tx: mpsc::Sender<UserRequest>,
    token: &str,
    max_len_username: usize,
    max_len_password: usize,
    mut sink: FlpcSink,
    mut stream: FlpcStream,
) {
    match unit {
        FlpcUnit::Client(ClientUnit::Login(request)) => {
            let Some((id, sink)) = handle_login_verification(
                request,
                user_tx,
                sink,
                max_len_username,
                max_len_password,
            ).await else {
                return;
            };
            todo!()
        }
        FlpcUnit::Client(ClientUnit::Register(request)) => {
            handle_register(
                request,
                user_tx,
                sink,
                token,
                max_len_username,
                max_len_password,
            ).await
        }
        FlpcUnit::Client(ClientUnit::RemoveSelf(request)) => {
            handle_removeself(
                request,
                user_tx,
                sink,
                max_len_username,
                max_len_password,
            ).await
        }
        _ => {
            let _ = send_wrongunit(sink);
        }
    }
}




async fn send_wrongunit(mut sink: FlpcSink) -> Result<(), CodecError> {
    sink.send(WrongUnit::new().into()).await
}
