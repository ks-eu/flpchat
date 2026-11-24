use std::sync::Arc;

use tokio::sync::{
    RwLock,
    mpsc,
};
use tokio::net::TcpListener;
use tokio_util::codec::{Framed};
use futures_util::{StreamExt, SinkExt};

use flpc::tokio::{FLPCodec, FLPCMsg};

use crate::users::{
    UserState,
    try_set_sender,
    try_remove,
    try_get_userjoin,
    try_mark_onboarded,
};
use crate::login::accept::{
    JoinOutcome,
    handle_join,
    send_joinfail,
    send_userjoin_buffer,
    send_joinsuccess,
};
use crate::message::event::{
    ServerEvent,
    ClientEvent,
};
use crate::message::route::{
    route_message,
    route_connected,
    route_disconnected,
};

mod login;
mod error;
mod users;
mod message;

const MAXUSERS: u16 = 256;
const MAXUSERNAMESIZE: u8 = 64;
const PORT: &str = "127.0.0.1:7878";
const MAINCAPACITY: usize = 2048;
const CLIENTCAPACITY: usize = 128;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let users = Arc::new(RwLock::new(UserState::new(MAXUSERS as usize)?));
    let listener = TcpListener::bind(PORT).await?;
    let (main_tx, mut main_rx) = mpsc::channel::<ClientEvent>(MAINCAPACITY);

    // Login Handling
    let users_accept = users.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(stream) => stream,
                Err(_) => break,
            };
            
            // Handles initial login, onboarding and disconnects
            let users_conn = users_accept.clone();
            let client_tx = main_tx.clone();
            tokio::spawn(async move {
                let mut stream = Framed::new(stream, FLPCodec::new());
                let users_initconn = users_conn.clone();

                // if successful, the id and username are added to the hashmap
                let id = match handle_join(
                    users_initconn,
                    &mut stream,
                    MAXUSERNAMESIZE,
                ).await {
                    JoinOutcome::Success(id) => id,
                    JoinOutcome::ClientFail => {
                        let _ = send_joinfail(&mut stream, 0).await;
                        return;
                    }  
                    JoinOutcome::TcpFail => return,
                }; 

                let (writer_tx, mut writer_rx) = mpsc
                    ::channel
                    ::<ServerEvent>(CLIENTCAPACITY);

                let users_setsender = users_conn.clone();
                if let Err(_) = try_set_sender(
                    users_setsender,
                    id,
                    writer_tx
                ).await {
                    let _ = send_joinfail(&mut stream, 0).await;
                    let _ = try_remove(users_conn, id).await;
                    return;
                }


                let users_userjoin_buffer = users_conn.clone();
                let usernames = match try_get_userjoin(
                    users_userjoin_buffer,
                ).await {
                    Ok(usernames) => usernames,
                    Err(_) => {
                        let _ = send_joinfail(&mut stream, 0).await;
                        let _ = try_remove(users_conn, id).await;
                        return;
                    }
                };

                if let Err(_) = send_userjoin_buffer(
                    &mut stream,
                    usernames,
                ).await {
                    let _ = send_joinfail(&mut stream, 0).await;
                    let _ = try_remove(users_conn, id).await;
                    return;
                }
                
                if let Err(_) = client_tx
                    .send(ClientEvent::Connected(id))
                    .await {
                        let _ = send_joinfail(&mut stream, 0).await;
                        let _ = try_remove(users_conn, id).await;
                        return;
                } 

                if try_mark_onboarded(users_conn.clone(), id).await.is_err()
                    || send_joinsuccess(&mut stream).await.is_err()
                {
                    let _ = client_tx
                        .send(ClientEvent::Disconnected(id))
                        .await;
                    let _ = send_joinfail(&mut stream, 0).await;
                    let _ = try_remove(users_conn, id).await;
                    return;
                }

                let (mut sink, mut stream) = stream.split();
                
                let mut writer = tokio::spawn(async move {
                   while let Some(msg) = writer_rx.recv().await {
                       if let Err(_) = sink.send(msg.into()).await {
                           break;
                       }
                   }
                   return
                });
                
                let reader_tx = client_tx.clone();
                let mut reader = tokio::spawn(async move {
                    while let Some(msg) = stream.next().await {
                        match msg {
                            Ok(FLPCMsg::Message(message)) => {
                                if let Err(_) = reader_tx.send(
                                    ClientEvent::Message(message, id)
                                ).await {
                                    break;
                                }
                            }
                            Ok(_) => break,
                            Err(_) => break,
                        }
                    }
                    return
                });


                tokio::select! {
                    result = &mut reader => {
                        match result {
                            Ok(()) => {
                                println!("Stream Closed)");
                            }
                            Err(e) => {
                                println!("Stream Closed, Error {e}");
                            }
                        }
                        let _ = writer.abort();
                        let _ = writer.await;
                    }

                    result = &mut writer => {
                        match result {
                            Ok(()) => {
                                println!("Stream Closed)");
                            }
                            Err(e) => {
                                println!("Stream Closed, Error {e}");
                            }
                        }
                        let _ = reader.abort();
                        let _ = reader.await;
                    }
                }

                let _ = client_tx.send(ClientEvent::Disconnected(id)).await;
                let _ = try_remove(users_conn, id).await;

                return
            });
        }
    });

    while let Some(event) = main_rx.recv().await {
        match event {
            ClientEvent::Message(message, sender_id) => {
                route_message(Arc::clone(&users), message, sender_id).await;
            }
            ClientEvent::Connected(id) => {
                route_connected(Arc::clone(&users), id).await;
            }
            ClientEvent::Disconnected(id) => {
                route_disconnected(Arc::clone(&users), id).await;
            }
        }
    }

    Ok(())
}
