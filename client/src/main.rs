use std::io;
use std::collections::HashMap;

use crossterm::execute;
use crossterm::terminal::{
    enable_raw_mode,
    disable_raw_mode,
    EnterAlternateScreen,
    LeaveAlternateScreen,
};

use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use tokio::sync::mpsc;
use tokio::net::TcpStream;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Framed;
use futures_util::{SinkExt, StreamExt};

use flpc::header::*;
use flpc::tokio::{FLPCodec, FLPCMsg};
use flpc::error::ProtocolError;

mod chatwindow;
mod input;
mod login;

use login::{LoginWindow, LoginStatus};
use chatwindow::ChatWindow;
use input::{
    InputEvent,
    keypress_sender,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    // Initial Terminal Setup
    enable_raw_mode()?;

    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    
    let mut connection: Option<Framed<TcpStream, FLPCodec>> = None;
    let mut users: HashMap<u16, String> = HashMap::new();

    let mut loginwindow = LoginWindow::new("127.0.0.1:7878", 16);
    let (send, mut recv) = mpsc::channel::<InputEvent>(100);
    let input_send = send.clone();
    let login_key_task  = keypress_sender(input_send);

    terminal.draw(|frame| {
        loginwindow.render(frame);
    })?;
    loop {
		match recv.recv().await {
			Some(InputEvent::KeyStroke(c)) => {
				loginwindow.add_input(c);
			}

			Some(InputEvent::BackSpace) => {
				loginwindow.remove_input();
			}

			Some(InputEvent::Enter) => {
                let username = loginwindow.send_input();
                let stream = match
                    TcpStream::connect(loginwindow.server()).await
                {
                    Ok(stream) => stream,
                    Err(_) => {
                        loginwindow.set_fail();
                        break;
                    }
                };

                connection = Some(Framed::new(stream, FLPCodec::new()));
                
                let framed = match connection.as_mut() {
                    Some(framed) => framed,
                    _ => {
                        loginwindow.set_fail();
                        break;
                    }
                };

                let join = match Join::new(&username) {
                    Ok(join) => join,
                    Err(_) => {
                        loginwindow.set_fail();
                        break;
                    }
                };

                if framed.send(FLPCMsg::Join(join)).await.is_err() {
                    loginwindow.set_fail();
                    continue;
                }

                loop {
                    match framed.next().await {
                        Some(Ok(FLPCMsg::UserJoin(join))) => {
                            users.insert(join.uuid(), join.username().to_owned());
                        }
                        Some(Ok(FLPCMsg::UserLeft(_))) => users.clear(),
                        Some(Ok(FLPCMsg::JoinSuccess(_))) => {
                            loginwindow.set_success();
                            break;
                        }
                        _ => {
                            users.clear();
                            loginwindow.set_fail();
                            break;
                        }
                    }
                }
                break;
            }
			

			Some(InputEvent::Exit) => {
				loginwindow.set_quit();
                break;
			}

			Some(InputEvent::Error) => {
                loginwindow.set_quit();
                break;
			}

            Some(_) => {
                continue;
            }
            
			None => {
                loginwindow.set_quit();
                break;
		    }
        }

        terminal.draw(|frame| {
            loginwindow.render(frame);
        })?;
    }
    
    drop(send);
    drop(recv);
    drop(login_key_task);

    match loginwindow.status() {
        LoginStatus::Success => {
            drop(loginwindow)
        },
        _ => {
        drop(loginwindow);
        disable_raw_mode()?;

        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen
        )?;

        return Ok(());
        }
    }

    // Chat Window - Main Program
    let mut chatwindow = ChatWindow::new(String::from("You"), 256, 200);
    let (send, mut recv) = mpsc::channel::<InputEvent>(512);
    let input_send = send.clone();
    let _ = keypress_sender(input_send);
    let framed = match connection.take() {
        Some(framed) => framed,
        _ => {
        disable_raw_mode()?;

        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen
        )?;

        return Ok(());
        }
    };
    let (mut writer, mut reader) = framed.split(); 

    let read_send = send.clone();
    tokio::spawn(async move {
        while let Some(result) = reader.next().await {
            let event = match result {
                Ok(FLPCMsg::ClientMessage(msg)) => {
                    InputEvent::Message(msg.uuid(), msg.msg().to_owned())
                }
                Ok(FLPCMsg::UserJoin(join)) => {
                    InputEvent::UserJoin(join.uuid(), join.username().to_owned())
                }
                Ok(FLPCMsg::UserLeft(left)) => InputEvent::UserLeft(left.uuid()),
                Err(ProtocolError::IoError(_)) => break,
                _ => continue,
            };
            if read_send.send(event).await.is_err() {
                break;
            }
        }
    });
                        
    terminal.draw(|frame| {
        chatwindow.render(frame, &users);
    })?;

    loop {
		match recv.recv().await {
			Some(InputEvent::KeyStroke(c)) => {
				chatwindow.add_input(c);
			}
			Some(InputEvent::BackSpace) => {
				chatwindow.remove_input();
			}
			Some(InputEvent::Enter) => {
				let output = chatwindow.send_input();
                let msg = format!("{}> {}", chatwindow.username(), &output);
                let message = match flpc::header::Message::new(&output) {
                    Ok(message) => message,
                    Err(_) => continue,
                };

                if writer.send(FLPCMsg::Message(message)).await.is_err() {
                    break;
                }
                chatwindow.add_message(msg);

			}
			Some(InputEvent::Exit) => {
				chatwindow.quit();
				break;
			}
			Some(InputEvent::Error) => {
				break;
			}
            Some(InputEvent::UserJoin(uuid, username)) => {
                if users.get(&uuid) != Some(&username) {
                    chatwindow.add_message(format!("<user joined: {username}>"));
                    users.insert(uuid, username);
                }
            }
            Some(InputEvent::UserLeft(uuid)) => {
                if let Some(username) = users.remove(&uuid) {
                    chatwindow.add_message(format!("<user left: {username}>"));
                }
            }
            Some(InputEvent::Message(uuid, message)) => {
                if let Some(username) = users.get(&uuid) {
                    chatwindow.add_message(format!("{username}> {message}"));
                } else {
                    chatwindow.add_unknown_message(uuid, message);
                }
            }
			None => break,
		}

        terminal.draw(|frame| {
            chatwindow.render(frame, &users);
        })?;
        
        if chatwindow.state() {
            break;
        }
    }

    disable_raw_mode()?;

    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen
    )?;

    Ok(())
}
