use crossterm::event::{
    Event,
    KeyCode,
    KeyEventKind,
    EventStream,
};

use tokio::sync::mpsc;
use futures_util::StreamExt;

pub enum InputEvent {
    KeyStroke(char),
    BackSpace,
    Enter,
    Exit,
    Message(u16, String),
    UserJoin(u16, String),
    UserLeft(u16),
    Error
}

fn keypress(event: Event) -> Option<InputEvent> {
    let Event::Key(key) = event else {
        return None;
    };

    if key.kind != KeyEventKind::Press {
        return None;
    }

    match key.code {
        KeyCode::Char(c) => Some(InputEvent::KeyStroke(c)),
        KeyCode::Backspace => Some(InputEvent::BackSpace),
        KeyCode::Enter => Some(InputEvent::Enter),
        KeyCode::Esc => Some(InputEvent::Exit),
        _ => None,
    }
}

pub fn keypress_sender(
    send: mpsc::Sender<InputEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut events = EventStream::new();
        while let Some(event) = events.next().await {
            let input_event = match event {
                Ok(event) => keypress(event),
                Err(_) => {
                    let _ = send.send(InputEvent::Error).await;
                    break;
                }
            };

            let Some(input_event) = input_event else {
                continue;
            };

            if send.send(input_event).await.is_err() {
                break;
            }
        }
    })
}


