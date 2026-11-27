use super::LimId;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

/// Serves ID requests until the channel closes, the clock is invalid, or IDs run out.
pub async fn run(rx: mpsc::Receiver<oneshot::Sender<LimId>>, previous_id: LimId) {
    run_with_clock(rx, previous_id, || {
        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        u64::try_from(elapsed.as_millis()).ok()
    })
    .await;
}

pub(crate) async fn run_with_clock(
    mut rx: mpsc::Receiver<oneshot::Sender<LimId>>,
    previous_id: LimId,
    mut now_ms: impl FnMut() -> Option<u64>,
) {
    let mut previous = previous_id.id;
    while let Some(reply) = rx.recv().await {
        let Some(first_this_ms) = now_ms().and_then(|ms| LimId::from_parts(ms, 0)) else {
            break;
        };
        let Some(successor) = previous.checked_add(1) else {
            break;
        };
        let id = successor.max(first_this_ms.id);
        if id == u64::MAX {
            break;
        }

        previous = id;
        let _ = reply.send(LimId { id });
    }
}
