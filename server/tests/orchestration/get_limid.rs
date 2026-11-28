use super::get_limid;
use crate::limid::{LimId, generator};
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn receives_ids_from_running_generator() {
    let (limid_tx, limid_rx) = mpsc::channel(1);
    let previous = LimId::from_parts(100, 4).unwrap();
    let worker = tokio::spawn(generator::run_with_clock(limid_rx, previous, || Some(100)));

    for sequence in [5, 6] {
        let id = timeout(TIMEOUT, get_limid(&limid_tx)).await.unwrap();
        let expected = LimId::from_parts(100, sequence).unwrap();
        assert_eq!(id.unwrap().to_be_bytes(), expected.to_be_bytes());
    }

    drop(limid_tx);
    timeout(TIMEOUT, worker).await.unwrap().unwrap();
}

#[tokio::test]
async fn propagates_recv_error_when_generator_exhausts_ids() {
    let (limid_tx, limid_rx) = mpsc::channel(1);
    let previous = LimId::from_parts(LimId::MAX_TIMESTAMP_MS, u16::MAX - 1).unwrap();
    let worker = tokio::spawn(generator::run_with_clock(limid_rx, previous, || Some(100)));

    let result = timeout(TIMEOUT, get_limid(&limid_tx)).await.unwrap();
    assert!(matches!(result, Err(error) if error.is::<oneshot::error::RecvError>()));
    timeout(TIMEOUT, worker).await.unwrap().unwrap();
}

#[tokio::test]
async fn propagates_send_error_when_request_channel_is_closed() {
    let (limid_tx, limid_rx) = mpsc::channel(1);
    drop(limid_rx);

    let result = timeout(TIMEOUT, get_limid(&limid_tx)).await.unwrap();
    assert!(matches!(
        result,
        Err(error) if error.is::<mpsc::error::SendError<oneshot::Sender<LimId>>>()
    ));
}
