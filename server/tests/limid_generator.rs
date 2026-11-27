#[path = "../src/limid/mod.rs"]
mod limid;

mod limid_generator {
    use super::limid::{LimId, generator};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
        time::timeout,
    };

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn id(ms: u64, sequence: u16) -> LimId {
        LimId::from_parts(ms, sequence).unwrap()
    }

    async fn request(tx: &mpsc::Sender<oneshot::Sender<LimId>>) -> LimId {
        let (reply, response) = oneshot::channel();
        tx.send(reply).await.unwrap();
        response.await.unwrap()
    }

    async fn join(task: JoinHandle<()>) {
        timeout(TIMEOUT, task).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn uses_wall_clock_and_exits_when_senders_close() {
        let (tx, rx) = mpsc::channel(1);
        let task = tokio::spawn(generator::run(rx, id(0, 0)));
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let reply = timeout(TIMEOUT, request(&tx)).await.unwrap();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        assert!(
            (before.as_millis()..=after.as_millis()).contains(&u128::from(reply.timestamp_ms()))
        );
        drop(tx);
        join(task).await;
    }

    #[tokio::test]
    async fn sequences_across_equal_backward_and_newer_milliseconds() {
        let (tx, rx) = mpsc::channel(1);
        let mut timestamps = [100, 100, 99, 101].into_iter();
        let task = tokio::spawn(generator::run_with_clock(rx, id(100, 4), move || {
            timestamps.next()
        }));
        for expected in [id(100, 5), id(100, 6), id(100, 7), id(101, 0)] {
            let reply = timeout(TIMEOUT, request(&tx)).await.unwrap();
            assert_eq!(reply.to_be_bytes(), expected.to_be_bytes());
        }
        drop(tx);
        join(task).await;
    }

    #[tokio::test]
    async fn carries_and_consumes_the_id_for_a_dropped_reply() {
        let (tx, rx) = mpsc::channel(2);
        let task = tokio::spawn(generator::run_with_clock(rx, id(100, u16::MAX), || {
            Some(100)
        }));
        let (reply, response) = oneshot::channel();
        drop(response);
        tx.send(reply).await.unwrap();
        let next = timeout(TIMEOUT, request(&tx)).await.unwrap();
        assert_eq!(next.to_be_bytes(), id(101, 1).to_be_bytes());
        drop(tx);
        join(task).await;
    }

    #[tokio::test]
    async fn closes_without_issuing_the_maximum_id() {
        for sequence in [u16::MAX - 1, u16::MAX] {
            let (tx, rx) = mpsc::channel(2);
            let (reply, response) = oneshot::channel();
            let (queued_reply, queued_response) = oneshot::channel();
            tx.send(reply).await.unwrap();
            tx.send(queued_reply).await.unwrap();
            let previous = id(LimId::MAX_TIMESTAMP_MS, sequence);
            let task = tokio::spawn(generator::run_with_clock(rx, previous, || Some(100)));
            assert!(timeout(TIMEOUT, response).await.unwrap().is_err());
            assert!(timeout(TIMEOUT, queued_response).await.unwrap().is_err());
            join(task).await;
            assert!(tx.is_closed());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn performance() {
        timeout(TIMEOUT, async {
            for fixed in [false, true] {
                let (tx, rx) = mpsc::channel(256);
                let (task, label) = if fixed {
                    (
                        tokio::spawn(generator::run_with_clock(rx, id(100, 0), || Some(100))),
                        "fixed clock (same millisecond)",
                    )
                } else {
                    (tokio::spawn(generator::run(rx, id(0, 0))), "wall clock")
                };

                let mut previous = u64::from_be_bytes(request(&tx).await.to_be_bytes());
                let mut samples = Vec::with_capacity(1_000);
                for _ in 0..1_000 {
                    let start = Instant::now();
                    let next = request(&tx).await;
                    samples.push(start.elapsed());
                    let packed = u64::from_be_bytes(next.to_be_bytes());
                    assert!(packed > previous);
                    if fixed {
                        assert_eq!(packed >> 16, 100);
                        assert_eq!(packed, previous + 1);
                    }
                    previous = packed;
                }
                samples.sort_unstable();
                println!(
                    "{label}: single request p50 {:?}, p95 {:?}",
                    samples[499], samples[949]
                );

                const BURST: usize = 10_000;
                let mut responses = Vec::with_capacity(BURST);
                let start = Instant::now();
                for _ in 0..BURST {
                    let (reply, response) = oneshot::channel();
                    tx.send(reply).await.unwrap();
                    responses.push(response);
                }
                for response in responses {
                    let packed = u64::from_be_bytes(response.await.unwrap().to_be_bytes());
                    assert!(packed > previous);
                    previous = packed;
                }
                let elapsed = start.elapsed();
                println!(
                    "{label}: burst of {BURST} in {elapsed:?}, {:.0} IDs/sec",
                    BURST as f64 / elapsed.as_secs_f64()
                );
                drop(tx);
                join(task).await;
            }
        })
        .await
        .unwrap();
    }
}
