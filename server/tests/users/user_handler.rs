use std::{sync::Arc, time::Duration};

use argon2::{
    Argon2, PasswordHasher,
    password_hash::{PasswordVerifier, phc::PasswordHash},
};
use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register, ServerUnit},
};
use futures_util::StreamExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::database::serial_tests::postgres_test;
use crate::users::registration::handle_register;

use super::{
    PgPool, RegRequest, RegResponse, RemoveSelfRequest, RemoveSelfResponse, UserHandler,
    UserRequest, transaction_pool::transaction_pool,
};

postgres_test! {
    async fn registration_persists_account_and_profile_through_worker(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let username = "loopback-user";
        let password = "café-password";
        let token = "registration-token";

        let frame = timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let peer = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            let (sink, _) = Framed::new(server, FlpcCodec).split();
            let mut peer = Framed::new(peer, FlpcCodec);

            let (user_tx, user_rx) = mpsc::channel(1);
            let permit = Arc::new(Semaphore::new(1));
            let worker = UserHandler::new(pool.clone(), user_rx, Arc::clone(&permit));
            let registration = handle_register(
                Register::new(token.into(), username.into(), password.into()),
                user_tx,
                permit,
                sink,
                token.into(),
                32,
                32,
            );
            tokio::join!(registration, worker.run());

            peer.next()
                .await
                .expect("server should send a registration frame")
                .expect("peer should decode the registration frame")
        })
        .await
        .expect("registration and worker shutdown should complete");
        let FlpcUnit::Server(ServerUnit::ServerRegister(response)) = frame else {
            panic!("expected ServerRegister, got {frame:?}");
        };
        assert_eq!(*response.outcome(), 0);

        let (user_id, encoded_hash) = sqlx::query_as::<_, (i32, String)>(
            "SELECT user_id, password_hash FROM users WHERE username = $1",
        )
        .bind(username)
        .fetch_one(&pool)
        .await?;
        let hash = PasswordHash::new(&encoded_hash).expect("stored password should be a PHC hash");
        Argon2::default()
            .verify_password(password.as_bytes(), &hash)
            .expect("stored hash should verify the submitted password");

        let profile_id =
            sqlx::query_scalar::<_, i32>("SELECT user_id FROM user_information WHERE username = $1")
                .bind(username)
                .fetch_one(&pool)
                .await?;
        assert_eq!(profile_id, user_id);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

fn queue_registration(
    user_tx: &mpsc::Sender<UserRequest>,
    username: &str,
    hash: &str,
) -> oneshot::Receiver<RegResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let request = RegRequest::new(username.into(), hash.into(), reply_tx);
    assert!(user_tx.try_send(UserRequest::RegRequest(request)).is_ok());
    reply_rx
}

fn queue_removal(
    user_tx: &mpsc::Sender<UserRequest>,
    username: &str,
    password: &str,
) -> oneshot::Receiver<RemoveSelfResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let request = RemoveSelfRequest::new(username.into(), password.into(), reply_tx);
    assert!(
        user_tx
            .try_send(UserRequest::RemoveSelfRequest(request))
            .is_ok()
    );
    reply_rx
}

postgres_test! {
    async fn worker_routes_registrations_and_all_removal_outcomes(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let password = "café🔑";
        let hash = Argon2::default().hash_password(password.as_bytes()).unwrap().to_string();
        let (user_tx, user_rx) = mpsc::channel(7);
        let registration = queue_registration(&user_tx, "target", &hash);
        let malformed_registration = queue_registration(&user_tx, "malformed", "invalid-phc");
        let missing = queue_removal(&user_tx, "missing", password);
        let wrong = queue_removal(&user_tx, "target", "wrong-password");
        let error = queue_removal(&user_tx, "malformed", password);
        let success = queue_removal(&user_tx, "target", password);
        let already_removed = queue_removal(&user_tx, "target", password);
        drop(user_tx);

        let worker = UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1)));
        timeout(Duration::from_secs(10), worker.run())
            .await
            .expect("worker should drain mixed requests and exit");

        let Ok(RegResponse::Success(registered_id)) = registration.await else {
            panic!("worker should register the account");
        };
        assert!(matches!(malformed_registration.await, Ok(RegResponse::Success(_))));
        assert!(matches!(missing.await, Ok(RemoveSelfResponse::UserNotFound)));
        assert!(matches!(wrong.await, Ok(RemoveSelfResponse::WrongCredentials)));
        assert!(matches!(error.await, Ok(RemoveSelfResponse::HandlerError)));
        assert!(matches!(
            success.await,
            Ok(RemoveSelfResponse::Success(removed_id)) if removed_id == registered_id
        ));
        assert!(matches!(already_removed.await, Ok(RemoveSelfResponse::UserNotFound)));

        let accounts = sqlx::query_as::<_, (String, String)>(
            "SELECT username, password_hash FROM users",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(accounts, [("malformed".to_owned(), "invalid-phc".to_owned())]);

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn worker_continues_after_registration_reply_receiver_is_dropped(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let (user_tx, user_rx) = mpsc::channel(2);
        drop(queue_registration(&user_tx, "first-user", "first-hash"));
        let reply = queue_registration(&user_tx, "second-user", "second-hash");

        let worker = UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1)));
        let ((), reply) = timeout(Duration::from_secs(5), async {
            tokio::join!(worker.run(), async move {
                let result = reply.await;
                drop(user_tx);
                result
            })
        })
        .await
        .expect("worker should process both registrations");
        assert!(matches!(reply, Ok(RegResponse::Success(_))));

        let accounts = sqlx::query_as::<_, (String, String)>(
            "SELECT username, password_hash FROM users ORDER BY username",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            accounts,
            [
                ("first-user".to_owned(), "first-hash".to_owned()),
                ("second-user".to_owned(), "second-hash".to_owned()),
            ],
        );

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}

postgres_test! {
    async fn worker_drains_queued_registration_before_shutting_down(pool: PgPool) -> sqlx::Result<()> {
        let pool = transaction_pool(&pool).await?;
        let (user_tx, user_rx) = mpsc::channel(1);
        let reply = queue_registration(&user_tx, "queued-user", "queued-hash");
        drop(user_tx);

        let worker = UserHandler::new(pool.clone(), user_rx, Arc::new(Semaphore::new(1)));
        let ((), reply) = timeout(Duration::from_secs(5), async {
            tokio::join!(worker.run(), reply)
        })
        .await
        .expect("worker should deliver its reply and exit");
        assert!(matches!(reply, Ok(RegResponse::Success(_))));

        sqlx::query("ROLLBACK").execute(&pool).await?;
        Ok(())
    }
}
