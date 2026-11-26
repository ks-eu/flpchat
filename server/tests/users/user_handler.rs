use std::{sync::Arc, time::Duration};

use argon2::{
    Argon2,
    password_hash::{PasswordVerifier, phc::PasswordHash},
};
use flpc::{
    tokio::FlpcCodec,
    unit::{FlpcUnit, Register, ServerUnit},
};
use futures_util::StreamExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::database::serial_tests::postgres_test;

use super::{
    PgPool, UserHandler, registration::handle_register, transaction_pool::transaction_pool,
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
            let worker = UserHandler::new(pool.clone(), user_rx);
            let registration = handle_register(
                Register::new(token.into(), username.into(), password.into()),
                user_tx,
                Arc::new(Semaphore::new(1)),
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
