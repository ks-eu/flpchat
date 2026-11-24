use tokio::sync::mpsc;
use sqlx::PgPool;

use crate::users::registration::{RegRequest, RegResponse};

mod registration;

pub enum UserRequest {
    RegRequest(RegRequest),
}

pub struct UserHandler {
    pool: PgPool,
    rx: mpsc::Receiver<UserRequest>,
}

impl UserHandler {
    pub fn new(
        pool: PgPool,
        rx: mpsc::Receiver<UserRequest>,
    ) -> Self {
        Self {
            pool,
            rx,
        }
    }

    pub async fn run(mut self) {
        while let Some(request) = self.rx.recv().await {
            match request {
                UserRequest::RegRequest(request) => {
                    todo!()
                }
            }
        }
    }

    async fn insert_user(
        &mut self,
        username: String,
        hashed_password: String,
    ) -> RegResponse {
        todo!()
    }
}

