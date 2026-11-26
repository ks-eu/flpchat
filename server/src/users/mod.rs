use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::users::registration::{RegRequest, RegResponse};
use crate::users::remove::{RemoveRequest, RemoveResponse};

mod registration;
mod remove;

pub enum UserRequest {
    RegRequest(RegRequest),
    RemoveRequest(RemoveRequest),
}

pub struct UserHandler {
    pool: PgPool,
    rx: mpsc::Receiver<UserRequest>,
}

impl UserHandler {
    pub fn new(pool: PgPool, rx: mpsc::Receiver<UserRequest>) -> Self {
        Self { pool, rx }
    }

    pub async fn run(mut self) {
        while let Some(request) = self.rx.recv().await {
            match request {
                UserRequest::RegRequest(request) => {
                    let response = self
                        .insert_user(request.username(), request.hashed_password())
                        .await;
                    let _ = request.try_send(response);
                }
                UserRequest::RemoveRequest(_) => {
                    todo!();
                }
            }
        }
    }

    async fn insert_user(&self, username: &str, hashed_password: &str) -> RegResponse {
        let registration =
            sqlx::query_as::<_, (bool, Option<i32>)>(include_str!("queries/insert_user.sql"))
                .bind(username)
                .bind(hashed_password)
                .fetch_one(&self.pool)
                .await;

        match registration {
            Ok((true, _)) => RegResponse::UsernameExists,
            Ok((false, None)) => RegResponse::NotEnoughSpace,
            Ok((false, Some(user_id))) => {
                // The users.user_id CHECK constraint enforces 1..=65535.
                RegResponse::Success(user_id as u16)
            }
            Err(sqlx::Error::Database(error))
                if error.is_unique_violation()
                    && error.constraint() == Some("users_username_key") =>
            {
                RegResponse::UsernameExists
            }
            Err(_) => RegResponse::HandlerError,
        }
    }

    async fn removeself_user(&self, username: &str, hashed_password: &str) -> RemoveResponse {
        let removal =
            sqlx::query_as::<_, (Option<bool>, bool)>(include_str!("queries/removeself_user.sql"))
                .bind(username)
                .bind(hashed_password)
                .fetch_one(&self.pool)
                .await;

        match removal {
            Ok((None, false)) => RemoveResponse::UserNotFound,
            Ok((Some(false), false)) => RemoveResponse::WrongCredentials,
            Ok((Some(true), true)) => RemoveResponse::Success,
            _ => RemoveResponse::HandlerError,
        }
    }
}

#[cfg(test)]
#[path = "../../tests/users/insert_user.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/users/transaction_pool.rs"]
mod transaction_pool;

#[cfg(test)]
#[path = "../../tests/users/removeself_user.rs"]
mod removeself_user_tests;

#[cfg(test)]
#[path = "../../tests/users/user_handler.rs"]
mod user_handler_tests;

#[cfg(test)]
#[path = "../../tests/users/query_performance.rs"]
mod query_performance_tests;
