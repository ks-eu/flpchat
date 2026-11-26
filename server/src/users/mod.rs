use sqlx::{PgExecutor, PgPool};
use tokio::sync::mpsc;

use crate::users::registration::{RegRequest, RegResponse};

mod registration;

pub enum UserRequest {
    RegRequest(RegRequest),
}

pub enum RemoveResponse {
    Success,
    UserNotFound,
    HandlerError,
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
                    let response = Self::insert_user(
                        &self.pool,
                        request.username(),
                        request.hashed_password(),
                    )
                    .await;
                    let _ = request.try_send(response);
                }
            }
        }
    }

    async fn insert_user<'e>(
        executor: impl PgExecutor<'e>,
        username: &str,
        hashed_password: &str,
    ) -> RegResponse {
        let registration =
            sqlx::query_as::<_, (bool, Option<i32>)>(include_str!("queries/insert_user.sql"))
                .bind(username)
                .bind(hashed_password)
                .fetch_one(executor)
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
}

pub async fn remove_user(pool: &PgPool, user_id: u16) -> RemoveResponse {
    let removal = sqlx::query(include_str!("queries/remove_user.sql"))
        .bind(i32::from(user_id))
        .execute(pool)
        .await;

    match removal {
        Ok(result) if result.rows_affected() == 1 => RemoveResponse::Success,
        Ok(_) => RemoveResponse::UserNotFound,
        Err(_) => RemoveResponse::HandlerError,
    }
}

#[cfg(test)]
#[path = "../../tests/users/insert_user.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/users/transaction_pool.rs"]
mod transaction_pool;

#[cfg(test)]
#[path = "../../tests/users/remove_user.rs"]
mod remove_user_tests;

#[cfg(test)]
#[path = "../../tests/users/user_handler.rs"]
mod user_handler_tests;

#[cfg(test)]
#[path = "../../tests/users/query_performance.rs"]
mod query_performance_tests;
