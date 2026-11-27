use std::error::Error;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::{mpsc, Semaphore};
use crate::users::registration::{RegRequest, RegResponse};
use crate::users::remove::{RemoveSelfRequest, RemoveSelfResponse};
use crate::users::login::{LoginRequest, LoginResponse};
use crate::database::hash::verify_password;

pub enum UserRequest {
    RegRequest(RegRequest),
    RemoveSelfRequest(RemoveSelfRequest),
    LoginRequest(LoginRequest),
}

enum VerifyResult {
    Success(String),
    WrongCredentials,
    UsernameNotFound,
}

enum VerifyError {
    SqlxError,
    Argon2Error,
    JoinError,
    PermitError,
}

pub struct UserHandler {
    pool: PgPool,
    rx: mpsc::Receiver<UserRequest>,
    permit: Arc<Semaphore>
}

impl UserHandler {
    pub fn new(pool: PgPool, rx: mpsc::Receiver<UserRequest>, permit: Arc<Semaphore>) -> Self {
        Self { pool, rx, permit }
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
                UserRequest::RemoveSelfRequest(request) => {
                    let hashed = match self.verify_credentials(
                        request.username(),
                        request.password(),
                    ).await {
                        Ok(VerifyResult::Success(hashed)) => hashed,
                        Ok(VerifyResult::WrongCredentials) => {
                            let _ = request.try_send(RemoveSelfResponse::WrongCredentials);
                            continue
                        }
                        Ok(VerifyResult::UsernameNotFound) => {
                            let _ = request.try_send(RemoveSelfResponse::UserNotFound);
                            continue
                        }
                        Err(_) => {
                            let _ = request.try_send(RemoveSelfResponse::HandlerError);
                            continue
                        }
                    };
                    let response = self
                        .removeself_user(
                            request.username(),
                            &hashed,
                        ).await;
                    let _ = request.try_send(response);
                }
                _ => continue,
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

    async fn removeself_user(&self, username: &str, hashed_password: &str) -> RemoveSelfResponse {
        let removal = sqlx::query_as::<_, (Option<bool>, Option<i32>)>(include_str!(
            "queries/removeself_user.sql"
        ))
        .bind(username)
        .bind(hashed_password)
        .fetch_one(&self.pool)
        .await;

        match removal {
            Ok((None, None)) => RemoveSelfResponse::UserNotFound,
            Ok((Some(false), None)) => RemoveSelfResponse::WrongCredentials,
            // The users.user_id CHECK constraint enforces 1..=65535.
            Ok((Some(true), Some(user_id))) => RemoveSelfResponse::Success(user_id as u16),
            _ => RemoveSelfResponse::HandlerError,
        }
    }

    async fn verify_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> Result<VerifyResult, VerifyError> {
        let hashed_password = match self.get_hashed_password(username).await {
            Ok(Some(hash)) => hash,
            Ok(None) => {
                return Ok(VerifyResult::UsernameNotFound)
            }
            Err(_) => {
                return Err(VerifyError::SqlxError)
            }
        };

        let permit = match self.permit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return Err(VerifyError::PermitError)
            }
        };
        let password = password.to_owned();
        let verify = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match verify_password(&password, &hashed_password) {
                Ok(true) => Ok(VerifyResult::Success(hashed_password)),
                Ok(false) => Ok(VerifyResult::WrongCredentials),
                Err(_) => Err(VerifyError::Argon2Error),
            }
        }).await;

        match verify {
            Ok(result) => result,
            Err(_) => Err(VerifyError::JoinError),
        }
    }

    async fn get_hashed_password(
        &self,
        username: &str,
    ) -> Result<Option<String>, sqlx::Error> {
        let hash: Option<String> = sqlx::query_scalar(
            "SELECT password_hash FROM users WHERE username = $1"
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        Ok(hash)
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
