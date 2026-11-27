use std::error::Error;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::{mpsc, Semaphore};
use crate::connection::registration::{RegRequest, RegResponse};
use crate::connection::remove::{RemoveSelfRequest, RemoveSelfResponse};
use crate::connection::login::{LoginRequest, LoginResponse};
use crate::database::hash::{hash_password, verify_password};

pub enum UserRequest {
    RegRequest(RegRequest),
    RemoveSelfRequest(RemoveSelfRequest),
    LoginRequest(LoginRequest),
}

enum VerifyResult {
    Success(i32),
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
}

impl UserHandler {
    pub fn new(pool: PgPool, rx: mpsc::Receiver<UserRequest>) -> Self {
        Self { pool, rx }
    }

    pub async fn run(mut self, permit: Arc<Semaphore>) {
        while let Some(request) = self.rx.recv().await {
            match request {
                UserRequest::RegRequest(request) => {
                    self.insert_user(request, Arc::clone(&permit)).await;
                }
                UserRequest::RemoveSelfRequest(request) => {
                    self.removeself_user(request, Arc::clone(&permit)).await;
                }
                UserRequest::LoginRequest(request) => {
                    self.login_user(request, Arc::clone(&permit)).await; 
                }

                _ => continue,
            }
        }
    }

    async fn insert_user(&self, request: RegRequest, permit: Arc<Semaphore>) {
        // Hold the provisional account and ID claim in one transaction through hashing.
        let Ok(mut tx) = self.pool.begin_with("BEGIN ISOLATION LEVEL READ COMMITTED").await else {
            let _ = request.try_send(RegResponse::HandlerError);
            return;
        };

        let username = request.username();
        let response = async {
            // Check the username, claim an ID, and insert the provisional account and profile.
            let provisional = sqlx::query_as::<_, (bool, Option<i32>)>(include_str!(
                "queries/insert_user.sql"
            ))
            .bind(username)
            .fetch_one(&mut *tx)
            .await;
            let user_id = match provisional {
                Ok((true, _)) => return RegResponse::UsernameExists,
                Ok((false, None)) => return RegResponse::NotEnoughSpace,
                Ok((false, Some(user_id))) => user_id,
                Err(sqlx::Error::Database(error))
                    if error.is_unique_violation()
                        && error.constraint() == Some("users_username_key") =>
                {
                    return RegResponse::UsernameExists;
                }
                Err(_) => return RegResponse::HandlerError,
            };

            let Ok(hash) = hash_password(request.password(), permit).await else {
                return RegResponse::HandlerError;
            };

            // Replace the provisional marker with the generated password hash.
            match sqlx::query_scalar::<_, i32>(include_str!("queries/set_password_hash.sql"))
                .bind(user_id)
                .bind(hash)
                .fetch_one(&mut *tx)
                .await
            {
                // The users.user_id CHECK constraint enforces 1..=65535.
                Ok(user_id) => RegResponse::Success(user_id as u16),
                Err(_) => RegResponse::HandlerError,
            }
        }
        .await;

        // Commit a complete registration or roll back the account, profile, and ID claim.
        let finished = if matches!(&response, RegResponse::Success(_)) {
            tx.commit().await
        } else {
            tx.rollback().await
        };
        let _ = request.try_send(if finished.is_ok() {
            response
        } else {
            RegResponse::HandlerError
        });
    }

    async fn removeself_user(&self, request: RemoveSelfRequest, permit: Arc<Semaphore>) {
        let response = match self
            .verify_credentials(request.username(), request.password(), permit)
            .await
        {
            Ok(VerifyResult::Success(user_id)) => {
                match sqlx::query_scalar::<_, i32>(include_str!("queries/remove_user.sql"))
                    .bind(user_id)
                    .fetch_one(&self.pool)
                    .await
                {
                    // The users.user_id CHECK constraint enforces 1..=65535.
                    Ok(user_id) => RemoveSelfResponse::Success(user_id as u16),
                    Err(_) => RemoveSelfResponse::HandlerError,
                }
            }
            Ok(VerifyResult::WrongCredentials) => RemoveSelfResponse::WrongCredentials,
            Ok(VerifyResult::UsernameNotFound) => RemoveSelfResponse::UserNotFound,
            Err(_) => RemoveSelfResponse::HandlerError,
        };
        let _ = request.try_send(response);
    }

    async fn login_user(
        &self,
        request: LoginRequest,
        permit: Arc<Semaphore>,
    ) {
        let response = match self
            .verify_credentials(request.username(), request.password(), permit)
            .await
        {
            Ok(VerifyResult::Success(id)) => LoginResponse::Success(id as u16),
            Ok(VerifyResult::WrongCredentials) => LoginResponse::WrongCredentials,
            Ok(VerifyResult::UsernameNotFound) => LoginResponse::UserNotFound,
            Err(_) => LoginResponse::HandlerError,
        };

        let _ = request.try_send(response);
    }
            

    async fn verify_credentials(
        &self,
        username: &str,
        password: &str,
        permit: Arc<Semaphore>,
    ) -> Result<VerifyResult, VerifyError> {
        let (user_id, hashed_password) = match self.get_credentials(username).await {
            Ok(Some(credentials)) => credentials,
            Ok(None) => {
                return Ok(VerifyResult::UsernameNotFound)
            }
            Err(_) => {
                return Err(VerifyError::SqlxError)
            }
        };

        let permit = match permit.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return Err(VerifyError::PermitError)
            }
        };
        let password = password.to_owned();
        let verify = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match verify_password(&password, &hashed_password) {
                Ok(true) => Ok(VerifyResult::Success(user_id)),
                Ok(false) => Ok(VerifyResult::WrongCredentials),
                Err(_) => Err(VerifyError::Argon2Error),
            }
        }).await;

        match verify {
            Ok(result) => result,
            Err(_) => Err(VerifyError::JoinError),
        }
    }

    async fn get_credentials(&self, username: &str) -> Result<Option<(i32, String)>, sqlx::Error> {
        sqlx::query_as("SELECT user_id, password_hash FROM users WHERE username = $1")
            .bind(username)
            .fetch_optional(&self.pool)
            .await
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
