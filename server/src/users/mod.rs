use sqlx::PgPool;
use tokio::sync::mpsc;

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
    pub fn new(pool: PgPool, rx: mpsc::Receiver<UserRequest>) -> Self {
        Self { pool, rx }
    }

    pub async fn run(mut self) {
        while let Some(request) = self.rx.recv().await {
            match request {
                UserRequest::RegRequest(request) => {
                    let response = self
                        .insert_user(
                            request.username(),
                            request.hashed_password(),
                        )
                        .await;
                    let _ = request.try_send(response);
                }
            }
        }
    }

    async fn insert_user(
        &self,
        username: &str,
        hashed_password: &str,
    ) -> RegResponse {
        let registration = sqlx::query_as::<_, (bool, Option<i32>)>(
            "WITH username_check AS (
                 SELECT EXISTS (
                     SELECT 1 FROM users WHERE username = $1
                 ) AS username_exists
             ), claimed_id AS (
                 DELETE FROM available_user_ids
                 WHERE user_id = (
                     SELECT user_id FROM available_user_ids
                     WHERE NOT (SELECT username_exists FROM username_check)
                     ORDER BY user_id LIMIT 1 FOR UPDATE
                 )
                 RETURNING user_id
             ), inserted_user AS (
                 INSERT INTO users (user_id, username, password_hash)
                 OVERRIDING SYSTEM VALUE
                 SELECT user_id, $1, $2 FROM claimed_id
                 RETURNING user_id, username
             ), inserted_profile AS (
                 INSERT INTO user_information (user_id, username, user_created)
                 SELECT user_id, username, date_trunc('milliseconds', statement_timestamp())
                 FROM inserted_user
                 RETURNING user_id
             )
             SELECT username_exists, (SELECT user_id FROM inserted_profile)
             FROM username_check",
        )
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
}
