use tokio::sync::oneshot;

pub enum LoginResponse {
    Success(u16),
    UserNotFound,
    WrongCredentials,
    HandlerError,
}

pub struct LoginRequest {
    username: String,
    password: String,
    response_tx: oneshot::Sender<LoginResponse>,
}

impl LoginRequest {
    pub fn new(
        username: String,
        password: String,
        response_tx: oneshot::Sender<LoginResponse>,
    ) -> Self {
        Self {
            username,
            password,
            response_tx,
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn try_send(
        self,
        response: LoginResponse,
    ) -> Result<(), LoginResponse> {
        self.response_tx.send(response)
    }
}
