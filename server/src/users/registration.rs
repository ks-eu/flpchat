use tokio::sync::oneshot;

pub enum RegResponse {
    Success(u16),
    NotEnoughSpace,
    UsernameExists,
    HandlerError,
}

pub struct RegRequest {
    username: String,
    hashed_password: String,
    response_tx: oneshot::Sender<RegResponse>,
}


impl RegRequest {
    pub fn new(
        username: String,
        hashed_password: String,
        response_tx: oneshot::Sender<RegResponse>,
    ) -> Self {
        Self {
            username,
            hashed_password,
            response_tx,
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn hashed_password(&self) -> &str {
        &self.hashed_password
    }
    
    pub fn try_send(
        self,
        response: RegResponse
    ) -> Result<(), RegResponse> {
        self.response_tx.send(response)
    }
}

