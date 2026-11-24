use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum UsersError {
    InvalidMaxCount {max_count: usize},
    AtCapacity,
    DuplicateUsername,
    UserNotFound {id: u16},
}

impl fmt::Display for UsersError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMaxCount {max_count} => write!(
                formatter,
                "user capacity {max_count} exceeds {}",
                u16::MAX
            ),
            Self::AtCapacity => write!(formatter, "user registry is full"),
            Self::DuplicateUsername => {
                write!(formatter, "username already exists")
            }
            Self::UserNotFound {id} => {
                write!(formatter, "user {id} not found")
            }
        }
    }
}

impl Error for UsersError {}
