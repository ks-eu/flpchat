//! basic spec, 64 bit id? just do:
//! - 6 bytes [timestamp]
//! - 2 bytes [id]

pub struct LimId {
    id: u64,
}

impl LimId {
    pub const MAX_TIMESTAMP_MS: u64 = (1 << 48) - 1;

    pub fn from_parts(timestamp_ms: u64, sequence: u16) -> Option<Self> {
        if timestamp_ms > Self::MAX_TIMESTAMP_MS {
            return None;
        }

        let id = (timestamp_ms << 16) | u64::from(sequence);
        Some(Self { id })
    }

    pub fn timestamp_ms(self) -> u64 {
        self.id >> 16
    }

    pub fn to_be_bytes(self) -> [u8; 8] {
        self.id.to_be_bytes()
    }
}
