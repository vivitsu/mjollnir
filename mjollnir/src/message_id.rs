use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Display, Copy, Clone, Serialize, Deserialize)]
#[display("{}", self.0)]
pub struct MessageId(usize);

impl MessageId {
    pub fn init() -> Self {
        Self(0)
    }

    pub fn inc(&mut self) {
        self.0 += 1;
    }
}

impl From<usize> for MessageId {
    fn from(msg_id: usize) -> Self {
        MessageId(msg_id)
    }
}

impl From<MessageId> for usize {
    fn from(msg_id: MessageId) -> usize {
        msg_id.0
    }
}
