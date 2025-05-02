use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct MessageId(usize);

impl MessageId {
    pub fn init() -> Self {
        Self { 0: 0 }
    }

    pub fn inc(&mut self) {
        self.0 = self.0 + 1;
    }

    pub fn into(self) -> usize {
        self.0
    }
}
