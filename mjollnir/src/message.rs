use serde::{Deserialize, Serialize};

use crate::{
    echo::{EchoOkPayload, EchoPayload},
    init::InitPayload,
    unique_id::{GenerateOkPayload, GeneratePayload},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum MessagePayload {
    Init(InitPayload),
    InitOk,
    Echo(EchoPayload),
    EchoOk(EchoOkPayload),
    Generate(GeneratePayload),
    GenerateOk(GenerateOkPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBody {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: MessagePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}
