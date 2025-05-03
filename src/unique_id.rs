use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{
    handle::Handle,
    message::{Message, MessageBody, MessagePayload},
    node::Node,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GenerateOkPayload {
    id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GeneratePayload;

impl Handle for GeneratePayload {
    fn handle(&self, node: &mut Node, req: &Message) -> Option<Message> {
        let msg_id = req.body.msg_id.unwrap();
        let now = SystemTime::now();
        let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        node.msg_id().inc();
        let id = format!("{}-{}-{}", duration, node.id(), msg_id);
        let payload = MessagePayload::GenerateOk(GenerateOkPayload { id });
        let resp = Message {
            src: node.id(),
            dest: req.src.clone(),
            body: MessageBody {
                msg_id: Some(node.msg_id().into()),
                in_reply_to: Some(msg_id),
                payload,
            },
        };
        Some(resp)
    }
}
