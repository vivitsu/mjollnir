use serde::{Deserialize, Serialize};

use crate::{
    handle::Handle,
    message::{Message, MessageBody, MessagePayload},
    node::Node,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

impl Handle for InitPayload {
    fn handle(&self, node: &Node, req: &Message) -> Option<Message> {
        let msg_id = req.body.msg_id.unwrap();
        let resp = Message {
            src: node.id(),
            dest: req.src.clone(),
            body: MessageBody {
                msg_id: Some(node.msg_id().into()),
                in_reply_to: Some(msg_id),
                payload: MessagePayload::InitOk,
            },
        };
        Some(resp)
    }
}
