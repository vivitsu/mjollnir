use serde::{Deserialize, Serialize};

use crate::{
    handle::Handle,
    message::{Message, MessageBody, MessagePayload},
    node::Node,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EchoOkPayload {
    echo: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EchoPayload {
    echo: String,
}

impl Handle for EchoPayload {
    fn handle(&self, node: &Node, req: &Message) -> Option<Message> {
        let msg_id = req.body.msg_id.unwrap();
        let echo = self.echo.clone();
        let payload = MessagePayload::EchoOk(EchoOkPayload { echo });
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
