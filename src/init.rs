use serde::{Deserialize, Serialize};

use crate::{
    handle::Handle,
    message::{Message, MessageBody, MessagePayload},
    node::Node,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitPayload {
    node_id: String,
    node_ids: Vec<String>,
}

impl Handle for InitPayload {
    fn handle(&self, node: &mut Node, req: &Message) -> Option<Message> {
        let msg_id = req.body.msg_id.unwrap();
        node.set_id(self.node_id.clone());
        eprintln!("My node_id is {}", self.node_id);
        node.set_node_ids(self.node_ids.clone());
        eprintln!("Other node_ids in the cluster are: {:?}", self.node_ids);
        node.msg_id().inc();
        let resp = Message {
            src: self.node_id.clone(),
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
