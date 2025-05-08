use crate::{message::Message, node::Node};

pub trait Handle {
    fn handle(&self, node: &Node, req: &Message) -> Option<Message>;
}
