use crate::{message::Message, node::Node};

pub trait Handle {
    fn handle(&self, node: &mut Node, req: &Message) -> Option<Message>;
}
