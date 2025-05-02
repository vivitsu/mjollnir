use std::io::{stdin, stdout};

use node::Node;

mod message;
mod message_id;
mod node;

fn main() -> anyhow::Result<()> {
    let stdin = stdin().lock();
    let mut stdout = stdout().lock();
    let mut node = Node::new();
    node.run(stdin, &mut stdout)?;
    Ok(())
}
