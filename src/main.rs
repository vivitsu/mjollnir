use std::io::{stdin, stdout};

use node::Node;

mod echo;
mod handle;
mod init;
mod message;
mod message_id;
mod node;
mod unique_id;

fn main() -> anyhow::Result<()> {
    let stdin = stdin().lock();
    let mut stdout = stdout().lock();
    let mut node = Node::new();
    node.run(stdin, &mut stdout)?;
    Ok(())
}
