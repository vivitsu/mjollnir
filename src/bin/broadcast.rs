use mjollnir::*;

use anyhow::Context;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

enum SignalPayload {
    Gossip,
}

struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,
    topology: HashMap<String, Vec<String>>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

impl Node<(), Payload, SignalPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, SignalPayload>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if let Err(_) = tx.send(Event::Signal(SignalPayload::Gossip)) {
                break;
            }
        });

        Ok(Self {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            topology: HashMap::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            neighborhood: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, SignalPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Signal(payload) => match payload {
                SignalPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known_to_n = &self.known[n];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                            .messages
                            .iter()
                            .copied()
                            .partition(|m| known_to_n.contains(m));
                        let mut rng = rand::thread_rng();
                        let additional_cap = (10 * already_known.len() / 100) as u32;
                        notify_of.extend(already_known.iter().filter(|_| {
                            rng.gen_ratio(
                                additional_cap.min(already_known.len() as u32),
                                already_known.len() as u32,
                            )
                        }));
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.messages.extend(seen);
                    }
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(output)?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(output)?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("No topology given for node {}", self.node));
                        self.topology = topology;
                        reply.body.payload = Payload::TopologyOk;
                        reply.send(output)?;
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, BroadcastNode, _, _>(())
}
