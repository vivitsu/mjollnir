use mjollnir::*;

use anyhow::Context;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, io::StdoutLock, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },
    CounterInit,
    CounterUpdate,
    #[serde(rename = "read")]
    SeqKvRead {
        key: String,
    },
    #[serde(rename = "read_ok")]
    SeqKvReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    Cas {
        key: String,
        from: usize,
        to: usize,
        #[serde(
            default,
            rename = "create_if_not_exists",
            skip_serializing_if = "is_ref_false"
        )]
        create: bool,
    },
    CasOk {},
}

enum SignalPayload {
    CounterInit,
    CounterUpdate,
}

const SEQ_KV_NODE: &'static str = "seq-kv";

struct CounterNode {
    node_id: String,
    deltas: VecDeque<usize>,
    id: usize,
}

impl Node<(), Payload, SignalPayload> for CounterNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, SignalPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = Self {
            node_id: init.node_id,
            deltas: VecDeque::new(),
            id: 1,
        };

        tx.send(Event::Signal(SignalPayload::CounterInit))?;

        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(100..150);
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(jitter));
            if let Err(_) = tx.send(Event::Signal(SignalPayload::CounterUpdate)) {
                break;
            }
        });

        Ok(node)
    }

    fn step(
        &mut self,
        input: Event<Payload, SignalPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Signal(payload) => match payload {
                SignalPayload::CounterInit => {
                    let counter_init = Payload::Cas {
                        key: "counter".to_string(),
                        from: 0,
                        to: 0,
                        create: true,
                    };
                    Message {
                        src: self.node_id.clone(),
                        dst: SEQ_KV_NODE.to_string(),
                        body: Body {
                            payload: counter_init,
                            id: Some(self.id),
                            in_reply_to: None,
                        },
                    }
                    .send(&mut *output)
                    .context("initalize counter")?;
                    self.id += 1;
                }
                SignalPayload::CounterUpdate => {}
            },
            Event::Message(_) => todo!(),
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, CounterNode, _, _>(())
}

fn is_ref_false(b: &bool) -> bool {
    !*b
}
