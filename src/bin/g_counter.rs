use mjollnir::*;

use anyhow::Context;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{StdoutLock, Write},
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    CounterInit,
    CounterUpdate { delta: usize },
}

enum SignalPayload {
    CounterInit,
    CounterUpdate,
}

const SEQ_KV_NODE: &'static str = "seq-kv";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum SeqKvPayload<T> {
    Read {
        key: String,
    },
    ReadOk {
        value: T,
    },
    Write {
        key: String,
        value: T,
    },
    Cas {
        key: String,
        from: T,
        to: T,
        #[serde(
            default,
            rename = "create_if_not_exists",
            skip_serializing_if = "is_ref_false"
        )]
        create: bool,
    },
    CasOk {},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeqKvMessage<SeqKvPayload> {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: SeqKvBody<SeqKvPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeqKvBody<SeqKvPayload> {
    payload: SeqKvPayload,
}

impl<SeqKvPayload> SeqKvMessage<SeqKvPayload> {
    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        SeqKvPayload: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("serialize response to generate")?;
        output.write_all(b"\n").context("write trailing newline")?;

        Ok(())
    }
}

struct CounterNode {
    node_id: String,
    deltas: VecDeque<usize>,
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
                    let counter_init = SeqKvPayload::Cas {
                        key: "counter".to_string(),
                        from: 0,
                        to: 0,
                        create: true,
                    };
                    SeqKvMessage {
                        src: self.node_id.clone(),
                        dst: SEQ_KV_NODE.to_string(),
                        body: SeqKvBody {
                            payload: counter_init,
                        },
                    }
                    .send(&mut *output)
                    .context("initalize counter")?;
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
