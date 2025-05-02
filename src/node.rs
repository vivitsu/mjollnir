use anyhow::Context;
use std::io::{BufRead, Write};

use crate::{
    message::{Message, MessageBody, MessagePayload},
    message_id::MessageId,
};

pub struct Node {
    msg_id: MessageId,
    node_id: String,
    node_ids: Vec<String>,
}

impl Node {
    pub fn new() -> Self {
        let node_id = String::new();
        let node_ids = Vec::new();
        let msg_id = MessageId::init();
        Self {
            msg_id,
            node_id,
            node_ids,
        }
    }

    pub fn run<R: BufRead, W: Write>(&mut self, reader: R, mut writer: W) -> anyhow::Result<()> {
        eprintln!("Starting node...");
        for line in reader.lines() {
            let line = line.context("Input from reader could not be read")?;
            let req: Message = serde_json::from_str(&line)?;
            let resp = self.handle(req)?;
            match resp {
                Some(resp) => {
                    serde_json::to_writer(&mut writer, &resp).context("serialize response")?;
                    writer.write_all(b"\n").context("write newline")?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn handle(&mut self, req: Message) -> anyhow::Result<Option<Message>> {
        match req.body.payload {
            MessagePayload::Init {
                node_id: nid,
                node_ids: nids,
            } => {
                let msg_id = req.body.msg_id.unwrap();
                self.node_id = nid;
                eprintln!("My node_id is {}", self.node_id);
                self.node_ids = nids;
                eprintln!("Other node_ids in the cluster are: {:?}", self.node_ids);
                self.msg_id.inc();
                let resp = Message {
                    src: self.node_id.clone(),
                    dest: req.src,
                    body: MessageBody {
                        msg_id: Some(self.msg_id.into()),
                        in_reply_to: Some(msg_id),
                        payload: MessagePayload::InitOk,
                    },
                };
                Ok(Some(resp))
            }
            MessagePayload::InitOk => Ok(None),
            MessagePayload::Echo { echo: echo_str } => {
                let msg_id = req.body.msg_id.unwrap();
                let echo = echo_str;
                let payload = MessagePayload::EchoOk { echo };
                self.msg_id.inc();
                let resp = Message {
                    src: self.node_id.clone(),
                    dest: req.src,
                    body: MessageBody {
                        msg_id: Some(self.msg_id.into()),
                        in_reply_to: Some(msg_id),
                        payload,
                    },
                };
                Ok(Some(resp))
            }
            MessagePayload::EchoOk { echo: _ } => Ok(None),
        }
    }
}
