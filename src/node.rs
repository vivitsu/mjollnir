use anyhow::Context;
use std::{
    collections::HashMap,
    io::{BufRead, Write},
};

use crate::{
    message::{Message, MessageBody, MessageType},
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
        match req.body.msg_type {
            MessageType::Init => {
                let msg_id = req.body.msg_id.unwrap();
                self.node_id = req.body.payload["node_id"].as_str().unwrap().to_string();
                eprintln!("My node_id is {}", self.node_id);
                let mut node_ids = vec![];
                for nid in req.body.payload["node_ids"].as_array().unwrap() {
                    node_ids.push(nid.as_str().unwrap().to_string());
                }
                self.node_ids = node_ids;
                eprintln!("Other node_ids in the cluster are: {:?}", self.node_ids);
                self.msg_id.inc();
                let resp = Message {
                    src: self.node_id.clone(),
                    dest: req.src,
                    body: MessageBody {
                        msg_type: MessageType::InitOk,
                        msg_id: Some(self.msg_id.into()),
                        in_reply_to: Some(msg_id),
                        payload: HashMap::new(),
                    },
                };
                Ok(Some(resp))
            }
            MessageType::InitOk => Ok(None),
            MessageType::Echo => {
                let msg_id = req.body.msg_id.unwrap();
                let echo = req.body.payload["echo"].as_str().unwrap().to_string();
                let mut payload = HashMap::new();
                payload.insert("echo".to_string(), serde_json::to_value(echo).unwrap());
                self.msg_id.inc();
                let resp = Message {
                    src: self.node_id.clone(),
                    dest: req.src,
                    body: MessageBody {
                        msg_type: MessageType::EchoOk,
                        msg_id: Some(self.msg_id.into()),
                        in_reply_to: Some(msg_id),
                        payload,
                    },
                };
                Ok(Some(resp))
            }
            MessageType::EchoOk => Ok(None),
        }
    }
}
