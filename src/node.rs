use anyhow::Context;
use std::io::{BufRead, Write};

use crate::{
    handle::Handle,
    message::{Message, MessagePayload},
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

    pub fn id(&self) -> String {
        self.node_id.clone()
    }

    pub fn set_id(&mut self, id: String) {
        self.node_id = id;
    }

    pub fn set_node_ids(&mut self, ids: Vec<String>) {
        self.node_ids = ids;
    }

    pub fn msg_id(&self) -> MessageId {
        self.msg_id
    }

    pub fn run<R: BufRead, W: Write>(&mut self, reader: R, mut writer: W) -> anyhow::Result<()> {
        eprintln!("Starting node...");
        for line in reader.lines() {
            let line = line.context("Input from reader could not be read")?;
            let req: Message = serde_json::from_str(&line)?;
            let resp = self.handle(&req)?;
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

    fn handle(&mut self, req: &Message) -> anyhow::Result<Option<Message>> {
        match &req.body.payload {
            MessagePayload::Init(payload) => {
                let resp = payload.handle(self, &req);
                Ok(resp)
            }
            MessagePayload::InitOk => Ok(None),
            MessagePayload::Echo(payload) => {
                let resp = payload.handle(self, &req);
                Ok(resp)
            }
            MessagePayload::EchoOk(_) => Ok(None),
            MessagePayload::Generate(payload) => {
                let resp = payload.handle(self, &req);
                Ok(resp)
            }
            MessagePayload::GenerateOk(_) => Ok(None),
        }
    }
}
