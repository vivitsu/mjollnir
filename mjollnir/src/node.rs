use anyhow::Context;
use std::io::{BufRead, Write};

use crate::{
    handle::Handle,
    message::{Message, MessagePayload},
    message_id::MessageId,
};

pub struct Node {
    msg_id: MessageId,
    id: String,
    node_ids: Vec<String>,
}

impl Node {
    pub fn new() -> Self {
        let id = String::new();
        let node_ids = Vec::new();
        let msg_id = MessageId::init();
        Self {
            msg_id,
            id,
            node_ids,
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn msg_id(&self) -> MessageId {
        self.msg_id
    }

    pub fn run<R: BufRead, W: Write>(&mut self, reader: R, mut writer: W) -> anyhow::Result<()> {
        eprintln!("Starting node...");
        for line in reader.lines() {
            let line = line.context("Input from reader could not be read")?;
            let req: Message = serde_json::from_str(&line)?;
            let resp = self.process(&req)?;
            if let Some(resp) = resp {
                self.reply(&resp, &mut writer)?;
            }
        }
        Ok(())
    }

    fn reply<W: Write>(&self, resp: &Message, mut writer: &mut W) -> anyhow::Result<()> {
        serde_json::to_writer(&mut writer, &resp).context("serialize response")?;
        writer.write_all(b"\n").context("write newline")?;
        Ok(())
    }

    fn process(&mut self, req: &Message) -> anyhow::Result<Option<Message>> {
        match &req.body.payload {
            MessagePayload::Init(payload) => {
                self.id = payload.node_id.clone();
                self.node_ids = payload.node_ids.clone();
                self.msg_id.inc();
                let resp = payload.handle(self, req);
                Ok(resp)
            }
            MessagePayload::InitOk => Ok(None),
            MessagePayload::Echo(payload) => {
                self.msg_id.inc();
                let resp = payload.handle(self, req);
                Ok(resp)
            }
            MessagePayload::EchoOk(_) => Ok(None),
            MessagePayload::Generate(payload) => {
                self.msg_id.inc();
                let resp = payload.handle(self, req);
                Ok(resp)
            }
            MessagePayload::GenerateOk(_) => Ok(None),
        }
    }
}
