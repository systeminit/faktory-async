use crate::{error::Result, Error, Job};
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(crate) struct Push(Job);

impl Push {
    pub(crate) fn new(job: Job) -> Self {
        Self(job)
    }

    pub(crate) async fn send<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"PUSH ").await?;
        w.write_all(&serde_json::to_vec(&self.0)?).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

pub(crate) struct Fetch<'a> {
    queues: &'a [String],
}

impl<'a> Fetch<'a> {
    pub(crate) fn new(queues: &'a [String]) -> Self {
        Self { queues }
    }

    pub(crate) async fn send<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
        if self.queues.is_empty() {
            w.write_all(b"FETCH\r\n").await?;
        } else {
            w.write_all(b"FETCH ").await?;
            w.write_all(self.queues.join(" ").as_bytes()).await?;
            w.write_all(b"\r\n").await?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
pub(crate) struct Hello {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    #[serde(rename = "v")]
    version: usize,

    /// Hash is hex(sha256(password + salt))
    #[serde(rename = "pwdhash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
}

impl Default for Hello {
    fn default() -> Self {
        Hello {
            hostname: None,
            wid: None,
            pid: None,
            labels: Vec::new(),
            password_hash: None,
            version: 2,
        }
    }
}

impl Hello {
    pub(crate) async fn send<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
        w.write_all(b"HELLO ").await?;
        w.write_all(&serde_json::to_vec(&self)?).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}
