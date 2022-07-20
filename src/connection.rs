use crate::protocol::{BatchConfig, BeatReply, FailConfig, HelloConfig};
use crate::{Config, Error, Job, Result};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Connection {
    wid: Option<String>,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    last_beat: BeatReply,
}

impl Connection {
    pub async fn new(config: &Config) -> Result<Self> {
        let (reader, writer) = TcpStream::connect(&config.uri).await?.into_split();
        let wid = if config.does_consume {
            Some(Uuid::new_v4().to_string())
        } else {
            None
        };
        let mut conn = Connection {
            wid: wid.clone(),
            reader: BufReader::new(reader),
            writer,
            last_beat: BeatReply::Ok,
        };
        // TODO: properly parse the HI response
        conn.validate_response("HI {\"v\":2}").await?;

        // TODO: improve hello config usage
        let mut config = HelloConfig::default();
        config.pid = Some(std::process::id() as usize);
        config.labels = vec!["faktory-async-rust".to_owned()];
        config.wid = wid;
        conn.hello(config).await?;

        Ok(conn)
    }

    pub fn last_beat(&self) -> BeatReply {
        self.last_beat
    }

    pub async fn close(mut self) -> Result<()> {
        self.send_command("END", vec![]).await?;
        Ok(())
    }

    // TODO: handle extra arguments: {wid: String, current_state: String, rss_kb: Integer}
    // https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md#beat-command
    pub async fn beat(&mut self) -> Result<BeatReply> {
        self.send_command(
            "BEAT",
            vec![serde_json::to_string(
                &serde_json::json!({ "wid": self.wid }),
            )?],
        )
        .await?;
        match self.read_string().await?.as_deref() {
            Some("OK") => Ok(BeatReply::Ok),
            Some(output) => Ok(serde_json::from_str(&output)?),
            None => Err(Error::ReceivedEmptyMessage),
        }
    }

    pub async fn hello(&mut self, config: HelloConfig) -> Result<()> {
        self.send_command("HELLO", vec![serde_json::to_string(&config)?])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn fetch(&mut self, queues: &[String]) -> Result<Option<Job>> {
        if queues.is_empty() {
            self.send_command("FETCH", vec![]).await?;
        } else {
            self.send_command("FETCH", queues.to_owned()).await?;
        }
        Ok(self
            .read_string()
            .await?
            .map(|msg| serde_json::from_str(&msg))
            .transpose()?)
    }

    pub async fn ack(&mut self, jid: String) -> Result<()> {
        self.send_command(
            "ACK",
            vec![serde_json::to_string(&serde_json::json!({ "jid": jid }))?],
        )
        .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn fail(&mut self, config: FailConfig) -> Result<()> {
        self.send_command("FAIL", vec![serde_json::to_string(&config)?])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn push(&mut self, job: Job) -> Result<()> {
        self.send_command("PUSH", vec![serde_json::to_string(&job)?])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn batch_create(&mut self, config: BatchConfig) -> Result<String> {
        self.send_command("BATCH NEW", vec![serde_json::to_string(&config)?])
            .await?;
        Ok(self
            .read_string()
            .await?
            .ok_or(Error::ReceivedEmptyMessage)?)
    }

    pub async fn batch_commit(&mut self, bid: String) -> Result<()> {
        self.send_command("BATCH COMMIT", vec![bid]).await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    async fn send_command(&mut self, key: impl Into<String>, args: Vec<String>) -> Result<()> {
        let mut args = vec![key.into(), args.into_iter().collect::<Vec<_>>().join(" ")].join(" ");
        args.push_str("\r\n");
        self.writer.write_all(dbg!(args).as_bytes()).await?;
        Ok(())
    }

    async fn validate_response(&mut self, expected: &str) -> Result<()> {
        let output = self
            .read_string()
            .await?
            .ok_or(Error::ReceivedEmptyMessage)?;
        if output != expected {
            return Err(Error::UnexpectedResponse(output, expected.to_owned()))?;
        }
        Ok(())
    }

    async fn read_string(&mut self) -> Result<Option<String>> {
        let mut output = String::new();
        self.reader.read_line(&mut output).await?;

        if dbg!(&output).is_empty() {
            return Err(Error::ReceivedEmptyMessage);
        }
        if !output.ends_with("\r\n") {
            return Err(Error::MissingCarriageReturn);
        }

        match output.remove(0) {
            '$' => {
                if output == "-1\r\n" {
                    return Ok(None);
                }

                let len: usize = output[0..output.len() - 2].parse()?;
                let mut output = vec![0; len];
                self.reader.read_exact(&mut output).await?;
                //self.reader.read_line(&mut String::new())?;
                self.reader.read_exact(&mut [0; 2]).await?;
                Ok(Some(String::from_utf8(output)?))
            }
            '+' => {
                output.truncate(output.len() - 2);
                Ok(Some(output))
            }
            '-' => {
                let (kind, msg) = output
                    .split_once(" ")
                    .ok_or_else(|| Error::ReceivedInvalidErrorMessage(output.clone()))?;
                Err(Error::ReceivedErrorMessage(
                    kind.to_owned(),
                    msg[..msg.len() - 2].to_owned(),
                ))
            }
            prefix => Err(Error::InvalidMessagePrefix(format!("{prefix}{output}"))),
        }
    }
}
