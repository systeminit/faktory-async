use crate::protocol::{BatchConfig, BeatReply, BeatState, FailConfig, HelloConfig};
use crate::{Config, Error, Job, Result};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Debug)]
pub(crate) struct Connection {
    config: Config,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    last_beat: BeatState,
}

impl Connection {
    pub async fn new(config: Config) -> Result<Self> {
        let (reader, writer) = TcpStream::connect(&config.uri).await?.into_split();
        let mut conn = Connection {
            config,
            reader: BufReader::new(reader),
            writer,
            last_beat: BeatState::Ok,
        };
        // TODO: properly parse the HI response
        conn.validate_response("HI {\"v\":2}").await?;
        conn.default_hello().await?;

        Ok(conn)
    }

    pub async fn default_hello(&mut self) -> Result<()> {
        // TODO: improve hello config usage
        let mut config = HelloConfig::default();
        config.pid = Some(std::process::id() as usize);
        config.labels = vec!["faktory-async-rust".to_owned()];
        config.wid = self.config.worker_id.clone();
        self.hello(config).await?;

        Ok(())
    }

    pub fn last_beat(&self) -> BeatState {
        self.last_beat
    }

    pub async fn end(&mut self) -> Result<()> {
        self.send_command("END", vec![]).await?;
        Ok(())
    }

    // TODO: handle extra arguments: {wid: String, current_state: String, rss_kb: Integer}
    // https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md#beat-command
    pub async fn beat(&mut self) -> Result<BeatState> {
        self.send_command(
            "BEAT",
            vec![serde_json::to_string(
                &serde_json::json!({ "wid": self.config.worker_id }),
            )?],
        )
        .await?;
        match self.read_string().await?.as_deref() {
            Some("OK") => {
                self.last_beat = BeatState::Ok;
                Ok(BeatState::Ok)
            }
            Some(output) => {
                self.last_beat = serde_json::from_str::<BeatReply>(output)?.state;
                Ok(self.last_beat)
            }
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

    pub async fn batch_new(&mut self, config: BatchConfig) -> Result<String> {
        self.send_command("BATCH NEW", vec![serde_json::to_string(&config)?])
            .await?;
        self.read_string().await?.ok_or(Error::ReceivedEmptyMessage)
    }

    pub async fn batch_commit(&mut self, bid: String) -> Result<()> {
        self.send_command("BATCH COMMIT", vec![bid]).await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    async fn send_command(&mut self, key: &'static str, args: Vec<String>) -> Result<()> {
        let mut args = vec![key.into(), args.into_iter().collect::<Vec<_>>().join(" ")].join(" ");
        args.push_str("\r\n");
        self.writer.write_all(dbg!(&args).as_bytes()).await?;
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
                self.reader.read_exact(&mut [0; 2]).await?;
                Ok(Some(String::from_utf8(output)?))
            }
            '+' => {
                output.truncate(output.len() - 2);
                Ok(Some(output))
            }
            '-' => {
                let (kind, msg) = output
                    .split_once(' ')
                    .ok_or_else(|| Error::ReceivedInvalidErrorMessage(output.clone()))?;
                Err(Error::ReceivedErrorMessage(
                    kind.to_owned(),
                    msg[..msg.len() - 2].to_owned(),
                ))
            }
            prefix => Err(Error::InvalidMessagePrefix(format!("{prefix}{output}"))),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn it_consumes_ack() {
        let mut conn = Connection::new(Config::from_uri("localhost:7419", None, None))
            .await
            .expect("unable to connect to faktory");

        let random_queue = Uuid::new_v4().to_string();
        let random_jid = Uuid::new_v4().to_string();

        assert!(conn
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        conn.push(Job {
            jid: random_jid.clone(),
            kind: "def".to_owned(),
            queue: Some(random_queue.clone()),
            args: Vec::new(),
            ..Default::default()
        })
        .await
        .expect("push failed");

        assert_eq!(
            conn.fetch(&[random_queue.clone()])
                .await
                .expect("fetch failed")
                .expect("got no job")
                .jid,
            random_jid
        );

        assert!(conn
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        conn.ack(random_jid.clone()).await.expect("ack failed");
        //assert!(client.ack(random_jid.clone()).await.is_err());
        assert!(conn
            .fail(FailConfig::new(
                random_jid.clone(),
                "my msg".to_owned(),
                "my-err-kind".to_owned(),
                None
            ))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn it_consumes_fail() {
        let mut conn = Connection::new(Config::from_uri("localhost:7419", None, None))
            .await
            .expect("unable to connect to faktory");

        let random_queue = Uuid::new_v4().to_string();
        let random_jid = Uuid::new_v4().to_string();

        assert!(conn
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        conn.push(Job {
            jid: random_jid.clone(),
            kind: "def".to_owned(),
            queue: Some(random_queue.clone()),
            args: Vec::new(),
            ..Default::default()
        })
        .await
        .expect("fetch failed");

        assert_eq!(
            conn.fetch(&[random_queue.clone()])
                .await
                .expect("fetch failed")
                .expect("got no job")
                .jid,
            random_jid
        );

        assert!(conn
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        conn.fail(FailConfig::new(
            random_jid.clone(),
            "my msg".to_owned(),
            "my-err-kind".to_owned(),
            None,
        ))
        .await
        .expect("unable to fail job");
        assert!(conn
            .fail(FailConfig::new(
                random_jid.clone(),
                "my msg".to_owned(),
                "my-err-kind".to_owned(),
                None
            ))
            .await
            .is_err());
        //assert!(client.ack(random_jid.clone()).await.is_err());

        //tokio::time::sleep(Duration::from_secs(40)).await;

        //assert_eq!(
        //    client
        //        .fetch(&[random_queue.clone()])
        //        .await
        //        .expect("fetch failed")
        //        .expect("got no job")
        //        .jid,
        //    random_jid
        //);
        //client.ack(random_jid.clone()).await.expect("ack failed");
    }

    #[tokio::test]
    async fn it_produces() {
        let mut conn = Connection::new(Config::from_uri("localhost:7419", None, None))
            .await
            .expect("unable to connect to faktory");
        let random_jid = Uuid::new_v4().to_string();

        conn.push(Job {
            jid: random_jid.clone(),
            queue: Some("it_produces".to_owned()),
            kind: "def".to_owned(),
            args: Vec::new(),
            ..Default::default()
        })
        .await
        .expect("push failed");
    }

    #[tokio::test]
    async fn it_beats() {
        let mut config = Config::from_uri("localhost:7419", None, None);
        config.worker_id = Some(Uuid::new_v4().to_string());

        let mut conn = Connection::new(config)
            .await
            .expect("unable to connect to faktory");

        assert_eq!(conn.beat().await.expect("unable to beat"), BeatState::Ok);
        assert_eq!(conn.last_beat(), BeatState::Ok);
        assert_eq!(conn.beat().await.expect("unable to beat"), BeatState::Ok);
        assert_eq!(conn.last_beat(), BeatState::Ok);
        assert_eq!(conn.beat().await.expect("unable to beat"), BeatState::Ok);
        assert_eq!(conn.last_beat(), BeatState::Ok);
        assert_eq!(conn.beat().await.expect("unable to beat"), BeatState::Ok);
    }
}
