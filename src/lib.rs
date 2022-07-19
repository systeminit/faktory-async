use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

// TODO: have our own error type
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
enum FaktoryError {
    #[error("unexpected response: got {0}, expected {1}")]
    UnexpectedResponse(String, String),
}

#[derive(Debug, Clone)]
pub struct Client {
    //password?: string;
    //labels: string[];
    //wid?: string;
    //connectionFactory: ConnectionFactory;
    //pool: Pool<Connection>;
    uri: String,
}

#[derive(Debug)]
pub struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Connection {
    pub async fn new(uri: &str) -> Result<Self> {
        let (reader, writer) = TcpStream::connect(&uri).await?.into_split();
        let mut conn = Connection {
            reader: BufReader::new(reader),
            writer,
        };
        println!("Created conn");
        conn.validate_response("+HI {\"v\":2}\r\n").await?;
        println!("Got OK to hi");

        conn.hello().await?;

        Ok(conn)
    }

    pub async fn hello(&mut self) -> Result<()> {
        println!("Sending hello");
        Hello::default().send(&mut self.writer).await?;
        println!("Sent hello");
        self.validate_response("+OK\r\n").await?;
        println!("Got OK to hello");

        Ok(())
    }

    async fn validate_response(&mut self, expected: &str) -> Result<()> {
        let mut output = String::new();
        self.reader.read_line(&mut output).await?;
        if output != expected {
            return Err(FaktoryError::UnexpectedResponse(
                output,
                expected.to_owned(),
            ))?;
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<()> {
        Ok(self.writer.write_all(b"END").await?)
    }
}

impl Client {
    pub async fn connect(&self) -> Result<Connection> {
        Connection::new(&self.uri).await
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    conn: Arc<Mutex<Connection>>,
    queues: Vec<String>,
}

impl Consumer {
    pub async fn fetch(&mut self) -> Result<Option<Job>> {
        println!("Sending fetch");
        Fetch {
            queues: &self.queues,
        }
        .send(&mut self.conn.lock().await.writer)
        .await?;
        println!("Sent");

        let mut output = String::new();
        self.conn.lock().await.reader.read_line(&mut output).await?;

        if !output.starts_with("$") || output.len() <= 3 {
            return Err(FaktoryError::UnexpectedResponse(output, "$".to_owned()))?;
        }
        if output == "$-1\r\n" {
            return Ok(None);
        }

        let len: usize = output[1..output.len() - 2].parse()?;
        let mut output = vec![0; len];
        self.conn
            .lock()
            .await
            .reader
            .read_exact(&mut output)
            .await?;
        self.conn
            .lock()
            .await
            .reader
            .read_exact(&mut [0; 2])
            .await?;
        Ok(Some(serde_json::from_slice(&output)?))
    }
}

#[derive(Debug, Clone)]
pub struct Producer {
    conn: Arc<Mutex<Connection>>,
}

impl Producer {
    pub async fn push(&mut self, job: Job) -> Result<()> {
        println!("Sending push");
        Push(job).send(&mut self.conn.lock().await.writer).await?;
        println!("Sent");
        self.conn.lock().await.validate_response("+OK\r\n").await?;
        Ok(())
    }
}

pub struct Push(Job);

impl Push {
    async fn send<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"PUSH ").await?;
        w.write_all(&serde_json::to_vec(&self.0)?).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

/// A Faktory job.
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Job {
    /// The job's unique identifier.
    pub(crate) jid: String,

    /// The queue this job belongs to. Usually `default`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// The job's type. Called `kind` because `type` is reserved.
    #[serde(rename = "jobtype")]
    pub(crate) kind: String,

    /// The arguments provided for this job.
    pub(crate) args: Vec<serde_json::Value>,

    /// When this job was created.
    // note that serializing works correctly here since the default chrono serialization
    // is RFC3339, which is also what Faktory expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    /// When this job was supplied to the Faktory server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enqueued_at: Option<DateTime<Utc>>,

    /// When this job is scheduled for.
    ///
    /// Defaults to immediately.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,

    /// How long to allow this job to run for.
    ///
    /// Defaults to 600 seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reserve_for: Option<usize>,

    /// Number of times to retry this job.
    ///
    /// Defaults to 25.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<isize>,

    /// The priority of this job from 1-9 (9 is highest).
    ///
    /// Pushing a job with priority 9 will effectively put it at the front of the queue.
    /// Defaults to 5.
    pub priority: Option<u8>,

    /// Number of lines of backtrace to keep if this job fails.
    ///
    /// Defaults to 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<usize>,

    /// Data about this job's most recent failure.
    ///
    /// This field is read-only.
    #[serde(skip_serializing)]
    failure: Option<Failure>,

    /// Extra context to include with the job.
    ///
    /// Faktory workers can have plugins and middleware which need to store additional context with
    /// the job payload. Faktory supports a custom hash to store arbitrary key/values in the JSON.
    /// This can be extremely helpful for cross-cutting concerns which should propagate between
    /// systems, e.g. locale for user-specific text translations, request_id for tracing execution
    /// across a complex distributed system, etc.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default = "HashMap::default")]
    pub custom: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Failure {
    retry_count: usize,
    failed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errtype")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<Vec<String>>,
}

pub struct Fetch<'a> {
    queues: &'a [String],
}

impl<'a> Fetch<'a> {
    async fn send<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
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
pub struct Hello {
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
    async fn send<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
        w.write_all(b"HELLO ").await?;
        w.write_all(&serde_json::to_vec(&self)?).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn it_consumes() {
        let client = Client {
            uri: "localhost:7419".to_string(),
        };
        let conn = client.connect().await.expect("unable to connect");
        panic!(
            "{:?}",
            Consumer {
                conn: Arc::new(conn.into()),
                queues: vec!["default".to_owned()]
            }
            .fetch()
            .await
            .expect("fetch failed"),
        );
    }

    #[tokio::test]
    async fn it_produces() {
        let client = Client {
            uri: "localhost:7419".to_string(),
        };
        let conn = client.connect().await.expect("unable to connect");
        let random_jid = Uuid::new_v4().to_string();

        Producer {
            conn: Arc::new(conn.into()),
        }
        .push(Job {
            jid: random_jid.clone(),
            kind: "def".to_owned(),
            args: Vec::new(),
            ..Default::default()
        })
        .await
        .expect("fetch failed");
    }
}
