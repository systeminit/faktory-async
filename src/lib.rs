mod connection;
mod error;
mod protocol;

pub use crate::error::Error;
pub use crate::protocol::{BatchConfig, BeatState, FailConfig};

use crate::connection::Connection;
use crate::error::Result;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Config {
    //password?: string;
    //labels: string[];
    does_consume: bool,
    hostname: Option<String>,
    uri: String,
}

impl Config {
    pub fn from_uri(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            hostname: None,
            does_consume: false,
        }
    }

    pub fn does_consume(&mut self) {
        self.does_consume = true;
    }

    pub fn set_hostname(&mut self, hostname: impl Into<String>) {
        self.hostname = Some(hostname.into());
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    config: Config,
    conn: Arc<Mutex<Option<Connection>>>,
}

impl Client {
    pub async fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            conn: Arc::new(Mutex::new(Some(Connection::new(config).await?))),
        })
    }

    pub async fn reconnect_if_needed(&self) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        if conn.is_none() {
            *conn = Some(Connection::new(&self.config).await?);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn conn(&self) -> Result<MappedMutexGuard<'_, Connection>> {
        let guard = MutexGuard::try_map(self.conn.lock().await, |guard| guard.as_mut())
            .map_err(|_| Error::ConnectionAlreadyClosed)?;
        Ok(guard)
    }

    pub async fn last_beat(&self) -> Result<BeatState> {
        Ok(self.conn().await?.last_beat())
    }

    pub async fn beat(&self) -> Result<BeatState> {
        self.conn().await?.beat().await
    }

    pub async fn fetch(&self, queues: &[String]) -> Result<Option<Job>> {
        self.conn().await?.fetch(queues).await
    }

    pub async fn ack(&self, jid: String) -> Result<()> {
        self.conn().await?.ack(jid).await
    }

    pub async fn fail(&self, config: FailConfig) -> Result<()> {
        self.conn().await?.fail(config).await
    }

    pub async fn push(&self, job: Job) -> Result<()> {
        self.conn().await?.push(job).await
    }

    pub async fn batch_create(&self, config: BatchConfig) -> Result<String> {
        self.conn().await?.batch_create(config).await
    }

    pub async fn batch_commit(&self, id: String) -> Result<()> {
        self.conn().await?.batch_commit(id).await
    }

    pub async fn close(&self) -> Result<()> {
        std::mem::take(&mut *self.conn.lock().await)
            .ok_or(Error::ConnectionAlreadyClosed)?
            .close()
            .await
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

impl Job {
    /// Create a new job of type `kind`, with the given arguments.
    pub fn new<S, A>(kind: S, args: Vec<A>) -> Self
    where
        S: Into<String>,
        A: Into<serde_json::Value>,
    {
        let random_jid = Uuid::new_v4().to_string();
        Job {
            jid: random_jid,
            queue: Some("default".into()),
            kind: kind.into(),
            args: args.into_iter().map(|s| s.into()).collect(),

            created_at: Some(Utc::now()),
            enqueued_at: None,
            at: None,
            reserve_for: Some(600),
            retry: Some(25),
            priority: Some(5),
            backtrace: Some(0),
            failure: None,
            custom: Default::default(),
        }
    }

    /// Place this job on the given `queue`.
    ///
    /// If this method is not called (or `self.queue` set otherwise), the queue will be set to
    /// "default".
    pub fn on_queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// This job's id.
    pub fn id(&self) -> &str {
        &self.jid
    }

    /// This job's type.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The arguments provided for this job.
    pub fn args(&self) -> &[serde_json::Value] {
        &self.args
    }

    /// Data about this job's most recent failure.
    pub fn failure(&self) -> &Option<Failure> {
        &self.failure
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_consumes_ack() {
        let client = Client::new(&Config::from_uri("localhost:7419"))
            .await
            .expect("unable to connect to faktory");

        let random_queue = Uuid::new_v4().to_string();
        let random_jid = Uuid::new_v4().to_string();

        assert!(client
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        client
            .push(Job {
                jid: random_jid.clone(),
                kind: "def".to_owned(),
                queue: Some(random_queue.clone()),
                args: Vec::new(),
                ..Default::default()
            })
            .await
            .expect("fetch failed");

        assert_eq!(
            client
                .fetch(&[random_queue.clone()])
                .await
                .expect("fetch failed")
                .expect("got no job")
                .jid,
            random_jid
        );

        assert!(client
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        client.ack(random_jid.clone()).await.expect("ack failed");
        //assert!(client.ack(random_jid.clone()).await.is_err());
        assert!(client
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
        let client = Client::new(&Config::from_uri("localhost:7419"))
            .await
            .expect("unable to connect to faktory");

        let random_queue = Uuid::new_v4().to_string();
        let random_jid = Uuid::new_v4().to_string();

        assert!(client
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        client
            .push(Job {
                jid: random_jid.clone(),
                kind: "def".to_owned(),
                queue: Some(random_queue.clone()),
                args: Vec::new(),
                ..Default::default()
            })
            .await
            .expect("fetch failed");

        assert_eq!(
            client
                .fetch(&[random_queue.clone()])
                .await
                .expect("fetch failed")
                .expect("got no job")
                .jid,
            random_jid
        );

        assert!(client
            .fetch(&["it_consumes".to_owned()])
            .await
            .expect("fetch failed")
            .is_none());

        client
            .fail(FailConfig::new(
                random_jid.clone(),
                "my msg".to_owned(),
                "my-err-kind".to_owned(),
                None,
            ))
            .await
            .expect("unable to fail job");
        assert!(client
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
        let client = Client::new(&Config::from_uri("localhost:7419"))
            .await
            .expect("unable to connect to faktory");
        let random_jid = Uuid::new_v4().to_string();

        client
            .push(Job {
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
    async fn it_doesnt_disconnect_twice() {
        let client = Client::new(&Config::from_uri("localhost:7419"))
            .await
            .expect("unable to connect to faktory");

        assert!(client.clone().close().await.is_ok());
        assert!(client.close().await.is_err());
        //assert_eq!(client.close().await, Err(FaktoryError::ConnectionAlreadyClosed));
    }

    #[tokio::test]
    async fn it_beats() {
        let client = Client::new(&Config::from_uri("localhost:7419"))
            .await
            .expect("unable to connect to faktory");

        assert_eq!(client.beat().await.expect("unable to beat"), BeatState::Ok);
        assert_eq!(
            client.last_beat().await.expect("unable to get last beat"),
            BeatState::Ok
        );
        assert_eq!(client.beat().await.expect("unable to beat"), BeatState::Ok);
        assert_eq!(
            client.last_beat().await.expect("unable to get last beat"),
            BeatState::Ok
        );
        assert_eq!(client.beat().await.expect("unable to beat"), BeatState::Ok);
        assert_eq!(
            client.last_beat().await.expect("unable to get last beat"),
            BeatState::Ok
        );
        assert_eq!(client.beat().await.expect("unable to beat"), BeatState::Ok);
    }
}
