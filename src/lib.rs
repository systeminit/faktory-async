mod connection;
mod error;
mod protocol;

pub use crate::error::Error;
pub use crate::protocol::{BatchConfig, BeatState, FailConfig};
use connection::Connection;

use crate::error::Result;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Config {
    //password?: string;
    //labels: string[];
    pub worker_id: Option<String>,
    pub hostname: Option<String>,
    pub uri: String,
}

impl Config {
    pub fn from_uri(uri: &str, hostname: Option<String>, worker_id: Option<String>) -> Self {
        Self {
            uri: uri.to_owned(),
            hostname,
            worker_id,
        }
    }
}

#[derive(Debug)]
pub enum FaktoryResponse {
    Beat(BeatState),
    Error(Error),
    Job(Option<Job>),
    Ok,
}

#[derive(Debug)]
pub enum FaktoryCommand {
    Ack(String),
    BatchCommit(String),
    BatchNew(BatchConfig),
    Beat,
    End,
    Fail(FailConfig),
    Fetch(Vec<String>),
    GetLastBeat,
    Push(Job),
}

pub type FaktoryCommandMessage = (FaktoryCommand, oneshot::Sender<FaktoryResponse>);
pub type FaktoryCommandSender = mpsc::Sender<FaktoryCommandMessage>;
pub type FaktoryCommandReceiver = mpsc::Receiver<FaktoryCommandMessage>;

#[derive(Debug)]
pub struct Client {
    config: Config,
    command_sender: FaktoryCommandSender,
    beat_state_channel: Option<broadcast::Receiver<BeatState>>,
    beat_shutdown_sender: Option<broadcast::Sender<()>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            command_sender: self.command_sender.clone(),
            beat_state_channel: self.beat_state_channel(),
            beat_shutdown_sender: self.beat_shutdown_sender.clone(),
        }
    }
}

impl Client {
    pub fn new(config: &Config, channel_size: usize) -> Self {
        let (command_sender, command_receiver) =
            mpsc::channel::<FaktoryCommandMessage>(channel_size);

        let (beat_state_sender, beat_state_channel) = broadcast::channel(1);
        let (beat_shutdown_sender, beat_shutdown_channel) = broadcast::channel(1);

        let client = Self {
            config: config.clone(),
            command_sender,
            beat_state_channel: if config.worker_id.is_some() {
                Some(beat_state_channel)
            } else {
                None
            },
            beat_shutdown_sender: if config.worker_id.is_some() {
                Some(beat_shutdown_sender)
            } else {
                None
            },
        };

        client.spawn_faktory_connection(command_receiver);

        if config.worker_id.is_some() {
            client.spawn_heartbeat(beat_state_sender, beat_shutdown_channel);
        }

        client
    }

    pub async fn beat(&self) -> Result<BeatState> {
        match self.send_command(FaktoryCommand::Beat).await? {
            FaktoryResponse::Beat(beat_state) => Ok(beat_state),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::BeatState".to_string(),
            )),
        }
    }

    pub async fn last_beat(&mut self) -> Result<BeatState> {
        match self.send_command(FaktoryCommand::GetLastBeat).await? {
            FaktoryResponse::Beat(beat_state) => Ok(beat_state),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::BeatState".to_string(),
            )),
        }
    }

    pub async fn fetch(&self, queues: &[String]) -> Result<Option<Job>> {
        match self
            .send_command(FaktoryCommand::Fetch(queues.to_owned()))
            .await?
        {
            FaktoryResponse::Job(job) => Ok(job),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Job".to_string(),
            )),
        }
    }

    pub async fn ack(&self, job_id: String) -> Result<()> {
        match self.send_command(FaktoryCommand::Ack(job_id)).await? {
            FaktoryResponse::Ok => Ok(()),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    pub async fn fail(&self, fail_config: FailConfig) -> Result<()> {
        match self.send_command(FaktoryCommand::Fail(fail_config)).await? {
            FaktoryResponse::Ok => Ok(()),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    pub async fn push(&self, job: Job) -> Result<()> {
        match self
            .send_command(FaktoryCommand::Push(job))
            .await?
        {
            FaktoryResponse::Ok => Ok(()),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    pub async fn batch_commit(&self, batch_id: String) -> Result<()> {
        match self
            .send_command(FaktoryCommand::BatchCommit(batch_id))
            .await?
        {
            FaktoryResponse::Ok => Ok(()),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    pub async fn batch_new(&self, batch_config: BatchConfig) -> Result<()> {
        match self
            .send_command(FaktoryCommand::BatchNew(batch_config))
            .await?
        {
            FaktoryResponse::Ok => Ok(()),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    pub async fn end(&self) -> Result<()> {
        match self.send_command(FaktoryCommand::End).await? {
            FaktoryResponse::Ok => Ok(()),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    pub fn beat_state_channel(&self) -> Option<broadcast::Receiver<BeatState>> {
        self.beat_state_channel
            .as_ref()
            .map(|chan| chan.resubscribe())
    }

    pub async fn send_command(&self, command: FaktoryCommand) -> Result<FaktoryResponse> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.command_sender.send((command, response_sender)).await?;
        match response_receiver.await? {
            FaktoryResponse::Error(err) => Err(err),
            other => Ok(other),
        }
    }

    // Spawns a task that owns our connection to the faktory server
    fn spawn_faktory_connection(&self, mut command_receiver: FaktoryCommandReceiver) {
        let config = self.config.clone();
        let beat_shutdown_sender = self.beat_shutdown_sender.clone();
        tokio::task::spawn(async move {
            let connection = Connection::new(config.clone()).await;
            if connection.is_err() {
                if let Some(beat_shutdown_sender) = beat_shutdown_sender {
                    let _ = beat_shutdown_sender.send(());
                }

                return;
            }

            let mut connection = connection.unwrap();
            while let Some((cmd, response)) = command_receiver.recv().await {
                match cmd {
                    FaktoryCommand::Ack(job_id) => match connection.ack(job_id).await {
                        Ok(_) => {
                            response.send(FaktoryResponse::Ok).unwrap();
                        }
                        Err(err) => {
                            response.send(FaktoryResponse::Error(err)).unwrap();
                        }
                    },
                    FaktoryCommand::BatchCommit(batch_id) => {
                        match connection.batch_commit(batch_id).await {
                            Ok(_) => {
                                response.send(FaktoryResponse::Ok).unwrap();
                            }
                            Err(err) => {
                                response.send(FaktoryResponse::Error(err)).unwrap();
                            }
                        }
                    }
                    FaktoryCommand::BatchNew(batch_config) => {
                        match connection.batch_new(batch_config).await {
                            Ok(_) => {
                                response.send(FaktoryResponse::Ok).unwrap();
                            }
                            Err(err) => {
                                response.send(FaktoryResponse::Error(err)).unwrap();
                            }
                        }
                    }
                    FaktoryCommand::Beat => match connection.beat().await {
                        Ok(beat_state) => {
                            response.send(FaktoryResponse::Beat(beat_state)).unwrap();
                        }
                        Err(err) => {
                            response.send(FaktoryResponse::Error(err)).unwrap();
                        }
                    },
                    // The end command will close this connection. It attemps to send "End" to the
                    // server, but will fail even if that fails (because the server has gone away,
                    // for example)
                    FaktoryCommand::End => {
                        match connection.end().await {
                            Ok(_) => {
                                response.send(FaktoryResponse::Ok).unwrap();
                            }
                            Err(err) => {
                                response.send(FaktoryResponse::Error(err)).unwrap();
                            }
                        }

                        if let Some(beat_shutdown_sender) = beat_shutdown_sender {
                            let _ = beat_shutdown_sender.send(());
                        }

                        // We sent end, so we should die, no matter what
                        return;
                    }
                    FaktoryCommand::Fetch(queues) => match connection.fetch(&queues).await {
                        Ok(job) => {
                            response.send(FaktoryResponse::Job(job)).unwrap();
                        }
                        Err(err) => {
                            response.send(FaktoryResponse::Error(err)).unwrap();
                        }
                    },
                    FaktoryCommand::Fail(fail_config) => match connection.fail(fail_config).await {
                        Ok(_) => {
                            response.send(FaktoryResponse::Ok).unwrap();
                        }
                        Err(err) => {
                            response.send(FaktoryResponse::Error(err)).unwrap();
                        }
                    },
                    FaktoryCommand::GetLastBeat => {
                        response
                            .send(FaktoryResponse::Beat(connection.last_beat()))
                            .unwrap();
                    }
                    FaktoryCommand::Push(job) => match connection.push(job).await {
                        Ok(_) => {
                            response.send(FaktoryResponse::Ok).unwrap();
                        }
                        Err(err) => {
                            // info!("Error pushing job to faktory: {err}");
                            response.send(FaktoryResponse::Error(err)).unwrap();
                        }
                    },
                }
            }
        });
    }

    // If we're a worker, spawn a heartbeat task that sends a BEAT message to faktory every
    // ~15 seconds
    fn spawn_heartbeat(
        &self,
        beat_state_sender: broadcast::Sender<BeatState>,
        mut beat_shutdown_channel: broadcast::Receiver<()>,
    ) {
        let _ = beat_state_sender.send(BeatState::Ok);
        let clone = self.clone();
        tokio::task::spawn(async move {
            loop {
                if let Ok(beat_state) = clone.beat().await {
                    let _ = beat_state_sender.send(beat_state);

                    match beat_state {
                        BeatState::Ok => {}
                        // Both the Quiet and Terminate states from the
                        // faktory server mean that we should initiate a
                        // shutdown. So don't send an additional beat.
                        BeatState::Quiet | BeatState::Terminate => break,
                    }
                }

                tokio::select! {
                    _ = beat_shutdown_channel.recv() => {
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                }
            }
        });
    }
}

/// A Faktory job.
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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
    use uuid::Uuid;

    #[tokio::test]
    async fn it_connects_sends_beats_and_shuts_down() {
        let client = Client::new(
            &Config::from_uri(
                "localhost:7419",
                Some("test".to_string()),
                Some(Uuid::new_v4().to_string()),
            ),
            256,
        );

        let mut beat_state_channel = client.beat_state_channel().unwrap();
        let mut beats = vec![];

        let client_clone = client.clone();
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            client_clone.end().await.unwrap();
        });

        while let Ok(state) = beat_state_channel.recv().await {
            beats.push(state);
        }

        assert_eq!(2, beats.len());
    }

    #[tokio::test]
    async fn it_pushes_and_fetches_and_acks() {
        let client = Client::new(
            &Config::from_uri(
                "localhost:7419",
                Some("test".to_string()),
                Some(Uuid::new_v4().to_string()),
            ),
            256,
        );

        let queue = Uuid::new_v4().to_string();
        let fetch_result = client.fetch(&[queue.clone()]).await;
        assert!(fetch_result.is_ok());
        assert!(fetch_result.unwrap().is_none());

        for _ in 0..5 {
            client
                .push(Job {
                    jid: Uuid::new_v4().to_string(),
                    kind: "def".to_owned(),
                    queue: Some(queue.clone()),
                    args: Vec::new(),
                    ..Default::default()
                })
                .await
                .expect("push failed");
        }

        let mut jobs = vec![];
        while let Ok(Some(job)) = client.fetch(&[queue.clone()]).await {
            jobs.push(job.clone());
            client.ack(job.jid).await.expect("could not ack");
        }

        assert_eq!(5, jobs.len());

        let _ = client.end().await;
    }
}
