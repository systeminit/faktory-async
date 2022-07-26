mod error;

pub use crate::error::{Error, Result};
pub use faktory_lib_async::{BatchConfig, BeatState, Config, Connection, FailConfig, Job};

use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Debug)]
pub enum FaktoryResponse {
    Beat(BeatState),
    Error(Error),
    Retry,
    Job(Option<Box<Job>>),
    Ok,
}

#[derive(Debug)]
pub enum FaktoryCommand {
    Ack(String),
    BatchCommit(String),
    BatchNew(Box<BatchConfig>),
    Beat,
    End,
    Fail(FailConfig),
    Fetch(Vec<String>),
    GetLastBeat,
    Push(Box<Job>),
}

pub type FaktoryCommandMessage = (FaktoryCommand, oneshot::Sender<FaktoryResponse>);
pub type FaktoryCommandSender = mpsc::Sender<FaktoryCommandMessage>;
pub type FaktoryCommandReceiver = mpsc::Receiver<FaktoryCommandMessage>;

#[derive(Debug, Clone)]
pub struct Client {
    config: Config,
    command_sender: FaktoryCommandSender,
    beat_shutdown_sender: Option<broadcast::Sender<()>>,
}

enum CommandOutcome {
    Shutdown,
    Reconnect,
    Ok,
}

impl Client {
    pub fn new(config: &Config, channel_size: usize) -> Self {
        let (command_sender, command_receiver) =
            mpsc::channel::<FaktoryCommandMessage>(channel_size);

        let (beat_shutdown_sender, beat_shutdown_channel) = broadcast::channel(1);

        let client = Self {
            config: config.clone(),
            command_sender,
            beat_shutdown_sender: if config.worker_id.is_some() {
                Some(beat_shutdown_sender)
            } else {
                None
            },
        };

        client.spawn_faktory_connection(command_receiver);

        if config.worker_id.is_some() {
            client.spawn_heartbeat(beat_shutdown_channel);
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
            FaktoryResponse::Job(Some(job)) => Ok(Some(*job)),
            FaktoryResponse::Job(None) => Ok(None),
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
        match self.send_command(FaktoryCommand::Push(job.into())).await? {
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
            .send_command(FaktoryCommand::BatchNew(batch_config.into()))
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

    pub async fn send_command(&self, command: FaktoryCommand) -> Result<FaktoryResponse> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.command_sender.send((command, response_sender)).await?;
        match response_receiver.await? {
            FaktoryResponse::Error(err) => Err(err),
            other => Ok(other),
        }
    }

    async fn handle_msg(
        msg: FaktoryCommandMessage,
        connection: &mut Connection,
        beat_shutdown_sender: Option<broadcast::Sender<()>>,
    ) -> CommandOutcome {
        let (cmd, responder) = msg;

        let response = match cmd {
            FaktoryCommand::End => {
                match connection.end().await {
                    Ok(_) => {
                        responder.send(FaktoryResponse::Ok).unwrap();
                    }
                    Err(err) => {
                        responder.send(FaktoryResponse::Error(err.into())).unwrap();
                    }
                };

                if let Some(beat_shutdown_sender) = beat_shutdown_sender {
                    let _ = beat_shutdown_sender.send(());
                }

                return CommandOutcome::Shutdown;
            }
            cmd => Client::handle_cmd(cmd, connection).await,
        };

        match response {
            FaktoryResponse::Error(err) => match err {
                Error::FaktoryLib(faktory_lib_async::Error::Io(_)) => {
                    responder.send(FaktoryResponse::Retry).unwrap();
                    CommandOutcome::Reconnect
                }
                _ => {
                    responder.send(FaktoryResponse::Error(err)).unwrap();
                    CommandOutcome::Ok
                }
            },
            ok_response => {
                responder.send(ok_response).unwrap();
                CommandOutcome::Ok
            }
        }
    }

    async fn handle_cmd(cmd: FaktoryCommand, connection: &mut Connection) -> FaktoryResponse {
        match cmd {
            FaktoryCommand::Ack(job_id) => match connection.ack(job_id).await {
                Ok(_) => FaktoryResponse::Ok,
                Err(err) => FaktoryResponse::Error(err.into()),
            },
            FaktoryCommand::BatchCommit(batch_id) => {
                match connection.batch_commit(batch_id).await {
                    Ok(_) => FaktoryResponse::Ok,
                    Err(err) => FaktoryResponse::Error(err.into()),
                }
            }
            FaktoryCommand::BatchNew(batch_config) => {
                match connection.batch_new(*batch_config).await {
                    Ok(_) => FaktoryResponse::Ok,
                    Err(err) => FaktoryResponse::Error(err.into()),
                }
            }
            FaktoryCommand::Beat => match connection.beat().await {
                Ok(beat_state) => FaktoryResponse::Beat(beat_state),
                Err(err) => FaktoryResponse::Error(err.into()),
            },
            FaktoryCommand::Fetch(queues) => match connection.fetch(&queues).await {
                Ok(job) => FaktoryResponse::Job(job.map(Into::into)),
                Err(err) => FaktoryResponse::Error(err.into()),
            },
            FaktoryCommand::Fail(fail_config) => match connection.fail(fail_config).await {
                Ok(_) => FaktoryResponse::Ok,
                Err(err) => FaktoryResponse::Error(err.into()),
            },
            FaktoryCommand::GetLastBeat => FaktoryResponse::Beat(connection.last_beat()),
            FaktoryCommand::Push(job) => match connection.push(*job).await {
                Ok(_) => FaktoryResponse::Ok,
                Err(err) => {
                    // info!("Error pushing job to faktory: {err}");
                    FaktoryResponse::Error(err.into())
                }
            },
            // End is handled in handle_msg
            _ => unimplemented!("this should not be reached"),
        }
    }

    // Attempts to connect to faktory with exponential backoff
    async fn attempt_connect(config: &Config) -> Option<Connection> {
        // 14 retries is 2^14 = 16,384 seconds or 4.5 hours
        for retries in 0..15 {
            match Connection::new(config.clone()).await {
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(1 << retries)).await;
                }
                Ok(connection) => return Some(connection),
            }
        }

        None
    }

    // Spawns a task that owns our connection to the faktory server
    // Right now we panic if it takes 4 hours to get a connection
    fn spawn_faktory_connection(&self, mut command_receiver: FaktoryCommandReceiver) {
        let config = self.config.clone();
        let beat_shutdown_sender = self.beat_shutdown_sender.clone();
        tokio::task::spawn(async move {
            let mut connection = Client::attempt_connect(&config).await.unwrap();
            let mut last_command_outcome: CommandOutcome = CommandOutcome::Ok;

            loop {
                match last_command_outcome {
                    CommandOutcome::Shutdown => return,
                    CommandOutcome::Reconnect => {
                        connection = Client::attempt_connect(&config).await.unwrap();
                    }
                    CommandOutcome::Ok => {}
                };

                if let Some(msg) = command_receiver.recv().await {
                    last_command_outcome =
                        Client::handle_msg(msg, &mut connection, beat_shutdown_sender.clone())
                            .await;
                }
            }
        });
    }

    // If we're a worker, spawn a heartbeat task that sends a BEAT message to faktory every
    // ~15 seconds
    fn spawn_heartbeat(&self, mut beat_shutdown_channel: broadcast::Receiver<()>) {
        let clone = self.clone();
        tokio::task::spawn(async move {
            loop {
                if let Ok(beat_state) = clone.beat().await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

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
            client
                .ack(job.id().to_string())
                .await
                .expect("could not ack");
        }

        assert_eq!(5, jobs.len());

        let _ = client.end().await;
    }

    // To use this test, you'll want to start it with faktory down, start faktory, then
    // shut faktory down, then bring it back up, and ensure we reconnect in every case.
    // runs in an infinite loop
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    #[ignore]
    async fn it_handles_disconnects() {
        let client = Client::new(
            &Config::from_uri(
                "localhost:7419",
                Some("test".to_string()),
                Some(Uuid::new_v4().to_string()),
            ),
            256,
        );

        let queue = Uuid::new_v4().to_string();

        loop {
            let _ = dbg!(client.fetch(&[queue.clone()]).await);
        }
    }
}
