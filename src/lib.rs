mod error;

pub use crate::error::{Error, Result};
pub use faktory_lib_async::{BatchConfig, BeatState, Config, Connection, FailConfig, Job};

use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Debug)]
enum FaktoryResponse {
    Beat(BeatState),
    Error(String, String),
    Job(Option<Box<Job>>),
    BatchId(String),
    Ok,
}

#[derive(Debug)]
enum FaktoryCommand {
    Ack(String),
    BatchCommit(String),
    BatchNew(Box<BatchConfig>),
    Beat,
    Fail(FailConfig),
    Fetch(Vec<String>),
    GetLastBeat,
    Push(Box<Job>),
}

type FaktoryCommandMessage = (FaktoryCommand, oneshot::Sender<FaktoryResponse>);
type FaktoryCommandSender = mpsc::Sender<FaktoryCommandMessage>;
type FaktoryCommandReceiver = mpsc::Receiver<FaktoryCommandMessage>;

#[derive(Debug, Clone)]
pub struct Client {
    config: Config,
    command_sender: FaktoryCommandSender,
    shutdown_sender: broadcast::Sender<()>,
}

impl Client {
    pub fn new(config: Config, channel_size: usize) -> Self {
        let (command_sender, command_receiver) =
            mpsc::channel::<FaktoryCommandMessage>(channel_size);

        let (shutdown_sender, shutdown_channel) = broadcast::channel(1);

        let has_wid = config.worker_id.is_some();
        let client = Self {
            config,
            command_sender,
            shutdown_sender,
        };

        client.spawn_faktory_connection(command_receiver, shutdown_channel.resubscribe());

        if has_wid {
            client.spawn_heartbeat(shutdown_channel);
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

    pub async fn batch_new(&self, batch_config: BatchConfig) -> Result<String> {
        match self
            .send_command(FaktoryCommand::BatchNew(batch_config.into()))
            .await?
        {
            FaktoryResponse::BatchId(bid) => Ok(bid),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::Ok".to_string(),
            )),
        }
    }

    /// Close the Faktory client. Will shutdown the connection task as well as the
    /// beat task (if running). If we still have a connection to faktory, this will
    /// send the END command as well.
    pub fn close(&self) -> Result<()> {
        // If the receiver isn't there, there is nothing to close
        let _ = self.shutdown_sender.send(());
        Ok(())
    }

    async fn send_command(&self, command: FaktoryCommand) -> Result<FaktoryResponse> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.command_sender
            .send((command, response_sender))
            .await
            .map_err(|_| Error::SendCommand)?;
        match response_receiver.await? {
            FaktoryResponse::Error(kind, msg) => Err(Error::Faktory(kind, msg)),
            other => Ok(other),
        }
    }

    async fn handle_msg(
        msg: FaktoryCommandMessage,
        connection: &mut Connection,
        config: &Config,
        shutdown_channel: &broadcast::Receiver<()>,
    ) {
        let (cmd, responder) = msg;

        loop {
            match Client::handle_cmd(&cmd, connection).await {
                // If the response receiver has gone away, it's because the caller killed it's tokio task
                // That means faktory will still get the command, but the caller won't get the response
                // So we can safely ignore channel failures here

                Err(faktory_lib_async::Error::ReceivedErrorMessage(kind, msg)) => {
                    let _ = responder.send(FaktoryResponse::Error(kind, msg));
                    break;
                }
                Ok(ok_response) => {
                    let _ = responder.send(ok_response);
                    break;
                }


                Err(_err) => {
                    // TODO: is there a case where retrying will keep failing because the user provided data
                    // for this request caused an unexpected error? should we give up, or re-enqueue this
                    // command at the end of the line?

                    //error!("faktory_lib_async returned a critical error, reconnecting and trying to send the message again: {_err}");

                    if let Some(conn) =
                        Client::attempt_connect(config, shutdown_channel.resubscribe()).await
                    {
                        *connection = conn;
                    }

                    // If no connection was obtained handle_cmd will certainly fail and trigger this branch again
                }
            }
        }
    }

    async fn handle_cmd(
        cmd: &FaktoryCommand,
        connection: &mut Connection,
    ) -> faktory_lib_async::Result<FaktoryResponse> {
        match cmd {
            FaktoryCommand::Ack(job_id) => {
                connection.ack(job_id).await.map(|()| FaktoryResponse::Ok)
            }
            FaktoryCommand::BatchCommit(batch_id) => connection
                .batch_commit(batch_id)
                .await
                .map(|()| FaktoryResponse::Ok),
            FaktoryCommand::BatchNew(batch_config) => connection
                .batch_new(&*batch_config)
                .await
                .map(FaktoryResponse::BatchId),
            FaktoryCommand::Beat => connection.beat().await.map(FaktoryResponse::Beat),
            FaktoryCommand::Fetch(queues) => connection
                .fetch(&queues.iter().map(|q| q.as_str()).collect::<Vec<_>>())
                .await
                .map(|job| FaktoryResponse::Job(job.map(Into::into))),
            FaktoryCommand::Fail(fail_config) => connection
                .fail(fail_config)
                .await
                .map(|()| FaktoryResponse::Ok),
            FaktoryCommand::GetLastBeat => Ok(FaktoryResponse::Beat(connection.last_beat())),
            FaktoryCommand::Push(job) => connection.push(&*job).await.map(|()| FaktoryResponse::Ok),
        }
    }

    // Attempts to connect to faktory with exponential backoff. Largest sleep is 32 seconds
    async fn attempt_connect(
        config: &Config,
        mut shutdown_channel: broadcast::Receiver<()>,
    ) -> Option<Connection> {
        let mut backoff = 0;
        loop {
            match shutdown_channel.try_recv() {
                Err(broadcast::error::TryRecvError::Empty) => {}
                _ => break,
            }

            match dbg!(Connection::new(config.clone()).await) {
                Err(_) => {
                    tokio::select! {
                        _ = shutdown_channel.recv() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1 << backoff)) => {}
                    }
                }
                Ok(connection) => return Some(connection),
            }

            if backoff < 5 {
                backoff += 1;
            }
        }

        None
    }

    // Spawns a task that owns our connection to the faktory server
    fn spawn_faktory_connection(
        &self,
        mut command_receiver: FaktoryCommandReceiver,
        mut shutdown_channel: broadcast::Receiver<()>,
    ) {
        let config = self.config.clone();

        tokio::task::spawn(async move {
            let mut connection =
                Client::attempt_connect(&config, shutdown_channel.resubscribe()).await;

            loop {
                match connection {
                    None => {
                        let shutdown_channel2 = shutdown_channel.resubscribe();
                        connection = tokio::select! {
                            _ = shutdown_channel.recv() => break,
                            conn = Client::attempt_connect(&config, shutdown_channel2) => conn,
                        }
                    }
                    Some(ref mut connection) => {
                        tokio::select! {
                            Some(msg) = command_receiver.recv() => {
                                Client::handle_msg(msg, connection, &config, &shutdown_channel).await
                            }
                            _ = shutdown_channel.recv() => {
                                let _ = connection.end().await;
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    // If we're a worker, spawn a heartbeat task that sends a BEAT message to faktory every
    // ~15 seconds
    fn spawn_heartbeat(&self, mut shutdown_channel: broadcast::Receiver<()>) {
        let clone = self.clone();
        tokio::task::spawn(async move {
            loop {
                let _ = clone.beat().await;

                tokio::select! {
                    _ = shutdown_channel.recv() => break,
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
            Config::from_uri(
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

        let _ = client.close();
    }

    // To use this test, you'll want to start it with faktory down, start faktory, then
    // shut faktory down, then bring it back up, and ensure we reconnect in every case.
    // runs in an infinite loop
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    #[ignore]
    async fn it_handles_reconnection() {
        let client = Client::new(
            Config::from_uri(
                "localhost:7419",
                Some("test".to_string()),
                Some(Uuid::new_v4().to_string()),
            ),
            256,
        );

        let queue = Uuid::new_v4().to_string();

        while dbg!(client.fetch(&[queue.clone()]).await).is_err() {
            println!("Retrying");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    #[ignore]
    async fn it_shuts_down() {
        let client = Client::new(
            Config::from_uri(
                "localhost:7419",
                Some("test".to_string()),
                Some(Uuid::new_v4().to_string()),
            ),
            256,
        );

        tokio::time::sleep(Duration::from_secs(16)).await;

        client.close().expect("able to close connection");

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
