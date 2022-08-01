mod error;

pub use crate::error::{Error, Result};

pub use faktory_lib_async::{BatchConfig, BeatState, Config, Connection, FailConfig, Job};
use rand::{thread_rng, Rng};
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{
    broadcast,
    mpsc::{self, error::TrySendError},
    oneshot,
};

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
    Fetch(Vec<Cow<'static, str>>),
    GetLastBeat,
    Push(Box<Job>),
}

type FaktoryCommandMessage = (FaktoryCommand, oneshot::Sender<FaktoryResponse>);
type FaktoryCommandSender = mpsc::Sender<FaktoryCommandMessage>;
type FaktoryCommandReceiver = mpsc::Receiver<FaktoryCommandMessage>;

#[derive(Debug)]
pub struct Client {
    config: Config,
    // The heartbeat task also will have one of these so we can't assume we will own the last one
    command_sender: FaktoryCommandSender,
    shutdown_request_tx: mpsc::Sender<()>,
    wait_for_shutdown_rx: broadcast::Receiver<()>,
    // Helps the user to identify that the queue is being emptied and that no new commands will be accepted
    // We have to cache to avoid messing with the mpsc::Receiver's state
    shutdown_request_cached: Arc<AtomicBool>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            command_sender: self.command_sender.clone(),
            shutdown_request_tx: self.shutdown_request_tx.clone(),
            // It's annoying that broadcast receiver doesn't implement clone
            // We never send anything to it, so we don't need to fear data loss on resubscribe
            // It uses the drop pattern so it blocks until all tx are dropped
            wait_for_shutdown_rx: self.wait_for_shutdown_rx.resubscribe(),
            shutdown_request_cached: self.shutdown_request_cached.clone(),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Initiate shutdown if we are the last Client tied to the underlying tasks
        if Arc::strong_count(&self.shutdown_request_cached) == 1 {
            // Only failures possible are Full and Close
            // If queue is full someone already scheduled the shutdown (by calling `Self::close`) so we don't need to do it again
            // If queue is closed the command task has gone away, so we already are effectively shutdown
            let _ = self.shutdown_request_tx.try_send(());

            // Since we are the last Client we don't need to update the shutdown_request_cached Arc
        }
    }
}

impl Client {
    pub fn new(config: Config, channel_size: usize) -> Self {
        let (command_sender, command_receiver) =
            mpsc::channel::<FaktoryCommandMessage>(channel_size);

        let has_wid = config.worker_id.is_some();
        let (shutdown_request_tx, shutdown_request_rx) = mpsc::channel(1);
        let (wait_for_shutdown_tx, wait_for_shutdown_rx) = broadcast::channel(1);
        let client = Self {
            config,
            command_sender,
            shutdown_request_tx,
            wait_for_shutdown_rx,
            shutdown_request_cached: Default::default(),
        };

        let (beat_shutdown_tx, beat_shutdown_rx) = oneshot::channel();

        client.spawn_faktory_connection(
            command_receiver,
            shutdown_request_rx,
            beat_shutdown_tx,
            wait_for_shutdown_tx.clone(),
        );

        if has_wid {
            client.spawn_heartbeat(beat_shutdown_rx, wait_for_shutdown_tx);
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

    pub async fn last_beat(&self) -> Result<BeatState> {
        match self.send_command(FaktoryCommand::GetLastBeat).await? {
            FaktoryResponse::Beat(beat_state) => Ok(beat_state),
            other => Err(Error::UnexpectedResponse(
                format!("{:?}", other),
                "FaktoryResponse::BeatState".to_string(),
            )),
        }
    }

    pub async fn fetch(&self, queues: Vec<Cow<'static, str>>) -> Result<Option<Job>> {
        match self.send_command(FaktoryCommand::Fetch(queues)).await? {
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

    /// Initiates the shutdown procedure, which will eventually close the Faktory client.
    /// The shutdown will only be finalized after the last enqueued command has been processed.
    /// New commands can't be enqueued after this. It will result in a `Error::ClientClosed`.
    ///
    /// Will shutdown the connection task as well as the beat task (if running)
    /// If we still have a connection to faktory, this will send the END command as well.
    pub async fn close(mut self) -> Result<()> {
        // Only failures possible are Full and Close
        // If queue is full someone already scheduled the shutdown (by calling `Self::close`) so we don't need to do it again
        // If queue is closed the command task has gone away, so we already are effectively shutdown
        let _ = self.shutdown_request_tx.try_send(());

        // TODO: check if relaxed is enough (it probably is as this is the only atomic used, things get messy with more than 1 atomic)
        self.shutdown_request_cached.store(true, Ordering::Relaxed);

        // Will bail out when tx is dropped, so we know all tasks are gone
        let _ = self.wait_for_shutdown_rx.recv().await;

        Ok(())
    }

    pub fn is_shutting_down(&self) -> bool {
        // TODO: check if relaxed is enough (it probably is as this is the only atomic used, things get messy with more than 1 atomic)
        self.shutdown_request_cached.load(Ordering::Relaxed)
    }

    async fn send_command(&self, command: FaktoryCommand) -> Result<FaktoryResponse> {
        if self.is_shutting_down() {
            return Err(Error::ClientClosed);
        }

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

    async fn handle_msg(msg: FaktoryCommandMessage, connection: &mut Connection, config: &Config) {
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

                    if let Some(conn) = Client::attempt_connect(config).await {
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
                .fetch(queues)
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

    // Attempts to connect to faktory with exponential backoff + jitter. Largest sleep is 32 seconds
    async fn attempt_connect(config: &Config) -> Option<Connection> {
        let mut backoff = 0;
        loop {
            let backoff_ms = (1 << backoff) * 1000;
            let jitter = thread_rng().gen_range(1..=backoff_ms);
            match dbg!(Connection::new(config.clone()).await) {
                Ok(connection) => return Some(connection),
                Err(_) => tokio::time::sleep(Duration::from_millis(jitter)).await,
            }

            if backoff < 5 {
                backoff += 1;
            }
        }
    }

    // Spawns a task that owns our connection to the faktory server
    fn spawn_faktory_connection(
        &self,
        mut command_receiver: FaktoryCommandReceiver,
        mut shutdown_request_rx: mpsc::Receiver<()>,
        beat_shutdown_tx: oneshot::Sender<()>,
        wait_for_shutdown_tx: broadcast::Sender<()>,
    ) {
        let config = self.config.clone();
        let shutdown_request_tx = self.shutdown_request_tx.clone();

        tokio::task::spawn(async move {
            // Used as a drop guard, when all wait_for_shutdown_tx are dropped recv will stop blocking
            let _wait_for_shutdown_tx = wait_for_shutdown_tx;

            let mut connection = None;

            loop {
                let msg = tokio::select! {
                    msg = command_receiver.recv() => match msg {
                        Some(msg) => msg,

                        // If no messages are in the channel and the sender is closed we can bail out
                        // The sender could be closed as a producer won't have a heartbeat task and all the Client's might be dropped
                        None => break,
                    },

                    _ = shutdown_request_rx.recv() => {
                        // Since we must shutdown, nothing can be enqueued anymore. If the queue is empty we bail out.
                        // If not, we empty it and reschedule the shutdown as we just consumed the shutdown command
                        match command_receiver.try_recv() {
                            Err(_) => break,
                            Ok(msg) => {
                                // Only failures possible are Full and Close
                                // If queue is full someone raced with us by scheduling the shutdown (by calling `Self::close`) so we don't need to do it again
                                // Closed should be impossible as this task owns the receiver, it will never go away from under us and we never call close on it
                                if let Err(TrySendError::Closed(_)) = shutdown_request_tx.try_send(()) {
                                    unreachable!();
                                }
                                msg
                            }
                        }
                    },
                };

                // We have a job to do, we must keep going until it's finished otherwise it will be lost with the rest of the channel
                loop {
                    match &mut connection {
                        Some(connection) => {
                            Client::handle_msg(msg, connection, &config).await;
                            break;
                        }
                        None => connection = Client::attempt_connect(&config).await,
                    }
                }
            }

            // This can only fail if beat_shutdown_rx has gone away, which means the heartbeat task has already shutdown
            // It shouldn't happen for consumers, but producers don't have a beat task
            let _ = beat_shutdown_tx.send(());

            if let Some(mut conn) = connection {
                // If end fails let's just close the connection without it, it's not a big deal
                let _ = conn.end().await;
            }
        });
    }

    // If we're a worker, spawn a heartbeat task that sends a BEAT message to faktory every
    // ~15 seconds with (-5..5) seconds of jitter
    fn spawn_heartbeat(
        &self,
        shutdown_rx: oneshot::Receiver<()>,
        wait_for_shutdown_tx: broadcast::Sender<()>,
    ) {
        let command_sender = self.command_sender.clone();
        tokio::task::spawn(async move {
            // Used as a drop guard, when all wait_for_shutdown_tx are dropped recv will stop blocking
            let _wait_for_shutdown_tx = wait_for_shutdown_tx;

            tokio::select! {
                _ = shutdown_rx => {},
                _ = async {
                    loop {
                        // We have to manually implement the beat function here to avoid cloning the shutdown_request_cached Arc
                        // Otherwise the shutdown procedure won't start
                        let (response_sender, response_receiver) = oneshot::channel();
                        if command_sender.send((FaktoryCommand::Beat, response_sender)).await.is_err() {
                            // If the command_receiver has gone away, the executor task isn't available so we can just bail out
                            break;
                        }

                        // If beat fails we will try again soon, before the 60 seconds limit
                        // The enqueued command execution logic will reconnect when needed
                        //
                        // Will we have a problem if beat takes too long? Or of it fails ~=6 times in a row?
                        let _ = response_receiver.await;

                        let interval = thread_rng().gen_range(10..20);
                        tokio::time::sleep(Duration::from_secs(interval)).await;
                    }
                } => {}
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
        let fetch_result = client.fetch(vec![queue.clone().into()]).await;
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
        while let Ok(Some(job)) = client.fetch(vec![queue.clone().into()]).await {
            jobs.push(job.clone());
            client
                .ack(job.id().to_string())
                .await
                .expect("could not ack");
        }

        assert_eq!(5, jobs.len());

        let _ = client.close().await;
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

        while dbg!(client.fetch(vec![queue.clone().into()]).await).is_err() {
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

        client.close().await.expect("able to close connection");

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn it_returns_faktory_err() {
        let client = Client::new(
            Config::from_uri(
                "localhost:7419",
                Some("test".to_string()),
                Some(Uuid::new_v4().to_string()),
            ),
            256,
        );

        assert!(client
            .fail(FailConfig::new(
                "doesnt exist".to_owned(),
                "yeahhhh".to_owned(),
                "inexistent".to_owned(),
                None
            ))
            .await
            .is_err());
    }
}
