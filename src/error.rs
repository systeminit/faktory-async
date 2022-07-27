pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("faktory replied with: {0} - {1}")]
    Faktory(String, String),

    #[error("unexpected response: got {0}, expected {1}")]
    UnexpectedResponse(String, String),

    #[error("oneshot allocated to get the faktory response for request went away: {0}")]
    ReceiveResponse(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("local queue has gone away, unable to send command, this faktory_async::Client isn't recoverable anymore")]
    SendCommand,
}
