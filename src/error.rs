use faktory_lib_async::Error as FaktoryLibAsyncError;
use crate::FaktoryCommandMessage;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    FaktoryLib(#[from] FaktoryLibAsyncError),

    #[error("unexpected response: got {0}, expected {1}")]
    UnexpectedResponse(String, String),

    #[error(transparent)]
    ReceiveResponse(#[from] tokio::sync::oneshot::error::RecvError),
    #[error(transparent)]
    SendCommand(#[from] tokio::sync::mpsc::error::SendError<FaktoryCommandMessage>),
    #[error(transparent)]
    BroadcastTryReceive(#[from] tokio::sync::broadcast::error::TryRecvError),
} 

