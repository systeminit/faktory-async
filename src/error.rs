// TODO: have our own error type
pub type Result<T, E = Error> = std::result::Result<T, E>;

use crate::FaktoryCommandMessage;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    ReceiveResponse(#[from] tokio::sync::oneshot::error::RecvError),
    #[error(transparent)]
    SendCommand(#[from] tokio::sync::mpsc::error::SendError<FaktoryCommandMessage>),
    #[error(transparent)]
    BroadcastTryReceive(#[from] tokio::sync::broadcast::error::TryRecvError),

    #[error("unexpected response: got {0}, expected {1}")]
    UnexpectedResponse(String, String),
    #[error("tried to operate on a connection that was explicitly closed")]
    ConnectionAlreadyClosed,
    #[error("received empty message")]
    ReceivedEmptyMessage,
    #[error("received invalid error message {0}")]
    ReceivedInvalidErrorMessage(String),
    #[error("missing carriage return")]
    MissingCarriageReturn,
    #[error("received error message of kind {0}: {1}")]
    ReceivedErrorMessage(String, String),
    // The ruby clients says that if you get this you should ditch the socket and create another one
    #[error("invalid message prefix {0}")]
    InvalidMessagePrefix(String),
}
