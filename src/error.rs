// TODO: have our own error type
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("unexpected response: got {0}, expected {1}")]
    UnexpectedResponse(String, String),
    #[error("tried to operate on a connection that was explicitly closed")]
    ConnectionAlreadyClosed,
}
