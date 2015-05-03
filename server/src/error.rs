use common::EncodingError;
use mio::TimerError;
use std::io;

/// Errors which can occur on the server.
#[derive(Debug)]
pub enum Error {
    Notify,
    OverLongMessage,
    Timer(TimerError),
    Encoding(EncodingError),
    Io(io::Error)
}

/// Result alias for the server.
pub type Result<T> = ::std::result::Result<T, Error>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error { Error::Io(err) }
}

impl From<TimerError> for Error {
    fn from(err: TimerError) -> Error { Error::Timer(err) }
}

impl From<EncodingError> for Error {
    fn from(err: EncodingError) -> Error { Error::Encoding(err) }
}

