use std::io;

/// Errors which can occur on the server.
pub enum Error {
    Notify,
    Io(io::Error)
}

/// Result alias for the server.
pub type Result<T> = ::std::result::Result<T, Error>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error { Error::Io(err) }
}

// TODO: Don't throw away information by using appropriate From impls.

