use common::{DecodingError, EncodingError};
use std::io;

pub enum Error {
    Encoding(EncodingError),
    Decoding(DecodingError),
    Io(io::Error)
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl From<DecodingError> for Error {
    fn from(err: DecodingError) -> Error { Error::Decoding(err) }
}

impl From<EncodingError> for Error {
    fn from(err: EncodingError) -> Error { Error::Encoding(err) }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error { Error::Io(err) }
}

