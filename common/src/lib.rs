extern crate bincode;
extern crate rustc_serialize;
extern crate uuid;

use uuid::Uuid;
use bincode::SizeLimit;
use std::io::{Read, Write};

pub use bincode::{EncodingResult, DecodingResult, EncodingError, DecodingError};

pub const MAX_CLIENT_MESSAGE_LEN: u64 = 2048;
pub const MAX_SERVER_MESSAGE_LEN: u64 = 2048;

const CLIENT_SIZE_LIMIT: SizeLimit = SizeLimit::Bounded(MAX_CLIENT_MESSAGE_LEN);
const SERVER_SIZE_LIMIT: SizeLimit = SizeLimit::Bounded(MAX_SERVER_MESSAGE_LEN);

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub enum ClientMessage {
    // FIXME: TyOverby/bincode#34
    // These Strings and Vec<u8>s should be RefBox's of str and [u8]

    /// Create a new queue.
    CreateQueue(String),

    /// Delete an existing queue.
    DeleteQueue(String),

    /// Enqueue a new object on an existing queue.
    Enqueue(String, Vec<u8>),

    /// Send an object from an existing queue.
    ///
    /// We give a timeout of an upper bound on how long we expect to spend processing
    /// this message. If the timeout elapses before we have sent confirmation, then
    /// the message will be requeued.
    ///
    /// Timeouts are given in milliseconds. A timeout of 0 indicates no timeout.
    Read(String, u64),

    /// Confirm that we have processed a message to the point that it should not
    /// be requeued.
    ///
    /// This should be called before the timeout on the associated Read message
    /// elapses.
    Confirm(String, Uuid)
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub enum ServerMessage {
    /// The requested queue was created and is ready to receive messages.
    QueueCreated,

    /// The requested queue was deleted, and can no longer receive messages.
    QueueDeleted,

    /// The response to Read ClientMessage's, which contains the data and
    /// the id of that data.
    // FIXME: TyOverby/bincode#34
    // This Vec<u8> should be a RefBox<'a, [u8]>
    Read(Uuid, Vec<u8>)
}

impl ClientMessage {
    /// Called on the client, to serialize over the wire.
    #[inline]
    pub fn encode_to<W: Write>(&self, write: &mut W) -> EncodingResult<()> {
        bincode::encode_into(self, write, CLIENT_SIZE_LIMIT)
    }

    /// Called on the server, to deserialize from a received message.
    #[inline]
    pub fn decode(buf: &[u8]) -> DecodingResult<ClientMessage> {
        bincode::decode(buf)
    }
}

impl ServerMessage {
    /// Called on the server, to serialize over the wire.
    #[inline]
    pub fn encode(&self) -> EncodingResult<Vec<u8>> {
        bincode::encode(self, SERVER_SIZE_LIMIT)
    }

    /// Called on the client, to deserialize over the wire.
    #[inline]
    pub fn decode_from<R: Read>(read: &mut R) -> DecodingResult<ServerMessage> {
        bincode::decode_from(read, SERVER_SIZE_LIMIT)
    }
}

