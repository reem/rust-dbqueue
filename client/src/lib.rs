//! The Client portion of the dbqueue system.
//!
//! Unlike the Server, the Client is built on a blocking model, since it
//! is feasible to use a new thread for each Client on the clients, but not
//! feasible to use a new thread for each client on the server.
//!

extern crate dbqueue_common as common;
extern crate uuid;

pub use common::{EncodingError, DecodingError};
pub use error::{Error, Result};
pub use pipeline::{Pipeline, ResponseIter};

use common::{ClientMessage, ServerMessage, StrBox, SliceBox};

use uuid::Uuid;
use std::net::{ToSocketAddrs, TcpStream};
use std::io::{self, Read, Write};

mod error;
mod pipeline;

pub struct Client<S: Read + Write = TcpStream> {
    pipeline: Pipeline<S>
}

pub struct Message {
    pub id: Uuid,
    pub data: Vec<u8>
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct QueueId<'a>(StrBox<'a>);

impl From<String> for QueueId<'static> {
    /// Construct a new QueueId from the queue name.
    fn from(name: String) -> QueueId<'static> { QueueId(StrBox::boxed(name)) }
}

impl<'a> From<&'a str> for QueueId<'a> {
    /// Construct a new QueueId from the queue name.
    fn from(name: &'a str) -> QueueId<'a> { QueueId(StrBox::new(name)) }
}

impl Client {
    /// Connect to an existing server, so we can start sending messages.
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<Client> {
        Ok(Client::new(try!(TcpStream::connect(addr))))
    }
}

impl<S: Read + Write> Client<S> {
    /// Create a new Client which reads and writes from the passed stream.
    pub fn new(stream: S) -> Client<S> {
        Client { pipeline: Pipeline::new(stream) }
    }

    /// Create a new queue.
    pub fn create<'a>(&mut self, queue_name: &'a str) -> Result<QueueId<'a>> {
        match try!(self.send_message(ClientMessage::CreateQueue(StrBox::new(queue_name)))) {
            ServerMessage::QueueCreated => Ok(QueueId::from(queue_name)),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Delete an existing queue.
    pub fn delete(&mut self, queue: QueueId) -> Result<()> {
        match try!(self.send_message(ClientMessage::DeleteQueue(queue.0.clone()))) {
            ServerMessage::QueueDeleted => Ok(()),
            ServerMessage::NoSuchEntity =>
                Err(Error::NoQueue(QueueId(queue.0.to_owned()))),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Send an object to an existing queue on the server.
    pub fn send(&mut self, queue: QueueId, data: &[u8]) -> Result<Uuid> {
        let message = ClientMessage::Enqueue(queue.0.clone(), SliceBox::new(data));
        let response = try!(self.send_message(message));

        match response {
            ServerMessage::ObjectQueued(id) => Ok(id),
            ServerMessage::Full(id, data) => Err(Error::Full(id, data.take())),
            ServerMessage::NoSuchEntity =>
                Err(Error::NoQueue(QueueId(queue.0.to_owned()))),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Request an object from an existing queue.
    ///
    /// We give a timeout of an upper bound on how long we expect to spend processing
    /// this message. If the timeout elapses before we have sent confirmation, then
    /// the message will be requeued.
    ///
    /// Timeouts are given in milliseconds. A timeout of 0 indicates no timeout.
    pub fn read_ms(&mut self, queue: QueueId, timeout: u64) -> Result<Message> {
        match try!(self.send_message(ClientMessage::Read(queue.0.clone(), timeout))) {
            ServerMessage::Read(id, data) =>
                Ok(Message { id: id, data: data.take() }),
            ServerMessage::Empty => Err(Error::Empty),
            ServerMessage::NoSuchEntity =>
                Err(Error::NoQueue(QueueId(queue.0.to_owned()))),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Confirm that we have processed a message to the point that it should not
    /// be requeued.
    ///
    /// This should be called before the timeout on the associated read elapses.
    pub fn confirm(&mut self, entity_id: Uuid) -> Result<()> {
        match try!(self.send_message(ClientMessage::Confirm(entity_id))) {
            ServerMessage::Confirmed => Ok(()),
            ServerMessage::Requeued => Err(Error::Requeued),
            ServerMessage::Full(id, data) => Err(Error::Full(id, data.take())),
            ServerMessage::NoSuchEntity => Err(Error::NoObject(entity_id)),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    fn send_message(&mut self, message: ClientMessage) -> Result<ServerMessage<'static>> {
        try!(self.pipeline.send(&message));
        self.pipeline.receive()
    }
}

/// An alternative Client which allows pipelining requests.
///
/// Requests can be sent using `send`, then responses waited on by
/// using `iter` and looping over incoming responses.
// FIXME: Providing a nicer Future-based API is blocked on eventual changes.
pub struct PipelinedClient<S: Read + Write> {
    pipeline: Pipeline<S>
}

impl PipelinedClient<TcpStream> {
    /// Connect to an existing server, so we can start sending messages.
    pub fn connect<T: ToSocketAddrs>(addr: T) -> io::Result<PipelinedClient<TcpStream>> {
        Ok(PipelinedClient::new(try!(TcpStream::connect(addr))))
    }
}

impl<S: Read + Write> PipelinedClient<S> {
    pub fn new(stream: S) -> PipelinedClient<S> {
        PipelinedClient { pipeline: Pipeline::new(stream) }
    }

    /// Send a ClientMessage, but do not wait for a response.
    // NOTE: This API already requires knowledge of the internals
    // for decoding ServerMessages, so not much harm done by not
    // providing as many convenience methods.
    pub fn send(&mut self, message: &ClientMessage) -> Result<()> {
        self.pipeline.send(message)
    }

    /// Get an iterator over all incoming responses.
    ///
    /// The Responses will be in the same order as the outgoing requests,
    /// in FIFO (or really FOFI, since we are receiving) order.
    pub fn iter(&mut self) -> ResponseIter<S> {
        self.pipeline.iter()
    }
}

