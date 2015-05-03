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

use common::{ClientMessage, ServerMessage};

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
pub struct QueueId(String);

impl From<String> for QueueId {
    /// Construct a new QueueId from the queue name.
    fn from(name: String) -> QueueId { QueueId(name) }
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
    pub fn create(&mut self, queue_name: String) -> Result<QueueId> {
        match try!(self.send_message(ClientMessage::CreateQueue(queue_name.clone()))) {
            ServerMessage::QueueCreated => Ok(QueueId::from(queue_name)),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Delete an existing queue.
    pub fn delete(&mut self, queue: QueueId) -> Result<()> {
        match try!(self.send_message(ClientMessage::DeleteQueue(queue.0.clone()))) {
            ServerMessage::QueueDeleted => Ok(()),
            ServerMessage::NoSuchEntity => Err(Error::NoQueue(queue)),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Send an object to an existing queue on the server.
    pub fn send(&mut self, queue: QueueId, data: Vec<u8>) -> Result<Uuid> {
        match try!(self.send_message(ClientMessage::Enqueue(queue.0.clone(), data))) {
            ServerMessage::ObjectQueued(id) => Ok(id),
            ServerMessage::NoSuchEntity => Err(Error::NoQueue(queue)),
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
            ServerMessage::Read(id, data) => {
                Ok(Message { id: id, data: data })
            },
            ServerMessage::Empty => Err(Error::Empty),
            ServerMessage::NoSuchEntity => Err(Error::NoQueue(queue)),
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
            ServerMessage::NoSuchEntity => Err(Error::NoObject(entity_id)),
            _ => panic!("Received incorrect message from the server.")
        }
    }

    fn send_message(&mut self, message: ClientMessage) -> Result<ServerMessage> {
        try!(self.pipeline.send(message));
        self.pipeline.receive()
    }
}


