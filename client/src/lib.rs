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

use uuid::Uuid;
use std::net::{ToSocketAddrs, TcpStream};
use std::io::{self, Read, Write};

mod error;

pub struct Client<S: Read + Write = TcpStream> {
    connection: S
}

pub struct Message {
    pub id: Uuid,
    pub data: Vec<u8>
}

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
        Client { connection: stream }
    }

    /// Create a new queue.
    pub fn create(&mut self, queue_name: String) -> Result<QueueId> {
        let outgoing = common::ClientMessage::CreateQueue(queue_name.clone());
        try!(outgoing.encode_to(&mut self.connection));

        let incoming = try!(common::ServerMessage::decode_from(&mut self.connection));

        match incoming {
            common::ServerMessage::QueueCreated => {
                Ok(QueueId::from(queue_name))
            },
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Delete an existing queue.
    pub fn delete(&mut self, queue: QueueId) -> Result<()> {
        let outgoing = common::ClientMessage::DeleteQueue(queue.0);
        try!(outgoing.encode_to(&mut self.connection));

        let incoming = try!(common::ServerMessage::decode_from(&mut self.connection));

        match incoming {
            common::ServerMessage::QueueDeleted => {
                Ok(())
            },
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
        let outgoing = common::ClientMessage::Read(queue.0, timeout);
        try!(outgoing.encode_to(&mut self.connection));

        let incoming = try!(common::ServerMessage::decode_from(&mut self.connection));

        match incoming {
            common::ServerMessage::Read(id, data) => {
                Ok(Message { id: id, data: data })
            },
            _ => panic!("Received incorrect message from the server.")
        }
    }

    /// Confirm that we have processed a message to the point that it should not
    /// be requeued.
    ///
    /// This should be called before the timeout on the associated read elapses.
    pub fn confirm(&mut self, queue: QueueId, entity_id: Uuid) -> Result<()> {
        let outgoing = common::ClientMessage::Confirm(queue.0, entity_id);
        Ok(try!(outgoing.encode_to(&mut self.connection)))
    }
}


