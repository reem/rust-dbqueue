use mio::{EventLoop, NonBlock};
use eventual::{self, Future, Async, Complete, AsyncError};
use uuid::Uuid;

use common::{ClientMessage, ServerMessage, MAX_CLIENT_MESSAGE_LEN};
use rt::Handler;
use queue::{Queue, Queues};

use std::net::TcpStream;
use std::io::{self, Cursor, ErrorKind};
use std::collections::{HashMap, VecDeque};

use {Error};

pub struct Connection<Q: Queue> {
    connection: NonBlock<TcpStream>,
    incoming: Vec<u8>,
    outgoing: VecDeque<Cursor<Vec<u8>>>,
    unconfirmed: HashMap<Uuid, (Complete<(), Error>, Future<(), (Q, Uuid, Vec<u8>)>)>
}

impl<Q: Queue> Connection<Q> {
    #[inline]
    pub fn new(connection: NonBlock<TcpStream>) -> Connection<Q> {
        Connection {
            connection: connection,
            incoming: Vec::new(),
            outgoing: VecDeque::new(),
            unconfirmed: HashMap::new()
        }
    }

    /// Access the underlying connection
    #[inline]
    pub fn connection(&self) -> &NonBlock<TcpStream> {
        &self.connection
    }

    #[inline]
    pub fn readable<Qu>(&mut self, queues: &Qu, evloop: &mut EventLoop<Handler<Qu>>)
        -> Result<(), Error>
    where Qu: Queues<Queue=Q> + Send {
        match io::copy(&mut self.connection, &mut self.incoming) {
            Ok(_) => {},
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {},
            Err(e) => return Err(Error::from(e)),
        };

        // Process 1 or more messages read into the incoming buffer.
        //
        // Sometimes more than one message will be transferred.
        while let Ok((message, message_len)) = ClientMessage::decode(&self.incoming) {
            // Chop off the message we just processed.
            self.incoming = self.incoming[message_len as usize..].to_vec();

            let outgoing = Cursor::new(try!(match message {
                ClientMessage::CreateQueue(id) => {
                    queues.insert(id);
                    ServerMessage::QueueCreated
                },

                ClientMessage::DeleteQueue(id) => {
                    queues.remove(&id)
                        .map(|_| ServerMessage::QueueDeleted)
                        .unwrap_or(ServerMessage::NoSuchEntity)
                },

                ClientMessage::Enqueue(id, object) => {
                    let uuid = Uuid::new_v4();
                    queues.queue(&id).map(|queue| {
                        match queue.enqueue(uuid.clone(), object) {
                            Ok(()) => ServerMessage::ObjectQueued(uuid),
                            Err((uuid, data)) => ServerMessage::Full(uuid, data)
                        }
                    }).unwrap_or(ServerMessage::NoSuchEntity)
                },

                ClientMessage::Read(id, timeout) =>
                    try!(self.read_ms(evloop, queues, id, timeout)),

                ClientMessage::Confirm(uuid) => self.confirm(&uuid)
            }.encode()));

            self.outgoing.push_back(outgoing);
        }

        if self.incoming.len() as u64 > MAX_CLIENT_MESSAGE_LEN {
            // The client has sent an overlong message.
            Err(Error::OverLongMessage)
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn writable(&mut self) {
        while self.outgoing.len() != 0 {
            let mut top = self.outgoing.pop_front().unwrap();
            match io::copy(&mut top, &mut self.connection) {
                Ok(0) | Err(_) => {
                    self.outgoing.push_front(top);
                    break
                },
                Ok(_) => continue,
            }
        }
    }

    fn read_ms<Qu>(&mut self, evloop: &mut EventLoop<Handler<Qu>>,
                  queues: &Qu, id: String, timeout: u64) -> Result<ServerMessage, Error>
    where Qu: Queues<Queue=Q> + Send {
        if let Some(queue) = queues.queue(&id) {
            let top = queue.dequeue();
            if let Some((uuid, object)) = top {
                let (timeout_tx, timeout_rx) = Future::pair();
                let (confirm_tx, confirm_rx) = Future::pair();
                let (cancellation_tx, cancellation_rx) = Future::pair();

                try!(evloop.timeout_ms(timeout_tx, timeout));

                let (cuuid, cobject) = (uuid.clone(), object.clone());
                eventual::select((timeout_rx, confirm_rx))
                    .map(move |(choice, _)| {
                        match choice {
                            // Timeout expired first.
                            0 => match queue.requeue(cuuid, cobject) {
                                Ok(()) => {},
                                Err((id, data)) => {
                                    cancellation_tx.fail((queue, id, data))
                                }
                            },
                            // Confirm received first.
                            1 => cancellation_tx.complete(()),
                            x => panic!("Received impossible hint {:?} from select", x)
                        }
                    }).fire();

                self.unconfirmed.insert(uuid.clone(),
                                        (confirm_tx, cancellation_rx));

                Ok(ServerMessage::Read(uuid, object))
            } else {
                Ok(ServerMessage::Empty)
            }
        } else {
            Ok(ServerMessage::NoSuchEntity)
        }
    }

    fn confirm(&mut self, uuid: &Uuid) -> ServerMessage {
        self.unconfirmed.remove(uuid)
            .map(|(confirm_tx, cancellation_rx)| {
                match cancellation_rx.poll() {
                    // The timeout has elapsed and data succesfully
                    // requeued.
                    Ok(Ok(())) => ServerMessage::Requeued,

                    // The timeout has elapsed, but the data was not
                    // succesfully requeued.
                    Ok(Err(AsyncError::Failed((queue, id, data)))) => {
                        // Try to queue again now.
                        match queue.requeue(id, data) {
                            Ok(()) => ServerMessage::Requeued,
                            Err((id, data)) => {
                                ServerMessage::Full(id, data)
                            }
                        }
                    },
                    Ok(_) => { panic!("Unexpected cancellation abort.") }
                    Err(_) => {
                        confirm_tx.complete(());
                        ServerMessage::Confirmed
                    }
                }
            }).unwrap_or(ServerMessage::NoSuchEntity)
    }
}

