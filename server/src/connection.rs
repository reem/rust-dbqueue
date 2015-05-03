use mio::{EventLoop, NonBlock};
use eventual::{self, Future, Async, Complete};
use uuid::Uuid;

use common::{ClientMessage, ServerMessage, MAX_CLIENT_MESSAGE_LEN};
use rt::{Handler, Queue};

use std::net::TcpStream;
use std::io::{self, Cursor, ErrorKind};
use std::collections::{HashMap, VecDeque};

use {Error};

pub struct Connection {
    connection: NonBlock<TcpStream>,
    incoming: Vec<u8>,
    outgoing: VecDeque<Cursor<Vec<u8>>>,
    unconfirmed: HashMap<Uuid, (Complete<(), Error>, Future<(), Error>)>
}

impl Connection {
    #[inline]
    pub fn new(connection: NonBlock<TcpStream>) -> Connection {
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
    pub fn readable(&mut self, queues: &mut HashMap<String, Queue>,
                    evloop: &mut EventLoop<Handler>) -> Result<(), Error> {
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

            let outgoing = Cursor::new(match message {
                ClientMessage::CreateQueue(id) => {
                    queues.entry(id)
                        .or_insert_with(|| Queue(Default::default()));
                    ServerMessage::QueueCreated
                },

                ClientMessage::DeleteQueue(id) => {
                    queues.remove(&id)
                        .map(|_| ServerMessage::QueueDeleted)
                        .unwrap_or(ServerMessage::NoSuchEntity)
                },

                ClientMessage::Enqueue(id, object) => {
                    let uuid = Uuid::new_v4();
                    queues.get(&id).map(|queue| {
                        queue.borrow_mut().push_back((uuid.clone(), object));
                        ServerMessage::ObjectQueued(uuid)
                    }).unwrap_or(ServerMessage::NoSuchEntity)
                },

                ClientMessage::Read(id, timeout) => {
                    if let Some(queue) = queues.get(&id).cloned() {
                        let top = queue.borrow_mut().pop_front();
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
                                        0 => queue.borrow_mut().push_front((cuuid, cobject)),
                                        // Confirm received first.
                                        1 => cancellation_tx.complete(()),
                                        x => panic!("Received impossible hint {:?} from select", x)
                                    }
                                }).fire();

                            self.unconfirmed.insert(uuid.clone(),
                                                    (confirm_tx, cancellation_rx));

                            ServerMessage::Read(uuid, object)
                        } else {
                            ServerMessage::Empty
                        }
                    } else {
                        ServerMessage::NoSuchEntity
                    }
                },

                ClientMessage::Confirm(uuid) => {
                    self.unconfirmed.remove(&uuid)
                        .map(|(confirm_tx, cancellation_rx)| {
                            // NOTE: Not racy because single-thread.

                            // The timeout has elapsed.
                            if cancellation_rx.is_ready() {
                                ServerMessage::Requeued
                            } else {
                                confirm_tx.complete(());
                                ServerMessage::Confirmed
                            }
                        }).unwrap_or(ServerMessage::NoSuchEntity)
                }
            }.encode().unwrap());

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
}

