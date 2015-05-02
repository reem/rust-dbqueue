use mio::{EventLoop, NonBlock};
use eventual::{self, Future, Async, Complete};
use uuid::Uuid;

use common::{ClientMessage, ServerMessage, MAX_CLIENT_MESSAGE_LEN};
use rt::{Handler, Queue};

use std::net::TcpStream;
use std::io::{self, Cursor};
use std::collections::HashMap;

use {Error};

pub struct Connection {
    connection: NonBlock<TcpStream>,
    incoming: Vec<u8>,
    outgoing: Vec<Cursor<Vec<u8>>>,
    unconfirmed: HashMap<Uuid, (Complete<(), Error>, Future<(), Error>)>
}

impl Connection {
    #[inline]
    pub fn new(connection: NonBlock<TcpStream>) -> Connection {
        Connection {
            connection: connection,
            incoming: Vec::new(),
            outgoing: vec![],
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
                    evloop: &mut EventLoop<Handler>) -> Result<(), ()> {
        // TODO: Error reporter
        // TODO: Check for WouldBlock
        let _ = io::copy(&mut self.connection, &mut self.incoming);

        let message = ClientMessage::decode(&self.incoming);

        if let Ok((message, message_len)) = message {
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
                    queues.get(&id).cloned().map(|queue| {
                        let (uuid, object) = match queue.borrow_mut().pop_front() {
                            Some(x) => x,
                            None => return ServerMessage::Empty
                        };

                        let (timeout_tx, timeout_rx) = Future::pair();
                        let (confirm_tx, confirm_rx) = Future::pair();
                        let (cancellation_tx, cancellation_rx) = Future::pair();

                        // TODO: Error reporter
                        let _ = evloop.timeout_ms(timeout_tx, timeout);

                        // Set up re-queuing.
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
                    }).unwrap_or(ServerMessage::NoSuchEntity)
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

            self.outgoing.push(outgoing);

            Ok(())
        } else if self.incoming.len() as u64 > MAX_CLIENT_MESSAGE_LEN {
            // The client has sent an overlong message.
            Err(())
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn writable(&mut self) {
        let len = self.outgoing.len();
        if len == 0 { return }
        let copied = if let Some(outgoing) = self.outgoing.get_mut(len - 1) {
            io::copy(outgoing, &mut self.connection)
        } else { Ok(0) };

        if let Ok(0) = copied { self.outgoing.pop(); }
    }
}

