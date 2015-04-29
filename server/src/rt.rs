use std::collections::HashMap;
use std::net::TcpListener;

use mio::{self, EventLoop, Token, ReadHint, Interest, PollOpt, NonBlock};
use mio::util::Slab;

use eventual::Complete;

use queue::Queue;
use connection::Connection;
use {Error};

/// Messages sent from the Server handle to the actual event loop,
/// through the event loop's notify queue.
///
/// The runtime is told to create and destroy queues this way,
/// and can also be requested to shut down.
pub enum Message {
    Create(Complete<(), Error>, String),
    Delete(Complete<(), Error>, String),
    Shutdown
}

pub struct Handler {
    slab: Slab<Registration>,
    queues: HashMap<String, Queue>
}

enum Registration {
    Acceptor(NonBlock<TcpListener>),
    Connection(Connection)
}

impl Handler {
    pub fn new(capacity: usize) -> Handler {
        Handler {
            slab: Slab::new(capacity),
            queues: HashMap::new()
        }
    }

    fn accept(&mut self, evloop: &mut EventLoop<Handler>, token: Token) {
        let connection = {
            if let &mut Registration::Acceptor(ref mut acceptor) = &mut self.slab[token] {
                acceptor.accept()
            } else {
                panic!("Handler tried to accept on a connection.");
            }
        };

        match connection {
            Ok(Some(connection)) => {
                let token = self.register(
                    Registration::Connection(Connection::new(connection)));

                // TODO: Error reporter
                let _ = evloop.register_opt(
                    // ugh
                    match self.slab[token] {
                        Registration::Connection(ref conn) => conn.connection(),
                        _ => panic!("Expected connection, found acceptor.")
                    },
                    token,
                    Interest::readable(),
                    PollOpt::edge()
                );
            },

            Ok(None) => {
                panic!("Handler tried to accept on a blocked acceptor.")
            },

            // TODO: Add an error reporter.
            Err(_) => {}
        }
    }

    fn advance_readable_connection(&mut self, evloop: &mut EventLoop<Handler>,
                                   token: Token) {

    }

    fn advance_writable_connection(&mut self, evloop: &mut EventLoop<Handler>,
                                   token: Token) {

    }

    fn register(&mut self, registration: Registration) -> Token {
        self.slab.insert(registration)
            .ok().expect("No space for a new registration in the handler slab.")
    }
}

impl mio::Handler for Handler {
    type Message = Message;
    type Timeout = Complete<(), Error>;

    fn readable(&mut self, evloop: &mut EventLoop<Handler>,
                token: Token, _: ReadHint) {
        if !self.slab.contains(token) { return }

        match self.slab[token] {
            Registration::Acceptor(_) => {
                self.accept(evloop, token)
            },
            Registration::Connection(_) => {
                self.advance_readable_connection(evloop, token)
            }
        }
    }

    fn writable(&mut self, evloop: &mut EventLoop<Handler>,
                 token: Token) {
        if !self.slab.contains(token) { return }

        match self.slab[token] {
            Registration::Acceptor(_) => { panic!("Received writable on an acceptor.") },
            Registration::Connection(_) => {
                self.advance_writable_connection(evloop, token)
            }
        }
    }

    fn notify(&mut self, evloop: &mut EventLoop<Handler>, message: Message) {
        match message {
            Message::Create(complete, id) => {
                // TODO: Add queue configuration.
                self.queues.entry(id).or_insert_with(Queue::new);
                complete.complete(());
            },

            Message::Delete(complete, id) => {
                self.queues.remove(&id);
                complete.complete(());
            },

            Message::Shutdown => {
                // Will trigger the shutdown future to complete.
                evloop.shutdown();
            }
        }
    }
}

