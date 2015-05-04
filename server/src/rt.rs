use std::collections::{VecDeque, HashMap};
use std::net::TcpListener;
use std::rc::Rc;
use std::cell::RefCell;
use std::ops;

use mio::{self, EventLoop, Token, ReadHint, Interest, PollOpt, NonBlock};
use mio::util::Slab;

use eventual::Complete;

use uuid::Uuid;

use connection::Connection;
use {Error};

/// Since we are single threaded, we can get away with the vast majority
/// of synchronization overhead and use a simple ring buffer for our queue.
///
/// We need shared ownership via Rc and RefCell to make handling of
/// re-queueing much simpler, as we can capture a queue.
///
/// If we were mulithreaded a lock free multiple producer multiple consumer
/// queue would be appropriate.
#[derive(Clone, Debug)]
pub struct Queue(pub Rc<RefCell<VecDeque<(Uuid, Vec<u8>)>>>);

// To work around hopefully temporarily limitations in eventual,
// we have to lie to the compiler about being Send.
//
// This is safe in our program because we are single threaded.
unsafe impl Send for Queue { }

impl ops::Deref for Queue {
    type Target = Rc<RefCell<VecDeque<(Uuid, Vec<u8>)>>>;
    fn deref(&self) -> &Rc<RefCell<VecDeque<(Uuid, Vec<u8>)>>> { &self.0 }
}

/// Messages sent from the Server handle to the actual event loop,
/// through the event loop's notify queue.
///
/// The runtime is told to create and destroy queues this way,
/// and can also be requested to shut down.
pub enum Message {
    Shutdown,
    Acceptor(NonBlock<TcpListener>, Complete<(), Error>)
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

    // This is a method on the Handler since it needs mutable access to the Slab
    // and Acceptor, which means we can't pass both as arguments and instead have to
    // just pass the Handler/Slab.
    #[inline]
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

                match evloop.register_opt(
                    self.connection_at(token).connection(),
                    token,
                    Interest::readable() | Interest::writable(),
                    PollOpt::level()
                ) {
                    Ok(()) => {},
                    Err(e) => {
                        error!("Error registering new connection: {:?}", e);
                        self.slab.remove(token);
                    }
                }
            },

            Ok(None) => {
                // Can occur when a client process dies.
                error!("Handler tried to accept on a blocked acceptor.")
            },

            Err(e) => {
                error!("Error accepting new connection: {:?}", e);
            }
        }
    }

    fn register(&mut self, registration: Registration) -> Token {
        self.slab.insert(registration)
            .ok().expect("No space for a new registration in the handler slab.")
    }

    fn disconnect(&mut self, token: Token, evloop: &mut EventLoop<Handler>) {
        match self.slab.remove(token).unwrap() {
            Registration::Acceptor(acc) => evloop.deregister(&acc).unwrap(),
            Registration::Connection(conn) => evloop.deregister(conn.connection()).unwrap(),
        }
    }

    fn acceptor_at(&self, token: Token) -> &NonBlock<TcpListener> {
        match &self.slab[token] {
            &Registration::Acceptor(ref acc) => acc,
            _ => panic!("Expected acceptor, found connection.")
        }
    }

    fn connection_at(&self, token: Token) -> &Connection {
        match &self.slab[token] {
            &Registration::Connection(ref conn) => conn,
            _ => panic!("Expected connection, found acceptor.")
        }
    }
}

impl mio::Handler for Handler {
    type Message = Message;
    type Timeout = Complete<(), Error>;

    fn readable(&mut self, evloop: &mut EventLoop<Handler>,
                token: Token, _: ReadHint) {
        if !self.slab.contains(token) { return }

        // We need this little next hack because we can't borrow self within
        // this match block, so we have to decide what to do and then do it
        // after the match has exited.
        let next = match &mut self.slab[token] {
            &mut Registration::Connection(ref mut conn) =>
                match conn.readable(&mut self.queues, evloop) {
                    Ok(()) => return,
                    Err(e) => {
                        error!("Connection readable error: {:?}", e);
                        true
                    }
                },
            _ => false
        };

        if next { // A connection hit a fatal error.
            self.disconnect(token, evloop)
        } else { // An acceptor is ready to accept a new connection.
            self.accept(evloop, token)
        }
    }

    fn writable(&mut self, _: &mut EventLoop<Handler>,
                 token: Token) {
        if !self.slab.contains(token) { return }

        match &mut self.slab[token] {
            &mut Registration::Connection(ref mut conn) => conn.writable(),
            _ => { error!("Received writable on an acceptor.") }
        }
    }

    fn notify(&mut self, evloop: &mut EventLoop<Handler>, message: Message) {
        match message {
            Message::Shutdown => {
                // Will trigger the shutdown future to complete.
                evloop.shutdown();
            },
            Message::Acceptor(acceptor, future) => {
                let token = self.register(Registration::Acceptor(acceptor));

                match evloop.register_opt(
                    self.acceptor_at(token),
                    token,
                    Interest::readable(),
                    PollOpt::level()
                ) {
                    Ok(()) => future.complete(()),
                    Err(e) => {
                        self.slab.remove(token);
                        future.fail(Error::from(e));
                    }
                }
            }
        }
    }

    fn timeout(&mut self, _: &mut EventLoop<Handler>, future: Complete<(), Error>) {
        future.complete(());
    }
}

