use std::net::TcpListener;

use mio::{self, EventLoop, Token, ReadHint, Interest, PollOpt, NonBlock};
use mio::util::Slab;

use eventual::Complete;

use queue::{Queue, Queues};
use connection::Connection;
use {Error};

/// Messages sent from the Server handle to the actual event loop,
/// through the event loop's notify queue.
pub enum Message {
    /// Shut down the event loop and drop the handler as soon as
    /// possible.
    Shutdown,

    /// Start listening on this acceptor. The future will be completed
    /// when the server is ready to accept new connections from this
    /// acceptor.
    Acceptor(NonBlock<TcpListener>, Complete<(), Error>)
}

/// Handler holds acceptors and connections and will manage
/// interfacing with the event loop.
///
/// Handler implements `mio::Handler`, and can be used to run a
/// `mio::EventLoop`.
pub struct Handler<Q: Queues> {
    /// The slab contains all of the registered acceptors and connections,
    /// and is mostly used to map tokens to their associated acceptor or
    /// connection.
    slab: Slab<Registration<Q::Queue>>,

    /// The queues used by this handler.
    ///
    /// They may be shared with over Handlers.
    queues: Q
}

/// Either an Acceptor or a Connection.
// TODO: Generalize to accept any io-registerable stream, so Servers and
// Clients could communicate using non-tcp streams, such as OS pipes.
enum Registration<Q: Queue> {
    Acceptor(NonBlock<TcpListener>),
    Connection(Connection<Q>)
}

impl<Q: Queues + Send> Handler<Q> {
    /// Create a new Handler with the specified slab capacity.
    pub fn new(capacity: usize, queues: Q) -> Handler<Q> {
        Handler {
            slab: Slab::new(capacity),
            queues: queues
        }
    }

    /// Accept a new connection on the acceptor with the specified token.
    // This is a method on the Handler since it needs mutable access to the Slab
    // and Acceptor, which means we can't pass both as arguments and instead have to
    // just pass the Handler/Slab.
    #[inline]
    fn accept(&mut self, evloop: &mut EventLoop<Handler<Q>>, token: Token) {
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

    /// Add this registration to the slab, and get its associated Token.
    fn register(&mut self, registration: Registration<Q::Queue>) -> Token {
        self.slab.insert(registration)
            .ok().expect("No space for a new registration in the handler slab.")
    }

    /// Remove the registration at this Token from the slab and deregister
    /// it from the event loop.
    fn disconnect(&mut self, token: Token, evloop: &mut EventLoop<Handler<Q>>) {
        match self.slab.remove(token).unwrap() {
            Registration::Acceptor(acc) => evloop.deregister(&acc).unwrap(),
            Registration::Connection(conn) => evloop.deregister(conn.connection()).unwrap(),
        }
    }

    /// Get the acceptor at the specified Token.
    ///
    /// ## Panics
    ///
    /// Panics if the Token is not contained in the slab or the Token
    /// is associated with a connection, not an acceptor.
    fn acceptor_at(&self, token: Token) -> &NonBlock<TcpListener> {
        match &self.slab[token] {
            &Registration::Acceptor(ref acc) => acc,
            _ => panic!("Expected acceptor, found connection.")
        }
    }

    /// Get the connection at the specified Token.
    ///
    /// ## Panics
    ///
    /// Panics if the Token is not contained in the slab or the Token
    /// is associated with an acceptor, not a connection.
    fn connection_at(&self, token: Token) -> &Connection<Q::Queue> {
        match &self.slab[token] {
            &Registration::Connection(ref conn) => conn,
            _ => panic!("Expected connection, found acceptor.")
        }
    }
}

impl<Q: Queues + Send> mio::Handler for Handler<Q> {
    type Message = Message;
    type Timeout = Complete<(), Error>;

    /// Respond to readable events on acceptors or connections.
    fn readable(&mut self, evloop: &mut EventLoop<Handler<Q>>,
                token: Token, _: ReadHint) {
        // If the token was deregistered, forget about it.
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

    /// Respond to writable events on a connection.
    fn writable(&mut self, _: &mut EventLoop<Handler<Q>>,
                 token: Token) {
        // If the token was deregistered, forget about it.
        if !self.slab.contains(token) { return }

        match &mut self.slab[token] {
            &mut Registration::Connection(ref mut conn) => conn.writable(),
            _ => { error!("Received writable on an acceptor.") }
        }
    }

    /// Respond to messages sent to us by the associated `Server`.
    fn notify(&mut self, evloop: &mut EventLoop<Handler<Q>>, message: Message) {
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

    /// Respond to timeouts, when they have elapsed.
    fn timeout(&mut self, _: &mut EventLoop<Handler<Q>>, future: Complete<(), Error>) {
        future.complete(());
    }
}

