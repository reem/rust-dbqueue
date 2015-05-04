#![feature(std_misc)]
//! # Queue Server
//!
//! The server component of this simple queueing is responsible
//! for dispatching incoming client messages to their proper queues
//! and also responding for completing client requests on those queues.
//!

extern crate dbqueue_common as common;
extern crate eventual;
extern crate mio;
extern crate iobuf;
extern crate uuid;
extern crate threadpool;
extern crate comm;

#[macro_use]
extern crate log;

pub use error::{Error, Result};
pub use executor::Executor;
pub use queue::{Queue, Queues};
pub use queue::concurrent::{ConcurrentQueue, ConcurrentQueues};

use mio::NonBlock;
use std::net::TcpListener;
use eventual::Future;

use queue::rcqueue::RcQueues;

/// Contains the error type used throughout this crate.
mod error;

/// Contains the Handler type and its implementation of `mio::Handler`.
///
/// This contains the logic for responding to `Server` messages,
/// accepting connections, and delegating readable and writable events.
mod rt;

/// An Executor trait, for being generic over thread pools and such.
mod executor;

/// The Connection type and associated logic.
///
/// The logic for processing ClientMessages and producing ServerMessages
/// is located here.
mod connection;

/// The Queue and Queues traits, and some concrete implementations.
///
/// Particularly RcQueue and RcQueues, a single threaded queue implementation,
/// and ConcurrentQueue and ConcurrentQueues, a queue safe to share between
/// threads which is lock free for enqueueing, requeueing, and dequeueing.
mod queue;

/// Server serves as a communication point with a running server
/// usually running on another thread.
///
/// Server can be used to pass messages to the running server.
///
/// You can instruct the server to start listening for new client
/// connections on a TcpListener using the `listen` method, and
/// can shutdown the server using the `shutdown` method.
pub struct Server {
    /// The notify queue is a concurrent queue provided by the `mio`
    /// event loop. We can use this end to send messages to the event loop
    /// where they are received and handled in `rt::Handler::notify`.
    notify: mio::Sender<rt::Message>,

    /// This future will be completed when the event loop is completely shut
    /// down and the handler has been dropped.
    shutdown: Future<(), Error>
}

impl Server {
    /// Create a server running on the passed executor.
    ///
    /// It will use the default event loop configuration, a slab size of
    /// ~32k, and a special single-threaded queue with low synchronization
    /// overhead.
    pub fn start<E>(exec: E) -> Result<Server> where E: Executor {
        Server::configured(exec, Default::default(), 32 * 1024)
    }

    /// Create a server using a specific event loop configuration
    /// and slab size.
    ///
    /// It will use a special single-threaded queue with low
    /// synchronization overhead.
    pub fn configured<E>(exec: E, config: mio::EventLoopConfig,
                         slab_size: usize) -> Result<Server>
    where E: Executor {
        let rcqueues: RcQueues = Default::default();
        Server::with_queues(exec, config, slab_size, rcqueues)
    }

    /// Create a server using a specific event loop configuration, and slab size,
    /// sharing an existing set of Queues, which may also be given to other Servers
    /// which are running concurrently.
    ///
    /// Usually you will want to use the `ConcurrentQueues` type to coordinate
    /// which sets of `ConcurrentQueue`s will be shared between Servers.
    ///
    /// You cannot initialize a Server with the special single threaded queue
    /// when using this constructor, as it could cause memory unsafety is a single
    /// threaded queue is shared between multiple threads.
    pub fn with_queues<E, Q>(exec: E, config: mio::EventLoopConfig,
                             slab_size: usize, queues: Q) -> Result<Server>
    where E: Executor, Q: Queues {
         let mut evloop = try!(mio::EventLoop::configured(config));
         let mut handler = rt::Handler::new(slab_size, queues);
         let notify = evloop.channel();

         let shutdown = {
            let (tx, rx) = Future::pair();

            exec.run(Box::new(move || {
                let res = evloop.run(&mut handler);

                // Ensure we drop the handler, causing connections and acceptors
                // to close, *before* we complete the future, and regardless of
                // errors.
                drop(handler);

                match res {
                    Ok(()) => tx.complete(()),
                    Err(e) => tx.fail(Error::from(e))
                }
            }));

            rx
         };

         Ok(Server {
             notify: notify,
             shutdown: shutdown
         })
    }

    /// Start listening on a new acceptor.
    ///
    /// The returned future will be completed when the acceptor is registered
    /// on the event loop and the server is ready to accept connections on it.
    ///
    /// It is safe to share multiple accepts created with `try_clone` among
    /// multiple servers, such that several servers are concurrently accepting
    /// connections at the same address.
    pub fn listen(&self, acceptor: NonBlock<TcpListener>) -> Future<(), Error> {
        let (tx, rx) = Future::pair();
        match self.notify.send(rt::Message::Acceptor(acceptor, tx)) {
            Ok(()) => rx,
            Err(_) => Future::error(Error::Notify)
        }
    }

    /// Shut down this server relatively gracefully.
    ///
    /// The returned future will be completed when the event loop has shut down
    /// and all connections and acceptors have been (with best effort) closed.
    pub fn shutdown(self) -> Future<(), Error> {
        match self.notify.send(rt::Message::Shutdown) {
            Ok(()) => self.shutdown,
            Err(_) => Future::error(Error::Notify)
        }
    }
}
