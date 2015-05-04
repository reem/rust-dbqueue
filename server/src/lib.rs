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

#[macro_use]
extern crate log;

pub use error::{Error, Result};
pub use executor::Executor;
pub use queue::{Queue, Queues};

use mio::NonBlock;
use std::net::TcpListener;
use eventual::Future;

use queue::rcqueue::RcQueues;

mod error;
mod rt;
mod executor;
mod connection;
mod queue;

pub struct Server {
    notify: mio::Sender<rt::Message>,
    shutdown: Future<(), Error>
}

impl Server {
    /// Create a server running on the passed executor.
    pub fn start<E>(exec: E) -> Result<Server> where E: Executor {
        Server::configured(exec, Default::default(), 32 * 1024)
    }

    /// Create a server using a specific event loop configuration.
    pub fn configured<E>(exec: E, config: mio::EventLoopConfig,
                         slab_size: usize) -> Result<Server>
    where E: Executor {
        let rcqueues: RcQueues = Default::default();
        Server::with_queues(exec, config, slab_size, rcqueues)
    }

    /// Create a server using a specific event loop configuration,
    /// sharing an existing set of Queues, which may also be given
    /// to other Servers.
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

    pub fn listen(&self, acceptor: NonBlock<TcpListener>) -> Future<(), Error> {
        let (tx, rx) = Future::pair();
        match self.notify.send(rt::Message::Acceptor(acceptor, tx)) {
            Ok(()) => rx,
            Err(_) => Future::error(Error::Notify)
        }
    }

    /// Shut down this server relatively gracefully.
    ///
    /// The returned future will be completed when the event loop has shut down.
    pub fn shutdown(self) -> Future<(), Error> {
        match self.notify.send(rt::Message::Shutdown) {
            Ok(()) => self.shutdown,
            Err(_) => Future::error(Error::Notify)
        }
    }
}
