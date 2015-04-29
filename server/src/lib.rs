#![feature(std_misc)]
//! # Queue Server
//!
//! The server component of this simple queueing is responsible
//! for dispatching incoming client messages to their proper queues
//! and also responding for completing client requests on those queues.
//!

// extern crate dbqueue_common as common;
extern crate eventual;
extern crate mio;

pub use error::{Error, Result};
pub use executor::Executor;

use eventual::Future;

mod error;
mod rt;
mod executor;
mod queue;
mod connection;

pub struct Server {
    notify: mio::Sender<rt::Message>,
    shutdown: Future<(), Error>
}

impl Server {
    /// Create a server running on the passed executor.
    pub fn start<E>(exec: E) -> Result<Server> where E: Executor {
        Server::configured(exec, Default::default())
    }

    /// Create a server using a specific event loop configuration.
    pub fn configured<E>(exec: E, config: mio::EventLoopConfig) -> Result<Server>
    where E: Executor {
         // TODO: Convert to try! by adding From impl
         let mut evloop = mio::EventLoop::configured(config).unwrap();
         // TODO: Make capacity configurable
         let mut handler = rt::Handler::new(32 * 1024);
         let notify = evloop.channel();

         let shutdown = {
            let (tx, rx) = Future::pair();

            exec.run(Box::new(move || {
                match evloop.run(&mut handler) {
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

    // NOTE:
    // Some unfortunate code duplication in these methods due to
    // the Shutdown message operating differently than the rest.

    /// Create a queue asynchronously.
    ///
    /// The returned future will be completed when the queue is fully
    /// initialized and ready to receive messages.
    pub fn create(&self, queue: String) -> Future<(), Error> {
        let (tx, rx) = Future::pair();

        match self.notify.send(rt::Message::Create(tx, queue)) {
            Ok(()) => {},
            Err(_) => return Future::error(Error::Notify)
        };

        rx
    }

    /// Delete a queue asynchronously.
    ///
    /// The returned future will be completed when the queue is torn down.
    pub fn delete(&self, queue: String) -> Future<(), Error> {
        let (tx, rx) = Future::pair();

        match self.notify.send(rt::Message::Delete(tx, queue)) {
            Ok(()) => {},
            Err(_) => return Future::error(Error::Notify)
        };

        rx
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

