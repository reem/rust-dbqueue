use eventual::Complete;
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
    x: i32
}

impl Handler {
    pub fn new() -> Handler {
        Handler { x: 0 }
    }
}

