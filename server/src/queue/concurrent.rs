use uuid::Uuid;
use comm::mpmc::bounded::Channel;

use queue::{Queue, Queues};

use std::sync::{Arc, RwLock};
use std::collections::HashMap;

#[derive(Clone)]
pub struct ConcurrentQueues(usize, Arc<RwLock<HashMap<String, ConcurrentQueue>>>);

impl ConcurrentQueues {
    /// Create a new collection of queueus.
    ///
    /// `capacity` will be used to set the capacity of the innner queues,
    /// which are bounded.
    pub fn new(capacity: usize) -> ConcurrentQueues {
        ConcurrentQueues(capacity, Arc::new(RwLock::new(HashMap::new())))
    }

    /// Insert an existing queue into this collection of queues.
    pub fn insert_queue(&self, name: String, queue: ConcurrentQueue) {
        self.1.write().unwrap().insert(name, queue);
    }
}

impl Queues for ConcurrentQueues {
    type Queue = ConcurrentQueue;

    fn insert(&self, name: String) {
        self.1.write().unwrap().entry(name)
            .or_insert_with(|| ConcurrentQueue::new(self.0));
    }

    fn remove(&self, name: &str) -> Option<ConcurrentQueue> {
        self.1.write().unwrap().remove(name)
    }

    fn queue(&self, name: &str) -> Option<ConcurrentQueue> {
        self.1.read().unwrap().get(name).cloned()
    }
}

#[derive(Clone)]
pub struct ConcurrentQueue(Arc<Channel<'static, (Uuid, Vec<u8>)>>);

impl ConcurrentQueue {
    /// Creat a new queue with the passed capacity.
    pub fn new(capacity: usize) -> ConcurrentQueue {
        ConcurrentQueue(Arc::new(Channel::new(capacity)))
    }
}

impl Queue for ConcurrentQueue {
    fn enqueue(&self, id: Uuid, data: Vec<u8>) -> Result<(), (Uuid, Vec<u8>)> {
        self.0.send_async((id, data)).map_err(|(data, _)| data)
    }

    fn requeue(&self, id: Uuid, data: Vec<u8>) -> Result<(), (Uuid, Vec<u8>)> {
        self.enqueue(id, data)
    }

    fn dequeue(&self) -> Option<(Uuid, Vec<u8>)> {
        self.0.recv_async().ok()
    }
}

