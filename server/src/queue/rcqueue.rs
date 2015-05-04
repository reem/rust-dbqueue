use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use queue::{Queue, Queues};
use uuid::Uuid;

/// In the single-threaded case, we can get away without the vast majority
/// of synchronization overhead and use a simple ring buffer for our queue.
#[derive(Clone, Debug, Default)]
pub struct RcQueue(pub Rc<RefCell<VecDeque<(Uuid, Vec<u8>)>>>);

#[derive(Clone, Debug, Default)]
pub struct RcQueues(pub Rc<RefCell<HashMap<String, RcQueue>>>);

// We lie to the compiler here about RcQueue's Send-ness, and will instead
// use the public API of Server to prevent RcQueue from being shared
// by multitple servers.
unsafe impl Send for RcQueue { }
unsafe impl Send for RcQueues { }

impl Queues for RcQueues {
    type Queue = RcQueue;

    fn insert(&self, name: String) {
        self.0.borrow_mut().entry(name).or_insert_with(Default::default);
    }

    fn remove(&self, name: &str) -> Option<RcQueue> {
        self.0.borrow_mut().remove(name)
    }

    fn queue(&self, name: &str) -> Option<RcQueue> {
        self.0.borrow().get(name).cloned()
    }
}

impl Queue for RcQueue {
    fn enqueue(&self, id: Uuid, data: Vec<u8>) -> Result<(), (Uuid, Vec<u8>)> {
        Ok(self.0.borrow_mut().push_back((id, data)))
    }

    fn requeue(&self, id: Uuid, data: Vec<u8>) -> Result<(), (Uuid, Vec<u8>)> {
        Ok(self.0.borrow_mut().push_front((id, data)))
    }

    fn dequeue(&self) -> Option<(Uuid, Vec<u8>)> {
        self.0.borrow_mut().pop_front()
    }
}

