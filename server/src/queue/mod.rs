use uuid::Uuid;

pub mod rcqueue;
pub mod concurrent;

pub trait Queues: Clone + Send + 'static {
    type Queue: Queue;

    fn insert(&self, name: String);
    fn remove(&self, name: &str) -> Option<Self::Queue>;

    fn queue(&self, name: &str) -> Option<Self::Queue>;
}

pub trait Queue: Clone + Send + 'static {
    fn enqueue(&self, id: Uuid, data: Vec<u8>) -> Result<(), (Uuid, Vec<u8>)>;
    fn requeue(&self, id: Uuid, data: Vec<u8>) -> Result<(), (Uuid, Vec<u8>)>;
    fn dequeue(&self) -> Option<(Uuid, Vec<u8>)>;
}

