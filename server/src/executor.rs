use std::thunk::Thunk;
use threadpool::ThreadPool;

pub trait Executor {
    fn run(&self, Thunk<'static>);
}

impl<F: Fn(Thunk<'static>)> Executor for F {
    fn run(&self, thunk: Thunk<'static>) {
        (self)(thunk);
    }
}

impl Executor for ThreadPool {
    fn run(&self, thunk: Thunk<'static>) {
        self.execute(thunk);
    }
}

