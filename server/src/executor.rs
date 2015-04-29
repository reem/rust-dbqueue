use std::thunk::Thunk;

pub trait Executor {
    fn run(&self, Thunk<'static>);
}

