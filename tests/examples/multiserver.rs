extern crate dbqueue_server;
extern crate mio;
extern crate eventual;

use dbqueue_server::{Server, ConcurrentQueues};
use mio::{NonBlock, Socket, tcp};
use eventual::Async;
use std::{thread, net};
use std::sync::mpsc::channel;

fn main() {
    let (tx, rx) = channel();

    let channel_executor = |x| {
        let guard = thread::spawn(x);
        tx.send(guard).unwrap();
    };

    let queues = ConcurrentQueues::new(128 * 1024);

    let servers = (0..4).map(|_| {
        Server::with_queues(
            |x| { channel_executor(x) },
            Default::default(), 4096, queues.clone()
        ).unwrap()
    }).collect::<Vec<_>>();

    for (i, server) in servers.iter().enumerate() {
        server.listen(
            listener(&format!("127.0.0.1:300{}", i).parse().unwrap())
        ).await().unwrap();
    }

    rx.iter().map(|guard| guard.join().unwrap()).collect::<Vec<_>>();
}

fn listener(addr: &net::SocketAddr) -> NonBlock<net::TcpListener> {
    let socket = tcp::v4().unwrap();
    socket.set_reuseaddr(true).unwrap();
    socket.bind(addr).unwrap();
    socket.listen(1024).unwrap()
}

