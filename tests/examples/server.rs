extern crate dbqueue_server;
extern crate mio;
extern crate eventual;

use dbqueue_server::Server;
use mio::{NonBlock, Socket, tcp};
use eventual::Async;
use std::{thread, net};
use std::sync::mpsc::channel;

fn main() {
    let (tx, rx) = channel();

    let server = Server::start(|x| {
        let guard = thread::spawn(x);
        tx.send(guard).unwrap();
    }).unwrap();

    server.listen(listener(&"127.0.0.1:3003".parse().unwrap())).await().unwrap();

    rx.recv().unwrap().join().unwrap();
}

fn listener(addr: &net::SocketAddr) -> NonBlock<net::TcpListener> {
    let socket = tcp::v4().unwrap();
    socket.set_reuseaddr(true).unwrap();
    socket.bind(addr).unwrap();
    socket.listen(1024).unwrap()
}

