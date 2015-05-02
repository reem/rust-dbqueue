#![cfg_attr(test, feature(ip_addr))]
extern crate dbqueue_server;
extern crate dbqueue_client;
extern crate dbqueue_common;
extern crate mio;
extern crate eventual;

#[cfg(test)]
mod test {
    use dbqueue_server::{Server};
    use dbqueue_client::{Client};

    use mio::tcp;
    use eventual::Async;

    use std::{thread, net};
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

    static PORT: AtomicUsize = ATOMIC_USIZE_INIT;

    fn sock() -> net::SocketAddr {
        let port = 3000 + PORT.fetch_add(1, Ordering::Relaxed) as u16;
        let ip = net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 1));
        net::SocketAddr::new(ip, port)
    }

    #[test]
    fn test_single_create_send_read_confirm() {
        let addr = sock();
        let server = Server::start(|x| { thread::spawn(x); }).unwrap();
        let acceptor = tcp::listen(&addr).unwrap();
        server.listen(acceptor).await().unwrap();

        let mut client = Client::connect(addr).unwrap();

        let foo = client.create(String::from("foo")).unwrap();
        client.send(foo.clone(), vec![16; 100]).unwrap();

        let response = client.read_ms(foo, 1000).unwrap();
        assert_eq!(response.data, vec![16; 100]);
        client.confirm(response.id).unwrap();
    }
}

