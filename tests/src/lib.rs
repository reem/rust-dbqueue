#![cfg_attr(test, feature(ip_addr))]
extern crate dbqueue_server;
extern crate dbqueue_client;
extern crate dbqueue_common;
extern crate mio;
extern crate eventual;

#[cfg(test)]
mod test {
    use dbqueue_server::{Server};
    use dbqueue_client::{Client, Message};

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
        server.listen(tcp::listen(&addr).unwrap()).await().unwrap();

        let mut client = Client::connect(addr).unwrap();

        let foo = client.create(String::from("foo")).unwrap();
        client.send(foo.clone(), vec![16; 100]).unwrap();

        let response = client.read_ms(foo, 1000).unwrap();
        assert_eq!(response.data, vec![16; 100]);
        client.confirm(response.id).unwrap();
    }

    #[test]
    fn test_multiple_items_in_one_queue() {
        let data = [vec![56; 200], vec![243;200], vec![78;200]];

        let addr = sock();
        let server = Server::start(|x| { thread::spawn(x); }).unwrap();
        server.listen(tcp::listen(&addr).unwrap()).await().unwrap();

        let mut client = Client::connect(addr).unwrap();

        let foo = client.create(String::from("foo")).unwrap();

        for datum in &data {
            client.send(foo.clone(), datum.clone()).unwrap();
        }

        let responses = (0..data.len()).map(|_| {
            client.read_ms(foo.clone(), 1000).unwrap()
        }).collect::<Vec<Message>>();

        for (response, data) in responses.iter().zip(data.iter()) {
            assert_eq!(&response.data, &*data);
        }

        for response in responses {
            client.confirm(response.id).unwrap();
        }
    }
}

