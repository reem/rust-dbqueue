#![cfg_attr(test, feature(ip_addr, test))]
extern crate dbqueue_server;
extern crate dbqueue_client;
extern crate dbqueue_common;
extern crate mio;
extern crate eventual;
extern crate uuid;
extern crate env_logger;

#[cfg(test)]
extern crate test;

#[cfg(test)]
mod tests {
    use dbqueue_server::{Server};
    use dbqueue_client::{Client, Message, PipelinedClient};
    use dbqueue_common::{ClientMessage, ServerMessage};

    use dbqueue_client::Error as ClientError;

    use mio::{EventLoopConfig, NonBlock, Socket, tcp};
    use eventual::Async;
    use uuid::Uuid;
    use test::Bencher;
    use env_logger;

    use std::{thread, net};
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

    static PORT: AtomicUsize = ATOMIC_USIZE_INIT;

    fn sock() -> net::SocketAddr {
        let _ = env_logger::init();

        let port = 3000 + PORT.fetch_add(1, Ordering::Relaxed) as u16;
        let ip = net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 1));
        net::SocketAddr::new(ip, port)
    }

    fn listener(addr: &net::SocketAddr) -> NonBlock<net::TcpListener> {
        let socket = tcp::v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(addr).unwrap();
        socket.listen(1024).unwrap()
    }

    #[test]
    fn test_single_create_send_read_confirm() {
        let addr = sock();
        let server = Server::start(|x| { thread::spawn(x); }).unwrap();
        server.listen(listener(&addr)).await().unwrap();

        let mut client = Client::connect(addr).unwrap();

        let foo = client.create(String::from("foo")).unwrap();
        client.send(foo.clone(), vec![16; 100]).unwrap();

        let response = client.read_ms(foo, 1000).unwrap();
        assert_eq!(response.data, vec![16; 100]);
        client.confirm(response.id).unwrap();

        server.shutdown().await().unwrap();
    }

    #[test]
    fn test_multiple_items_in_one_queue() {
        let data = [vec![56; 200], vec![243;200], vec![78;200]];

        let addr = sock();
        let server = Server::start(|x| { thread::spawn(x); }).unwrap();
        server.listen(listener(&addr)).await().unwrap();

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

        server.shutdown().await().unwrap();
    }

    #[test]
    fn test_read_confirm_timeout() {
        let addr = sock();
        let server = Server::configured(
            |x| { thread::spawn(x); },
            EventLoopConfig {
                io_poll_timeout_ms: 1000,
                notify_capacity: 4096,
                messages_per_tick: 256,
                // Changed to 1 from default 100.
                timer_tick_ms: 1,
                timer_wheel_size: 1024,
                timer_capacity: 65536
            },
            128).unwrap();
        server.listen(listener(&addr)).await().unwrap();

        let mut client = Client::connect(addr).unwrap();

        let foo = client.create(String::from("foo")).unwrap();
        client.send(foo.clone(), vec![1; 128]).unwrap();
        let message = client.read_ms(foo.clone(), 10).unwrap();
        thread::sleep_ms(20);

        if let ClientError::Requeued = client.confirm(message.id).unwrap_err() {}
        else { panic!("Confirm sent after timeout elapsed, but data not requeued.") }

        // After a Requeue, the next message should be the requeued one.
        let message = client.read_ms(foo.clone(), 100).unwrap();
        assert_eq!(message.data, vec![1; 128]);
        client.confirm(message.id).unwrap();

        server.shutdown().await().unwrap();
    }

    #[test]
    fn test_request_pipelining() {
        let requests_phase_1 = [
            ClientMessage::CreateQueue(String::from("foo")),
            ClientMessage::Enqueue(String::from("foo"), vec![1; 128]),
            ClientMessage::Enqueue(String::from("foo"), vec![2; 128]),
            ClientMessage::Read(String::from("foo"), 1000),
            ClientMessage::Enqueue(String::from("foo"), vec![3; 128]),
            ClientMessage::Read(String::from("foo"), 1000),
            ClientMessage::Read(String::from("foo"), 1000),
            // We will send Confirm requests once we get the data.
        ];

        let addr = sock();
        let server = Server::start(|x| { thread::spawn(x); }).unwrap();
        server.listen(listener(&addr)).await().unwrap();

        let mut client = PipelinedClient::connect(addr).unwrap();

        // Send all requests without waiting for responses.
        for request in &requests_phase_1 {
            client.send(request).unwrap();
        }

        let (id1, id2, id3, data1, data2, data3) = {
            let mut responses = client.iter();
            assert_eq!(responses.next().unwrap(), ServerMessage::QueueCreated);

            let id1 = unwrap_queued_message(responses.next().unwrap());
            let id2 = unwrap_queued_message(responses.next().unwrap());
            let data1 = unwrap_data_message(responses.next().unwrap()).1;
            let id3 = unwrap_queued_message(responses.next().unwrap());
            let data2 = unwrap_data_message(responses.next().unwrap()).1;
            let data3 = unwrap_data_message(responses.next().unwrap()).1;


            (id1, id2, id3, data1, data2, data3)
        };

        assert_eq!(data1, vec![1; 128]);
        assert_eq!(data2, vec![2; 128]);
        assert_eq!(data3, vec![3; 128]);

        let requests_phase_2 = [
            ClientMessage::Confirm(id1),
            ClientMessage::Confirm(id2),
            ClientMessage::Confirm(id3),
        ];

        for request in &requests_phase_2 {
            client.send(request).unwrap();
        }

        let mut responses = client.iter();

        assert_eq!(responses.next().unwrap(), ServerMessage::Confirmed);
        assert_eq!(responses.next().unwrap(), ServerMessage::Confirmed);
        assert_eq!(responses.next().unwrap(), ServerMessage::Confirmed);

        server.shutdown().await().unwrap();
    }

    fn unwrap_queued_message(message: ServerMessage) -> Uuid {
        match message {
            ServerMessage::ObjectQueued(id) => id,
            x => panic!("Expected ObjectQueued, received {:?}", x)
        }
    }

    fn unwrap_data_message(message: ServerMessage) -> (Uuid, Vec<u8>) {
        match message {
            ServerMessage::Read(id, data) => (id, data),
            x => panic!("Expected Read, received {:?}", x)
        }
    }

    #[bench]
    fn bench_roundtrip_pipelining(b: &mut Bencher) {
        let addr = sock();
        let server = Server::start(|x| { thread::spawn(x); }).unwrap();
        server.listen(listener(&addr)).await().unwrap();

        let mut client = Client::connect(&addr).unwrap();
        client.create(String::from("foo")).unwrap();

        let mut pipelined = PipelinedClient::connect(&addr).unwrap();

        b.iter(|| {
            for i in (0..32) {
                pipelined
                    .send(&ClientMessage::Enqueue(String::from("foo"), vec![i; 256]))
                    .unwrap();
            }

            for response in pipelined.iter() {
                unwrap_queued_message(response);
            }

            let message = ClientMessage::Read(String::from("foo"), 1000);
            for _ in (0..32) {
                pipelined.send(&message).unwrap();
            }

            let ids = pipelined.iter().map(unwrap_data_message).enumerate()
                .map(|(index, (id, data))| {
                    assert_eq!(data, vec![index as u8; 256]);
                    id
                }).collect::<Vec<_>>();

            for id in ids {
                pipelined.send(&ClientMessage::Confirm(id)).unwrap();
            }

            for response in pipelined.iter() {
                assert_eq!(response, ServerMessage::Confirmed);
            }
        });

        server.shutdown().await().unwrap();
    }
}

