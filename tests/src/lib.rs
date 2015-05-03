#![cfg_attr(test, feature(ip_addr))]
extern crate dbqueue_server;
extern crate dbqueue_client;
extern crate dbqueue_common;
extern crate mio;
extern crate eventual;
extern crate uuid;

#[cfg(test)]
mod test {
    use dbqueue_server::{Server};
    use dbqueue_client::{Client, Message, PipelinedClient};
    use dbqueue_common::{ClientMessage, ServerMessage};

    use dbqueue_client::Error as ClientError;

    use mio::{EventLoopConfig, tcp};
    use eventual::Async;
    use uuid::Uuid;

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

        server.shutdown().await().unwrap();
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
        server.listen(tcp::listen(&addr).unwrap()).await().unwrap();

        let mut client = Client::connect(addr).unwrap();

        let foo = client.create(String::from("foo")).unwrap();
        client.send(foo.clone(), vec![1; 128]).unwrap();
        let message = client.read_ms(foo.clone(), 10).unwrap();
        thread::sleep_ms(20);

        if let ClientError::Requeued = client.confirm(message.id).unwrap_err() {}
        else { panic!("Confirm sent after timeout elapsed, but data not requeued.") }

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
        server.listen(tcp::listen(&addr).unwrap()).await().unwrap();

        let mut client = PipelinedClient::connect(addr).unwrap();

        println!("Connected");

        // Send all requests without waiting for responses.
        for request in &requests_phase_1 {
            println!("Sending: {:?}", request);
            client.send(request).unwrap();
        }

        println!("Sent requests_phase_1");

        let (id1, id2, id3, data1, data2, data3) = {
            let mut responses = client.iter();
            assert_eq!(responses.next().unwrap(), ServerMessage::QueueCreated);
            println!("Received a response.");

            let id1 = unwrap_queued_message(responses.next().unwrap());
            println!("Received a response.");
            let id2 = unwrap_queued_message(responses.next().unwrap());
            println!("Received a response.");
            let data1 = unwrap_data_message(responses.next().unwrap());
            println!("Received a response.");
            let id3 = unwrap_queued_message(responses.next().unwrap());
            println!("Received a response.");
            let data2 = unwrap_data_message(responses.next().unwrap());
            println!("Received a response.");
            let data3 = unwrap_data_message(responses.next().unwrap());

            println!("Received responses.");

            (id1, id2, id3, data1, data2, data3)
        };

        println!("Received responses to phase 1");

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

    fn unwrap_data_message(message: ServerMessage) -> Vec<u8> {
        match message {
            ServerMessage::Read(_, data) => data,
            x => panic!("Expected Read, received {:?}", x)
        }
    }
}

