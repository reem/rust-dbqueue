extern crate dbqueue_client;
extern crate dbqueue_common;
extern crate mio;
extern crate eventual;
extern crate uuid;
extern crate chrono;
extern crate env_logger;

use dbqueue_client::{Client, PipelinedClient};
use dbqueue_common::{ClientMessage, ServerMessage};

use std::thread::{self, JoinHandle};
use std::net::{self, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

static REQUESTS: AtomicUsize = ATOMIC_USIZE_INIT;

const PIPELINE: u64 = 32;
const ITERS_PER_THREAD: u64 = 1000;
const THREADS_PER_GROUP: u64 = 16;
const SERVERS: u64 = 4;
const CLIENTS_PER_SERVER: u64 = (THREADS_PER_GROUP * 2) / SERVERS;

fn main() {
    env_logger::init().unwrap();

    let mut client = Client::connect("127.0.0.1:3000").unwrap();
    client.create(String::from("foo")).unwrap();

    let mut clients = vec![];

    for i in 0..SERVERS {
        clients.extend((0..CLIENTS_PER_SERVER).map(|_| {
            PipelinedClient::connect(
                format!("127.0.0.1:300{}", i).parse::<net::SocketAddr>().unwrap()
            ).unwrap()
        }));
    }

    let start = chrono::Local::now();
    do_work(&mut clients);
    let end = chrono::Local::now();

    println!("Took: {:?}", end - start);
    println!("Requests: {:?}", REQUESTS.load(Ordering::SeqCst));
}

fn do_work(clients: &mut Vec<PipelinedClient<TcpStream>>) {
    let mut handles = spawn_group(move |client| {
        for _ in 0..PIPELINE {
            client.send(&ClientMessage::Enqueue(String::from("foo"), vec![1; 128]))
                .unwrap();
        }

        for response in client.iter() {
            match response {
                ServerMessage::ObjectQueued(_) => {},
                x => panic!("Received incorrect response: {:?}.", x)
            }
        }

        REQUESTS.fetch_add(PIPELINE as usize, Ordering::SeqCst);
    }, clients);

    handles.extend(spawn_group(move |client| {
        for _ in 0..PIPELINE {
            client.send(&ClientMessage::Read(String::from("foo"), 1000)).unwrap();
        }

        for response in client.iter().collect::<Vec<_>>() {
            match response {
                ServerMessage::Read(id, _) => {
                    client.send(&ClientMessage::Confirm(id)).unwrap();
                },
                ServerMessage::Empty => {},
                x => panic!("Received incorrect response: {:?}.", x)
            }
        }

        for response in client.iter() {
            match response {
                ServerMessage::Confirmed => {},
                x => panic!("Received incorrect response: {:?}.", x)
            }
        }

        REQUESTS.fetch_add(PIPELINE as usize * 2, Ordering::SeqCst);
    }, clients));

    for handle in handles { handle.join().unwrap() }
}

fn spawn_group<F>(action: F, clients: &mut Vec<PipelinedClient<TcpStream>>)
    -> Vec<JoinHandle<()>>
where F: Fn(&mut PipelinedClient<TcpStream>) + Send + Sync + 'static {
    let action = Arc::new(action);

    (0..THREADS_PER_GROUP).map(|_| {
        let mut client = clients.pop().unwrap();
        let action = action.clone();
        thread::spawn(move || {
            for _ in 0..ITERS_PER_THREAD {
                action(&mut client);
            }
        })
    }).collect()
}

