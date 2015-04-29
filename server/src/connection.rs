use mio::{NonBlock};
use std::net::TcpStream;

pub struct Connection {
    connection: NonBlock<TcpStream>
}

impl Connection {
    pub fn new(connection: NonBlock<TcpStream>) -> Connection {
        Connection {
            connection: connection
        }
    }

    pub fn connection(&self) -> &NonBlock<TcpStream> {
        &self.connection
    }
}

