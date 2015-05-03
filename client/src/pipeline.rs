use common::{ServerMessage, ClientMessage};

use std::io::{Read, Write};
use {Error, Result};

pub struct Pipeline<S: Read + Write> {
    stream: S,
    expecting: u32
}

impl<S: Read + Write> Pipeline<S> {
    pub fn new(stream: S) -> Pipeline<S> {
        Pipeline {
            stream: stream,
            expecting: 0
        }
    }

    pub fn send(&mut self, data: ClientMessage) -> Result<()> {
        try!(data.encode_to(&mut self.stream));
        self.expecting += 1;
        Ok(())
    }

    pub fn incoming(&self) -> u32 { self.expecting }

    pub fn receive(&mut self) -> Result<ServerMessage> {
        if self.expecting == 0 {
            Err(Error::NoResponseExpected)
        } else {
            let message = try!(ServerMessage::decode_from(&mut self.stream));
            self.expecting -= 1;
            Ok(message.0)
        }
    }

    pub fn iter(&mut self) -> ResponseIter<S> {
        ResponseIter { parent: self }
    }
}

pub struct ResponseIter<'a, S: Read + Write + 'a> {
    parent: &'a mut Pipeline<S>
}

impl<'a, S: Read + Write> Iterator for ResponseIter<'a, S> {
    type Item = ServerMessage;

    fn next(&mut self) -> Option<ServerMessage> {
        if self.parent.expecting == 0 {
            None
        } else {
            match ServerMessage::decode_from(&mut self.parent.stream) {
                Ok(message) => Some(message.0),
                _ => None
            }
        }
    }
}

