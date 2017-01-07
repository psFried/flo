use std::io::{self, Write, Read};
use protocol::{ProtocolMessage, ServerMessage};

use flo_event::{FloEvent, OwnedFloEvent};


const BUFFER_LENGTH: usize = 8 * 1024;

pub struct FloConnection<T: Write + Read> {
    writer: T,
}

impl <T: Write + Read> FloConnection<T> {
    pub fn write(&mut self, message: &mut ProtocolMessage) -> io::Result<()> {
        let mut buffer = [0; BUFFER_LENGTH];
        let nread = message.read(&mut buffer[..])?;
        self.writer.write_all(&buffer[..nread])
    }


//    pub fn read(&mut self) -> io::Result<ServerMessage<OwnedFloEvent>> {
//        let mut buffer = [0; BUFFER_LENGTH];
//        let nread = self.writer.read(&mut buffer[..])?;
//
//    }
}
