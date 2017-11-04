
use std::io::{self, Write};
use std::fmt::{self, Debug};

use futures::{Sink, AsyncSink, StartSend, Poll, Async};
use protocol::*;


pub trait MessageSink: Sink<SinkItem=ProtocolMessage, SinkError=io::Error> + Debug {
}

pub struct MessageSendSink<W: Write> {
    message_buffer: Vec<MessageWriter<'static>>,
    writer: W
}

impl <W: Write> MessageSendSink<W> {
    pub fn new(writer: W) -> MessageSendSink<W> {
        MessageSendSink {
            message_buffer: Vec::with_capacity(8),
            writer: writer
        }
    }
}

impl <W: Write> MessageSink for MessageSendSink<W> { }

impl <W: Write> Debug for MessageSendSink<W> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MessageSink{{ message_buffer: {:?}, .. }}", self.message_buffer)
    }
}

impl <W: Write> Sink for MessageSendSink<W> {
    type SinkItem = ProtocolMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.message_buffer.push(MessageWriter::new_owned(item));
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let MessageSendSink {ref mut message_buffer, ref mut writer} = *self;
        while !message_buffer.is_empty() {

            {
                let message: &mut MessageWriter<'static> = message_buffer.first_mut().unwrap();
                match message.write(writer) {
                    Ok(()) => {
                        if !message.is_done() {
                            return Ok(Async::NotReady);
                        }
                        // If message is done, then we'll loop around and try to write another one
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(Async::NotReady);
                    },
                    Err(io_err) => {
                        return Err(io_err);
                    }
                }
            }
            message_buffer.pop();
        }
        // Once the message buffer is empty, return an Ok
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.poll_complete()
    }
}


