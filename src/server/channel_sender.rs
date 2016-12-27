
use std::sync::mpsc::Sender;

use server::engine::api::{ClientMessage};


pub struct ChannelSender {
    pub producer_manager: Sender<ClientMessage>,
    pub consumer_manager: Sender<ClientMessage>,
}

enum Recipient {
    ProducerManager,
    ConsumerManager,
    Both
}

fn do_send(channel_type: &'static str, channel: &Sender<ClientMessage>, message: ClientMessage) -> Result<(), String> {
    trace!("Sending to channel: {}, message: {:?}", channel_type, message);
    channel.send(message).map_err(|err| {
        format!("Error sending to channel: {}, err: {:?}", channel_type, err)
    })
}

impl ChannelSender {
    pub fn send(&self, message: ClientMessage) -> Result<(), String> {
        match get_recepient(&message) {
            Recipient::ConsumerManager => {
                do_send("ConsumerManager", &self.consumer_manager, message)
            }
            Recipient::ProducerManager => {
                do_send("ProducerManager", &self.producer_manager, message)
            }
            Recipient::Both => {
                do_send("ProducerManager", &self.producer_manager, message.clone()).and_then(|()| {
                    do_send("ConsumerManager", &self.consumer_manager, message)
                })
            }
        }
    }
}

fn get_recepient(message: &ClientMessage) -> Recipient {
    match *message {
        ClientMessage::Produce(_) => Recipient::ProducerManager,
        ClientMessage::StartConsuming(_, _) => Recipient::ConsumerManager,
        ClientMessage::UpdateMarker(_, _) => Recipient::ConsumerManager,
        _ => Recipient::Both
    }
}


