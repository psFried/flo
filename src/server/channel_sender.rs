
use std::sync::mpsc::Sender;

use server::engine::api::{ClientMessage, ConsumerMessage, ProducerMessage};


pub struct ChannelSender {
    pub producer_manager: Sender<ProducerMessage>,
    pub consumer_manager: Sender<ConsumerMessage>,
}

const CONSUMER_CHANNEL: &'static str = "ConsumerManager";
const PRODUCER_CHANNEL: &'static str = "ProducerManager";

fn do_send<T>(channel_type: &'static str, channel: &Sender<T>, message: T) -> Result<(), String> {
    channel.send(message).map_err(|err| {
        format!("Error sending to channel: {}, err: {:?}", channel_type, err)
    })
}

impl ChannelSender {
    pub fn send(&self, message: ClientMessage) -> Result<(), String> {
        match message {
            ClientMessage::Consumer(msg) => {
                do_send(CONSUMER_CHANNEL, &self.consumer_manager, msg)
            }
            ClientMessage::Producer(msg) => {
                do_send(PRODUCER_CHANNEL, &self.producer_manager, msg)
            }
            ClientMessage::Both(consumer_msg, producer_msg) => {
                do_send(PRODUCER_CHANNEL, &self.producer_manager, producer_msg).and_then(|()| {
                    do_send(CONSUMER_CHANNEL, &self.consumer_manager, consumer_msg)
                })
            }
        }
    }
}
