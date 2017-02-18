use std::net::SocketAddr;

use flo_sync_client::{SyncConnection, FloEventId};
use super::{Context, FloCliCommand};

pub struct ProduceOptions {
    pub host: String,
    pub port: u16,
    pub namespace: String,
    pub event_data: Vec<Vec<u8>>,
    pub parent_id: Option<FloEventId>,
}


pub struct Producer;

impl FloCliCommand for Producer {
    type Input = ProduceOptions;
    type Error = String;

    fn run(ProduceOptions{host, port, namespace, event_data, parent_id}: ProduceOptions, output: &Context) -> Result<(), Self::Error> {
        let server_address = format!("{}:{}", host, port);
        output.verbose(format!("Attempting connection to: {:?}", &server_address));
        SyncConnection::connect(&server_address).map_err(|io_err| {
            format!("Error establishing connection to flo server: {}", io_err)
        }).and_then(|mut client| {
            output.verbose(format!("connected to {}", &server_address));

            event_data.into_iter().fold(Ok(0), |events_produced, event_data| {
                events_produced.and_then(|count| {
                    client.produce_with_parent(parent_id, &namespace, event_data).map_err(|client_err| {
                        format!("Failed to produce event: {:?}", client_err)
                    }).map(|produced_id| {

                        output.normal(produced_id);
                        count + 1
                    })
                })
            })
        }).map(|produced_count| {
            output.normal(format!("Successfully produced {} events to {}", produced_count, namespace));
        })
    }
}

