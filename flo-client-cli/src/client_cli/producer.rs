use flo_client_lib::sync::SyncConnection;
use flo_client_lib::codec::RawCodec;
use flo_client_lib::{FloEventId, ActorId};
use super::{Context, FloCliCommand};

pub struct ProduceOptions {
    pub host: String,
    pub port: u16,
    pub namespace: String,
    pub partition: ActorId,
    pub event_data: Vec<Vec<u8>>,
    pub parent_id: Option<FloEventId>,
}


pub struct Producer;

impl FloCliCommand for Producer {
    type Input = ProduceOptions;
    type Error = String;

    fn run(ProduceOptions{host, port, partition, namespace, event_data, parent_id}: ProduceOptions, output: &Context) -> Result<(), Self::Error> {
        let server_address = format!("{}:{}", host, port);
        output.verbose(format!("Attempting connection to: {:?} to produce {} events", &server_address, event_data.len()));
        SyncConnection::connect_from_str(&server_address, "flo-client-cli", RawCodec, None).map_err(|handshake_err| {
            format!("Error establishing connection to flo server: {}", handshake_err)
        }).and_then(|mut connection| {
            output.debug(format!("connected to {}", &server_address));

            event_data.into_iter().fold(Ok(0), |events_produced, event_data| {
                events_produced.and_then(|count| {
                    connection.produce_to(partition, namespace.as_str(), parent_id, event_data).map_err(|client_err| {
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

