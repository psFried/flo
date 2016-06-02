pub mod consumer;
pub mod producer;
pub mod namespace;
pub mod context;


use rotor::{Scope, Time};
use rotor_http::server::{RecvMode, Server, Head, Response, Fsm};
use rotor::mio::tcp::TcpListener;

use self::context::FloContext;
use self::consumer::RotorConsumerNotifier;
use event_store::FileSystemEventStore;

use std::time::Duration;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;


#[derive(Debug)]
pub enum FloServer {
    Producer(String),
    Consumer(usize)
}

impl <'a> Server for FloServer {
    type Seed = ();
    type Context = FloContext<RotorConsumerNotifier, FileSystemEventStore>;

    fn headers_received(_seed: (),
                        head: Head,
                        res: &mut Response,
                        scope: &mut Scope<Self::Context>) -> Option<(Self, RecvMode, Time)>
    {
        match head.method {
            "GET" => {
                self::consumer::init_consumer(head, res, scope)
            },
            "PUT" => {
				let namespace = get_namespace_from_path(head.path).to_string();
                Some((FloServer::Producer(namespace), RecvMode::Buffered(1024), producer::timeout(scope.now())))
            },
            _ => None
        }
    }

    fn request_chunk(self, _chunk: &[u8], _response: &mut Response, _scope: &mut Scope<Self::Context>) -> Option<Self> {
        unreachable!()
    }

    /// End of request body, only for Progressive requests
    fn request_end(self, _response: &mut Response, _scope: &mut Scope<Self::Context>) -> Option<Self> {
        unreachable!()
    }

    fn timeout(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<(Self, Time)> {
        debug!("** Timeout occured for {:?} ** reponse complete: {:?}", self, response.is_complete());
        match self {
            FloServer::Consumer(_idx) => {
                Some((self, scope.now() + Duration::new(30, 0)))
            },
            FloServer::Producer(_) => None
        }
    }

    fn wakeup(self, response: &mut Response, scope: &mut Scope<Self::Context>) -> Option<Self> {
        if let FloServer::Consumer(id) = self {
            self::consumer::on_wakeup(id, response, scope);
        } else {
            warn!("waking up producer");
        }
        Some(self)
    }

    fn request_received(self, data: &[u8], res: &mut Response, scope: &mut Scope<Self::Context>)
            -> Option<Self>
    {
        if let FloServer::Producer(namespace) = self {
            producer::handle_request(data, &namespace, res, scope);
            None
        } else {
            // Consumer won't have anything to do here
            Some(self)
        }
    }
}

pub fn get_namespace_from_path<'a>(request_path: &'a str) -> String {
	request_path.chars().skip(1).take_while(|char| *char != '?').collect::<String>()
}

#[derive(Debug)]
pub struct ServerOptions {
    pub port: u16,
    pub storage_dir: PathBuf,
}

pub fn start_server(opts: ServerOptions) {
    use rotor::{Loop, Config};

    info!("Starting server with options: {:?}", opts);
    let flo_context = FloContext::new(opts.storage_dir);

    let event_loop = Loop::new(&Config::new()).unwrap();
    let mut loop_inst = event_loop.instantiate(flo_context);
    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), opts.port));

    let listener = TcpListener::bind(&address).unwrap();
    loop_inst.add_machine_with(|scope| {
        Fsm::<FloServer, _>::new(listener, (), scope)
    }).unwrap();
    loop_inst.run().unwrap();
}

#[cfg(test)]
mod test {
	use super::get_namespace_from_path;
    
    #[test]
    fn get_namespace_from_path_returns_path_without_leading_slash() {
        let input = "/theNamespace";
        assert_eq!("theNamespace", &get_namespace_from_path(input));
    }

	#[test]
	fn get_namespace_from_path_excludes_everything_after_question_mark() {
	    let input = "/theNamespace?queryParam=paramValue";
	    assert_eq!("theNamespace", &get_namespace_from_path(input));
	}

}
