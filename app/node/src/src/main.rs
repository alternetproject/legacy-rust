


#![deny(clippy::correctness)]
#![deny(clippy::nursery)]
#![deny(clippy::pedantic)]
#![deny(clippy::perf)]
#![deny(clippy::cargo)]
#![deny(clippy::style)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::similar_names)]

use color_eyre::eyre::eyre;
use libp2p::Transport as _;
use futures::StreamExt as _;
use futures::AsyncReadExt as _;
use futures::AsyncWriteExt as _;
use colored::Colorize as _;
use clap::Parser as _;
use ubyte::ToByteUnit as _;
use num::ToPrimitive as _;

mod grpc;
mod prim;
mod protocol;
mod sub_system;

#[derive(Clone)]
#[derive(derive_more::Deref)]
#[derive(derive_more::DerefMut)]
#[derive(derive_more::Into)]
struct JointSender<T>(tokio::sync::broadcast::Sender<T>);

impl<T> JointSender<T> {
	pub fn unbox(self) -> tokio::sync::broadcast::Sender<T> {
		self.0
	}
}

impl<A> JointSender<A>
where
	A: 'static + Clone + Send {
	pub fn from_channels<B, C>(mut lhs_rx: tokio::sync::mpsc::Receiver<B>, mut rhs_rx: tokio::sync::mpsc::Receiver<C>) -> Self
	where
		A: From<B> + From<C>,
		B: 'static + Send,
		C: 'static + Send {
		let (out_sx, _) = tokio::sync::broadcast::channel(8);
		let lhs_to_out_sx: tokio::sync::broadcast::Sender<_> = out_sx.clone();
		let rhs_to_out_sx: tokio::sync::broadcast::Sender<_> = out_sx.clone();

		tokio::task::spawn(async move {
			while let Some(event) = lhs_rx.recv().await {
				if lhs_to_out_sx.send(A::from(event)).is_err() {
					break
				}
			}
		});

		tokio::task::spawn(async move {
			while let Some(event) = rhs_rx.recv().await {
				if rhs_to_out_sx.send(A::from(event)).is_err() {
					break
				}
			}
		});

		Self(out_sx)
	}
}


pub type Result<T = ()> = color_eyre::Result<T>;

pub type Swarm = libp2p::swarm::Swarm<Behaviour>;
pub type SwarmEvent = libp2p::swarm::SwarmEvent<BehaviourEvent>;

#[derive(libp2p::swarm::NetworkBehaviour)]
pub struct Behaviour {
	pub autonat: libp2p::swarm::behaviour::toggle::Toggle<libp2p::autonat::Behaviour>,
	pub dcutr: libp2p::swarm::behaviour::toggle::Toggle<libp2p::dcutr::Behaviour>,
	pub kad: libp2p::swarm::behaviour::toggle::Toggle<libp2p::kad::Behaviour<libp2p::kad::store::MemoryStore>>,
	pub relay_server: libp2p::swarm::behaviour::toggle::Toggle<libp2p::relay::Behaviour>,
	pub relay_client: libp2p::swarm::behaviour::toggle::Toggle<libp2p::relay::client::Behaviour>,
	pub identify: libp2p::swarm::behaviour::toggle::Toggle<libp2p::identify::Behaviour>,
	pub stream: libp2p::swarm::behaviour::toggle::Toggle<libp2p_stream::Behaviour>
}


#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[derive(clap::ValueEnum)]
#[derive(strum::Display)]
#[strum(serialize_all = "kebab-case")]
enum Role {
	Bootstrap,
	Relay,
	#[default]
	Client,
	Server
}


#[derive(Debug)]
#[derive(clap::Parser)]
#[command(author)]
#[command(version)]
#[command(about)]
struct Configuration {
    #[arg(long)]
    #[arg(default_value_t = Role::Client)]
    pub role: Role,
    #[arg(long)]
    #[arg(default_value_t = std::net::SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0, 0, 0, 0), 8080)))]
    pub grpc_endpoint: std::net::SocketAddr,
    #[arg(long)]
    pub dial: Vec<libp2p::Multiaddr>,
    #[arg(long, value_parser = Self::parse_hex)]
    pub seed: Option<bytes_guard::NonEmpty>
}

impl Configuration {
	fn parse_hex(s: &str) -> std::result::Result<bytes_guard::NonEmpty, String> {
		let s: &str = s.trim_start_matches("0x");
		let s: &str = s.trim_start_matches("b");
		let out: Vec<_> = hex::decode(s).map_err(|error| format!("{}", error))?;
		let out: bytes::Bytes = out.into();
		let out: bytes_guard::Min<1> = out.try_into().map_err(|error| format!("{}", error))?;
		let out: bytes_guard::NonEmpty = out.into();
		Ok(out)
	}
}


#[derive(Debug, Clone)]
struct Dial {
	pub domain: prim::Domain,
	pub rpc_sx: tokio::sync::mpsc::Sender<bytes::Bytes>,
	pub reg_sx: tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<bytes::Bytes>>
}

impl Dial {
	pub async fn to_channel(&self, buffer: usize) -> Result<(tokio::sync::mpsc::Sender<bytes::Bytes>, tokio::sync::mpsc::Receiver<bytes::Bytes>)> {
		let rpc_sx: tokio::sync::mpsc::Sender<_> = self.rpc_sx.clone();
		let reg_sx: tokio::sync::mpsc::Sender<_> = self.reg_sx.clone();
		let (out_sx, out_rx) = tokio::sync::mpsc::channel(buffer);

		reg_sx.send(out_sx).await?;

		Ok((rpc_sx, out_rx))
	}
}

#[derive(Clone)]
pub enum Event {
	Framework(std::sync::Arc<SwarmEvent>),
	Dial(Dial),
	ClientServicePeerIdRequest(gantry::Request<libp2p::Multiaddr, Vec<Vec<libp2p::Multiaddr>>>),
	ClientServiceAvailableCircuitsRequest(gantry::Request<(), libp2p::PeerId>),
	RouteComputationRequest(gantry::Request<libp2p::Multiaddr, Vec<sub_system::path_finder::Circuit>>),
}

impl From<SwarmEvent> for Event {
	fn from(value: SwarmEvent) -> Self {
		Self::Framework(std::sync::Arc::new(value))
	}
}


#[tokio::main]
async fn main() -> Result {
	let configuration: Configuration = Configuration::parse();

    fern::Dispatch::new()
        .format(|out, message, record| {
            let record_time: std::time::SystemTime = std::time::SystemTime::now();
            let record_time: humantime::Rfc3339Timestamp = humantime::format_rfc3339(record_time);
            let record_level: colored::ColoredString = match record.level() {
                log::Level::Debug => record.level().to_string().blue().bold(),
                log::Level::Trace => record.level().to_string().magenta().bold(),
                log::Level::Error => record.level().to_string().red().bold(),
                log::Level::Info => record.level().to_string().green().bold(),
                log::Level::Warn => record.level().to_string().yellow().bold()
            };
            let record_target: &str = record.target();
            let record: std::fmt::Arguments<'_> = format_args!("[{} {}] {}", record_level, record_target, message);
            out.finish(record);
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()?;

    let version: &str = env!("CARGO_PKG_VERSION");
    let protocol_version: String = format!("/an/{}", version);
    let protocol_name: libp2p::StreamProtocol = libp2p::StreamProtocol::new("/an");

    let agent_version: String = format!("an-{}/{}", configuration.role, version);

    let identify_cache_size: usize = match configuration.role {
   		Role::Bootstrap => 50000,
     	Role::Relay => 5000,
      	Role::Client => 1000,
       	Role::Server => 2000
    };

    let identify_interval: std::time::Duration = match configuration.role {
    	Role::Bootstrap => std::time::Duration::from_secs(5),
     	Role::Relay => std::time::Duration::from_mins(5),
      	Role::Client => std::time::Duration::from_mins(5),
       	Role::Server => std::time::Duration::from_mins(5)
    };

    let local_keypair: libp2p::identity::Keypair = if let Some(seed) = &mut configuration.seed {
    	let seed: bytes_guard::Min<1> = seed.into_inner();
     	let seed: bytes::Bytes = seed.into_inner();
      	let seed: Vec<_> = seed.into();
   		libp2p::identity::Keypair::ed25519_from_bytes(seed)?
    } else {
   		libp2p::identity::Keypair::generate_ed25519()
    };

    let local_public_key: libp2p::identity::PublicKey = local_keypair.public();
    let local_peer_id: libp2p::PeerId = local_keypair.public().into();

    let mut quic_configuration: libp2p::quic::Config = libp2p::quic::Config::new(&local_keypair);
    quic_configuration.handshake_timeout = std::time::Duration::from_millis(3000);
    quic_configuration.keep_alive_interval = std::time::Duration::from_secs(10);
    quic_configuration.max_concurrent_stream_limit = 512;
    quic_configuration.max_connection_data = 10.megabytes().as_u64().to_u32().unwrap();
    quic_configuration.max_idle_timeout = 60000;
    quic_configuration.max_stream_data = 1.megabytes().as_u64().to_u32().unwrap();

    let quic = libp2p::quic::tokio::Transport::new(quic_configuration.to_owned()).map(|(peer_id, muxer), _| (peer_id, libp2p::core::muxing::StreamMuxerBox::new(muxer)));

    let mut yamux_config = libp2p::yamux::Config::default();
    yamux_config.set_receive_window_size(512 * 1024);
    yamux_config.set_max_buffer_size(2 * 1024 * 1024);

    let tls_config = libp2p::tls::Config::new(&local_keypair)?;

    let tcp_config: libp2p::tcp::Config = libp2p::tcp::Config::default().nodelay(true);
    let tcp = libp2p::tcp::tokio::Transport::new(tcp_config).upgrade(libp2p::core::upgrade::Version::V1).authenticate(tls_config.clone()).multiplex(yamux_config.clone()).map(|(peer_id, muxer), _| (peer_id, libp2p::core::muxing::StreamMuxerBox::new(muxer)));

	let ws = libp2p::websocket::WsConfig::new(libp2p::dns::tokio::Transport::system(libp2p::tcp::tokio::Transport::new(libp2p::tcp::Config::default()))?)
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(tls_config)
        .multiplex(yamux_config)
		.map(|(peer_id, muxer), _| (peer_id, libp2p::core::muxing::StreamMuxerBox::new(muxer)));

	let transport: libp2p::core::transport::Boxed<_> = quic
    	.or_transport(tcp)
       	.or_transport(ws)
       	.map(|either_output, _| match either_output {
                futures::future::Either::Left(inner) => match inner {
                    futures::future::Either::Left(res) => res,
                    futures::future::Either::Right(res) => res,
                },
                futures::future::Either::Right(res) => res,
            })
        .boxed();

    let mut autonat_configuration = libp2p::autonat::Config::default();
    autonat_configuration.boot_delay = std::time::Duration::from_secs(1);
    autonat_configuration.confidence_max = 3;
    autonat_configuration.max_peer_addresses = 5;
    autonat_configuration.only_global_ips = false;
    autonat_configuration.refresh_interval = std::time::Duration::from_mins(15);
    autonat_configuration.retry_interval = std::time::Duration::from_secs(30);
    autonat_configuration.throttle_clients_global_max = 0;
    autonat_configuration.throttle_clients_peer_max = 0;
    autonat_configuration.throttle_clients_period = std::time::Duration::from_secs(60);
    autonat_configuration.throttle_server_period = std::time::Duration::from_secs(60);
    autonat_configuration.timeout = std::time::Duration::from_secs(15);
    autonat_configuration.use_connected = true;

    let autonat: libp2p::autonat::Behaviour = libp2p::autonat::Behaviour::new(local_peer_id, autonat_configuration);

    let dcutr: libp2p::dcutr::Behaviour = libp2p::dcutr::Behaviour::new(local_peer_id);

    let kad_store: libp2p::kad::store::MemoryStore = libp2p::kad::store::MemoryStore::new(local_peer_id);

    let mut kad_configuration: libp2p::kad::Config = libp2p::kad::Config::new(protocol_name);
    kad_configuration.disjoint_query_paths(true);
    kad_configuration.set_caching(libp2p::kad::Caching::Enabled{ max_peers: 64 });
    kad_configuration.set_kbucket_inserts(libp2p::kad::BucketInserts::Manual);
    kad_configuration.set_kbucket_pending_timeout(std::time::Duration::from_mins(1));
    kad_configuration.set_kbucket_size(libp2p::kad::K_VALUE);
    kad_configuration.set_max_packet_size(
        1.kilobytes().as_u64().to_usize().unwrap()
    );
    kad_configuration.set_parallelism(libp2p::kad::ALPHA_VALUE);
    kad_configuration.set_periodic_bootstrap_interval(Some(std::time::Duration::from_mins(5)));
    kad_configuration.set_provider_publication_interval(None);
    kad_configuration.set_provider_record_ttl(None);
    kad_configuration.set_publication_interval(None);
    kad_configuration.set_query_timeout(std::time::Duration::from_mins(1));
    kad_configuration.set_record_filtering(libp2p::kad::StoreInserts::FilterBoth);
    kad_configuration.set_record_ttl(Some(std::time::Duration::from_hours(48)));
    kad_configuration.set_replication_factor(libp2p::kad::K_VALUE);
    kad_configuration.set_replication_interval(None);
    kad_configuration.set_substreams_timeout(std::time::Duration::from_secs(10));

    let mut kad: libp2p::kad::Behaviour<_> = libp2p::kad::Behaviour::with_config(local_peer_id, kad_store, kad_configuration);

    let identify_configuration: libp2p::identify::Config = libp2p::identify::Config::new(protocol_version, local_public_key)
        .with_agent_version(agent_version)
        .with_cache_size(identify_cache_size)
        .with_hide_listen_addrs(true)
        .with_interval(identify_interval)
        .with_push_listen_addr_updates(true);

    let identify: libp2p::identify::Behaviour = libp2p::identify::Behaviour::new(identify_configuration);

    let mut relay_server_configuration: libp2p::relay::Config = libp2p::relay::Config::default();
    relay_server_configuration.max_reservations = 128;
    relay_server_configuration.reservation_duration = std::time::Duration::from_secs(3600);
    relay_server_configuration.max_circuits = 16;
    relay_server_configuration.max_circuit_duration = std::time::Duration::from_secs(20);
    relay_server_configuration.max_circuit_bytes = 1 << 17;

    let relay_server: libp2p::relay::Behaviour = libp2p::relay::Behaviour::new(local_peer_id, relay_server_configuration);

    let stream: libp2p_stream::Behaviour = libp2p_stream::Behaviour::new();
    let stream_control: libp2p_stream::Control = stream.new_control();

    let swarm: libp2p::Swarm<_> = match configuration.role {
   		Role::Bootstrap =>
	    	libp2p::SwarmBuilder::with_existing_identity(local_keypair)
		  		.with_tokio()
		  		.with_other_transport(|_| transport)?
				.with_behaviour(|_| {
		 			Behaviour {
						autonat: Some(autonat).into(),
			 			dcutr: Some(dcutr).into(),
			     		kad: Some(kad).into(),
			      		relay_server: Some(relay_server).into(),
			      		relay_client: None.into(),
			           	identify: None.into(),
			           	stream: None.into()
		  			}
				})?
				.build(),
		Role::Relay =>
	    	libp2p::SwarmBuilder::with_existing_identity(local_keypair)
		  		.with_tokio()
		  		.with_other_transport(|_| transport)?
				.with_behaviour(|_| {
		 			Behaviour {
						autonat: Some(autonat).into(),
			 			dcutr: Some(dcutr).into(),
			     		kad: Some(kad).into(),
			      		relay_server: Some(relay_server).into(),
			      		relay_client: None.into(),
			           	identify: None.into(),
			           	stream: Some(stream).into()
		  			}
				})?
				.build(),
		Role::Client =>
	    	libp2p::SwarmBuilder::with_existing_identity(local_keypair)
		  		.with_tokio()
		  		.with_other_transport(|_| transport)?
				.with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)?
				.with_behaviour(|_, relay_client| {
		 			Behaviour {
						autonat: Some(autonat).into(),
			 			dcutr: Some(dcutr).into(),
			     		kad: Some(kad).into(),
			      		relay_server: None.into(),
			      		relay_client: Some(relay_client).into(),
			           	identify: None.into(),
			           	stream: Some(stream).into()
		  			}
				})?
				.build(),
		Role::Server =>
	    	libp2p::SwarmBuilder::with_existing_identity(local_keypair)
		  		.with_tokio()
		  		.with_other_transport(|_| transport)?
				.with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)?
				.with_behaviour(|_, relay_client| {
		 			Behaviour {
						autonat: Some(autonat).into(),
			 			dcutr: Some(dcutr).into(),
			     		kad: Some(kad).into(),
			      		relay_server: None.into(),
			      		relay_client: Some(relay_client).into(),
			           	identify: None.into(),
			           	stream: Some(stream).into()
		  			}
				})?
				.build()
    };

    swarm.listen_on("/ip4/0.0.0.0/udp/4001/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/4001".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/4002/ws".parse()?)?;

	let swarm: std::sync::Arc<_> = std::sync::Arc::new(tokio::sync::Mutex::new(swarm));

	let (p2p_sx, p2p_rx) = tokio::sync::mpsc::channel(100);
	let (ext_sx, ext_rx) = tokio::sync::mpsc::channel(100);

    let grpc_endpoint: std::net::SocketAddr = configuration.grpc_endpoint;
    let grpc_server: grpc::Server = grpc::Server::new(ext_sx);
    let grpc_server: grpc::dial_service_server::DialServiceServer<_> = grpc::dial_service_server::DialServiceServer::new(grpc_server);
    let grpc = tonic::transport::Server::builder().add_service(grpc_server).serve(grpc_endpoint);

    tokio::task::spawn(async move {
    	grpc.await;
    });

	tokio::task::spawn({
		let swarm: std::sync::Arc<_> = swarm.clone();
		async move {
			loop {
				let event: Option<_> = {
					let out: tokio::sync::MutexGuard<_> = swarm.lock().await;
					let out: Option<_> = out.next().await;
					out
				};

				let Some(event) = event else {
					break
				};

				if p2p_sx.send(event).await.is_err() {
					break
				}
			}
		}
	});

	let runtime_sx: tokio::sync::broadcast::Sender<_> = JointSender::from_channels(p2p_rx, ext_rx).into();
	let runtime: gantry::Runtime<_, _> = gantry::Runtime::from_existing(swarm, runtime_sx);

	if configuration.role == Role::Client || configuration.role == Role::Server {
		runtime.mount(sub_system::relay_client_linker::RelayClientLinker);
		runtime.mount(sub_system::path_finder::PathFinder);
	}

	if let Role::Client = configuration.role {
		let dial_service_bridge_control: libp2p_stream::Control = stream_control.clone();
		let dial_service_bridge: sub_system::dial_service_bridge::DialServiceBridge = sub_system::dial_service_bridge::DialServiceBridge::builder().control(dial_service_bridge_control).build();

		runtime.mount(dial_service_bridge);
	}

	if let Role::Server = configuration.role {
		runtime.mount(sub_system::poll_service_bridge::PollServiceBridge);
	}

	tokio::signal::ctrl_c().await.expect("ctrl");
	Ok(())
}
