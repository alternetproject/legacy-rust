use super::*;

pub struct RelayClientLinker;

#[async_trait::async_trait]
impl gantry::SubSystem for RelayClientLinker {
	type Event = Event;
	type Resource = Swarm;
	
	async fn receive(&mut self, runtime: gantry::RuntimeRef<'_, Self::Event, Self::Resource>, event: Self::Event) {
		match event {
			Event::Framework(event) => match &*event {
				SwarmEvent::ConnectionEstablished {
					peer_id,
					connection_id,
					endpoint,
					..
				} => {
					let circuit_endpoint = endpoint.get_remote_address().clone().with(libp2p::multiaddr::Protocol::P2pCircuit);
				
					match runtime.environment.lock().await.listen_on(circuit_endpoint) {
						_ => todo!()
					}
				},
				SwarmEvent::NewListenAddr {
					address,
					..
				} => {
					println!("listening on relayed address: {}", address);
				},
				_ => ()
			},
			_ => ()
		}
	}
}