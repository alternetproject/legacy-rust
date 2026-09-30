use super::*;

#[derive(bon::Builder)]
pub struct DialServiceBridge {
	control: libp2p_stream::Control
}

#[async_trait::async_trait]
impl gantry::SubSystem for DialServiceBridge {
	type Event = Event;
	type Resource = Swarm;
	
	async fn receive(&mut self, runtime: gantry::RuntimeRef<'_, Self::Event, Self::Resource>, event: Self::Event) {
		let Event::Dial(dial) = event else {
			return
		};
		
		let mut control = self.control.clone();
		let (session_sx, session_rx) = dial.to_channel(64).await.unwrap();
		
		tokio::spawn(async move {
			// open stream with custom protocol and join with session from grpc
			
			dial.domain;
			
			// connect to the given circuits, 

			match control.open_stream(dst_peer_id, protocol::KEY).await {
				Ok(mut stream) => {
					// ...
				},
				_ => ()
			}
		});
	}
}