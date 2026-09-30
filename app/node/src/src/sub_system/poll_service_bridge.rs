use super::*;

pub struct PollServiceBridge {
	control: libp2p_stream::Control
}

#[async_trait::async_trait]
impl gantry::SubSystem for PollServiceBridge {
	type Event = Event;
	type Resource = Swarm;

	async fn receive(&mut self, runtime: gantry::RuntimeRef<'_, Self::Event, Self::Resource>, event: Self::Event) {
		// intercept streams, yeet it to the poll grpc server...

	}
}
