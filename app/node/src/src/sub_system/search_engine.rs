use super::*;

pub struct SearchEngine {
	
}

#[async_trait::async_trait]
impl gantry::SubSystem for SearchEngine {
	type Event = Event;
	type Resource = Swarm;
	
	async fn receive(&mut self, runtime: gantry::RuntimeRef<'_, Self::Event, Self::Resource>, event: Self::Event) {
		
	}
}