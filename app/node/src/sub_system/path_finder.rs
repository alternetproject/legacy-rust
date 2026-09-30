use super::*;

trait MultiAddrExt {
	fn peer_id(&self) -> Option<libp2p::PeerId>;
}

impl MultiAddrExt for libp2p::Multiaddr {
	fn peer_id(&self) -> Option<libp2p::PeerId> {
		let mut out: Option<_> = None;
	
		for protocol in self.iter() {
			let libp2p::multiaddr::Protocol::P2p(peer_id) = protocol else {
				continue
			};
			
			out = Some(peer_id);
			break
		};
		
		out
	}
}


pub type Result<T> = color_eyre::Result<T, Error>; 

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
	#[error("hop source and destination address must not be identical")]
	Loopback,
	#[error("circuit must contain at least one hop")]
	EmptyHops
}


#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(derive_more::From)]
#[derive(derive_more::Into)]
#[from((libp2p::Multiaddr, libp2p::Multiaddr))]
#[into((libp2p::Multiaddr, libp2p::Multiaddr))]
pub struct Hop {
	pub src_addr: libp2p::Multiaddr,
	pub dst_addr: libp2p::Multiaddr
}

impl Hop {
	pub fn new(src_addr: libp2p::Multiaddr, dst_addr: libp2p::Multiaddr) -> Result<Self> {
		if src_addr == dst_addr {
			return Err(Error::Loopback)
		}
		Ok(Self {
			src_addr,
			dst_addr
		})
	}
}

impl std::fmt::Display for Hop {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{} >>> {}", self.src_addr, self.dst_addr)
	}
}


#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(derive_more::Deref)]
#[derive(derive_more::IntoIterator)]
pub struct Circuit(Vec<Hop>);

impl Circuit {
	pub fn new(hops: Vec<Hop>) -> Result<Self> {
		if hops.is_empty() {
			return Err(Error::EmptyHops)
		}
		Ok(Self(hops))
	}
}

impl std::fmt::Display for Circuit {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let segments: Vec<_> = self.iter().cloned().map(|hop| vec![hop.src_addr, hop.dst_addr]).flatten().collect();
		
		let mut segment_sets: std::collections::HashSet<_> = std::collections::HashSet::default();
		
		segments.iter().map(|segment| segment_sets.insert(segment));
		
		let segments: Vec<_> = segment_sets.iter().map(|addr| addr.to_string()).collect();
		let segments: String = segments.join(" >>> ");
		
		write!(f, "{}", segments)
	}
}

#[derive(Debug)]
pub struct PathFinder;

impl PathFinder {
	fn look_up(
		&self,
		src_addr: libp2p::Multiaddr,
		dst_addr: libp2p::Multiaddr,
		connected: &std::collections::HashSet<libp2p::Multiaddr>
	) -> Result<Option<Circuit>> {
		if !connected.contains(&dst_addr) {
			return Ok(None)
		}
		Ok(Some(Circuit::new(vec![Hop::new(src_addr, dst_addr)?])?))
	}
	
	fn look_up_single_hop(
		&self,
		src_addr: libp2p::Multiaddr,
		dst_addr: libp2p::Multiaddr,
		connected: &std::collections::HashSet<libp2p::Multiaddr>,
		kad: &mut libp2p::kad::Behaviour<libp2p::kad::store::MemoryStore>
	) -> Result<Vec<Circuit>> {
		let mut out: Vec<_> = vec![];
		
		let Some(dst_peer_id) = dst_addr.peer_id() else {
			return Ok(vec![])
		};
		
		for connected_addr in connected {			
			let Some(relay_peer_id) = connected_addr.peer_id() else {
				continue
			};
			
			if relay_peer_id == dst_peer_id {
				continue
			}
			
			if kad.kbucket(relay_peer_id).is_some() {
				let circuit_relay_addr: libp2p::Multiaddr = connected_addr
					.clone()
					.with(libp2p::multiaddr::Protocol::P2pCircuit)
					.with(libp2p::multiaddr::Protocol::P2p(dst_peer_id));
				
				let src_addr: libp2p::Multiaddr = src_addr.clone();
				let start_hop_connected_addr: libp2p::Multiaddr = connected_addr.clone();
				let final_hop_connected_addr: libp2p::Multiaddr = connected_addr.clone();
				
				let start_hop: Hop = Hop::new(src_addr, start_hop_connected_addr)?;
				let final_hop: Hop = Hop::new(final_hop_connected_addr, circuit_relay_addr)?;
			
				let circuit: Circuit = Circuit::new(vec![
					start_hop,
					final_hop
				])?;
				
				out.push(circuit);
			}
		}
		
		Ok(out)
	}
	
	fn look_up_multi_hop(
		&self,
		src_addr: libp2p::Multiaddr,
		dst_addr: libp2p::Multiaddr,
		connected: &std::collections::HashSet<libp2p::Multiaddr>,
		kad: &mut libp2p::kad::Behaviour<libp2p::kad::store::MemoryStore>,
		k_shortest: usize
	) -> Result<Vec<Circuit>> {
		let addrs = kad.kbuckets().flat_map(|bucket| {
			let mut out: Vec<_> = vec![];
			
			for item in bucket.iter() {
				let peer_id: libp2p::PeerId = *item.node.key.preimage();
				let peer_addr: libp2p::Multiaddr = libp2p::Multiaddr::empty().with(libp2p::multiaddr::Protocol::P2p(peer_id));

				out.push(peer_addr);
			}
			
			out			
		});
		let addrs: Vec<_> = addrs.collect();

		let bfs_success = |current: &libp2p::Multiaddr| current == &dst_addr;
		let bfs_successors = |current: &libp2p::Multiaddr| {
			let successors: Vec<_> = if current == &src_addr {
				connected.iter().cloned().collect::<Vec<_>>()
			} else if connected.contains(current) {
				addrs.clone()
			} else {
				vec![]
			};
			
			successors.into_iter().map(|addr| (addr, 1))
		};
		
		let mut out: Vec<_> = vec![];
		
		for (path, _) in pathfinding::prelude::yen(&src_addr, bfs_successors, bfs_success, k_shortest) {
			let hops = path.windows(2).map(|pair| {
				let start_hop_addr: libp2p::Multiaddr = pair.get(0).unwrap().clone();
				let final_hop_addr: libp2p::Multiaddr = pair.get(1).unwrap().clone();
				let out: Result<_> = Hop::new(start_hop_addr, final_hop_addr);
				out
			});
			
			let hops: Result<Vec<_>> = hops.collect();
			let hops: Vec<_> = hops.unwrap();
			
			out.push(Circuit::new(hops)?);
		}

		Ok(out)
	}
}

#[async_trait::async_trait]
impl gantry::SubSystem for PathFinder {
	type Event = Event;
	type Resource = Swarm;
	
	async fn receive(&mut self, runtime: gantry::RuntimeRef<'_, Self::Event, Self::Resource>, event: Self::Event) {
		let Event::RouteComputationRequest(mut request) = event else {
			return
		};
		
		let mut swarm: tokio::sync::MutexGuard<_> = runtime.environment.lock().await;
		let mut out: Vec<_> = vec![];
		
		let src_peer_id: &libp2p::PeerId = swarm.local_peer_id();
		let src_peer_id: libp2p::PeerId = src_peer_id.clone();
		let src_addr = swarm.listeners().next().cloned().unwrap_or(libp2p::Multiaddr::empty().with(libp2p::multiaddr::Protocol::P2p(src_peer_id)));
		
		let connected: std::collections::HashSet<_> = swarm.connected_peers().map(|peer_id| libp2p::Multiaddr::empty().with(libp2p::multiaddr::Protocol::P2p(peer_id.clone()))).collect();
		
		if let Ok(Some(circuit)) = self.look_up(src_addr.clone(), request.content().clone(), &connected) {
			out.push(circuit);
		}
		
		let Some(ref mut kad) = swarm.behaviour_mut().kad.as_mut() else {
			request.reply(out).await;
			
			return
		};

		let single_hop_src_addr: libp2p::Multiaddr = src_addr.clone();
		let single_hop_dst_addr: libp2p::Multiaddr = request.content().clone();
		let multi_hop_src_addr: libp2p::Multiaddr = src_addr;
		let multi_hop_dst_addr: libp2p::Multiaddr = request.content().clone();
		let k_shortest: usize = 5;
		
		if let Ok(circuits) = self.look_up_single_hop(single_hop_src_addr, single_hop_dst_addr, &connected, kad) {
			out.extend(circuits);
		}
		
		if let Ok(circuits) = self.look_up_multi_hop(multi_hop_src_addr, multi_hop_dst_addr, &connected, kad, k_shortest) {
			out.extend(circuits);
		}
		
		request.reply(out).await;
	}
}