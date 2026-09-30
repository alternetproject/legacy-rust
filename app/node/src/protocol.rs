use super::*;

// custom stream bejaviour, relay cheque mechanisms

pub const KEY: libp2p::StreamProtocol = libp2p::StreamProtocol::new("/an/voucher/0.1.0");

#[derive(Debug)]
pub struct Frame {
	pub payload: Vec<u8>
}

impl Frame {
	pub async fn write<T>(&self, content: &mut T) -> std::io::Result<()>
	where
		T: Unpin + futures::AsyncWriteExt {

		content.write_all(&self.payload).await?;
		content.flush().await?;
		Ok(())
	}
}
