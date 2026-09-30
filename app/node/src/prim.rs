use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(derive_more::AsRef)]
#[derive(derive_more::Deref)]
pub struct Domain(String);

impl Domain {
	pub fn into_inner(self) -> String {
		self.0
	}
}

impl TryFrom<String> for Domain {
	type Error = Box<dyn std::error::Error>;
	
	fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
    	
		
		Ok(Self(value))
	}
}