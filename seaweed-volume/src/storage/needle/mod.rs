pub mod crc;
#[expect(clippy::module_inception, reason = "needle/needle.rs mirrors the Go package layout")]
pub mod needle;
pub mod ttl;

pub use crc::CRC;
pub use needle::Needle;
pub use ttl::TTL;
