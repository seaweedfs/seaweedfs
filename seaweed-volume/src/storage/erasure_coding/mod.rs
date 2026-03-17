//! Erasure coding module for volume data protection.
//!
//! Encodes a volume's .dat file into 10 data + 4 parity shards using
//! Reed-Solomon erasure coding. Can reconstruct from any 10 of 14 shards.

pub mod ec_decoder;
pub mod ec_encoder;
pub mod ec_locate;
pub mod ec_shard;
pub mod ec_volume;

pub use ec_shard::{
    EcVolumeShard, ShardId, DATA_SHARDS_COUNT, MAX_SHARD_COUNT, MIN_TOTAL_DISKS,
    PARITY_SHARDS_COUNT, TOTAL_SHARDS_COUNT,
};
pub use ec_volume::EcVolume;
