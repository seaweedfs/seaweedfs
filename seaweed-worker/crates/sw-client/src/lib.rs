//! SeaweedFS client library.
//!
//! Speaks the filer gRPC metadata plane and the volume-server HTTP data plane,
//! and resolves files (lists of immutable chunks) into byte ranges. The vector
//! engine, the Lance maintenance worker and its CLI reach the cluster's
//! storage through it; the Enterprise repository's VFS client shares its API.

/// Generated protobuf + gRPC client stubs for the SeaweedFS filer
/// (`package filer_pb;` in `weed/pb/filer.proto`).
pub mod filer_pb {
    #![allow(clippy::all)]
    #![allow(rustdoc::all)]
    tonic::include_proto!("filer_pb");
}

mod cache;
pub mod filer;
pub mod location_cache;
pub mod read;
pub mod transport;
pub mod wbuf;
pub mod write;

/// Upper bound on a single read's up-front allocation. `read_file` allocates the
/// requested span, so a malformed request with an absurd size could otherwise
/// trigger a huge allocation (OOM/DoS); a read span over this is rejected. This
/// is purely a per-read guard — file *size* is unbounded, since writes stream
/// chunk-by-chunk via [`wbuf::StreamingWriteBuffer`].
pub const MAX_READ_BYTES: u64 = 1 << 30; /* 1 GiB */

pub use filer::{
    AssignVolumeOptions, Assignment, EntryExists, EntryPage, FilerClient, FilerConfig, GrpcTls,
    LogPos, Seek, VolumeLocation, is_entry_exists, is_unavailable, seek_page,
};
pub use location_cache::{DEFAULT_RETAIN_GENERATIONS, LocationCache};
pub use read::{
    ChunkDecoder, ReadOptions, ResolvedChunks, VolumeReader, compact_shadowed, read_file,
    read_file_with, read_resolved, resolve_chunks,
};
pub use transport::{
    CommitAmbiguous, HttpTransport, HttpsTls, NotWritten, VolumeTransport, VolumeWriteReceipt,
    install_crypto_provider, is_not_written,
};
pub use wbuf::{SealPiece, StreamingWriteBuffer};
pub use write::{Writer, flush_file, now_ns, now_unix, upload_pieces, upload_pieces_with_options};
