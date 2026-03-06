mod config;
mod storage;
mod security;
mod server;

/// Generated protobuf modules.
pub mod pb {
    pub mod remote_pb {
        tonic::include_proto!("remote_pb");
    }
    pub mod volume_server_pb {
        tonic::include_proto!("volume_server_pb");
    }
    pub mod master_pb {
        tonic::include_proto!("master_pb");
    }
}

fn main() {
    let cli = config::parse_cli();
    println!("SeaweedFS Volume Server (Rust)");
    println!("Configuration: {:#?}", cli);
}
