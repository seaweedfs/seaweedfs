//! Standalone RDMA-READ target: load a file, register it for `REMOTE_READ`, and
//! serve one client over SoftRoCE (rxe) — a real, separate-process precursor to
//! a volume-side RDMA target. Pairs with `rdma-client`.
//!
//!   cargo run --features real-rdma --bin rdma-target -- <bind_ip> <port> <file>
//!
//! `<bind_ip>` must be an IP on the rxe-backing netdev (e.g. eth0's IP).

use std::fs;

use rdma_engine::rdma_real::RealResponder;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: rdma-target <bind_ip> <port> <file>");
        std::process::exit(2);
    }
    let ip = args[1].clone();
    let port: u16 = args[2].parse().expect("port");
    let file = &args[3];

    let mut data = fs::read(file).expect("read source file");
    eprintln!(
        "target: serving {} bytes of {file} on {ip}:{port} (REMOTE_READ)",
        data.len()
    );

    match RealResponder::serve_once(&ip, port, &mut data, || {
        eprintln!("target: listening; waiting for a client");
    }) {
        Ok(()) => eprintln!("target: served one client; done"),
        Err(e) => {
            eprintln!("target error: {e}");
            std::process::exit(1);
        }
    }
}
