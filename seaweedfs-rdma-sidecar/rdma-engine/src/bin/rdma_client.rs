//! Standalone RDMA-READ client: connect to an `rdma-target`, RDMA-READ its whole
//! registered buffer into a fresh local buffer (separate process / address
//! space), and write it out. md5 of the output vs the target's source file
//! proves a real cross-process RDMA read of real data.
//!
//!   cargo run --features real-rdma --bin rdma-client -- <target_ip> <port> <out_file>

use std::fs;

use rdma_engine::rdma_real::RealRdmaContext;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: rdma-client <target_ip> <port> <out_file>");
        std::process::exit(2);
    }
    let ip = args[1].clone();
    let port: u16 = args[2].parse().expect("port");
    let out = &args[3];

    let (ctx, raddr, rkey, rlen) = match RealRdmaContext::connect(&ip, port) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("client connect failed: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("client: connected to {ip}:{port}, remote MR len={rlen} rkey=0x{rkey:x}");

    let mut dest = vec![0u8; rlen];
    let (_mi, mr) = ctx.register_memory(&mut dest).expect("register dest");
    ctx.post_read(&mut dest, mr, raddr, rkey, 1).expect("post_read");
    let wc = ctx.poll_completion().expect("poll completion");
    ctx.finish();
    eprintln!(
        "client: RDMA READ status={:?} byte_len={}",
        wc.status, wc.byte_len
    );

    fs::write(out, &dest).expect("write output");
    eprintln!("client: wrote {} bytes to {out}", dest.len());
}
