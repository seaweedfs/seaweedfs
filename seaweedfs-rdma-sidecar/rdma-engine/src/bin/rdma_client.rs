//! Standalone RDMA-READ client: connect to an `rdma-target`, RDMA-READ its whole
//! registered buffer into a fresh local buffer (separate process / address
//! space), and write it out. md5 of the output vs the target's source file
//! proves a real cross-process RDMA read of real data.
//!
//!   cargo run --features real-rdma --bin rdma-client -- <target_ip> <port> <out_file> [offset] [size]
//!
//! With [offset]/[size] it RDMA-READs only that byte range (the real read API
//! shape: file_id + offset + size); without them it reads the whole MR.

use std::fs;

use rdma_engine::rdma_real::RealRdmaContext;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: rdma-client <target_ip> <port> <out_file> [offset] [size]");
        std::process::exit(2);
    }
    let ip = args[1].clone();
    let port: u16 = args[2].parse().expect("port");
    let out = &args[3];
    let offset: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);

    let (ctx, raddr, rkey, rlen) = match RealRdmaContext::connect(&ip, port) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("client connect failed: {e}");
            std::process::exit(1);
        }
    };
    // Default to the rest of the MR from `offset`.
    let size: usize = args
        .get(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(rlen - offset.min(rlen as u64) as usize);
    eprintln!(
        "client: connected to {ip}:{port}, remote MR len={rlen} rkey=0x{rkey:x}; reading [{offset}, {})",
        offset as usize + size
    );
    assert!(offset as usize + size <= rlen, "range exceeds remote MR");

    let mut dest = vec![0u8; size];
    let (_mi, mr) = ctx.register_memory(&mut dest).expect("register dest");
    // RDMA-READ the sub-range: remote source is the MR base + offset.
    ctx.post_read(&mut dest, mr, raddr + offset, rkey, 1)
        .expect("post_read");
    let wc = ctx.poll_completion().expect("poll completion");
    ctx.finish();
    eprintln!(
        "client: RDMA READ status={:?} byte_len={}",
        wc.status, wc.byte_len
    );

    fs::write(out, &dest).expect("write output");
    eprintln!("client: wrote {} bytes to {out}", dest.len());
}
