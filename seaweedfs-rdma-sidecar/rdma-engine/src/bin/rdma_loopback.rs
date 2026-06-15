//! Phase-1 proof: a real `IBV_WR_RDMA_READ` over SoftRoCE (rxe) loopback.
//!
//! Spawns a [`RealResponder`] that registers a buffer holding a verifiable
//! pattern (with a `REALRDMA` magic header — deliberately NOT the mock's `i%256`),
//! then a [`RealRdmaContext`] client connects, RDMA-READs the buffer, and asserts
//! the bytes match. Proves the engine moves real bytes over a genuine RDMA verb.
//!
//!   rmmod-free run (after SoftRoCE is up):
//!     cargo run --features real-rdma --bin rdma-loopback -- <rxe_ip> [port] [len]
//!
//! `<rxe_ip>` must be an IP on the netdev backing the rxe link (e.g. eth0's IP),
//! not 127.0.0.1 (rxe is bound to that netdev, not loopback).

use std::sync::mpsc;
use std::time::Instant;

use rdma_engine::rdma_real::{RealRdmaContext, RealResponder};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ip = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(7471);
    let len: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1 << 20);

    // A verifiable pattern, distinct from the mock's i%256, with a magic header.
    let mut src = vec![0u8; len];
    for (i, b) in src.iter_mut().enumerate() {
        *b = (i.wrapping_mul(131).wrapping_add(17) & 0xff) as u8;
    }
    let magic = b"REALRDMA";
    src[..magic.len()].copy_from_slice(magic);
    let expected = src.clone();

    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let sip = ip.clone();
    let server = std::thread::spawn(move || {
        let mut data = src;
        if let Err(e) = RealResponder::serve_once(&sip, port, &mut data, move || {
            let _ = ready_tx.send(());
        }) {
            eprintln!("responder error: {e}");
        }
    });
    // Wait until the responder is listening before connecting.
    let _ = ready_rx.recv();

    let t0 = Instant::now();
    let (ctx, raddr, rkey, rlen) = match RealRdmaContext::connect(&ip, port) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("client connect failed: {e}");
            let _ = server.join();
            std::process::exit(1);
        }
    };
    println!(
        "connected: device={:?} remote MR addr=0x{raddr:x} rkey=0x{rkey:x} len={rlen}",
        ctx.device_info().name
    );

    let mut dest = vec![0u8; rlen];
    let (_mrinfo, mr) = ctx.register_memory(&mut dest).expect("register dest");
    ctx.post_read(&mut dest, mr, raddr, rkey, 1).expect("post_read");
    let wc = ctx.poll_completion().expect("poll completion");
    let dt = t0.elapsed();
    ctx.finish();
    let _ = server.join();

    let head: String = dest[..16.min(dest.len())]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    println!(
        "RDMA READ done: status={:?} byte_len={}  dest head: {head}  (\"{}\")",
        wc.status,
        wc.byte_len,
        String::from_utf8_lossy(&dest[..magic.len().min(dest.len())])
    );

    if dest == expected {
        let mibps = (rlen as f64 / (1024.0 * 1024.0)) / dt.as_secs_f64();
        println!("VERIFY OK: {rlen} bytes match the responder's registered buffer");
        println!("({mibps:.1} MiB/s incl. connection setup; rxe on a 2-vCPU VM is a correctness proof, not a benchmark)");
        println!("REAL-RDMA-READ-SUCCESS");
    } else {
        let mismatch = dest
            .iter()
            .zip(expected.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(0);
        eprintln!("VERIFY FAILED: first mismatch at byte {mismatch}");
        std::process::exit(1);
    }
}
