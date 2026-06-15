# Real RDMA datapath (`real-rdma` feature) — proof of plumbing

The engine's default backend is a **mock** (`src/rdma.rs` `MockRdmaContext`): its
`post_read` fabricates `byte = i % 256` and never touches a wire (the Go sidecar
in `../pkg/rdma/client.go` even discards the engine's buffer and re-fakes the
same pattern). This module replaces that with a **real** libibverbs / `rdma_cm`
datapath, validated over SoftRoCE (rxe).

## What's here

- `src/rdma_real.rs`
  - `RealRdmaContext` — the real client backend: `connect` (via `rdma_cm` /
    `rdma_create_ep`), `register_memory`, `post_read` (a genuine
    `IBV_WR_RDMA_READ` via `rdma_post_read`), `poll_completion`, `device_info`.
  - `RealResponder` — a throwaway loopback target: registers a buffer for
    `REMOTE_READ` (`rdma_reg_read`) and hands the client its `{addr, rkey, len}`.
- `src/bin/rdma_loopback.rs` — the proof: a responder registers a buffer with a
  verifiable pattern (a `REALRDMA` magic header — deliberately **not** the mock's
  `i%256`), the client RDMA-READs it, and asserts the bytes match.

Connection setup and QP state transitions use `rdma_cm` (much simpler than a
manual GID/QPN/LID handshake for RoCEv2 addressing).

## Build & run (Linux + SoftRoCE only)

Requires `libibverbs-dev librdmacm-dev ibverbs-providers ibverbs-utils
rdma-core clang libclang-dev`, and a SoftRoCE link:

```sh
sudo modprobe rdma_rxe rdma_ucm
sudo rdma link add rxe0 type rxe netdev eth0     # eth0 = your primary netdev
ibv_rc_pingpong -d rxe0 -g 1                      # sanity-check the fabric

cargo build --features real-rdma --bin rdma-loopback
sudo ./target/debug/rdma-loopback <rxe_ip> 7471 1048576   # rxe_ip = eth0's IP
# => REAL-RDMA-READ-SUCCESS, dest head "REALRDMA", bytes match
```

(rxe on a low-vCPU VM is a **correctness** proof, not a representative benchmark.)

## Why `vendor/rdma-sys`

`rdma-sys 0.3.0` (crates.io) pins **bindgen 0.59.2**, which cannot parse the
`rdma-core 56` headers (`ib_uverbs_flow_action_esp_encap_union_… is not a valid
Ident`). `vendor/rdma-sys` is rdma-sys 0.3.0 with bindgen bumped to 0.69.4 (and
the two methods removed in modern bindgen — `rustfmt_bindings` / `size_t_is_usize`
— dropped from its `build.rs`); `Cargo.toml` redirects to it via
`[patch.crates-io]`. This is a temporary workaround — upstreaming a bindgen bump
to rdma-sys, or moving to a maintained bindings crate, is the cleaner long-term
fix.

## Not done (the real RDMA *win* is multi-week)

This is Phase-1 proof-of-plumbing only. A demonstrable zero-copy read of real
volume data still needs: a needle-backed RDMA **target** in the volume server
(none exists today), a control-plane handshake to publish `{remote_addr, rkey}`,
a breaking IPC-contract change (Rust + Go, in lockstep), mount wiring, and then
the zero-copy READ path. See the scoping notes for the phased plan.
