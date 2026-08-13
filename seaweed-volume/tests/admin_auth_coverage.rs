//! Parity guard for the gRPC admin-auth gate.
//!
//! Every `VolumeServer` handler must either call `check_grpc_admin_auth` or be
//! listed here as intentionally ungated, with the reason it stays open. Mirrors
//! the Go side's `TestVolumeServerAdminAuthCoverage`
//! (`weed/server/volume_grpc_admin_auth_coverage_test.go`). The gate had
//! silently dropped off 15 handlers on the Rust side; this keeps the two
//! implementations from drifting apart again.

use std::collections::{HashMap, HashSet};

/// Handlers that intentionally run without `check_grpc_admin_auth`, each with
/// the reason. Kept in sync with `ungatedVolumeServerRPCs` on the Go side.
///
/// The split is by caller, not by how destructive the call is: the guard checks
/// the peer IP against `-whiteList`, which holds masters, shell hosts and
/// workers -- not every peer volume server. Gating a volume-server ->
/// volume-server call therefore breaks replication, EC and tiering, so those
/// calls stay open and need a cluster-peer identity before they can be closed.
fn ungated_handlers() -> HashMap<&'static str, &'static str> {
    [
        // Cluster-internal: issued volume server -> volume server.
        ("copy_file", "replica sync and EC task pull whole files from a peer"),
        ("read_needle_blob", "replica sync, vacuum and EC rebuild read needles from a peer"),
        ("read_needle_meta", "replica sync compares needle metadata across peers"),
        ("write_needle_blob", "replica sync repairs a peer's needle"),
        ("receive_file", "EC shard distribution pushes shards to a peer"),
        ("read_volume_file_status", "the copy path queries the source volume server"),
        ("volume_ec_shard_read", "a volume server reads EC shards held by a peer"),
        ("volume_ec_blob_delete", "EC delete is fanned out to the shard holders"),
        ("volume_ec_shards_info", "EC verification polls shard holders"),
        ("volume_ec_shards_mount", "EC shard distribution mounts on the receiving peer"),
        ("volume_incremental_copy", "volume backup pulls increments from a peer"),
        ("volume_sync_status", "sync compares volume state across peers"),
        ("volume_tail_sender", "the tail source streams to the receiving peer"),
        ("volume_status", "replica sync and the master's vacuum loop poll volume status"),
        // Read-only or liveness: no state change.
        ("ping", "liveness probe"),
        ("get_state", "read-only volume server state"),
        ("query", "read-only data query"),
        ("vacuum_volume_check", "read-only garbage ratio; the vacuum steps that act on it are gated"),
        ("volume_server_status", "read-only status, the gRPC counterpart of the /status page"),
    ]
    .into_iter()
    .collect()
}

/// Whether a handler body actually invokes the gate. We can't run a real AST
/// pass like the Go side without pulling in a parser, so approximate it: drop
/// `//` line comments (the realistic way a stray mention would sneak in) and
/// look for the call form `check_grpc_admin_auth(`, not the bare identifier, so
/// a comment or doc reference can't make an ungated handler pass.
fn calls_gate(body: &str) -> bool {
    body.lines()
        .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
        .any(|code| code.contains("check_grpc_admin_auth("))
}

/// Enumerate the `VolumeServer` trait handlers and whether each gates on
/// `check_grpc_admin_auth`, by scanning the source of the trait impl block.
fn handler_gating(src: &str) -> Vec<(String, bool)> {
    let impl_start = src
        .find("impl VolumeServer for VolumeGrpcService {")
        .expect("locate the VolumeServer trait impl");
    let after = &src[impl_start..];
    // rustfmt keeps every method body indented, so the impl's own closing brace
    // is the first line that begins at column 0.
    let impl_end = after
        .match_indices("\n}")
        .next()
        .map(|(i, _)| i + 1)
        .expect("locate the end of the trait impl");
    let block = &after[..impl_end];

    let marker = "\n    async fn ";
    let starts: Vec<usize> = block.match_indices(marker).map(|(i, _)| i).collect();
    let mut handlers = Vec::with_capacity(starts.len());
    for (i, &start) in starts.iter().enumerate() {
        let name_start = start + marker.len();
        let name_end = name_start
            + block[name_start..]
                .find('(')
                .expect("handler signature has an argument list");
        let name = block[name_start..name_end].to_string();
        let body_end = starts.get(i + 1).copied().unwrap_or(block.len());
        let gated = calls_gate(&block[start..body_end]);
        handlers.push((name, gated));
    }
    handlers
}

#[test]
fn volume_server_admin_auth_coverage() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/server/grpc_server.rs"
    ))
    .expect("read grpc_server.rs");

    let handlers = handler_gating(&src);
    assert!(
        handlers.len() >= 40,
        "parsed only {} handlers from the trait impl, expected the full service",
        handlers.len()
    );

    let ungated = ungated_handlers();
    let mut problems = Vec::new();
    for (name, gated) in &handlers {
        match (gated, ungated.contains_key(name.as_str())) {
            (true, true) => problems.push(format!(
                "{name} calls check_grpc_admin_auth but is also listed as intentionally ungated; drop it from ungated_handlers"
            )),
            (false, false) => problems.push(format!(
                "{name} does not call check_grpc_admin_auth and is not listed as intentionally ungated; gate it, or add it with the reason it must stay open"
            )),
            _ => {}
        }
    }

    // Keep the exemption list honest: an entry for a handler that no longer
    // exists hides the fact that nothing is being exempted.
    let names: HashSet<&str> = handlers.iter().map(|(n, _)| n.as_str()).collect();
    for name in ungated.keys() {
        if !names.contains(name) {
            problems.push(format!(
                "ungated_handlers lists \"{name}\", which is not a VolumeServer handler"
            ));
        }
    }

    assert!(problems.is_empty(), "admin-auth coverage gaps:\n{}", problems.join("\n"));
}
