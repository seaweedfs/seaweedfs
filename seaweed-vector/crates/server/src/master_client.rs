//! Self-registration with the weed master, mirroring how filer/s3 keep
//! connected: hold a KeepConnected stream open to the leader and the node
//! shows up in ListClusterNodes(client_type="vector").

use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;
use tonic::transport::Endpoint;

use crate::master_pb::{seaweed_client::SeaweedClient, KeepConnectedRequest};

fn registration(client_addr: &str) -> KeepConnectedRequest {
    KeepConnectedRequest {
        client_type: "vector".into(),
        client_address: client_addr.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        filer_group: String::new(),
        data_center: String::new(),
        rack: String::new(),
        leave_lock_ring: false,
        ..Default::default()
    }
}

/// Masters are configured by their HTTP address; gRPC lives +10000 like the
/// weed conventions (server_address.ToGrpcAddress).
// A master address may carry an explicit gRPC port as `host:http.grpc` —
// use it directly; otherwise the gRPC port is http + 10000.
fn grpc_addr(master: &str) -> String {
    let (scheme, rest) = master.split_once("://").unwrap_or(("http", master));
    if let Some((host, port)) = rest.rsplit_once(':') {
        if let Some((_, grpc)) = port.split_once('.') {
            if grpc.parse::<u16>().is_ok() {
                return format!("{scheme}://{host}:{grpc}");
            }
        }
        if let Ok(port) = port.parse::<u16>() {
            return format!("{scheme}://{host}:{}", port + 10000);
        }
    }
    format!("{scheme}://{rest}")
}

// `host:http.grpc` normalizes to `host:http` so a leader hint naming the
// dialed master does not read as a redirect.
fn http_addr(master: &str) -> String {
    let rest = master.rsplit_once("://").map(|(_, r)| r).unwrap_or(master);
    match rest.rsplit_once(':') {
        Some((host, port)) => {
            format!("{host}:{}", port.split('.').next().unwrap_or(port))
        }
        None => rest.to_string(),
    }
}

/// Retry forever: try each master until a stream opens, hold it until it
/// errors, then rediscover — same shape as wdclient.KeepConnectedToMaster.
/// A follower response's leader hint takes priority on the next pass.
pub async fn keep_connected(masters: Vec<String>, client_addr: String) {
    let mut leader: Option<String> = None;
    loop {
        let mut seeds: Vec<String> = Vec::new();
        if let Some(l) = &leader {
            seeds.push(l.clone());
        }
        seeds.extend(masters.iter().cloned());
        for master in &seeds {
            match stream_to(master, &client_addr).await {
                Ok(Some(l)) => {
                    leader = Some(l);
                    break;
                }
                Ok(None) => break,
                Err(e) => tracing::warn!("master {master}: {e:#}"),
            }
        }
        sleep(Duration::from_secs(3)).await;
    }
}

async fn stream_to(master: &str, client_addr: &str) -> Result<Option<String>> {
    let channel = Endpoint::new(grpc_addr(master))?.connect().await?;
    let mut client = SeaweedClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(registration(client_addr)).await?;
    let mut stream = client
        .keep_connected(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    // The master keeps the node live while the stream is open; resend the
    // registration periodically so a reconnect never leaves a stale entry.
    loop {
        tokio::select! {
            msg = stream.message() => match msg? {
                Some(resp) => {
                    // A volume_location appears on every response — only
                    // follow it when the hint points at a different master,
                    // otherwise the leader keeps identifying itself and we
                    // would reconnect forever.
                    if let Some(loc) = resp.volume_location {
                        if !loc.leader.is_empty()
                            && http_addr(&loc.leader) != http_addr(master)
                        {
                            return Ok(Some(loc.leader));
                        }
                    }
                }
                None => anyhow::bail!("master closed the keep-connected stream"),
            },
            _ = sleep(Duration::from_secs(5)) => {
                tx.send(registration(client_addr)).await?;
            }
        }
    }
}
