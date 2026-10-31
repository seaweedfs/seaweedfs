//! Self-registration with the weed master, mirroring how filer/s3 keep
//! connected: hold a KeepConnected stream open to the leader and the node
//! shows up in ListClusterNodes(client_type="vector").

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::sleep;
use tonic::transport::Endpoint;

use crate::master_pb::{KeepConnectedRequest, seaweed_client::SeaweedClient};

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
pub fn grpc_addr(master: &str) -> Result<String> {
    let (scheme, rest) = master.split_once("://").unwrap_or(("http", master));
    if let Some((host, port)) = rest.rsplit_once(':') {
        if let Some((_, grpc)) = port.split_once('.') {
            if grpc.parse::<u16>().is_ok() {
                return Ok(format!("{scheme}://{host}:{grpc}"));
            }
        }
        if let Ok(port) = port.parse::<u16>() {
            let grpc = port.checked_add(10000).with_context(|| {
                format!(
                    "master {master}: gRPC port {port}+10000 is past 65535; \
                     give it explicitly as {host}:{port}.<grpc port>"
                )
            })?;
            return Ok(format!("{scheme}://{host}:{grpc}"));
        }
    }
    Ok(format!("{scheme}://{rest}"))
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
        for master in &seeds(&mut leader, &masters) {
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

/// A hint is tried once, as wdclient.tryAllMasters keeps it for one cycle, so
/// a former leader that went away does not delay every later pass.
fn seeds(leader: &mut Option<String>, masters: &[String]) -> Vec<String> {
    leader
        .take()
        .into_iter()
        .chain(masters.iter().cloned())
        .collect()
}

/// Bounds the TCP connect, the 5s wdclient gives a master call; a black-holed
/// master otherwise costs the kernel's SYN timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

async fn stream_to(master: &str, client_addr: &str) -> Result<Option<String>> {
    let channel = Endpoint::new(grpc_addr(master)?)?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_hint_is_tried_once() {
        let masters = vec!["m1:9333".to_string(), "m2:9333".to_string()];
        let mut leader = Some("m3:9333".to_string());
        assert_eq!(
            seeds(&mut leader, &masters),
            ["m3:9333", "m1:9333", "m2:9333"]
        );
        assert_eq!(seeds(&mut leader, &masters), ["m1:9333", "m2:9333"]);
    }

    #[tokio::test]
    async fn dial_to_a_black_holed_master_is_bounded() {
        // TEST-NET-1 (RFC 5737) is reserved, so nothing answers the SYN.
        let dial = stream_to("192.0.2.1:9333", "127.0.0.1:8080");
        let result = tokio::time::timeout(CONNECT_TIMEOUT * 3, dial).await;
        assert!(matches!(result, Ok(Err(_))), "dial was not bounded");
    }

    #[test]
    fn grpc_addr_refuses_a_port_past_65535() {
        assert_eq!(grpc_addr("m1:9333").unwrap(), "http://m1:19333");
        assert_eq!(grpc_addr("m1:55535").unwrap(), "http://m1:65535");
        assert_eq!(grpc_addr("m1:60000.7000").unwrap(), "http://m1:7000");
        assert!(grpc_addr("m1:55536").is_err());
        assert!(grpc_addr("https://m1:60000").is_err());
    }
}
