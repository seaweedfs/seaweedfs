//! Self-registration with the weed master, mirroring how filer/s3 keep
//! connected: hold a KeepConnected stream open and the node shows up in
//! ListClusterNodes(client_type="vector").

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
    }
}

/// Retry forever: try each master until a stream opens, hold it until it
/// errors, then rediscover — same shape as wdclient.KeepConnectedToMaster.
pub async fn keep_connected(masters: Vec<String>, client_addr: String) {
    loop {
        for master in &masters {
            match stream_to(master, &client_addr).await {
                Ok(()) => break,
                Err(e) => tracing::warn!("master {master}: {e:#}"),
            }
        }
        sleep(Duration::from_secs(3)).await;
    }
}

async fn stream_to(master: &str, client_addr: &str) -> Result<()> {
    let channel = Endpoint::new(format!("http://{master}"))?.connect().await?;
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
                Some(_) => {}
                None => anyhow::bail!("master closed the keep-connected stream"),
            },
            _ = sleep(Duration::from_secs(5)) => {
                tx.send(registration(client_addr)).await?;
            }
        }
    }
}
