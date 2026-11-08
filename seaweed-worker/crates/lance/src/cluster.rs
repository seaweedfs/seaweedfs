//! The cluster as a Lance process joins it: the masters dialed as weed
//! clients dial them, the filers they list, and the storage connection built
//! from them and security.toml that the engine, the worker and the CLI share.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};

use crate::direct_read::{Connection, DirectReads, FilerDialer};
use crate::grpc_tls;
use crate::master_pb::{ListClusterNodesRequest, seaweed_client::SeaweedClient};
use crate::security::{GrpcTls, Security};

/// Masters are configured by their HTTP address; gRPC lives +10000 like the
/// weed conventions (server_address.ToGrpcAddress).
// A master address may carry an explicit gRPC port as `host:http.grpc` —
// use it directly; otherwise the gRPC port is http + 10000.
pub fn grpc_addr(master: &str) -> Result<String> {
    let (scheme, rest) = master.split_once("://").unwrap_or(("http", master));
    if let Some((host, port)) = rest.rsplit_once(':') {
        if let Some((_, grpc)) = port.split_once('.') {
            if let Ok(grpc) = grpc.parse::<u64>() {
                let grpc = u16::try_from(grpc)
                    .with_context(|| format!("master {master}: gRPC port {grpc} is past 65535"))?;
                return Ok(format!("{scheme}://{host}:{grpc}"));
            }
        }
        if let Ok(port) = port.parse::<u64>() {
            let grpc = port
                .checked_add(10000)
                .and_then(|grpc| u16::try_from(grpc).ok())
                .with_context(|| {
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

/// Bounds the credential reads and the connect, TLS handshake included, and
/// then the master's first answer, the 5s wdclient gives a master call; a
/// black-holed or silent master otherwise holds up the other masters.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A channel to the master's gRPC, over TLS with `grpc.client`'s credentials
/// when the cluster has them.
pub async fn connect(master: &str, tls: Option<&GrpcTls>) -> Result<Channel> {
    let addr = grpc_addr(master)?;
    tokio::time::timeout(CONNECT_TIMEOUT, async {
        Ok(match tls {
            None => Endpoint::new(addr)?.connect().await?,
            // The connector does the TLS; tonic adds its own only for an
            // https URI, which it refuses without a tls_config.
            Some(tls) => {
                Endpoint::new(addr.replacen("https://", "http://", 1))?
                    .connect_with_connector(grpc_tls::connector(tls.clone()))
                    .await?
            }
        })
    })
    .await
    .context("master did not complete the connection")?
}

/// The filers of `filer_group` registered with the masters, as gRPC
/// `host:port`s, in registration order; the masters are all asked, since a
/// follower may know fewer than the leader. An error means no master answered.
pub async fn list_filers(
    masters: &[String],
    filer_group: &str,
    tls: Option<&GrpcTls>,
) -> Result<Vec<String>> {
    let mut filers = Vec::new();
    let mut answered = false;
    let mut last = None;
    for master in masters {
        match filers_of(master, filer_group, tls).await {
            Ok(found) => {
                answered = true;
                for filer in found {
                    if !filers.contains(&filer) {
                        filers.push(filer);
                    }
                }
            }
            Err(err) => {
                tracing::warn!("master {master}: listing filers: {err:#}");
                last = Some(err);
            }
        }
    }
    match (answered, last) {
        (false, Some(err)) => Err(err),
        (false, None) => Err(anyhow::anyhow!("no master given")),
        _ => Ok(filers),
    }
}

async fn filers_of(master: &str, filer_group: &str, tls: Option<&GrpcTls>) -> Result<Vec<String>> {
    let channel = connect(master, tls).await?;
    let request = ListClusterNodesRequest {
        client_type: "filer".into(),
        filer_group: filer_group.into(),
        ..Default::default()
    };
    let nodes = tokio::time::timeout(
        CONNECT_TIMEOUT,
        SeaweedClient::new(channel).list_cluster_nodes(request),
    )
    .await
    .context("master did not answer")??
    .into_inner()
    .cluster_nodes;
    Ok(nodes
        .iter()
        .filter_map(|node| match filer_grpc_addr(&node.address) {
            Ok(addr) => Some(addr),
            Err(err) => {
                tracing::warn!("master {master} lists filer {:?}: {err:#}", node.address);
                None
            }
        })
        .collect())
}

/// A filer registers as `host:port.grpc`, or `host:port` with its gRPC at
/// port + 10000.
pub fn filer_grpc_addr(address: &str) -> Result<String> {
    let addr = grpc_addr(address)?;
    Ok(addr
        .split_once("://")
        .map_or(addr.as_str(), |(_, rest)| rest)
        .to_string())
}

/// How often the masters are asked for the filers.
const FILER_REFRESH: Duration = Duration::from_secs(30);
/// How long startup waits for a filer to be registered.
const FILER_WAIT: Duration = Duration::from_secs(120);

/// The filers the masters list, waiting for the first to register.
async fn discover_filers(
    masters: &[String],
    filer_group: &str,
    tls: Option<&GrpcTls>,
) -> Result<Vec<String>> {
    let deadline = tokio::time::Instant::now() + FILER_WAIT;
    loop {
        match list_filers(masters, filer_group, tls).await {
            Ok(filers) if !filers.is_empty() => return Ok(filers),
            Ok(_) => tracing::warn!("no filer is registered with the masters yet"),
            Err(err) => tracing::warn!("listing filers: {err:#}"),
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("no filer registered with the masters within {FILER_WAIT:?}");
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn follow_filers(
    reads: DirectReads,
    masters: Vec<String>,
    filer_group: String,
    tls: Option<GrpcTls>,
) {
    loop {
        tokio::time::sleep(FILER_REFRESH).await;
        match list_filers(&masters, &filer_group, tls.as_ref()).await {
            Ok(filers) if !filers.is_empty() => {
                if let Err(err) = reads.set_filers(&filers) {
                    tracing::warn!("filers {}: {err:#}", filers.join(","));
                }
            }
            // Keep the last known filers while the masters list none.
            Ok(_) => tracing::warn!("the masters list no filer; keeping the known ones"),
            Err(err) => tracing::warn!("listing filers: {err:#}"),
        }
    }
}

/// The comma-separated masters, each with a valid gRPC address.
pub fn master_addresses(masters: &str) -> Result<Vec<String>> {
    let masters: Vec<String> = masters
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if masters.is_empty() {
        anyhow::bail!("the master list names no master address");
    }
    for master in &masters {
        grpc_addr(master)?;
    }
    Ok(masters)
}

/// What the cluster's storage is reached with: the filers the masters list,
/// dialed as the masters are, and the keys security.toml holds.
pub struct Cluster {
    pub masters: Vec<String>,
    pub filer_group: String,
    /// `grpc.client`'s credentials for the masters, when the cluster has them.
    pub master_tls: Option<GrpcTls>,
}

impl Cluster {
    pub fn new(masters: &str, filer_group: &str, security: &Security) -> Result<Self> {
        Ok(Self {
            masters: master_addresses(masters)?,
            filer_group: filer_group.to_string(),
            master_tls: security.grpc_client_tls("grpc.client", "grpc.master")?,
        })
    }

    /// Connects to the cluster's storage and follows the masters' filer list.
    /// Chunks are encrypted for the volume servers when the filer is
    /// configured to (`weed filer -encryptVolumeData`), as `weed s3` does.
    pub async fn storage(&self, security: &Security) -> Result<DirectReads> {
        // The filers are dialed as the masters are: grpc.client's credentials
        // and server allowlist, through this crate's own connector.
        let filer_dialer: Option<FilerDialer> = security
            .grpc_client_tls("grpc.client", "grpc.filer")?
            .map(|tls| {
                Arc::new(move |addr: &str| {
                    let channel = Endpoint::new(format!("http://{addr}"))?
                        .connect_timeout(Duration::from_secs(10))
                        .connect_with_connector_lazy(grpc_tls::connector(tls.clone()));
                    Ok(sw_client::FilerClient::from_channel(addr, channel))
                }) as FilerDialer
            });
        let conn = Connection {
            filers: discover_filers(&self.masters, &self.filer_group, self.master_tls.as_ref())
                .await?,
            read_jwt_key: security.jwt_read_key(),
            sse: security.sse(),
            grpc_tls: None,
            https_tls: security.https_client_tls()?,
            filer_dialer,
            filer_group: self.filer_group.clone(),
        };
        let storage = DirectReads::connect(&conn)
            .await
            .context("direct storage")?;
        tracing::info!(
            "tables kept through {storage:?}; filer gRPC {}, volume servers {}; SSE-S3 objects {}",
            if conn.filer_dialer.is_some() {
                "over TLS"
            } else {
                "plaintext"
            },
            if conn.https_tls.is_some() {
                "over HTTPS"
            } else {
                "over HTTP"
            },
            if storage.sse_s3_configured() {
                "are readable"
            } else {
                "need s3.sse.kek or s3.sse.key"
            },
        );
        tokio::spawn(follow_filers(
            storage.clone(),
            self.masters.clone(),
            self.filer_group.clone(),
            self.master_tls.clone(),
        ));
        Ok(storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_master_list_is_refused() {
        assert!(master_addresses("").is_err());
        assert!(master_addresses(" , ").is_err());
        assert_eq!(
            master_addresses(" m1:9333,,m2:9333 ").unwrap(),
            ["m1:9333", "m2:9333"]
        );
    }
}
