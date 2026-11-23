//! The cluster as admin describes it with every request: where the Lance
//! Namespace is served and which filers keep the tables. A worker run under
//! admin needs to be told nothing else, as `weed worker` is told only -admin.

use std::hash::BuildHasher;
use std::sync::RwLock;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use seaweed_worker_core::ClusterObserver;
use seaweed_worker_core::pb::ClusterContext;

use crate::catalog::{NamespaceClient, SigningKey};
use crate::cluster::{filer_grpc_addr, open_storage};
use crate::dataset::Storage;
use crate::direct_read::{Connection, DirectReads};

pub struct AdminCluster {
    connection: Connection,
    signing_key: Option<SigningKey>,
    /// Which of the gateways admin names this process uses, so the workers of
    /// a cluster spread over them rather than all picking the first.
    pick: usize,
    namespaces: RwLock<Vec<String>>,
    storage: RwLock<Option<DirectReads>>,
    storage_error: RwLock<Option<String>>,
    connecting: tokio::sync::Mutex<()>,
}

impl AdminCluster {
    /// `connection` is everything but the filers, which admin supplies.
    pub fn new(connection: Connection, signing_key: Option<SigningKey>) -> Self {
        Self {
            connection,
            signing_key,
            pick: std::collections::hash_map::RandomState::new().hash_one(()) as usize,
            namespaces: RwLock::new(Vec::new()),
            storage: RwLock::new(None),
            storage_error: RwLock::new(None),
            connecting: tokio::sync::Mutex::new(()),
        }
    }

    /// One of the Lance Namespaces admin last named.
    pub fn namespace_url(&self) -> Result<String> {
        let namespaces = self.namespaces.read().unwrap();
        match namespaces.len() {
            0 => Err(anyhow!(
                "admin names no Lance Namespace: no S3 gateway serves one, or they predate saying so"
            )),
            n => Ok(namespaces[self.pick % n].clone()),
        }
    }

    pub fn client(&self) -> Result<NamespaceClient> {
        Ok(NamespaceClient::new(self.namespace_url()?).with_signing_key(self.signing_key.as_ref()))
    }

    pub fn storage(&self) -> Result<Storage> {
        if let Some(storage) = self.storage.read().unwrap().clone() {
            return Ok(Storage(Some(storage)));
        }
        match self.storage_error.read().unwrap().clone() {
            Some(err) => Err(anyhow!("no storage: {err}")),
            None => Err(anyhow!("no storage: admin has named no filer yet")),
        }
    }

    async fn take_filers(&self, addresses: &[String]) {
        let filers: Vec<String> = addresses
            .iter()
            .filter_map(|address| match filer_grpc_addr(address) {
                Ok(addr) => Some(addr),
                Err(err) => {
                    tracing::warn!("admin names filer {address:?}: {err:#}");
                    None
                }
            })
            .collect();
        // Keep the last known filers while admin lists none.
        if filers.is_empty() {
            return;
        }
        let _connecting = self.connecting.lock().await;
        let connected = self.storage.read().unwrap().clone();
        if let Some(storage) = connected {
            if let Err(err) = storage.set_filers(&filers) {
                tracing::warn!("filers {}: {err:#}", filers.join(","));
            }
            return;
        }
        let mut conn = self.connection.clone();
        conn.filers = filers;
        match open_storage(&conn).await {
            Ok(storage) => {
                *self.storage.write().unwrap() = Some(storage);
                *self.storage_error.write().unwrap() = None;
            }
            Err(err) => {
                tracing::warn!("connecting to the filers admin names: {err:#}");
                *self.storage_error.write().unwrap() = Some(format!("{err:#}"));
            }
        }
    }
}

/// A cluster admin has said nothing about yet.
impl Default for AdminCluster {
    fn default() -> Self {
        Self::new(Connection::default(), None)
    }
}

#[async_trait]
impl ClusterObserver for AdminCluster {
    async fn observe(&self, cluster: &ClusterContext) {
        // As with the filers, keep the last known namespaces while admin lists
        // none: admin sends its cached filers but no gateways when a master
        // call fails, and the gateways serve the namespace regardless.
        if !cluster.lance_namespace_urls.is_empty() {
            *self.namespaces.write().unwrap() = cluster.lance_namespace_urls.clone();
        }
        self.take_filers(&cluster.filer_addresses).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> AdminCluster {
        AdminCluster::default()
    }

    #[tokio::test]
    async fn follows_the_namespaces_admin_names() {
        let admin = admin();
        let err = admin.namespace_url().unwrap_err().to_string();
        assert!(err.contains("names no Lance Namespace"), "{err}");

        admin
            .observe(&ClusterContext {
                lance_namespace_urls: vec!["http://10.0.0.5:9101".into()],
                ..Default::default()
            })
            .await;
        assert_eq!(admin.namespace_url().unwrap(), "http://10.0.0.5:9101");

        // A context naming no gateway, as admin sends when listing them from
        // the master fails, keeps the namespace.
        admin.observe(&ClusterContext::default()).await;
        assert_eq!(admin.namespace_url().unwrap(), "http://10.0.0.5:9101");

        admin
            .observe(&ClusterContext {
                lance_namespace_urls: vec!["http://10.0.0.6:9101".into()],
                ..Default::default()
            })
            .await;
        assert_eq!(admin.namespace_url().unwrap(), "http://10.0.0.6:9101");
    }

    #[tokio::test]
    async fn the_pick_stays_among_the_named_gateways() {
        let admin = admin();
        let urls: Vec<String> = (1..=3).map(|i| format!("http://10.0.0.{i}:9101")).collect();
        admin
            .observe(&ClusterContext {
                lance_namespace_urls: urls.clone(),
                ..Default::default()
            })
            .await;
        let first = admin.namespace_url().unwrap();
        assert!(urls.contains(&first));
        assert_eq!(
            admin.namespace_url().unwrap(),
            first,
            "one process keeps one gateway"
        );
    }

    #[test]
    fn storage_before_admin_names_a_filer_is_an_error() {
        let err = admin().storage().err().unwrap().to_string();
        assert!(err.contains("no filer"), "{err}");
    }
}
