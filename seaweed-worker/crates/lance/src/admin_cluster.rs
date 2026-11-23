//! The cluster as admin describes it with every request: where the Lance
//! Namespace is served and which filers keep the tables. A worker run under
//! admin needs to be told nothing else, as `weed worker` is told only -admin.

use std::hash::BuildHasher;
use std::sync::{Arc, RwLock};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use seaweed_worker_core::ClusterObserver;
use seaweed_worker_core::pb::ClusterContext;

use crate::cluster::{filer_grpc_addr, open_storage};
use crate::dataset::Storage;
use crate::direct_read::{Connection, DirectReads};

pub struct AdminCluster {
    connection: Connection,
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
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            pick: std::collections::hash_map::RandomState::new().hash_one(()) as usize,
            namespaces: RwLock::new(Vec::new()),
            storage: RwLock::new(None),
            storage_error: RwLock::new(None),
            connecting: tokio::sync::Mutex::new(()),
        }
    }

    /// One of the Lance Namespaces admin last named, or None when it named
    /// none.
    pub fn namespace(&self) -> Option<String> {
        let namespaces = self.namespaces.read().unwrap();
        match namespaces.len() {
            0 => None,
            n => Some(namespaces[self.pick % n].clone()),
        }
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

#[async_trait]
impl ClusterObserver for AdminCluster {
    async fn observe(&self, cluster: &ClusterContext) {
        *self.namespaces.write().unwrap() = cluster.lance_namespace_urls.clone();
        self.take_filers(&cluster.filer_addresses).await;
    }
}

/// Where a handler finds the catalog and the tables: what admin names when the
/// worker runs under one, else what the handler was built with.
#[derive(Clone, Default)]
pub struct Locator {
    namespace_url: String,
    storage: Storage,
    admin: Option<Arc<AdminCluster>>,
}

impl Locator {
    /// `namespace_url` is used when admin names no Lance Namespace, as an
    /// admin or gateways from before they could do so name none; empty means
    /// there is no such fallback.
    pub fn new(namespace_url: String) -> Self {
        Self {
            namespace_url,
            ..Default::default()
        }
    }

    pub fn with_storage(mut self, storage: Storage) -> Self {
        self.storage = storage;
        self
    }

    pub fn with_admin(mut self, admin: Option<Arc<AdminCluster>>) -> Self {
        self.admin = admin;
        self
    }

    pub fn namespace_url(&self) -> Result<String> {
        if let Some(url) = self.admin.as_ref().and_then(|admin| admin.namespace()) {
            return Ok(url);
        }
        if self.namespace_url.is_empty() {
            bail!(
                "admin names no Lance Namespace: no S3 gateway serves one, or the gateways \
                 or admin predate saying so, in which case give this worker --namespace"
            );
        }
        Ok(self.namespace_url.clone())
    }

    pub fn storage(&self) -> Result<Storage> {
        match &self.admin {
            Some(admin) => admin.storage(),
            None => Ok(self.storage.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> Arc<AdminCluster> {
        Arc::new(AdminCluster::new(Connection {
            filers: Vec::new(),
            read_jwt_key: None,
            sse: Default::default(),
            grpc_tls: None,
            https_tls: None,
            filer_dialer: None,
            filer_group: String::new(),
        }))
    }

    #[tokio::test]
    async fn admin_named_namespace_wins_over_the_fallback() {
        let admin = admin();
        let locator = Locator::new("http://fallback:9101".into()).with_admin(Some(admin.clone()));
        assert_eq!(locator.namespace_url().unwrap(), "http://fallback:9101");

        admin
            .observe(&ClusterContext {
                lance_namespace_urls: vec!["http://10.0.0.5:9101".into()],
                ..Default::default()
            })
            .await;
        assert_eq!(locator.namespace_url().unwrap(), "http://10.0.0.5:9101");

        // Admin stops naming any, as when its gateways go away.
        admin.observe(&ClusterContext::default()).await;
        assert_eq!(locator.namespace_url().unwrap(), "http://fallback:9101");
    }

    #[tokio::test]
    async fn no_namespace_anywhere_says_how_to_give_one() {
        let locator = Locator::new(String::new()).with_admin(Some(admin()));
        let err = locator.namespace_url().unwrap_err().to_string();
        assert!(err.contains("--namespace"), "{err}");
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
        let first = admin.namespace().unwrap();
        assert!(urls.contains(&first));
        assert_eq!(
            admin.namespace().unwrap(),
            first,
            "one process keeps one gateway"
        );
    }

    #[test]
    fn storage_before_admin_names_a_filer_is_an_error() {
        let err = Locator::default()
            .with_admin(Some(admin()))
            .storage()
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("no filer"), "{err}");
        // Without admin, the storage the handler was built with is used.
        assert!(Locator::default().storage().unwrap().0.is_none());
    }
}
