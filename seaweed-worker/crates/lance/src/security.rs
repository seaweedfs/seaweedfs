//! The cluster's `security.toml`, found and read the way weed processes do:
//! the file in `-config_dir`, `.`, `~/.seaweedfs`, `/usr/local/etc/seaweedfs`
//! or `/etc/seaweedfs`, with `WEED_*` environment variables over it.

use std::path::{Path, PathBuf};

use crate::sse::SseConfig;
use anyhow::{Context, Result, bail};
use sw_client::HttpsTls;

const FILE_NAME: &str = "security.toml";

/// What a weed gRPC server is dialed with, as `security.LoadClientTLS` reads
/// it: the files, re-read at every dial, and the server identities allowed.
/// The server's certificate must chain to `grpc.ca`; like weed clients, this
/// one does not check it against the host dialed, only against the allowlist
/// when one is set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcTls {
    /// `grpc.ca`
    pub ca_path: String,
    /// The component's `client_cert` and `client_key`, else its `cert` and
    /// `key`.
    pub cert_path: String,
    pub key_path: String,
    /// `allowed_server_commonNames`: when set, the server certificate's common
    /// name or a DNS SAN must be one of these or lie under the wildcard
    /// domain; a URI SAN must be one of these exactly.
    pub allowed_server_names: Vec<String>,
    /// `allowed_server_wildcard_domain`
    pub allowed_server_wildcard_domain: String,
}

impl GrpcTls {
    /// The CA, certificate and key as the files hold them now, so a dial
    /// after the files rotate presents the new pair.
    pub async fn pems(&self) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        async fn read(path: &str) -> Result<Vec<u8>> {
            tokio::fs::read(path)
                .await
                .with_context(|| format!("reading {path}"))
        }
        Ok((
            read(&self.ca_path).await?,
            read(&self.cert_path).await?,
            read(&self.key_path).await?,
        ))
    }
}

/// What a gRPC listener serves with, as `security.LoadServerTLS` reads it:
/// the files, re-read when they change, and the client identities allowed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcServerTls {
    pub cert_path: String,
    pub key_path: String,
    /// `grpc.ca`, which every client certificate must chain to.
    pub ca_path: String,
    /// `<component>.allowed_commonNames`: when set, the client certificate's
    /// common name must be one of these, or lie under the wildcard domain.
    pub allowed_common_names: Vec<String>,
    /// `grpc.allowed_wildcard_domain`
    pub allowed_wildcard_domain: String,
}

pub struct Security {
    path: Option<PathBuf>,
    table: toml::Table,
    env: Box<dyn Fn(&str) -> Option<String> + Send + Sync>,
}

impl std::fmt::Debug for Security {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Security")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

// weed's util.ResolvePath for "~" and "~/..."
fn resolve_path(path: &Path, home: Option<&Path>) -> PathBuf {
    match (path.strip_prefix("~"), home) {
        (Ok(rest), Some(home)) => home.join(rest),
        _ => path.to_path_buf(),
    }
}

fn candidates(
    config_dir: Option<&Path>,
    current_dir: Option<&Path>,
    home: Option<&Path>,
) -> Vec<PathBuf> {
    let mut found = Vec::new();
    if let Some(dir) = config_dir {
        found.push(resolve_path(dir, home).join(FILE_NAME));
    }
    if let Some(dir) = current_dir {
        found.push(dir.join(FILE_NAME));
    }
    if let Some(home) = home {
        found.push(home.join(".seaweedfs").join(FILE_NAME));
    }
    found.push(PathBuf::from("/usr/local/etc/seaweedfs").join(FILE_NAME));
    found.push(PathBuf::from("/etc/seaweedfs").join(FILE_NAME));
    found
}

/// A comma-separated list such as `allowed_commonNames`, trimmed, without
/// empties.
fn names(list: Option<String>) -> Vec<String> {
    list.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

// viper keys are case-insensitive; of two spellings, a set value wins over an
// empty one
fn entry<'a>(table: &'a toml::Table, key: &str) -> Option<&'a toml::Value> {
    let set = |v: &&toml::Value| v.as_str() != Some("");
    table.get(key).filter(set).or_else(|| {
        table
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v)
            .find(set)
    })
}

impl Security {
    /// Reads `path`, or the first `security.toml` where weed looks: the
    /// `-config_dir`, then `.`, `~/.seaweedfs`, `/usr/local/etc/seaweedfs`
    /// and `/etc/seaweedfs`. Without one, only the environment applies.
    pub fn load(path: Option<&Path>, config_dir: Option<&Path>) -> Result<Self> {
        let path = match path {
            Some(path) => Some(path.to_path_buf()),
            None => {
                let home = std::env::var_os("HOME").map(PathBuf::from);
                candidates(
                    config_dir,
                    std::env::current_dir().ok().as_deref(),
                    home.as_deref(),
                )
                .into_iter()
                .find(|p| p.is_file())
            }
        };
        Self::from_file(path, Box::new(|name| std::env::var(name).ok()))
    }

    fn from_file(
        path: Option<PathBuf>,
        env: Box<dyn Fn(&str) -> Option<String> + Send + Sync>,
    ) -> Result<Self> {
        let table = match &path {
            Some(path) => std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?
                .parse::<toml::Table>()
                .with_context(|| format!("parsing {}", path.display()))?,
            None => toml::Table::new(),
        };
        Ok(Self { path, table, env })
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn raw(&self, key: &str) -> Option<String> {
        let env_name = format!("WEED_{}", key.to_uppercase().replace('.', "_"));
        // viper treats an empty variable as unset
        if let Some(value) = (self.env)(&env_name).filter(|v| !v.is_empty()) {
            return Some(value);
        }
        let mut parts = key.split('.');
        let mut value = entry(&self.table, parts.next()?)?;
        for part in parts {
            value = entry(value.as_table()?, part)?;
        }
        match value {
            toml::Value::String(s) => Some(s.clone()),
            toml::Value::Boolean(b) => Some(b.to_string()),
            toml::Value::Integer(i) => Some(i.to_string()),
            _ => None,
        }
    }

    /// A dotted key such as `grpc.ca`, from `WEED_GRPC_CA` when set, else
    /// the file; empty counts as unset.
    pub fn get(&self, key: &str) -> Option<String> {
        self.raw(key).filter(|v| !v.is_empty())
    }

    /// A boolean as weed reads one: viper accepts what Go's strconv.ParseBool
    /// does, `1`, `t`, `T`, `TRUE`, `true` and `True`.
    pub fn get_bool(&self, key: &str) -> bool {
        self.raw(key)
            .is_some_and(|v| matches!(v.trim(), "1" | "t" | "T" | "TRUE" | "true" | "True"))
    }

    /// The file a key names, read whole.
    pub fn read(&self, key: &str, path: &str) -> Result<Vec<u8>> {
        std::fs::read(path).with_context(|| format!("{key} = {path:?}"))
    }

    fn pair(&self, cert: &str, key: &str) -> Result<Option<(String, String)>> {
        match (self.get(cert), self.get(key)) {
            (Some(cert), Some(key)) => Ok(Some((cert, key))),
            (None, None) => Ok(None),
            _ => bail!("{cert} and {key} must both be set"),
        }
    }

    /// The credentials `component` (such as `grpc.client`) dials `target`
    /// (such as `grpc.master`) with, following `security.LoadClientTLS`: a
    /// dedicated client certificate is preferred over the component's own,
    /// and once either pair is set `grpc.ca` is required. The server
    /// allowlist comes from `<component>.<target>_allowed_server_commonNames`,
    /// else `<component>.allowed_server_commonNames`, else
    /// `grpc.allowed_server_commonNames`, and the wildcard domain likewise.
    /// `None` keeps plaintext.
    pub fn grpc_client_tls(&self, component: &str, target: &str) -> Result<Option<GrpcTls>> {
        let client = (
            format!("{component}.client_cert"),
            format!("{component}.client_key"),
        );
        let own = (format!("{component}.cert"), format!("{component}.key"));
        let (cert, key) = match self.pair(&client.0, &client.1)? {
            Some(pair) => pair,
            None => match self.pair(&own.0, &own.1)? {
                Some(pair) => pair,
                None => return Ok(None),
            },
        };
        let ca = self
            .get("grpc.ca")
            .with_context(|| format!("{component} cert and key are set but grpc.ca is not"))?;
        self.read("grpc.ca", &ca)?;
        self.read(&format!("{component} cert"), &cert)?;
        self.read(&format!("{component} key"), &key)?;
        let prefix = if target == component {
            String::new()
        } else {
            format!("{}_", target.trim_start_matches("grpc."))
        };
        let first = |suffix: &str| {
            [
                format!("{component}.{prefix}{suffix}"),
                format!("{component}.{suffix}"),
                format!("grpc.{suffix}"),
            ]
            .iter()
            .find_map(|key| self.get(key))
        };
        Ok(Some(GrpcTls {
            ca_path: ca,
            cert_path: cert,
            key_path: key,
            allowed_server_names: names(first("allowed_server_commonNames")),
            allowed_server_wildcard_domain: first("allowed_server_wildcard_domain")
                .unwrap_or_default(),
        }))
    }

    /// The identity `component` (such as `grpc.vector`) serves with, following
    /// `security.LoadServerTLS`: its `cert` and `key`, with `grpc.ca` as the
    /// root every client certificate must chain to, and the component's
    /// `allowed_commonNames`. `None` keeps plaintext. The files are read by
    /// [`crate::tls::ServerTls`], which keeps re-reading them.
    pub fn grpc_server_tls(&self, component: &str) -> Result<Option<GrpcServerTls>> {
        let allowed = format!("{component}.allowed_commonNames");
        let Some((cert_path, key_path)) =
            self.pair(&format!("{component}.cert"), &format!("{component}.key"))?
        else {
            if self.get(&allowed).is_some() {
                bail!("{allowed} is set but {component}.cert and {component}.key are not");
            }
            return Ok(None);
        };
        let ca_path = self
            .get("grpc.ca")
            .with_context(|| format!("{component} cert and key are set but grpc.ca is not"))?;
        Ok(Some(GrpcServerTls {
            cert_path,
            key_path,
            ca_path,
            allowed_common_names: names(self.get(&allowed)),
            allowed_wildcard_domain: self.get("grpc.allowed_wildcard_domain").unwrap_or_default(),
        }))
    }
}

impl Security {
    /// `https.client` for volume servers; `None` unless enabled.
    pub fn https_client_tls(&self) -> Result<Option<HttpsTls>> {
        if !self.get_bool("https.client.enabled") {
            return Ok(None);
        }
        let (cert, key) = match (self.get("https.client.cert"), self.get("https.client.key")) {
            (Some(cert), Some(key)) => (
                Some(self.read("https.client.cert", &cert)?),
                Some(self.read("https.client.key", &key)?),
            ),
            (None, None) => (None, None),
            _ => bail!("https.client.cert and https.client.key must both be set"),
        };
        Ok(Some(HttpsTls {
            ca_pem: match self.get("https.client.ca") {
                Some(ca) => Some(self.read("https.client.ca", &ca)?),
                None => None,
            },
            cert_pem: cert,
            key_pem: key,
            insecure_skip_verify: self.get_bool("https.client.insecure_skip_verify"),
        }))
    }

    /// `jwt.signing.read.key`, which volume reads must be signed with.
    pub fn jwt_read_key(&self) -> Option<String> {
        self.get("jwt.signing.read.key")
    }

    /// The S3 gateway's SSE-S3 settings.
    pub fn sse(&self) -> SseConfig {
        SseConfig {
            kek_hex: self.get("s3.sse.kek"),
            key: self.get("s3.sse.key"),
            passphrase: self.get("s3.sse.kek.passphrase"),
            require_commitment: self.get_bool("s3.require_key_commitment"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn security(toml: &str, env: &[(&str, &str)]) -> Security {
        let env: HashMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Security {
            path: None,
            table: toml.parse().unwrap(),
            env: Box::new(move |name| env.get(name).cloned()),
        }
    }

    /// Placeholder credential files: the loader reads them, it does not
    /// parse them.
    fn files() -> (tempfile::TempDir, String, String, String, String) {
        let dir = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        for (name, body) in [
            ("ca.pem", "CA"),
            ("cert.pem", "CERT"),
            ("key.pem", "KEY"),
            ("client.pem", "CLIENT"),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            paths.push(path.display().to_string());
        }
        let [ca, cert, key, client]: [String; 4] = paths.try_into().unwrap();
        (dir, ca, cert, key, client)
    }

    #[test]
    fn the_environment_wins_over_the_file() {
        let s = security(
            "[jwt.signing.read]\nkey = \"from-file\"\n[https.client]\nenabled = true\n",
            &[("WEED_JWT_SIGNING_READ_KEY", "from-env")],
        );
        assert_eq!(s.get("jwt.signing.read.key").as_deref(), Some("from-env"));
        assert!(s.get_bool("https.client.enabled"));
        let s = security("[jwt.signing.read]\nkey = \"from-file\"\n", &[]);
        assert_eq!(s.get("jwt.signing.read.key").as_deref(), Some("from-file"));
        let s = security(
            "[jwt.signing.read]\nkey = \"from-file\"\n",
            &[("WEED_JWT_SIGNING_READ_KEY", "")],
        );
        assert_eq!(s.get("jwt.signing.read.key").as_deref(), Some("from-file"));
        let s = security("[grpc.vector]\nallowed_commonnames = \"s3\"\n", &[]);
        assert_eq!(
            s.get("grpc.vector.allowed_commonNames").as_deref(),
            Some("s3")
        );
        let s = security(
            "[grpc.client]\nallowed_server_commonNames = \"\"\nallowed_server_commonnames = \"master\"\n",
            &[],
        );
        assert_eq!(
            s.get("grpc.client.allowed_server_commonNames").as_deref(),
            Some("master")
        );
        assert!(security("", &[]).get("jwt.signing.read.key").is_none());
        assert!(
            security("[grpc]\nca = \"\"\n", &[])
                .get("grpc.ca")
                .is_none()
        );
    }

    #[tokio::test]
    async fn client_credentials_follow_the_gateways_rules() {
        let (_dir, ca, cert, key, client) = files();
        assert!(
            security("", &[])
                .grpc_client_tls("grpc.client", "grpc.master")
                .unwrap()
                .is_none()
        );

        let tls = security(
            &format!("[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"),
            &[],
        )
        .grpc_client_tls("grpc.client", "grpc.master")
        .unwrap()
        .unwrap();
        assert_eq!(
            tls,
            GrpcTls {
                ca_path: ca.clone(),
                cert_path: cert.clone(),
                key_path: key.clone(),
                allowed_server_names: Vec::new(),
                allowed_server_wildcard_domain: String::new(),
            }
        );
        assert_eq!(
            tls.pems().await.unwrap(),
            (b"CA".to_vec(), b"CERT".to_vec(), b"KEY".to_vec())
        );
        // Each dial reads the files, so a rotated pair is picked up.
        std::fs::write(&cert, "ROTATED").unwrap();
        assert_eq!(tls.pems().await.unwrap().1, b"ROTATED");

        // A dedicated client certificate is preferred, and the environment
        // can supply it.
        let tls = security(
            &format!("[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"),
            &[
                ("WEED_GRPC_CLIENT_CLIENT_CERT", &client),
                ("WEED_GRPC_CLIENT_CLIENT_KEY", &key),
            ],
        )
        .grpc_client_tls("grpc.client", "grpc.master")
        .unwrap()
        .unwrap();
        assert_eq!(tls.pems().await.unwrap().1, b"CLIENT");

        for broken in [
            format!("[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"),
            format!("[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\n"),
            format!("[grpc]\nca = {ca:?}\n[grpc.client]\nclient_key = {key:?}\n"),
        ] {
            assert!(
                security(&broken, &[])
                    .grpc_client_tls("grpc.client", "grpc.master")
                    .is_err(),
                "{broken}"
            );
        }
    }

    #[test]
    fn server_allowlists_fall_back_from_target_to_component_to_grpc() {
        let (_dir, ca, cert, key, _client) = files();
        let base = format!("[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n");
        let load = |grpc: &str, client_extra: &str| {
            security(
                &format!("[grpc]\nca = {ca:?}\n{grpc}{base}{client_extra}"),
                &[],
            )
            .grpc_client_tls("grpc.client", "grpc.master")
            .unwrap()
            .unwrap()
        };
        let tls = load(
            "allowed_server_commonNames = \"any\"\nallowed_server_wildcard_domain = \".any\"\n",
            "",
        );
        assert_eq!(tls.allowed_server_names, ["any"]);
        assert_eq!(tls.allowed_server_wildcard_domain, ".any");
        let tls = load(
            "allowed_server_commonNames = \"any\"\n",
            "allowed_server_commonNames = \"client, wide\"\n",
        );
        assert_eq!(tls.allowed_server_names, ["client", "wide"]);
        let tls = load(
            "allowed_server_commonNames = \"any\"\n",
            "allowed_server_commonNames = \"client\"\nmaster_allowed_server_commonNames = \"master\"\n",
        );
        assert_eq!(tls.allowed_server_names, ["master"]);
        assert!(tls.allowed_server_wildcard_domain.is_empty());
        // An empty spelling falls through, as firstConfigString skips "".
        let tls = load(
            "allowed_server_commonNames = \"any\"\n",
            "master_allowed_server_commonNames = \"\"\n",
        );
        assert_eq!(tls.allowed_server_names, ["any"]);
    }

    #[test]
    fn server_identity_needs_both_halves_and_the_ca() {
        let (_dir, ca, cert, key, _client) = files();
        assert!(
            security("", &[])
                .grpc_server_tls("grpc.vector")
                .unwrap()
                .is_none()
        );
        let tls = security(
            &format!("[grpc]\nca = {ca:?}\n[grpc.vector]\ncert = {cert:?}\nkey = {key:?}\n"),
            &[],
        )
        .grpc_server_tls("grpc.vector")
        .unwrap()
        .unwrap();
        assert_eq!(
            tls,
            GrpcServerTls {
                cert_path: cert.clone(),
                key_path: key.clone(),
                ca_path: ca.clone(),
                allowed_common_names: Vec::new(),
                allowed_wildcard_domain: String::new(),
            }
        );
        let tls = security(
            &format!(
                "[grpc]\nca = {ca:?}\nallowed_wildcard_domain = \".internal\"\n[grpc.vector]\ncert = {cert:?}\nkey = {key:?}\nallowed_commonNames = \"gateway, s3 ,\"\n"
            ),
            &[],
        )
        .grpc_server_tls("grpc.vector")
        .unwrap()
        .unwrap();
        assert_eq!(tls.allowed_common_names, ["gateway", "s3"]);
        assert_eq!(tls.allowed_wildcard_domain, ".internal");
        for broken in [
            format!("[grpc.vector]\ncert = {cert:?}\nkey = {key:?}\n"),
            format!("[grpc]\nca = {ca:?}\n[grpc.vector]\nkey = {key:?}\n"),
            format!("[grpc]\nca = {ca:?}\n[grpc.vector]\nallowed_commonNames = \"s3\"\n"),
        ] {
            assert!(
                security(&broken, &[])
                    .grpc_server_tls("grpc.vector")
                    .is_err(),
                "{broken}"
            );
        }
    }

    #[test]
    fn the_file_is_searched_where_weed_looks() {
        let found = candidates(
            Some(Path::new("/etc/weed")),
            Some(Path::new("/cwd")),
            Some(Path::new("/home/u")),
        );
        assert_eq!(
            found,
            [
                "/etc/weed/security.toml",
                "/cwd/security.toml",
                "/home/u/.seaweedfs/security.toml",
                "/usr/local/etc/seaweedfs/security.toml",
                "/etc/seaweedfs/security.toml"
            ]
            .map(PathBuf::from)
        );
        let home = Some(Path::new("/home/u"));
        for (dir, want) in [
            ("~", "/home/u/security.toml"),
            ("~/cluster", "/home/u/cluster/security.toml"),
            ("~other/cluster", "~other/cluster/security.toml"),
            ("/srv/~/cluster", "/srv/~/cluster/security.toml"),
        ] {
            assert_eq!(
                candidates(Some(Path::new(dir)), None, home)[0],
                PathBuf::from(want),
                "{dir}"
            );
        }
    }

    #[test]
    fn load_takes_the_config_dir_before_the_usual_places() {
        let dir = tempfile::tempdir().unwrap();
        let in_config_dir = dir.path().join(FILE_NAME);
        std::fs::write(&in_config_dir, "").unwrap();
        let loaded = Security::load(None, Some(dir.path())).unwrap();
        assert_eq!(loaded.path(), Some(in_config_dir.as_path()));
        // An explicit file still wins.
        let explicit = dir.path().join("other.toml");
        std::fs::write(&explicit, "").unwrap();
        let loaded = Security::load(Some(&explicit), Some(dir.path())).unwrap();
        assert_eq!(loaded.path(), Some(explicit.as_path()));
    }
}
