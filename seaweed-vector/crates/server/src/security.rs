//! The cluster's `security.toml`, found and read the way weed processes do:
//! the file in `.`, `~/.seaweedfs`, `/usr/local/etc/seaweedfs` or
//! `/etc/seaweedfs`, with `WEED_*` environment variables over it.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

const FILE_NAME: &str = "security.toml";

/// Credentials for dialing a weed gRPC server under TLS, as
/// `security.LoadClientTLS` builds them: the server's certificate must chain
/// to `grpc.ca` and name the host dialed, and this client presents its own.
#[derive(Clone)]
pub struct GrpcTls {
    /// `grpc.ca`
    pub ca_pem: Vec<u8>,
    /// The component's `client_cert` and `client_key`, else its `cert` and
    /// `key`.
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    /// The name the server's certificate is checked against, when it is not
    /// the host in the server's address.
    pub server_name: Option<String>,
}

impl std::fmt::Debug for GrpcTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTls")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
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

fn candidates(current_dir: Option<&Path>, home: Option<&Path>) -> Vec<PathBuf> {
    let mut found = Vec::new();
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

// viper keys are case-insensitive
fn entry<'a>(table: &'a toml::Table, key: &str) -> Option<&'a toml::Value> {
    table.get(key).or_else(|| {
        table
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v)
    })
}

impl Security {
    /// Reads `path`, or the first `security.toml` where weed looks; without
    /// one, only the environment applies.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = match path {
            Some(path) => Some(path.to_path_buf()),
            None => {
                let home = std::env::var_os("HOME").map(PathBuf::from);
                candidates(std::env::current_dir().ok().as_deref(), home.as_deref())
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

    pub fn get_bool(&self, key: &str) -> bool {
        self.raw(key).is_some_and(|v| v.trim() == "true")
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

    /// weed checks a dialed server's identity against these on top of
    /// `grpc.ca`; the engine's master dial cannot, so it refuses to start
    /// rather than accept every master certificate the CA signed.
    fn refuse_server_allowlists(&self, keys: &[String]) -> Result<()> {
        if let Some(key) = keys.iter().find(|key| self.get(key).is_some()) {
            bail!(
                "{key} is set, but weed-vector does not check master certificates against it; \
                 pass a --security-toml without it to accept any master certificate under grpc.ca"
            );
        }
        Ok(())
    }

    /// The credentials `component` (such as `grpc.client`) dials servers with,
    /// following `security.LoadClientTLS`: a dedicated client certificate is
    /// preferred over the component's own, and once either pair is set
    /// `grpc.ca` is required. `None` keeps plaintext.
    pub fn grpc_client_tls(
        &self,
        component: &str,
        server_name: Option<String>,
    ) -> Result<Option<GrpcTls>> {
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
        self.refuse_server_allowlists(&[
            format!("{component}.allowed_server_commonNames"),
            "grpc.allowed_server_commonNames".into(),
            format!("{component}.allowed_server_wildcard_domain"),
            "grpc.allowed_server_wildcard_domain".into(),
        ])?;
        Ok(Some(GrpcTls {
            ca_pem: self.read("grpc.ca", &ca)?,
            cert_pem: self.read(&format!("{component} cert"), &cert)?,
            key_pem: self.read(&format!("{component} key"), &key)?,
            server_name,
        }))
    }

    /// The identity `component` (such as `grpc.vector`) serves with, following
    /// `security.LoadServerTLS`: its `cert` and `key`, with `grpc.ca` as the
    /// root every client certificate must chain to, and the component's
    /// `allowed_commonNames`. `None` keeps plaintext. The files are read by
    /// [`crate::tls::ServerTls`], which keeps re-reading them.
    pub fn grpc_server_tls(&self, component: &str) -> Result<Option<GrpcServerTls>> {
        let Some((cert_path, key_path)) =
            self.pair(&format!("{component}.cert"), &format!("{component}.key"))?
        else {
            return Ok(None);
        };
        let ca_path = self
            .get("grpc.ca")
            .with_context(|| format!("{component} cert and key are set but grpc.ca is not"))?;
        Ok(Some(GrpcServerTls {
            cert_path,
            key_path,
            ca_path,
            allowed_common_names: self
                .get(&format!("{component}.allowed_commonNames"))
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect(),
            allowed_wildcard_domain: self.get("grpc.allowed_wildcard_domain").unwrap_or_default(),
        }))
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
        assert!(security("", &[]).get("jwt.signing.read.key").is_none());
        assert!(
            security("[grpc]\nca = \"\"\n", &[])
                .get("grpc.ca")
                .is_none()
        );
    }

    #[test]
    fn client_credentials_follow_the_gateways_rules() {
        let (_dir, ca, cert, key, client) = files();
        assert!(
            security("", &[])
                .grpc_client_tls("grpc.client", None)
                .unwrap()
                .is_none()
        );

        let tls = security(
            &format!("[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"),
            &[],
        )
        .grpc_client_tls("grpc.client", Some("master.internal".into()))
        .unwrap()
        .unwrap();
        assert_eq!(
            (tls.ca_pem, tls.cert_pem, tls.key_pem),
            (b"CA".to_vec(), b"CERT".to_vec(), b"KEY".to_vec())
        );
        assert_eq!(tls.server_name.as_deref(), Some("master.internal"));

        // A dedicated client certificate is preferred, and the environment
        // can supply it.
        let tls = security(
            &format!("[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"),
            &[
                ("WEED_GRPC_CLIENT_CLIENT_CERT", &client),
                ("WEED_GRPC_CLIENT_CLIENT_KEY", &key),
            ],
        )
        .grpc_client_tls("grpc.client", None)
        .unwrap()
        .unwrap();
        assert_eq!(tls.cert_pem, b"CLIENT");

        for broken in [
            format!("[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"),
            format!("[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\n"),
            format!("[grpc]\nca = {ca:?}\n[grpc.client]\nclient_key = {key:?}\n"),
            format!(
                "[grpc]\nca = {ca:?}\nallowed_server_wildcard_domain = \".internal\"\n\
                 [grpc.client]\ncert = {cert:?}\nkey = {key:?}\n"
            ),
            format!(
                "[grpc]\nca = {ca:?}\n[grpc.client]\ncert = {cert:?}\nkey = {key:?}\n\
                 allowed_server_commonNames = \"master\"\n"
            ),
        ] {
            assert!(
                security(&broken, &[])
                    .grpc_client_tls("grpc.client", None)
                    .is_err(),
                "{broken}"
            );
        }
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
        let found = candidates(Some(Path::new("/cwd")), Some(Path::new("/home/u")));
        assert_eq!(
            found,
            [
                "/cwd/security.toml",
                "/home/u/.seaweedfs/security.toml",
                "/usr/local/etc/seaweedfs/security.toml",
                "/etc/seaweedfs/security.toml"
            ]
            .map(PathBuf::from)
        );
    }
}
