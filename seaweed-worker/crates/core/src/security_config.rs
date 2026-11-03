//! The admin-stream signing key, read from where the Go worker reads it.
//!
//! `weed worker` takes `jwt.filer_signing` from security.toml, with the
//! `WEED_JWT_FILER_SIGNING_*` environment variables overriding the file the
//! way viper does. A Rust worker started beside it, from the same image with
//! the same mounts, has to find the key in the same places or it connects
//! with no token and admin turns it away.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use seaweed_common::security_config as location;

/// `jwt.filer_signing` as the Go worker resolves it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilerSigning {
    pub key: String,
    /// Zero mints a token without an expiry, the Go default.
    pub expires_after_seconds: u64,
    /// Where the key came from, for the startup log.
    pub source: String,
}

/// The key from the process environment and the discovered security.toml, or
/// None when neither configures one.
pub fn filer_signing() -> Option<FilerSigning> {
    let env = |key: &str| std::env::var_os(key);
    let file = location::find(
        std::env::current_dir().ok().as_deref(),
        location::home_dir(&env).as_deref(),
    );
    filer_signing_from(&env, file.as_deref())
}

fn filer_signing_from(
    env: &dyn Fn(&str) -> Option<OsString>,
    file: Option<&Path>,
) -> Option<FilerSigning> {
    // Viper treats an empty environment value as unset.
    let env_string = |key: &str| {
        env(key)
            .and_then(|v| v.into_string().ok())
            .filter(|v| !v.is_empty())
    };
    let (file_key, file_expires) = file.map(read_file).unwrap_or_default();

    let (key, source) = match env_string("WEED_JWT_FILER_SIGNING_KEY") {
        Some(key) => (key, "WEED_JWT_FILER_SIGNING_KEY".to_string()),
        None => (file_key?, file.map(PathBuf::from)?.display().to_string()),
    };
    if key.is_empty() {
        return None;
    }
    let expires_after_seconds = env_string("WEED_JWT_FILER_SIGNING_EXPIRES_AFTER_SECONDS")
        .and_then(|v| v.parse().ok())
        .or(file_expires)
        .unwrap_or(0);
    Some(FilerSigning {
        key,
        expires_after_seconds,
        source,
    })
}

/// `[jwt.filer_signing]` key and expires_after_seconds from one file. A file
/// that cannot be read or parsed configures nothing, as it does for viper.
fn read_file(path: &Path) -> (Option<String>, Option<u64>) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return (None, None);
    };
    let Ok(table) = content.parse::<toml::Table>() else {
        return (None, None);
    };
    let section = table
        .get("jwt")
        .and_then(|jwt| jwt.get("filer_signing"))
        .and_then(|section| section.as_table());
    let Some(section) = section else {
        return (None, None);
    };
    let key = section
        .get("key")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let expires = section
        .get("expires_after_seconds")
        .and_then(|v| v.as_integer())
        .and_then(|v| u64::try_from(v).ok());
    (key, expires)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, OsString> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), OsString::from(v)))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    fn write(dir: &tempfile::TempDir, content: &str) -> PathBuf {
        let path = dir.path().join(location::SECURITY_CONFIG_FILE_NAME);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn reads_the_section_the_go_worker_reads() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write(
            &dir,
            r#"
[jwt.signing]
key = "volume-key"

[jwt.filer_signing]
key = "cluster-key"
expires_after_seconds = 300

[jwt.filer_signing.read]
key = "read-key"
"#,
        );
        let got = filer_signing_from(&env_of(&[]), Some(&path)).unwrap();
        assert_eq!(got.key, "cluster-key");
        assert_eq!(got.expires_after_seconds, 300);
        assert_eq!(got.source, path.display().to_string());
    }

    #[test]
    fn environment_overrides_the_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write(
            &dir,
            "[jwt.filer_signing]\nkey = \"file-key\"\nexpires_after_seconds = 5\n",
        );
        let env = env_of(&[
            ("WEED_JWT_FILER_SIGNING_KEY", "env-key"),
            ("WEED_JWT_FILER_SIGNING_EXPIRES_AFTER_SECONDS", "60"),
        ]);
        let got = filer_signing_from(&env, Some(&path)).unwrap();
        assert_eq!(got.key, "env-key");
        assert_eq!(got.expires_after_seconds, 60);
        assert_eq!(got.source, "WEED_JWT_FILER_SIGNING_KEY");
    }

    #[test]
    fn environment_key_alone_is_enough() {
        let got =
            filer_signing_from(&env_of(&[("WEED_JWT_FILER_SIGNING_KEY", "k")]), None).unwrap();
        assert_eq!(got.key, "k");
        assert_eq!(got.expires_after_seconds, 0);
    }

    #[test]
    fn empty_environment_values_fall_back_to_the_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write(
            &dir,
            "[jwt.filer_signing]\nkey = \"file-key\"\nexpires_after_seconds = 5\n",
        );
        let env = env_of(&[
            ("WEED_JWT_FILER_SIGNING_KEY", ""),
            ("WEED_JWT_FILER_SIGNING_EXPIRES_AFTER_SECONDS", ""),
        ]);
        let got = filer_signing_from(&env, Some(&path)).unwrap();
        assert_eq!(got.key, "file-key");
        assert_eq!(got.expires_after_seconds, 5);
        assert_eq!(got.source, path.display().to_string());
    }

    #[test]
    fn nothing_configured_is_none() {
        let dir = tempfile::TempDir::new().unwrap();
        let other_section = write(&dir, "[jwt.signing]\nkey = \"volume-key\"\n");
        assert_eq!(filer_signing_from(&env_of(&[]), Some(&other_section)), None);
        assert_eq!(filer_signing_from(&env_of(&[]), None), None);
        let empty = env_of(&[("WEED_JWT_FILER_SIGNING_KEY", "")]);
        assert_eq!(filer_signing_from(&empty, None), None);
    }
}
