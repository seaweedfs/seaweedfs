//! The FIPS 140-3 mode a SeaweedFS process runs in.
//!
//! One setting states it for a whole deployment: `[fips] mode` in
//! security.toml, or `WEED_FIPS_MODE`, which wins over the file as viper's
//! `AutomaticEnv` does for the Go processes. Go processes take their mode from
//! `GODEBUG=fips140` and refuse to start when it differs from the setting; the
//! Rust binaries take it from the setting alone, and under [`FipsMode::On`]
//! and [`FipsMode::Strict`] restrict TLS to what Go's `crypto/tls` allows in
//! FIPS mode (see [`crate::tls`]). This restricts behaviour only: the
//! cryptography underneath is still the non-validated aws-lc-rs build.

use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The security.toml setting, as `section.key`.
pub const SETTING: &str = "fips.mode";
/// The environment variable that overrides [`SETTING`].
pub const ENV: &str = "WEED_FIPS_MODE";

/// The modes `fips.mode` names. Unset reads as [`FipsMode::Off`], which is
/// how every Rust binary behaved before the setting existed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FipsMode {
    /// No FIPS restrictions; Go processes need `fips140=off`.
    #[default]
    Off,
    /// FIPS-mode TLS; Go processes need `GODEBUG=fips140=on`.
    On,
    /// FIPS-mode TLS; Go processes need `GODEBUG=fips140=only`, which also
    /// refuses non-approved algorithms outside TLS.
    Strict,
}

impl FipsMode {
    /// Parses a value of `fips.mode`: `off`, `on` or `strict`, or `None` for
    /// an empty value, which leaves the setting unset.
    pub fn parse(value: &str) -> Result<Option<Self>, FipsModeError> {
        match value {
            "" => Ok(None),
            "off" => Ok(Some(Self::Off)),
            "on" => Ok(Some(Self::On)),
            "strict" => Ok(Some(Self::Strict)),
            other => Err(FipsModeError::Unknown {
                source: SETTING.to_string(),
                value: other.to_string(),
            }),
        }
    }

    /// Whether TLS is restricted to the FIPS 140-3 lists.
    pub fn is_fips(self) -> bool {
        self != Self::Off
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
            Self::Strict => "strict",
        }
    }
}

impl fmt::Display for FipsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why the mode could not be resolved or recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FipsModeError {
    /// `source` holds a value that is not `off`, `on` or `strict`.
    Unknown { source: String, value: String },
    /// The process mode was already set to a different mode.
    AlreadySet { set: FipsMode, wanted: FipsMode },
}

impl fmt::Display for FipsModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { source, value } => {
                write!(f, "{source} is {value:?}; want off, on or strict")
            }
            Self::AlreadySet { set, wanted } => {
                write!(f, "FIPS mode is already {set}, cannot switch to {wanted}")
            }
        }
    }
}

impl std::error::Error for FipsModeError {}

/// The `fips.mode` value in security.toml text, `None` when the file does
/// not set it. Keys match case-insensitively, as viper's do; a non-string
/// value reads as its TOML text, as viper's `GetString` turns it into one
/// that then fails [`FipsMode::parse`].
pub fn setting_in(text: &str) -> Result<Option<String>, String> {
    let table = text.parse::<toml::Table>().map_err(|e| e.to_string())?;
    let lookup = |table: &toml::Table, key: &str| {
        table
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.clone())
    };
    let Some(section) = lookup(&table, "fips") else {
        return Ok(None);
    };
    let Some(section) = section.as_table() else {
        return Ok(None);
    };
    Ok(lookup(section, "mode").map(|value| match value {
        toml::Value::String(s) => s,
        other => other.to_string(),
    }))
}

/// A resolved `fips.mode`: the mode, and where it was set (`None` when
/// nothing set it and the mode is [`FipsMode::Off`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Setting {
    pub mode: FipsMode,
    pub source: Option<String>,
}

impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(f, "FIPS mode {} ({source})", self.mode),
            None => write!(f, "FIPS mode {} ({SETTING} unset)", self.mode),
        }
    }
}

/// Resolves the mode for a process that reads `security_toml` (`None` when it
/// found no file): `WEED_FIPS_MODE` when set and non-empty, else the file's
/// `fips.mode`, else [`FipsMode::Off`]. An unknown value is an error naming
/// where it came from. A file that cannot be read or parsed sets nothing, as
/// viper skips such a file for the Go processes; the binary's own
/// security.toml loader reports it.
pub fn resolve(
    security_toml: Option<&Path>,
    env: &dyn Fn(&str) -> Option<OsString>,
) -> Result<Setting, FipsModeError> {
    let from_env = env(ENV)
        .map(|v| v.to_string_lossy().into_owned())
        .filter(|v| !v.is_empty());
    let from_file = || {
        let path = security_toml?;
        let value = setting_in(&std::fs::read_to_string(path).ok()?).ok()??;
        Some((format!("{}: {SETTING}", path.display()), value))
    };
    let Some((source, value)) = from_env.map(|v| (ENV.to_string(), v)).or_else(from_file) else {
        return Ok(Setting {
            mode: FipsMode::Off,
            source: None,
        });
    };
    match FipsMode::parse(&value) {
        Ok(Some(mode)) => Ok(Setting {
            mode,
            source: Some(source),
        }),
        Ok(None) => Ok(Setting {
            mode: FipsMode::Off,
            source: None,
        }),
        Err(_) => Err(FipsModeError::Unknown { source, value }),
    }
}

/// The security.toml a process finds where weed looks, for binaries that do
/// not otherwise read the file.
pub fn find_security_toml(env: &dyn Fn(&str) -> Option<OsString>) -> Option<PathBuf> {
    crate::security_config::find(
        std::env::current_dir().ok().as_deref(),
        crate::security_config::home_dir(env).as_deref(),
    )
}

static PROCESS_MODE: OnceLock<FipsMode> = OnceLock::new();

/// Records the mode this process runs in. A second call with the same mode is
/// a no-op; one with another mode is an error, since providers and
/// configurations built under the first would not change.
pub fn set_process_mode(mode: FipsMode) -> Result<(), FipsModeError> {
    let set = *PROCESS_MODE.get_or_init(|| mode);
    if set != mode {
        return Err(FipsModeError::AlreadySet { set, wanted: mode });
    }
    Ok(())
}

/// The mode recorded by [`set_process_mode`], [`FipsMode::Off`] before.
pub fn process_mode() -> FipsMode {
    PROCESS_MODE.get().copied().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<OsString> {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| OsString::from(v))
        }
    }

    fn file(text: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("security.toml");
        std::fs::write(&path, text).unwrap();
        (dir, path)
    }

    #[test]
    fn parse_knows_three_modes_and_empty() {
        assert_eq!(FipsMode::parse(""), Ok(None));
        assert_eq!(FipsMode::parse("off"), Ok(Some(FipsMode::Off)));
        assert_eq!(FipsMode::parse("on"), Ok(Some(FipsMode::On)));
        assert_eq!(FipsMode::parse("strict"), Ok(Some(FipsMode::Strict)));
        assert!(FipsMode::parse("only").is_err());
        assert!(FipsMode::parse("On").is_err());
    }

    #[test]
    fn unset_everywhere_is_off() {
        let off = Setting {
            mode: FipsMode::Off,
            source: None,
        };
        assert_eq!(resolve(None, &env_of(&[])), Ok(off.clone()));
        let (_dir, path) = file("[grpc]\nca = \"\"\n");
        assert_eq!(resolve(Some(&path), &env_of(&[])), Ok(off.clone()));
        let (_dir, path) = file("[fips]\nmode = \"\"\n");
        assert_eq!(resolve(Some(&path), &env_of(&[])), Ok(off.clone()));
        assert_eq!(off.to_string(), "FIPS mode off (fips.mode unset)");
    }

    #[test]
    fn the_file_sets_the_mode() {
        let (_dir, path) = file("[fips]\nmode = \"strict\"\n");
        let setting = resolve(Some(&path), &env_of(&[])).unwrap();
        assert_eq!(setting.mode, FipsMode::Strict);
        assert_eq!(
            setting.to_string(),
            format!("FIPS mode strict ({}: fips.mode)", path.display())
        );
        let (_dir, path) = file("[FIPS]\nMode = 'on' # comment\n");
        assert_eq!(
            resolve(Some(&path), &env_of(&[])).unwrap().mode,
            FipsMode::On
        );
    }

    #[test]
    fn the_environment_wins_over_the_file() {
        let (_dir, path) = file("[fips]\nmode = \"off\"\n");
        let env = env_of(&[("WEED_FIPS_MODE", "on")]);
        let setting = resolve(Some(&path), &env).unwrap();
        assert_eq!(setting.mode, FipsMode::On);
        assert_eq!(setting.to_string(), "FIPS mode on (WEED_FIPS_MODE)");
        assert_eq!(resolve(None, &env).unwrap().mode, FipsMode::On);
    }

    #[test]
    fn an_empty_environment_variable_counts_as_unset() {
        let (_dir, path) = file("[fips]\nmode = \"on\"\n");
        let env = env_of(&[("WEED_FIPS_MODE", "")]);
        assert_eq!(resolve(Some(&path), &env).unwrap().mode, FipsMode::On);
    }

    #[test]
    fn an_unknown_value_is_refused_naming_its_source() {
        let err = resolve(None, &env_of(&[("WEED_FIPS_MODE", "yes")])).unwrap_err();
        assert_eq!(
            err.to_string(),
            "WEED_FIPS_MODE is \"yes\"; want off, on or strict"
        );
        let (_dir, path) = file("[fips]\nmode = true\n");
        let err = resolve(Some(&path), &env_of(&[])).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "{}: fips.mode is \"true\"; want off, on or strict",
                path.display()
            )
        );
    }

    #[test]
    fn a_file_that_does_not_parse_sets_nothing() {
        let (_dir, path) = file("[fips\nmode = \"on\"\n");
        assert_eq!(
            resolve(Some(&path), &env_of(&[])).unwrap().mode,
            FipsMode::Off
        );
        let env = env_of(&[("WEED_FIPS_MODE", "strict")]);
        assert_eq!(resolve(Some(&path), &env).unwrap().mode, FipsMode::Strict);
    }

    #[test]
    fn the_process_mode_is_set_once() {
        assert_eq!(set_process_mode(process_mode()), Ok(()));
        let other = if process_mode() == FipsMode::On {
            FipsMode::Off
        } else {
            FipsMode::On
        };
        assert!(set_process_mode(other).is_err());
    }
}
