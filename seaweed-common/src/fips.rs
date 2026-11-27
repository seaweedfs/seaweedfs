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
//!
//! Each binary reads `fips.mode` with the parser it uses for the rest of
//! security.toml and hands the value to [`resolve`], so the mode and the JWT
//! keys are either both read from a file or both missed.

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
    /// The security.toml at `path` exists but could not be read or parsed, so
    /// whether it sets `fips.mode` is unknown.
    File { path: PathBuf, reason: String },
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
            Self::File { path, reason } => write!(
                f,
                "{}: {reason}; cannot tell whether it sets {SETTING}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for FipsModeError {}

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

/// Resolves the mode of a process whose own security.toml loader read
/// `file_value` for `fips.mode` from `file` (`None` when there is no file or
/// it does not set the key): `WEED_FIPS_MODE` when set and non-empty, else
/// `file_value`, else [`FipsMode::Off`]. An unknown value is an error naming
/// where it came from.
pub fn resolve(
    file: Option<&Path>,
    file_value: Option<&str>,
    env: &dyn Fn(&str) -> Option<OsString>,
) -> Result<Setting, FipsModeError> {
    let from_env = env(ENV)
        .map(|v| v.to_string_lossy().into_owned())
        .filter(|v| !v.is_empty());
    let from_file = || {
        let source = match file {
            Some(path) => format!("{}: {SETTING}", path.display()),
            None => SETTING.to_string(),
        };
        file_value.map(|value| (source, value.to_string()))
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

    const FILE_PATH: &str = "/etc/seaweedfs/security.toml";

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
        assert_eq!(resolve(None, None, &env_of(&[])), Ok(off.clone()));
        assert_eq!(
            resolve(Some(Path::new(FILE_PATH)), None, &env_of(&[])),
            Ok(off.clone())
        );
        assert_eq!(
            resolve(Some(Path::new(FILE_PATH)), Some(""), &env_of(&[])),
            Ok(off.clone())
        );
        assert_eq!(off.to_string(), "FIPS mode off (fips.mode unset)");
    }

    #[test]
    fn the_file_sets_the_mode() {
        let setting = resolve(Some(Path::new(FILE_PATH)), Some("strict"), &env_of(&[])).unwrap();
        assert_eq!(setting.mode, FipsMode::Strict);
        assert_eq!(
            setting.to_string(),
            "FIPS mode strict (/etc/seaweedfs/security.toml: fips.mode)"
        );
    }

    #[test]
    fn the_environment_wins_over_the_file() {
        let env = env_of(&[("WEED_FIPS_MODE", "on")]);
        let setting = resolve(Some(Path::new(FILE_PATH)), Some("off"), &env).unwrap();
        assert_eq!(setting.mode, FipsMode::On);
        assert_eq!(setting.to_string(), "FIPS mode on (WEED_FIPS_MODE)");
        assert_eq!(resolve(None, None, &env).unwrap().mode, FipsMode::On);
        assert_eq!(
            resolve(Some(Path::new(FILE_PATH)), Some("[\"strict\"]"), &env)
                .unwrap()
                .mode,
            FipsMode::On
        );
    }

    #[test]
    fn an_empty_environment_variable_counts_as_unset() {
        let env = env_of(&[("WEED_FIPS_MODE", "")]);
        assert_eq!(
            resolve(Some(Path::new(FILE_PATH)), Some("on"), &env)
                .unwrap()
                .mode,
            FipsMode::On
        );
    }

    #[test]
    fn an_unknown_value_is_refused_naming_its_source() {
        let err = resolve(None, None, &env_of(&[("WEED_FIPS_MODE", "yes")])).unwrap_err();
        assert_eq!(
            err.to_string(),
            "WEED_FIPS_MODE is \"yes\"; want off, on or strict"
        );
        let err = resolve(
            Some(Path::new(FILE_PATH)),
            Some("[\"strict\"]"),
            &env_of(&[]),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "/etc/seaweedfs/security.toml: fips.mode is \"[\\\"strict\\\"]\"; want off, on or strict"
        );
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
