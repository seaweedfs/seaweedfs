//! Where a SeaweedFS process finds security.toml, the way `weed` does.
//!
//! Mirrors the search list in `weed/util/config.go`: the working directory,
//! `$HOME/.seaweedfs`, `/usr/local/etc/seaweedfs` and `/etc/seaweedfs`, first
//! existing file wins. The volume server and the workers each read the file
//! for different sections, but an operator mounts it in one place and expects
//! every binary to pick it up from there.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub const SECURITY_CONFIG_FILE_NAME: &str = "security.toml";

/// The locations searched for security.toml, in order.
pub fn candidates(current_dir: Option<&Path>, home_dir: Option<&Path>) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(dir) = current_dir {
        candidates.push(dir.join(SECURITY_CONFIG_FILE_NAME));
    }
    if let Some(home) = home_dir {
        candidates.push(home.join(".seaweedfs").join(SECURITY_CONFIG_FILE_NAME));
    }
    candidates.push(PathBuf::from("/usr/local/etc/seaweedfs").join(SECURITY_CONFIG_FILE_NAME));
    candidates.push(PathBuf::from("/etc/seaweedfs").join(SECURITY_CONFIG_FILE_NAME));
    candidates
}

/// The first candidate that is a file.
pub fn find(current_dir: Option<&Path>, home_dir: Option<&Path>) -> Option<PathBuf> {
    candidates(current_dir, home_dir)
        .into_iter()
        .find(|candidate| candidate.is_file())
}

/// `$HOME`, or `%USERPROFILE%` where that is what the platform sets, read
/// through an injected lookup so tests need not touch the process environment.
pub fn home_dir(env: &dyn Fn(&str) -> Option<OsString>) -> Option<PathBuf> {
    env("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env("USERPROFILE")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_order_matches_go() {
        let found = candidates(Some(Path::new("/work")), Some(Path::new("/home/u")));
        assert_eq!(
            found,
            vec![
                PathBuf::from("/work/security.toml"),
                PathBuf::from("/home/u/.seaweedfs/security.toml"),
                PathBuf::from("/usr/local/etc/seaweedfs/security.toml"),
                PathBuf::from("/etc/seaweedfs/security.toml"),
            ]
        );
    }

    #[test]
    fn home_falls_back_to_userprofile() {
        let env = |key: &str| match key {
            "USERPROFILE" => Some(OsString::from("/users/u")),
            _ => None,
        };
        assert_eq!(home_dir(&env), Some(PathBuf::from("/users/u")));
        let empty_home = |key: &str| match key {
            "HOME" => Some(OsString::new()),
            _ => None,
        };
        assert_eq!(home_dir(&empty_home), None);
    }
}
