//! Version helpers aligned with Go's util/version package.

use std::sync::OnceLock;

#[cfg(feature = "5bytes")]
const SIZE_LIMIT: &str = "8000GB"; // Matches Go production builds (5BytesOffset)
#[cfg(not(feature = "5bytes"))]
const SIZE_LIMIT: &str = "30GB"; // Matches Go default build (!5BytesOffset)

pub fn size_limit() -> &'static str {
    SIZE_LIMIT
}

pub fn commit() -> &'static str {
    option_env!("SEAWEEDFS_COMMIT")
        .or(option_env!("GIT_COMMIT"))
        .or(option_env!("GIT_SHA"))
        .unwrap_or("")
}

pub fn version_number() -> &'static str {
    static VERSION_NUMBER: OnceLock<String> = OnceLock::new();
    VERSION_NUMBER
        .get_or_init(|| parse_go_version_number().unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()))
        .as_str()
}

pub fn version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| format!("{} {}", size_limit(), version_number()))
        .as_str()
}

pub fn full_version() -> &'static str {
    static FULL: OnceLock<String> = OnceLock::new();
    FULL.get_or_init(|| format!("{} {}", version(), commit())).as_str()
}

pub fn server_header() -> &'static str {
    static HEADER: OnceLock<String> = OnceLock::new();
    HEADER
        .get_or_init(|| format!("SeaweedFS Volume {}", version()))
        .as_str()
}

fn parse_go_version_number() -> Option<String> {
    let src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../weed/util/version/constants.go"));
    let mut major: Option<u32> = None;
    let mut minor: Option<u32> = None;
    for line in src.lines() {
        let l = line.trim();
        if l.starts_with("MAJOR_VERSION") {
            major = parse_int32_line(l);
        } else if l.starts_with("MINOR_VERSION") {
            minor = parse_int32_line(l);
        }
        if major.is_some() && minor.is_some() {
            break;
        }
    }
    match (major, minor) {
        (Some(maj), Some(min)) => Some(format!("{}.{}", maj, format!("{:02}", min))),
        _ => None,
    }
}

fn parse_int32_line(line: &str) -> Option<u32> {
    let start = line.find("int32(")? + "int32(".len();
    let rest = &line[start..];
    let end = rest.find(')')?;
    rest[..end].trim().parse::<u32>().ok()
}
