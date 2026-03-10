use clap::Parser;
use std::net::UdpSocket;

/// SeaweedFS Volume Server (Rust implementation)
///
/// Start a volume server to provide storage spaces.
#[derive(Parser, Debug)]
#[command(name = "weed-volume", version, about)]
pub struct Cli {
    /// HTTP listen port
    #[arg(long = "port", default_value_t = 8080)]
    pub port: u16,

    /// gRPC listen port. If 0, defaults to port + 10000.
    #[arg(long = "port.grpc", default_value_t = 0)]
    pub port_grpc: u16,

    /// Port opened to public. If 0, defaults to same as --port.
    #[arg(long = "port.public", default_value_t = 0)]
    pub port_public: u16,

    /// IP or server name, also used as identifier.
    /// If empty, auto-detected.
    #[arg(long = "ip", default_value = "")]
    pub ip: String,

    /// Volume server ID. If empty, defaults to ip:port.
    #[arg(long = "id", default_value = "")]
    pub id: String,

    /// Publicly accessible address.
    #[arg(long = "publicUrl", default_value = "")]
    pub public_url: String,

    /// IP address to bind to. If empty, defaults to same as --ip.
    #[arg(long = "ip.bind", default_value = "")]
    pub bind_ip: String,

    /// Comma-separated master server addresses.
    #[arg(long = "master", default_value = "localhost:9333")]
    pub master: String,

    /// Comma-separated master servers (deprecated, use --master instead).
    #[arg(long = "mserver", default_value = "")]
    pub mserver: String,

    /// Number of seconds between stop sending heartbeats and stopping the volume server.
    #[arg(long = "preStopSeconds", default_value_t = 10)]
    pub pre_stop_seconds: u32,

    /// Connection idle seconds.
    #[arg(long = "idleTimeout", default_value_t = 30)]
    pub idle_timeout: u32,

    /// Current volume server's data center name.
    #[arg(long = "dataCenter", default_value = "")]
    pub data_center: String,

    /// Current volume server's rack name.
    #[arg(long = "rack", default_value = "")]
    pub rack: String,

    /// Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.
    #[arg(long = "index", default_value = "memory")]
    pub index: String,

    /// [hdd|ssd|<tag>] hard drive or solid state drive or any tag.
    #[arg(long = "disk", default_value = "")]
    pub disk: String,

    /// Comma-separated tag groups per data dir; each group uses ':' (e.g. fast:ssd,archive).
    #[arg(long = "tags", default_value = "")]
    pub tags: String,

    /// Adjust jpg orientation when uploading.
    #[arg(long = "images.fix.orientation", default_value_t = false)]
    pub fix_jpg_orientation: bool,

    /// [local|proxy|redirect] how to deal with non-local volume.
    #[arg(long = "readMode", default_value = "proxy")]
    pub read_mode: String,

    /// CPU profile output file.
    #[arg(long = "cpuprofile", default_value = "")]
    pub cpu_profile: String,

    /// Memory profile output file.
    #[arg(long = "memprofile", default_value = "")]
    pub mem_profile: String,

    /// Limit background compaction or copying speed in mega bytes per second.
    #[arg(long = "compactionMBps", default_value_t = 0)]
    pub compaction_mb_per_second: u32,

    /// Limit maintenance (replication/balance) IO rate in MB/s. 0 means no limit.
    #[arg(long = "maintenanceMBps", default_value_t = 0)]
    pub maintenance_mb_per_second: u32,

    /// Limit file size to avoid out of memory.
    #[arg(long = "fileSizeLimitMB", default_value_t = 256)]
    pub file_size_limit_mb: u32,

    /// Limit total concurrent upload size in MB, 0 means unlimited.
    #[arg(long = "concurrentUploadLimitMB", default_value_t = 0)]
    pub concurrent_upload_limit_mb: u32,

    /// Limit total concurrent download size in MB, 0 means unlimited.
    #[arg(long = "concurrentDownloadLimitMB", default_value_t = 0)]
    pub concurrent_download_limit_mb: u32,

    /// Enable pprof-equivalent HTTP handlers. Precludes --memprofile and --cpuprofile.
    #[arg(long = "pprof", default_value_t = false)]
    pub pprof: bool,

    /// Prometheus metrics listen port.
    #[arg(long = "metricsPort", default_value_t = 0)]
    pub metrics_port: u16,

    /// Metrics listen IP. If empty, defaults to same as --ip.bind.
    #[arg(long = "metricsIp", default_value = "")]
    pub metrics_ip: String,

    /// Directories to store data files. dir[,dir]...
    #[arg(long = "dir", default_value = "/tmp")]
    pub dir: String,

    /// Directory to store .idx files.
    #[arg(long = "dir.idx", default_value = "")]
    pub dir_idx: String,

    /// Maximum numbers of volumes, count[,count]...
    /// If set to zero, the limit will be auto configured as free disk space divided by volume size.
    #[arg(long = "max", default_value = "8")]
    pub max: String,

    /// Comma separated IP addresses having write permission. No limit if empty.
    #[arg(long = "whiteList", default_value = "")]
    pub white_list: String,

    /// Minimum free disk space (default to 1%). Low disk space will mark all volumes as ReadOnly.
    /// Deprecated: use --minFreeSpace instead.
    #[arg(long = "minFreeSpacePercent", default_value = "1")]
    pub min_free_space_percent: String,

    /// Min free disk space (value<=100 as percentage like 1, other as human readable bytes, like 10GiB).
    /// Low disk space will mark all volumes as ReadOnly.
    #[arg(long = "minFreeSpace", default_value = "")]
    pub min_free_space: String,

    /// Inflight upload data wait timeout of volume servers.
    #[arg(long = "inflightUploadDataTimeout", default_value = "60s")]
    pub inflight_upload_data_timeout: String,

    /// Inflight download data wait timeout of volume servers.
    #[arg(long = "inflightDownloadDataTimeout", default_value = "60s")]
    pub inflight_download_data_timeout: String,

    /// <experimental> if true, prevents slow reads from blocking other requests,
    /// but large file read P99 latency will increase.
    #[arg(long = "hasSlowRead", default_value_t = true)]
    pub has_slow_read: bool,

    /// <experimental> larger values can optimize query performance but will increase memory usage.
    /// Use with hasSlowRead normally.
    #[arg(long = "readBufferSizeMB", default_value_t = 4)]
    pub read_buffer_size_mb: u32,

    /// Alive time for leveldb (default to 0). If leveldb of volume is not accessed in
    /// ldbTimeout hours, it will be offloaded to reduce opened files and memory consumption.
    #[arg(long = "index.leveldbTimeout", default_value_t = 0)]
    pub ldb_timeout: i64,

    /// Serves runtime profiling data on the port specified by --debug.port.
    #[arg(long = "debug", default_value_t = false)]
    pub debug: bool,

    /// HTTP port for debugging.
    #[arg(long = "debug.port", default_value_t = 6060)]
    pub debug_port: u16,

    /// Path to security.toml configuration file for JWT signing keys.
    #[arg(long = "securityFile", default_value = "")]
    pub security_file: String,

    /// A file of command line options, each line in optionName=optionValue format.
    #[arg(long = "options", default_value = "")]
    pub options: String,
}

/// Resolved configuration after applying defaults and validation.
#[derive(Debug)]
pub struct VolumeServerConfig {
    pub port: u16,
    pub grpc_port: u16,
    pub public_port: u16,
    pub ip: String,
    pub bind_ip: String,
    pub public_url: String,
    pub id: String,
    pub masters: Vec<String>,
    pub pre_stop_seconds: u32,
    pub idle_timeout: u32,
    pub data_center: String,
    pub rack: String,
    pub index_type: NeedleMapKind,
    pub disk_type: String,
    pub folders: Vec<String>,
    pub folder_max_limits: Vec<i32>,
    pub folder_tags: Vec<Vec<String>>,
    pub min_free_spaces: Vec<MinFreeSpace>,
    pub disk_types: Vec<String>,
    pub idx_folder: String,
    pub white_list: Vec<String>,
    pub fix_jpg_orientation: bool,
    pub read_mode: ReadMode,
    pub compaction_byte_per_second: i64,
    pub maintenance_byte_per_second: i64,
    pub file_size_limit_bytes: i64,
    pub concurrent_upload_limit: i64,
    pub concurrent_download_limit: i64,
    pub inflight_upload_data_timeout: std::time::Duration,
    pub inflight_download_data_timeout: std::time::Duration,
    pub has_slow_read: bool,
    pub read_buffer_size_mb: u32,
    pub ldb_timeout: i64,
    pub pprof: bool,
    pub metrics_port: u16,
    pub metrics_ip: String,
    pub debug: bool,
    pub debug_port: u16,
    pub ui_enabled: bool,
    pub jwt_signing_key: Vec<u8>,
    pub jwt_signing_expires_seconds: i64,
    pub jwt_read_signing_key: Vec<u8>,
    pub jwt_read_signing_expires_seconds: i64,
    pub https_cert_file: String,
    pub https_key_file: String,
    pub https_ca_file: String,
    pub grpc_cert_file: String,
    pub grpc_key_file: String,
    pub grpc_ca_file: String,
    /// Enable batched write queue for improved throughput under load.
    pub enable_write_queue: bool,
    /// Path to security.toml — stored for SIGHUP reload.
    pub security_file: String,
}

pub use crate::storage::needle_map::NeedleMapKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Local,
    Proxy,
    Redirect,
}

#[derive(Debug, Clone)]
pub enum MinFreeSpace {
    Percent(f64),
    Bytes(u64),
}

/// Convert single-dash long options to double-dash for clap compatibility.
/// Go's `flag` package uses `-port`, clap expects `--port`.
/// This allows both `-port 8080` and `--port 8080` to work.
fn normalize_args_vec(args: Vec<String>) -> Vec<String> {
    let mut args = args;
    // Skip args[0] (binary name).
    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        // Stop processing after "--"
        if arg == "--" {
            break;
        }
        // Already double-dash or not a flag: leave as-is
        if arg.starts_with("--") || !arg.starts_with('-') {
            i += 1;
            continue;
        }
        // Single char flags like -h, -V: leave as-is
        let without_dash = &arg[1..];
        // Check if it's a single-dash long option: more than 1 char and not a negative number
        if without_dash.len() > 1 && !without_dash.starts_with(|c: char| c.is_ascii_digit()) {
            // Handle -key=value format
            if let Some(eq_pos) = without_dash.find('=') {
                let key = &without_dash[..eq_pos];
                if key.len() > 1 {
                    args[i] = format!("--{}", without_dash);
                }
            } else {
                args[i] = format!("-{}", arg);
            }
        }
        i += 1;
    }
    args
}

/// Parse CLI arguments and resolve all defaults — mirroring Go's `runVolume()` + `startVolumeServer()`.
///
/// Supports `-options <file>` to load defaults from a file (same format as Go's fla9).
/// CLI arguments take precedence over file values.
pub fn parse_cli() -> VolumeServerConfig {
    let args: Vec<String> = std::env::args().collect();
    let normalized = normalize_args_vec(args);
    let merged = merge_options_file(normalized);
    let cli = Cli::parse_from(merged);
    resolve_config(cli)
}

/// Find `-options`/`--options` in args, parse the referenced file, and inject
/// file-based defaults for any flags not already set on the command line.
///
/// File format (matching Go's fla9.ParseFile):
///   - One option per line: `key=value`, `key value`, or `key:value`
///   - Lines starting with `#` are comments; blank lines are ignored
///   - Leading `-` on key names is stripped
///   - CLI arguments take precedence over file values
fn merge_options_file(args: Vec<String>) -> Vec<String> {
    // Find the options file path from the args
    let options_path = find_options_arg(&args);
    if options_path.is_empty() {
        return args;
    }

    let content = match std::fs::read_to_string(&options_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("WARNING: could not read options file {}: {}", options_path, e);
            return args;
        }
    };

    // Collect which flags are already explicitly set on the command line.
    let mut cli_flags: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut i = 1; // skip binary name
    while i < args.len() {
        let arg = &args[i];
        if arg == "--" {
            break;
        }
        if arg.starts_with("--") {
            let key = if let Some(eq) = arg.find('=') {
                arg[2..eq].to_string()
            } else {
                arg[2..].to_string()
            };
            cli_flags.insert(key);
        } else if arg.starts_with('-') && arg.len() > 2 {
            // Single-dash long option (already normalized to -- at this point,
            // but handle both for safety)
            let without_dash = &arg[1..];
            let key = if let Some(eq) = without_dash.find('=') {
                without_dash[..eq].to_string()
            } else {
                without_dash.to_string()
            };
            cli_flags.insert(key);
        }
        i += 1;
    }

    // Parse file and append missing options
    let mut extra_args: Vec<String> = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Split on first `=`, ` `, or `:`
        let (name, value) = if let Some(pos) = trimmed.find(|c: char| c == '=' || c == ' ' || c == ':') {
            (trimmed[..pos].trim().to_string(), trimmed[pos + 1..].trim().to_string())
        } else {
            (trimmed.to_string(), String::new())
        };

        // Strip leading dashes from name
        let name = name.trim_start_matches('-').to_string();
        if name.is_empty() || name == "options" {
            continue;
        }

        // Skip if already set on CLI
        if cli_flags.contains(&name) {
            continue;
        }

        extra_args.push(format!("--{}", name));
        if !value.is_empty() {
            extra_args.push(value);
        }
    }

    let mut merged = args;
    merged.extend(extra_args);
    merged
}

/// Extract the options file path from args (looks for --options or -options).
fn find_options_arg(args: &[String]) -> String {
    for i in 1..args.len() {
        if args[i] == "--options" || args[i] == "-options" {
            if i + 1 < args.len() {
                return args[i + 1].clone();
            }
        }
        if let Some(rest) = args[i].strip_prefix("--options=") {
            return rest.to_string();
        }
        if let Some(rest) = args[i].strip_prefix("-options=") {
            return rest.to_string();
        }
    }
    String::new()
}

/// Parse a duration string like "60s", "5m", "1h" into a std::time::Duration.
fn parse_duration(s: &str) -> std::time::Duration {
    let s = s.trim();
    if s.is_empty() {
        return std::time::Duration::from_secs(60);
    }
    if let Some(secs) = s.strip_suffix('s') {
        if let Ok(v) = secs.parse::<u64>() {
            return std::time::Duration::from_secs(v);
        }
    }
    if let Some(mins) = s.strip_suffix('m') {
        if let Ok(v) = mins.parse::<u64>() {
            return std::time::Duration::from_secs(v * 60);
        }
    }
    if let Some(hours) = s.strip_suffix('h') {
        if let Ok(v) = hours.parse::<u64>() {
            return std::time::Duration::from_secs(v * 3600);
        }
    }
    // Fallback: try parsing as raw seconds
    if let Ok(v) = s.parse::<u64>() {
        return std::time::Duration::from_secs(v);
    }
    std::time::Duration::from_secs(60)
}

/// Parse minFreeSpace / minFreeSpacePercent into MinFreeSpace values.
/// Mirrors Go's `util.MustParseMinFreeSpace()`.
fn parse_min_free_spaces(min_free_space: &str, min_free_space_percent: &str) -> Vec<MinFreeSpace> {
    // If --minFreeSpace is provided, use it (takes precedence).
    let source = if !min_free_space.is_empty() {
        min_free_space
    } else {
        min_free_space_percent
    };

    source
        .split(',')
        .map(|s| {
            let s = s.trim();
            // Try parsing as a percentage (value <= 100)
            if let Ok(v) = s.parse::<f64>() {
                if v <= 100.0 {
                    return MinFreeSpace::Percent(v);
                }
                // Treat as bytes if > 100
                return MinFreeSpace::Bytes(v as u64);
            }
            // Try parsing human-readable bytes: e.g. "10GiB", "500MiB", "1TiB"
            let s_upper = s.to_uppercase();
            if let Some(rest) = s_upper.strip_suffix("TIB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64);
                }
            }
            if let Some(rest) = s_upper.strip_suffix("GIB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1024.0 * 1024.0 * 1024.0) as u64);
                }
            }
            if let Some(rest) = s_upper.strip_suffix("MIB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1024.0 * 1024.0) as u64);
                }
            }
            if let Some(rest) = s_upper.strip_suffix("KIB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1024.0) as u64);
                }
            }
            if let Some(rest) = s_upper.strip_suffix("TB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1_000_000_000_000.0) as u64);
                }
            }
            if let Some(rest) = s_upper.strip_suffix("GB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1_000_000_000.0) as u64);
                }
            }
            if let Some(rest) = s_upper.strip_suffix("MB") {
                if let Ok(v) = rest.trim().parse::<f64>() {
                    return MinFreeSpace::Bytes((v * 1_000_000.0) as u64);
                }
            }
            // Default: 1%
            MinFreeSpace::Percent(1.0)
        })
        .collect()
}

/// Parse comma-separated tag groups like "fast:ssd,archive" into per-folder tag vectors.
/// Mirrors Go's `parseVolumeTags()`.
fn parse_volume_tags(tags_arg: &str, folder_count: usize) -> Vec<Vec<String>> {
    if folder_count == 0 {
        return vec![];
    }
    let tags_arg = tags_arg.trim();
    let tag_entries: Vec<&str> = if tags_arg.is_empty() {
        vec![]
    } else {
        tags_arg.split(',').collect()
    };

    let mut folder_tags: Vec<Vec<String>> = vec![vec![]; folder_count];

    if tag_entries.len() == 1 && !tag_entries[0].is_empty() {
        // Single entry: replicate to all folders
        let normalized: Vec<String> = tag_entries[0]
            .split(':')
            .map(|t| t.trim().to_lowercase())
            .filter(|t| !t.is_empty())
            .collect();
        for tags in folder_tags.iter_mut() {
            *tags = normalized.clone();
        }
    } else {
        for (i, tags) in folder_tags.iter_mut().enumerate() {
            if i < tag_entries.len() {
                *tags = tag_entries[i]
                    .split(':')
                    .map(|t| t.trim().to_lowercase())
                    .filter(|t| !t.is_empty())
                    .collect();
            }
        }
    }

    folder_tags
}

fn resolve_config(cli: Cli) -> VolumeServerConfig {
    // Backward compatibility: --mserver overrides --master
    let master_string = if !cli.mserver.is_empty() {
        &cli.mserver
    } else {
        &cli.master
    };
    let masters: Vec<String> = master_string
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Parse folders
    let folders: Vec<String> = cli
        .dir
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let folder_count = folders.len();

    // Parse max volume counts
    let mut folder_max_limits: Vec<i32> = cli
        .max
        .split(',')
        .map(|s| {
            s.trim().parse::<i32>().unwrap_or_else(|_| {
                panic!("The max specified in --max is not a valid number: {}", s)
            })
        })
        .collect();
    // Replicate single value to all folders
    if folder_max_limits.len() == 1 && folder_count > 1 {
        let v = folder_max_limits[0];
        folder_max_limits.resize(folder_count, v);
    }
    if folders.len() != folder_max_limits.len() {
        panic!(
            "{} directories by --dir, but only {} max is set by --max",
            folders.len(),
            folder_max_limits.len()
        );
    }

    // Parse min free spaces
    let mut min_free_spaces =
        parse_min_free_spaces(&cli.min_free_space, &cli.min_free_space_percent);
    if min_free_spaces.len() == 1 && folder_count > 1 {
        let v = min_free_spaces[0].clone();
        min_free_spaces.resize(folder_count, v);
    }
    if folders.len() != min_free_spaces.len() {
        panic!(
            "{} directories by --dir, but only {} minFreeSpace values",
            folders.len(),
            min_free_spaces.len()
        );
    }

    // Parse disk types
    let mut disk_types: Vec<String> = cli.disk.split(',').map(|s| s.trim().to_string()).collect();
    if disk_types.len() == 1 && folder_count > 1 {
        let v = disk_types[0].clone();
        disk_types.resize(folder_count, v);
    }
    if folders.len() != disk_types.len() {
        panic!(
            "{} directories by --dir, but only {} disk types by --disk",
            folders.len(),
            disk_types.len()
        );
    }

    // Parse tags
    let folder_tags = parse_volume_tags(&cli.tags, folder_count);

    // Resolve IP
    let ip = if cli.ip.is_empty() {
        detect_host_address()
    } else {
        cli.ip
    };

    // Resolve bind IP
    let bind_ip = if cli.bind_ip.is_empty() {
        ip.clone()
    } else {
        cli.bind_ip
    };

    // Resolve public port
    let public_port = if cli.port_public == 0 {
        cli.port
    } else {
        cli.port_public
    };

    // Resolve gRPC port
    let grpc_port = if cli.port_grpc == 0 {
        10000 + cli.port
    } else {
        cli.port_grpc
    };

    // Resolve public URL
    let public_url = if cli.public_url.is_empty() {
        format!("{}:{}", ip, public_port)
    } else {
        cli.public_url
    };

    // Resolve volume server ID
    let id = if cli.id.is_empty() {
        format!("{}:{}", ip, cli.port)
    } else {
        cli.id
    };

    // Resolve metrics IP
    let metrics_ip = if !cli.metrics_ip.is_empty() {
        cli.metrics_ip
    } else if !bind_ip.is_empty() {
        bind_ip.clone()
    } else {
        ip.clone()
    };

    // Parse index type
    let index_type = match cli.index.as_str() {
        "memory" => NeedleMapKind::InMemory,
        "leveldb" => NeedleMapKind::LevelDb,
        "leveldbMedium" => NeedleMapKind::LevelDbMedium,
        "leveldbLarge" => NeedleMapKind::LevelDbLarge,
        other => panic!(
            "Unknown index type: {}. Use memory|leveldb|leveldbMedium|leveldbLarge",
            other
        ),
    };

    // Parse read mode
    let read_mode = match cli.read_mode.as_str() {
        "local" => ReadMode::Local,
        "proxy" => ReadMode::Proxy,
        "redirect" => ReadMode::Redirect,
        other => panic!("Unknown readMode: {}. Use local|proxy|redirect", other),
    };

    // Parse security config from TOML file
    let sec = parse_security_config(&cli.security_file);

    // Parse whitelist: merge CLI --whiteList with guard.white_list from security.toml
    let mut white_list: Vec<String> = cli
        .white_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    white_list.extend(sec.guard_white_list.iter().cloned());

    // Parse durations
    let inflight_upload_data_timeout = parse_duration(&cli.inflight_upload_data_timeout);
    let inflight_download_data_timeout = parse_duration(&cli.inflight_download_data_timeout);

    VolumeServerConfig {
        port: cli.port,
        grpc_port,
        public_port,
        ip,
        bind_ip,
        public_url,
        id,
        masters,
        pre_stop_seconds: cli.pre_stop_seconds,
        idle_timeout: cli.idle_timeout,
        data_center: cli.data_center,
        rack: cli.rack,
        index_type,
        disk_type: cli.disk,
        folders,
        folder_max_limits,
        folder_tags,
        min_free_spaces,
        disk_types,
        idx_folder: cli.dir_idx,
        white_list,
        fix_jpg_orientation: cli.fix_jpg_orientation,
        read_mode,
        compaction_byte_per_second: cli.compaction_mb_per_second as i64 * 1024 * 1024,
        maintenance_byte_per_second: cli.maintenance_mb_per_second as i64 * 1024 * 1024,
        file_size_limit_bytes: cli.file_size_limit_mb as i64 * 1024 * 1024,
        concurrent_upload_limit: cli.concurrent_upload_limit_mb as i64 * 1024 * 1024,
        concurrent_download_limit: cli.concurrent_download_limit_mb as i64 * 1024 * 1024,
        inflight_upload_data_timeout,
        inflight_download_data_timeout,
        has_slow_read: cli.has_slow_read,
        read_buffer_size_mb: cli.read_buffer_size_mb,
        ldb_timeout: cli.ldb_timeout,
        pprof: cli.pprof,
        metrics_port: cli.metrics_port,
        metrics_ip,
        debug: cli.debug,
        debug_port: cli.debug_port,
        ui_enabled: sec.jwt_signing_key.is_empty() || sec.access_ui,
        jwt_signing_key: sec.jwt_signing_key,
        jwt_signing_expires_seconds: sec.jwt_signing_expires,
        jwt_read_signing_key: sec.jwt_read_signing_key,
        jwt_read_signing_expires_seconds: sec.jwt_read_signing_expires,
        https_cert_file: sec.https_cert_file,
        https_key_file: sec.https_key_file,
        https_ca_file: sec.https_ca_file,
        grpc_cert_file: sec.grpc_cert_file,
        grpc_key_file: sec.grpc_key_file,
        grpc_ca_file: sec.grpc_ca_file,
        enable_write_queue: std::env::var("SEAWEED_WRITE_QUEUE")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false),
        security_file: cli.security_file,
    }
}

/// Parsed security configuration from security.toml.
#[derive(Debug, Default)]
pub struct SecurityConfig {
    pub jwt_signing_key: Vec<u8>,
    pub jwt_signing_expires: i64,
    pub jwt_read_signing_key: Vec<u8>,
    pub jwt_read_signing_expires: i64,
    pub https_cert_file: String,
    pub https_key_file: String,
    pub https_ca_file: String,
    pub grpc_cert_file: String,
    pub grpc_key_file: String,
    pub grpc_ca_file: String,
    pub access_ui: bool,
    /// IPs from [guard] white_list in security.toml
    pub guard_white_list: Vec<String>,
}

/// Parse a security.toml file to extract JWT signing keys and TLS configuration.
/// Format:
/// ```toml
/// [jwt.signing]
/// key = "secret"
/// expires_after_seconds = 60
///
/// [jwt.signing.read]
/// key = "read-secret"
/// expires_after_seconds = 60
///
/// [https.volume]
/// cert = "/path/to/cert.pem"
/// key = "/path/to/key.pem"
///
/// [grpc.volume]
/// cert = "/path/to/cert.pem"
/// key = "/path/to/key.pem"
/// ```
pub fn parse_security_config(path: &str) -> SecurityConfig {
    if path.is_empty() {
        return SecurityConfig::default();
    }
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return SecurityConfig::default(),
    };

    let mut cfg = SecurityConfig::default();

    #[derive(PartialEq)]
    enum Section {
        None,
        JwtSigning,
        JwtSigningRead,
        HttpsVolume,
        GrpcVolume,
        Guard,
        Access,
    }

    let mut section = Section::None;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if trimmed == "[jwt.signing.read]" {
            section = Section::JwtSigningRead;
            continue;
        }
        if trimmed == "[jwt.signing]" {
            section = Section::JwtSigning;
            continue;
        }
        if trimmed == "[https.volume]" {
            section = Section::HttpsVolume;
            continue;
        }
        if trimmed == "[grpc.volume]" {
            section = Section::GrpcVolume;
            continue;
        }
        if trimmed == "[guard]" {
            section = Section::Guard;
            continue;
        }
        if trimmed == "[access]" {
            section = Section::Access;
            continue;
        }
        if trimmed.starts_with('[') {
            section = Section::None;
            continue;
        }

        if let Some((key, value)) = trimmed.split_once('=') {
            let key = key.trim();
            let value = value.trim().trim_matches('"');
            match section {
                Section::JwtSigningRead => match key {
                    "key" => cfg.jwt_read_signing_key = value.as_bytes().to_vec(),
                    "expires_after_seconds" => {
                        cfg.jwt_read_signing_expires = value.parse().unwrap_or(0)
                    }
                    _ => {}
                },
                Section::JwtSigning => match key {
                    "key" => cfg.jwt_signing_key = value.as_bytes().to_vec(),
                    "expires_after_seconds" => cfg.jwt_signing_expires = value.parse().unwrap_or(0),
                    _ => {}
                },
                Section::HttpsVolume => match key {
                    "cert" => cfg.https_cert_file = value.to_string(),
                    "key" => cfg.https_key_file = value.to_string(),
                    "ca" => cfg.https_ca_file = value.to_string(),
                    _ => {}
                },
                Section::GrpcVolume => match key {
                    "cert" => cfg.grpc_cert_file = value.to_string(),
                    "key" => cfg.grpc_key_file = value.to_string(),
                    "ca" => cfg.grpc_ca_file = value.to_string(),
                    _ => {}
                },
                Section::Guard => match key {
                    "white_list" => {
                        cfg.guard_white_list = value
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    _ => {}
                },
                Section::Access => match key {
                    "ui" => cfg.access_ui = value.parse().unwrap_or(false),
                    _ => {}
                },
                Section::None => {}
            }
        }
    }

    cfg
}

/// Detect the host's IP address.
/// Mirrors Go's `util.DetectedHostAddress()`.
fn detect_host_address() -> String {
    // Connect to a remote address to determine the local outbound IP
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                return addr.ip().to_string();
            }
        }
    }
    "localhost".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("60s"), std::time::Duration::from_secs(60));
        assert_eq!(parse_duration("5m"), std::time::Duration::from_secs(300));
        assert_eq!(parse_duration("1h"), std::time::Duration::from_secs(3600));
        assert_eq!(parse_duration("30"), std::time::Duration::from_secs(30));
        assert_eq!(parse_duration(""), std::time::Duration::from_secs(60));
    }

    #[test]
    fn test_parse_min_free_spaces_percent() {
        let result = parse_min_free_spaces("", "1");
        assert_eq!(result.len(), 1);
        match &result[0] {
            MinFreeSpace::Percent(v) => assert!((v - 1.0).abs() < f64::EPSILON),
            _ => panic!("Expected Percent"),
        }
    }

    #[test]
    fn test_parse_min_free_spaces_bytes() {
        let result = parse_min_free_spaces("10GiB", "");
        assert_eq!(result.len(), 1);
        match &result[0] {
            MinFreeSpace::Bytes(v) => assert_eq!(*v, 10 * 1024 * 1024 * 1024),
            _ => panic!("Expected Bytes"),
        }
    }

    #[test]
    fn test_parse_volume_tags_single() {
        let tags = parse_volume_tags("fast:ssd", 3);
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0], vec!["fast", "ssd"]);
        assert_eq!(tags[1], vec!["fast", "ssd"]);
        assert_eq!(tags[2], vec!["fast", "ssd"]);
    }

    #[test]
    fn test_parse_volume_tags_multi() {
        let tags = parse_volume_tags("fast:ssd,archive", 3);
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0], vec!["fast", "ssd"]);
        assert_eq!(tags[1], vec!["archive"]);
        assert_eq!(tags[2], Vec::<String>::new());
    }

    #[test]
    fn test_parse_volume_tags_empty() {
        let tags = parse_volume_tags("", 2);
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], Vec::<String>::new());
        assert_eq!(tags[1], Vec::<String>::new());
    }

    #[test]
    fn test_normalize_args_single_dash_to_double() {
        let args = vec![
            "bin".into(),
            "-port".into(), "8080".into(),
            "-ip.bind".into(), "127.0.0.1".into(),
            "-dir".into(), "/data".into(),
        ];
        let norm = normalize_args_vec(args);
        assert_eq!(norm, vec![
            "bin", "--port", "8080", "--ip.bind", "127.0.0.1", "--dir", "/data",
        ]);
    }

    #[test]
    fn test_normalize_args_double_dash_unchanged() {
        let args = vec![
            "bin".into(),
            "--port".into(), "8080".into(),
            "--master".into(), "localhost:9333".into(),
        ];
        let norm = normalize_args_vec(args);
        assert_eq!(norm, vec![
            "bin", "--port", "8080", "--master", "localhost:9333",
        ]);
    }

    #[test]
    fn test_normalize_args_single_char_flags_unchanged() {
        let args = vec!["bin".into(), "-h".into(), "-V".into()];
        let norm = normalize_args_vec(args);
        assert_eq!(norm, vec!["bin", "-h", "-V"]);
    }

    #[test]
    fn test_normalize_args_equals_format() {
        let args = vec!["bin".into(), "-port=8080".into(), "-ip.bind=0.0.0.0".into()];
        let norm = normalize_args_vec(args);
        assert_eq!(norm, vec!["bin", "--port=8080", "--ip.bind=0.0.0.0"]);
    }

    #[test]
    fn test_normalize_args_stop_at_double_dash() {
        let args = vec!["bin".into(), "-port".into(), "8080".into(), "--".into(), "-notaflag".into()];
        let norm = normalize_args_vec(args);
        assert_eq!(norm, vec!["bin", "--port", "8080", "--", "-notaflag"]);
    }

    #[test]
    fn test_parse_security_config_access_ui() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            r#"
[jwt.signing]
key = "secret"

[access]
ui = true
"#,
        )
        .unwrap();

        let cfg = parse_security_config(tmp.path().to_str().unwrap());
        assert_eq!(cfg.jwt_signing_key, b"secret");
        assert!(cfg.access_ui);
    }

    #[test]
    fn test_merge_options_file_basic() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "port=9999\ndir=/data\nmaster=localhost:9333\n",
        )
        .unwrap();

        let args = vec![
            "bin".into(),
            "--options".into(),
            tmp.path().to_str().unwrap().into(),
        ];
        let merged = merge_options_file(args);
        // Should contain the original args plus the file-based ones
        assert!(merged.contains(&"--port".to_string()));
        assert!(merged.contains(&"9999".to_string()));
        assert!(merged.contains(&"--dir".to_string()));
        assert!(merged.contains(&"/data".to_string()));
    }

    #[test]
    fn test_merge_options_file_cli_precedence() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "port=9999\ndir=/data\n",
        )
        .unwrap();

        let args = vec![
            "bin".into(),
            "--port".into(),
            "8080".into(),
            "--options".into(),
            tmp.path().to_str().unwrap().into(),
        ];
        let merged = merge_options_file(args);
        // port should NOT be duplicated from file since CLI already set it
        let port_count = merged.iter().filter(|a| *a == "--port").count();
        assert_eq!(port_count, 1, "CLI port should take precedence, file port skipped");
        // dir should be added from file
        assert!(merged.contains(&"--dir".to_string()));
    }

    #[test]
    fn test_merge_options_file_comments_and_blanks() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "# this is a comment\n\nport=9999\n# another comment\ndir=/data\n",
        )
        .unwrap();

        let args = vec![
            "bin".into(),
            "--options".into(),
            tmp.path().to_str().unwrap().into(),
        ];
        let merged = merge_options_file(args);
        assert!(merged.contains(&"--port".to_string()));
        assert!(merged.contains(&"--dir".to_string()));
    }

    #[test]
    fn test_merge_options_file_with_dashes_in_key() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "-port=9999\n--dir=/data\nip.bind=0.0.0.0\n",
        )
        .unwrap();

        let args = vec![
            "bin".into(),
            "--options".into(),
            tmp.path().to_str().unwrap().into(),
        ];
        let merged = merge_options_file(args);
        assert!(merged.contains(&"--port".to_string()));
        assert!(merged.contains(&"--dir".to_string()));
        assert!(merged.contains(&"--ip.bind".to_string()));
    }

    #[test]
    fn test_find_options_arg() {
        assert_eq!(
            find_options_arg(&["bin".into(), "--options".into(), "/tmp/opts".into()]),
            "/tmp/opts"
        );
        assert_eq!(
            find_options_arg(&["bin".into(), "-options".into(), "/tmp/opts".into()]),
            "/tmp/opts"
        );
        assert_eq!(
            find_options_arg(&["bin".into(), "--options=/tmp/opts".into()]),
            "/tmp/opts"
        );
        assert_eq!(
            find_options_arg(&["bin".into(), "--port".into(), "8080".into()]),
            ""
        );
    }
}
