use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use pprof::protos::Message;

use crate::config::VolumeServerConfig;

const GO_CPU_PROFILE_FREQUENCY: i32 = 100;
const GO_PPROF_BLOCKLIST: [&str; 4] = ["libc", "libgcc", "pthread", "vdso"];

pub struct CpuProfileSession {
    output_path: PathBuf,
    guard: pprof::ProfilerGuard<'static>,
}

impl CpuProfileSession {
    pub fn start(config: &VolumeServerConfig) -> Result<Option<Self>, String> {
        if config.cpu_profile.is_empty() {
            if !config.mem_profile.is_empty() && !config.pprof {
                tracing::warn!(
                    "--memprofile is not yet supported in the Rust volume server; ignoring '{}'",
                    config.mem_profile
                );
            }
            return Ok(None);
        }

        if config.pprof {
            tracing::info!(
                "--pprof is enabled; ignoring --cpuprofile '{}' and --memprofile '{}'",
                config.cpu_profile,
                config.mem_profile
            );
            return Ok(None);
        }

        if !config.mem_profile.is_empty() {
            tracing::warn!(
                "--memprofile is not yet supported in the Rust volume server; only --cpuprofile '{}' will be written",
                config.cpu_profile
            );
        }

        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(GO_CPU_PROFILE_FREQUENCY)
            .blocklist(&GO_PPROF_BLOCKLIST)
            .build()
            .map_err(|e| {
                format!(
                    "Failed to start CPU profiler '{}': {}",
                    config.cpu_profile, e
                )
            })?;

        Ok(Some(Self {
            output_path: PathBuf::from(&config.cpu_profile),
            guard,
        }))
    }

    pub fn finish(self) -> Result<(), String> {
        let report = self
            .guard
            .report()
            .build()
            .map_err(|e| format!("Failed to build CPU profile report: {}", e))?;
        let profile = report
            .pprof()
            .map_err(|e| format!("Failed to encode CPU profile report: {}", e))?;

        let mut bytes = Vec::new();
        profile
            .encode(&mut bytes)
            .map_err(|e| format!("Failed to serialize CPU profile report: {}", e))?;

        let mut file = File::create(&self.output_path).map_err(|e| {
            format!(
                "Failed to create CPU profile '{}': {}",
                self.output_path.display(),
                e
            )
        })?;
        file.write_all(&bytes).map_err(|e| {
            format!(
                "Failed to write CPU profile '{}': {}",
                self.output_path.display(),
                e
            )
        })?;
        file.flush().map_err(|e| {
            format!(
                "Failed to flush CPU profile '{}': {}",
                self.output_path.display(),
                e
            )
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::CpuProfileSession;
    use crate::config::{NeedleMapKind, ReadMode, VolumeServerConfig};
    use crate::security::tls::TlsPolicy;

    fn sample_config() -> VolumeServerConfig {
        VolumeServerConfig {
            port: 8080,
            grpc_port: 18080,
            public_port: 8080,
            ip: "127.0.0.1".to_string(),
            bind_ip: "127.0.0.1".to_string(),
            public_url: "127.0.0.1:8080".to_string(),
            id: "127.0.0.1:8080".to_string(),
            masters: vec![],
            pre_stop_seconds: 0,
            idle_timeout: 0,
            data_center: String::new(),
            rack: String::new(),
            index_type: NeedleMapKind::InMemory,
            disk_type: String::new(),
            folders: vec!["/tmp".to_string()],
            folder_max_limits: vec![8],
            folder_tags: vec![vec![]],
            min_free_spaces: vec![],
            disk_types: vec![String::new()],
            idx_folder: String::new(),
            white_list: vec![],
            fix_jpg_orientation: false,
            read_mode: ReadMode::Local,
            cpu_profile: String::new(),
            mem_profile: String::new(),
            compaction_byte_per_second: 0,
            maintenance_byte_per_second: 0,
            file_size_limit_bytes: 0,
            concurrent_upload_limit: 0,
            concurrent_download_limit: 0,
            inflight_upload_data_timeout: std::time::Duration::from_secs(0),
            inflight_download_data_timeout: std::time::Duration::from_secs(0),
            has_slow_read: false,
            read_buffer_size_mb: 4,
            ldb_timeout: 0,
            pprof: false,
            metrics_port: 0,
            metrics_ip: String::new(),
            debug: false,
            debug_port: 0,
            ui_enabled: false,
            jwt_signing_key: vec![],
            jwt_signing_expires_seconds: 0,
            jwt_read_signing_key: vec![],
            jwt_read_signing_expires_seconds: 0,
            https_cert_file: String::new(),
            https_key_file: String::new(),
            https_ca_file: String::new(),
            https_client_enabled: false,
            https_client_cert_file: String::new(),
            https_client_key_file: String::new(),
            https_client_ca_file: String::new(),
            grpc_cert_file: String::new(),
            grpc_key_file: String::new(),
            grpc_ca_file: String::new(),
            grpc_allowed_wildcard_domain: String::new(),
            grpc_volume_allowed_common_names: vec![],
            tls_policy: TlsPolicy::default(),
            enable_write_queue: false,
            security_file: String::new(),
        }
    }

    #[test]
    fn test_cpu_profile_session_skips_when_disabled() {
        let config = sample_config();
        assert!(CpuProfileSession::start(&config).unwrap().is_none());
    }

    #[test]
    fn test_cpu_profile_session_skips_when_pprof_enabled() {
        let mut config = sample_config();
        config.cpu_profile = "/tmp/cpu.pb".to_string();
        config.pprof = true;
        assert!(CpuProfileSession::start(&config).unwrap().is_none());
    }
}
