//! How much the engine caches: a share of the memory this process may use,
//! so a small pod is not overcommitted and a large host is not left idle.

use std::path::{Path, PathBuf};

/// The read, index and metadata caches, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheSizes {
    pub read: u64,
    pub index: u64,
    pub metadata: u64,
}

/// Where no limit can be read, the sizes the flags used to default to.
const UNKNOWN_LIMIT_BUDGET: u64 = 1792 << 20;

impl CacheSizes {
    /// A quarter of the memory limit, split 2:4:1 between file reads, loaded
    /// indexes and table metadata.
    pub fn for_limit(limit: Option<u64>) -> Self {
        let budget = limit.map_or(UNKNOWN_LIMIT_BUDGET, |limit| limit / 4);
        let seventh = budget / 7;
        Self {
            read: seventh * 2,
            index: seventh * 4,
            metadata: seventh,
        }
    }

    pub fn detect() -> Self {
        Self::for_limit(memory_limit())
    }
}

/// The smallest of the machine's memory and the limits of this process's
/// memory cgroup and its ancestors: a systemd service is limited in its own
/// group, a container usually at the root it sees.
fn memory_limit() -> Option<u64> {
    let read = |path: &Path| std::fs::read_to_string(path).ok();
    let total = read(Path::new("/proc/meminfo")).and_then(|info| mem_total(&info));
    let groups = read(Path::new("/proc/self/cgroup"))
        .map(|groups| limit_files(&groups))
        .unwrap_or_default();
    groups
        .iter()
        .filter_map(|file| read(file).and_then(|limit| cgroup_limit(&limit)))
        .chain(total)
        .min()
}

/// The limit files of the memory cgroup `/proc/self/cgroup` names and of its
/// ancestors: v1's memory controller where it has one, else v2's unified group.
fn limit_files(self_cgroup: &str) -> Vec<PathBuf> {
    let entries: Vec<(&str, &str)> = self_cgroup
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, ':');
            fields.next()?;
            Some((fields.next()?, fields.next()?))
        })
        .collect();
    let v1 = entries
        .iter()
        .find(|(controllers, _)| controllers.split(',').any(|c| c == "memory"))
        .map(|(_, group)| ("/sys/fs/cgroup/memory", *group, "memory.limit_in_bytes"));
    let v2 = entries
        .iter()
        .find(|(controllers, _)| controllers.is_empty())
        .map(|(_, group)| ("/sys/fs/cgroup", *group, "memory.max"));
    let Some((root, group, file)) = v1.or(v2) else {
        return Vec::new();
    };
    Path::new(group)
        .ancestors()
        .map(|dir| {
            Path::new(root)
                .join(dir.strip_prefix("/").unwrap_or(dir))
                .join(file)
        })
        .collect()
}

fn mem_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib << 10)
}

/// cgroup v2 writes "max" for no limit; v1 writes a number near u64::MAX.
fn cgroup_limit(limit: &str) -> Option<u64> {
    let bytes: u64 = limit.trim().parse().ok()?;
    (bytes < 1 << 60).then_some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quarter_of_the_limit_split_two_four_one() {
        let sizes = CacheSizes::for_limit(Some(28 << 30));
        assert_eq!(sizes.read, 2 << 30);
        assert_eq!(sizes.index, 4 << 30);
        assert_eq!(sizes.metadata, 1 << 30);
    }

    #[test]
    fn no_limit_keeps_the_former_defaults() {
        let sizes = CacheSizes::for_limit(None);
        assert_eq!(
            (sizes.read, sizes.index, sizes.metadata),
            (512 << 20, 1024 << 20, 256 << 20)
        );
    }

    #[test]
    fn looks_up_the_process_group_and_its_ancestors() {
        let paths = |groups: &str| -> Vec<String> {
            limit_files(groups)
                .iter()
                .map(|p| p.display().to_string().replace('\\', "/"))
                .collect()
        };
        assert_eq!(
            paths("0::/system.slice/weed-vector.service\n"),
            [
                "/sys/fs/cgroup/system.slice/weed-vector.service/memory.max",
                "/sys/fs/cgroup/system.slice/memory.max",
                "/sys/fs/cgroup/memory.max",
            ]
        );
        // A container's namespace shows its own group as the root.
        assert_eq!(paths("0::/\n"), ["/sys/fs/cgroup/memory.max"]);
        // On a hybrid host the memory controller is v1's.
        assert_eq!(
            paths("12:cpu,cpuacct:/a\n4:memory:/user.slice\n0::/user.slice\n"),
            [
                "/sys/fs/cgroup/memory/user.slice/memory.limit_in_bytes",
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            ]
        );
        assert!(paths("").is_empty());
    }

    #[test]
    fn reads_the_limits_the_kernel_writes() {
        assert_eq!(
            mem_total("MemTotal:       16384000 kB\nMemFree: 1 kB\n"),
            Some(16_384_000 << 10)
        );
        assert_eq!(cgroup_limit("4294967296\n"), Some(4 << 30));
        assert_eq!(cgroup_limit("max\n"), None);
        assert_eq!(cgroup_limit("9223372036854771712\n"), None);
    }
}
