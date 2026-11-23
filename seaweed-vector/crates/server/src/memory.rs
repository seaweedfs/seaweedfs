//! How much the engine caches: a share of the memory this process may use,
//! so a small pod is not overcommitted and a large host is not left idle.

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

/// The smaller of the cgroup's memory limit and the machine's memory.
fn memory_limit() -> Option<u64> {
    let total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|info| mem_total(&info));
    let cgroup = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| std::fs::read_to_string(path).ok())
    .and_then(|limit| cgroup_limit(&limit));
    match (total, cgroup) {
        (Some(total), Some(cgroup)) => Some(total.min(cgroup)),
        (total, cgroup) => total.or(cgroup),
    }
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
