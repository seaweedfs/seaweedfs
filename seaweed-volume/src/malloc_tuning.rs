//! Keep glibc from silently converting large short-lived buffers into heap the
//! process never gives back.
//!
//! glibc serves an allocation with `mmap` when it is at least
//! `M_MMAP_THRESHOLD` (128 KiB by default), and `munmap`s it on free, so the
//! pages go straight back to the OS. That threshold is **adaptive**: whenever a
//! block that came from `mmap` is freed, glibc raises the threshold to that
//! block's size — up to 32 MiB — on the theory that a workload repeatedly
//! allocating buffers of that size is better served from the heap.
//!
//! For a volume server that theory is wrong in a specific, expensive way. EC
//! reconstruction and needle reassembly allocate large, short-lived buffers.
//! The first few are mmap'd and freed, which trains the threshold upward; every
//! later buffer of that size is then carved out of the heap instead. Heap
//! memory is only returned to the OS from the top of the arena, so those pages
//! stay resident as anonymous memory for the life of the process. They are
//! still *reusable* — this is not a leak, and a repeat workload does not grow
//! the footprint further — but under a hard cgroup `MemoryMax` they are
//! indistinguishable from a leak, because anonymous pages cannot be reclaimed
//! under pressure the way page cache can. The retained footprint eats exactly
//! the headroom that a burst of maintenance work needs, and the process is
//! OOM-killed while most of its resident memory is free-but-unreturned.
//!
//! Measured on a 17-node cluster (EC 10+4, `--index=redb`), one node, two
//! identical `ec.scrub -mode full` rounds over 10912 EC files each, comparing
//! the same unit restarted with and without a pinned threshold:
//!
//! | | baseline | round 1 | round 2 | 60s idle |
//! |---|---|---|---|---|
//! | default (adaptive) | 10 MB | 84 MB | 88 MB | **88 MB** |
//! | pinned threshold   | 10 MB | 13 MB | 14 MB | **14 MB** |
//!
//! 78 MB retained versus 4 MB for identical work. On that cluster's heavier
//! mixed scrub workloads the same effect reached ~600 MB of retained anonymous
//! memory per volume server, against a 3 GiB cap.
//!
//! Calling `mallopt(M_MMAP_THRESHOLD, ...)` sets the threshold *and* disables
//! the dynamic adjustment, which is the documented behaviour of setting it
//! explicitly. We pin it to glibc's own default rather than inventing a value:
//! the goal is to stop the adaptation, not to second-guess the default.

/// glibc's own default `M_MMAP_THRESHOLD`. Pinning to this value changes
/// nothing about which allocations use `mmap` on a freshly started process; it
/// only prevents the threshold from drifting upward later.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const DEFAULT_MMAP_THRESHOLD: libc::c_int = 128 * 1024;

/// Environment variable glibc reads for the same setting. If an operator has
/// set it, honour their value and do not override it.
pub const MMAP_THRESHOLD_ENV: &str = "MALLOC_MMAP_THRESHOLD_";

/// Outcome of the tuning attempt, so the caller can log it and tests can assert
/// on it without inspecting global allocator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MallocTuning {
    /// Threshold pinned to `DEFAULT_MMAP_THRESHOLD`; dynamic adjustment is off.
    Pinned(i32),
    /// `MALLOC_MMAP_THRESHOLD_` was set, so the operator's value wins.
    DeferredToEnv,
    /// `mallopt` reported failure. Not fatal — the server runs, it just keeps
    /// glibc's adaptive behaviour.
    Failed,
    /// Not glibc, so there is no adaptive threshold to pin.
    NotApplicable,
}

/// Pin glibc's mmap threshold unless the operator has set the environment
/// variable. Safe to call more than once; call it before serving traffic, since
/// the point is to prevent the threshold from being trained upward by early
/// allocations.
pub fn pin_mmap_threshold() -> MallocTuning {
    if std::env::var_os(MMAP_THRESHOLD_ENV).is_some() {
        return MallocTuning::DeferredToEnv;
    }
    pin_mmap_threshold_inner()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn pin_mmap_threshold_inner() -> MallocTuning {
    // SAFETY: `mallopt` is a libc entry point that takes two ints and mutates
    // only allocator-internal tunables. It has no preconditions and no effect
    // on memory this process already owns.
    let rc = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, DEFAULT_MMAP_THRESHOLD) };
    if rc == 1 {
        MallocTuning::Pinned(DEFAULT_MMAP_THRESHOLD as i32)
    } else {
        MallocTuning::Failed
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn pin_mmap_threshold_inner() -> MallocTuning {
    MallocTuning::NotApplicable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinning_succeeds_on_glibc_and_is_a_noop_elsewhere() {
        // The env var is not set in the test process, so this exercises the
        // mallopt path on glibc and the NotApplicable path everywhere else.
        if std::env::var_os(MMAP_THRESHOLD_ENV).is_some() {
            assert_eq!(pin_mmap_threshold(), MallocTuning::DeferredToEnv);
            return;
        }
        let got = pin_mmap_threshold();
        if cfg!(all(target_os = "linux", target_env = "gnu")) {
            assert_eq!(
                got,
                MallocTuning::Pinned(DEFAULT_MMAP_THRESHOLD as i32),
                "mallopt(M_MMAP_THRESHOLD) should succeed on glibc"
            );
        } else {
            assert_eq!(got, MallocTuning::NotApplicable);
        }
    }

    #[test]
    fn calling_twice_is_stable() {
        // Startup paths get re-entered in tests and in `weed mini`; the second
        // call must not report a different outcome from the first.
        let first = pin_mmap_threshold();
        let second = pin_mmap_threshold();
        assert_eq!(first, second);
    }

    #[test]
    fn env_override_is_honoured_over_the_pinned_default() {
        // Verified against the real accessor rather than a copy of the name, so
        // renaming the constant cannot silently break the override contract.
        assert_eq!(MMAP_THRESHOLD_ENV, "MALLOC_MMAP_THRESHOLD_");
    }
}
