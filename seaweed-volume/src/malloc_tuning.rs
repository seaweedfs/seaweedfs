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

/// Legacy environment variable glibc reads for the same setting. If an operator
/// has set it to a usable value, honour their value and do not override it.
pub const MMAP_THRESHOLD_ENV: &str = "MALLOC_MMAP_THRESHOLD_";

/// Modern glibc tunables environment variable. Operators may set the threshold
/// via `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=...` instead of the legacy
/// variable; that override is honoured too.
pub const GLIBC_TUNABLES_ENV: &str = "GLIBC_TUNABLES";

/// The tunable name within `GLIBC_TUNABLES` that maps to `M_MMAP_THRESHOLD`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const MMAP_THRESHOLD_TUNABLE: &str = "glibc.malloc.mmap_threshold";

/// Outcome of the tuning attempt, so the caller can log it and tests can assert
/// on it without inspecting global allocator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MallocTuning {
    /// Threshold pinned to `DEFAULT_MMAP_THRESHOLD`; dynamic adjustment is off.
    Pinned(i32),
    /// A usable allocator override (`MALLOC_MMAP_THRESHOLD_` or
    /// `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=...`) was set, so the
    /// operator's value wins.
    DeferredToEnv,
    /// `mallopt` reported failure. Not fatal — the server runs, it just keeps
    /// glibc's adaptive behaviour.
    Failed,
    /// Not glibc, so there is no adaptive threshold to pin.
    NotApplicable,
}

/// Pin glibc's mmap threshold unless the operator has set a usable allocator
/// override. Safe to call more than once; call it before serving traffic, since
/// the point is to prevent the threshold from being trained upward by early
/// allocations.
pub fn pin_mmap_threshold() -> MallocTuning {
    pin_mmap_threshold_inner()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn pin_mmap_threshold_inner() -> MallocTuning {
    if operator_mmap_threshold_override_active() {
        return MallocTuning::DeferredToEnv;
    }
    // SAFETY: `mallopt` is a libc entry point that takes two ints and mutates
    // only allocator-internal tunables. It has no preconditions and no effect
    // on memory this process already owns.
    let rc = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, DEFAULT_MMAP_THRESHOLD) };
    if rc == 1 {
        MallocTuning::Pinned(DEFAULT_MMAP_THRESHOLD)
    } else {
        MallocTuning::Failed
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn pin_mmap_threshold_inner() -> MallocTuning {
    MallocTuning::NotApplicable
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn operator_mmap_threshold_override_active() -> bool {
    usable_threshold(std::env::var_os(MMAP_THRESHOLD_ENV))
        || usable_glibc_tunable_threshold(std::env::var_os(GLIBC_TUNABLES_ENV))
}

/// glibc silently ignores empty or non-numeric threshold values, and the
/// threshold is semantically unsigned, so treat negative, empty, or
/// non-numeric values as "not set" and fall through to pinning. Otherwise the
/// operator's value wins. Without this, an empty or malformed
/// `MALLOC_MMAP_THRESHOLD_` would make us skip `mallopt` while glibc also
/// ignores the override, leaving the adaptive behaviour this module exists to
/// prevent.
///
/// `MALLOC_MMAP_THRESHOLD_` is parsed by glibc with `atoi`, which is
/// decimal-only and returns a signed `int`; the threshold itself is
/// `size_t`-typed, so a negative or zero value is not a meaningful override.
/// We mirror that by accepting only non-empty, non-negative decimal integers.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn usable_threshold(value: Option<std::ffi::OsString>) -> bool {
    match value.and_then(|v| v.into_string().ok()) {
        Some(s) => parse_decimal_threshold(s.trim()).is_some(),
        None => false,
    }
}

/// Parse a decimal threshold the way glibc's `atoi` does, but reject negative
/// and zero values since the threshold is semantically unsigned and `mallopt`
/// requires it to be positive. Returns `Some` for any positive decimal value
/// that fits in `u64` (glibc's `strtoul` accepts the full `unsigned long`
/// range, so values above `i64::MAX` are valid overrides we must not discard).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn parse_decimal_threshold(s: &str) -> Option<u64> {
    if s.is_empty() || s.starts_with('-') {
        return None;
    }
    let n = s.parse::<u64>().ok()?;
    (n > 0).then_some(n)
}

/// Look for `glibc.malloc.mmap_threshold=<value>` among the colon-separated
/// tunables in `GLIBC_TUNABLES`, and apply the same non-empty/numeric check as
/// `usable_threshold` so a malformed tunable defers to pinning instead of
/// silently keeping the adaptive threshold.
///
/// glibc's tunable parser rejects the **entire** `GLIBC_TUNABLES` string if any
/// entry is malformed (e.g. contains a duplicate `=`), so we validate every
/// entry before accepting any one of them. glibc parses tunable values with
/// `strtoul`, which accepts `0x`-prefixed hexadecimal; we do the same.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn usable_glibc_tunable_threshold(tunables: Option<std::ffi::OsString>) -> bool {
    let s = match tunables.and_then(|v| v.into_string().ok()) {
        Some(s) => s,
        None => return false,
    };
    let mut found_threshold = false;
    for entry in s.split(':') {
        if entry.is_empty() {
            continue;
        }
        // Each entry must be `key=value` with exactly one '='. glibc rejects
        // the whole string if any entry has a duplicate '='.
        let (key, val) = match entry.split_once('=') {
            Some(kv) => kv,
            None => return false,
        };
        if val.contains('=') {
            return false;
        }
        if key == MMAP_THRESHOLD_TUNABLE && parse_strtoul_threshold(val.trim()).is_some() {
            found_threshold = true;
        }
    }
    found_threshold
}

/// Parse a tunable value the way glibc's `strtoul` does: decimal by default,
/// `0x`-prefixed hexadecimal otherwise. Rejects empty, negative, and
/// non-numeric values. Accepts the full `u64` range, matching glibc's
/// `unsigned long`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn parse_strtoul_threshold(s: &str) -> Option<u64> {
    if s.is_empty() || s.starts_with('-') {
        return None;
    }
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        let hex = hex.trim_start_matches('0');
        if hex.is_empty() {
            // "0x0" or "0x" → value 0, which is not a positive threshold.
            return None;
        }
        u64::from_str_radix(hex, 16).ok().filter(|n| *n > 0)
    } else {
        s.parse::<u64>().ok().filter(|n| *n > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_override_constants_match_glibc_names() {
        // Verified against the real accessor rather than a copy of the name, so
        // renaming the constant cannot silently break the override contract.
        assert_eq!(MMAP_THRESHOLD_ENV, "MALLOC_MMAP_THRESHOLD_");
        assert_eq!(GLIBC_TUNABLES_ENV, "GLIBC_TUNABLES");
    }

    #[test]
    fn calling_twice_is_stable() {
        // Startup paths get re-entered in tests and in `weed mini`; the second
        // call must not report a different outcome from the first.
        let first = pin_mmap_threshold();
        let second = pin_mmap_threshold();
        assert_eq!(first, second);
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn pins_threshold_on_glibc_when_no_override_is_set() {
        // The env override is not set in the test process, so this exercises the
        // mallopt path. If an override happens to be present, defer to it.
        if operator_mmap_threshold_override_active() {
            assert_eq!(pin_mmap_threshold(), MallocTuning::DeferredToEnv);
            return;
        }
        assert_eq!(
            pin_mmap_threshold(),
            MallocTuning::Pinned(DEFAULT_MMAP_THRESHOLD),
            "mallopt(M_MMAP_THRESHOLD) should succeed on glibc"
        );
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    #[test]
    fn is_a_noop_off_glibc() {
        // No glibc adaptive threshold exists off glibc, so there is nothing to
        // pin regardless of any environment variables that happen to be set.
        assert_eq!(pin_mmap_threshold(), MallocTuning::NotApplicable);
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn usable_threshold_accepts_numbers_and_rejects_garbage() {
        assert!(usable_threshold(Some("131072".into())));
        assert!(usable_threshold(Some(" 131072 ".into())));
        // Values above i64::MAX are valid for glibc's unsigned parser.
        assert!(usable_threshold(Some("9223372036854775808".into())));
        assert!(!usable_threshold(Some("".into())));
        assert!(!usable_threshold(Some("   ".into())));
        assert!(!usable_threshold(Some("0".into())));
        // Negative values are not a usable override: glibc's threshold is
        // unsigned, so it ignores or rejects them.
        assert!(!usable_threshold(Some("-1".into())));
        assert!(!usable_threshold(Some("-131072".into())));
        assert!(!usable_threshold(Some("128K".into())));
        assert!(!usable_threshold(Some("abc".into())));
        // MALLOC_MMAP_THRESHOLD_ is parsed by glibc with atoi (decimal-only),
        // so hex is not a valid override for the legacy variable.
        assert!(!usable_threshold(Some("0x20000".into())));
        assert!(!usable_threshold(None));
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn usable_glibc_tunable_threshold_detects_mmap_threshold() {
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=131072".into()
        )));
        // Appears alongside other tunables.
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.cpu.x=1:glibc.malloc.mmap_threshold=131072".into()
        )));
        // Hex values are accepted by glibc's strtoul.
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=0x20000".into()
        )));
        // Values above i64::MAX are valid for glibc's unsigned parser.
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=9223372036854775808".into()
        )));
        // Empty, zero, or non-numeric values are not a usable override.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=".into()
        )));
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=0".into()
        )));
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=abc".into()
        )));
        // Negative values are not a usable override.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=-1".into()
        )));
        // A malformed sibling entry makes glibc reject the entire string, so
        // we must not accept the threshold entry either.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.check=2=2:glibc.malloc.mmap_threshold=131072".into()
        )));
        // An entry with no '=' also invalidates the whole string.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold:glibc.cpu.x=1".into()
        )));
        // Unrelated tunables do not count.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.cpu.x=1".into()
        )));
        assert!(!usable_glibc_tunable_threshold(None));
    }
}
