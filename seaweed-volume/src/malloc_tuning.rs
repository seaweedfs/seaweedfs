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
/// has set it, honour their value and do not override it.
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
    /// An allocator override (`MALLOC_MMAP_THRESHOLD_` or
    /// `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=...`) was set, so the
    /// operator's value wins.
    DeferredToEnv,
    /// `mallopt` reported failure. Not fatal — the server runs, it just keeps
    /// glibc's adaptive behaviour.
    Failed,
    /// Not glibc, so there is no adaptive threshold to pin.
    NotApplicable,
}

/// Pin glibc's mmap threshold unless the operator has set an allocator override.
/// Safe to call more than once; call it before serving traffic, since the point
/// is to prevent the threshold from being trained upward by early allocations.
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
    // MALLOC_MMAP_THRESHOLD_: glibc calls atoi(value) then mallopt, which
    // always sets the threshold and disables dynamic adjustment — even for
    // empty, negative, or non-numeric values (atoi returns 0). So any presence
    // of the variable means the operator's override is in effect.
    std::env::var_os(MMAP_THRESHOLD_ENV).is_some()
        || usable_glibc_tunable_threshold(std::env::var_os(GLIBC_TUNABLES_ENV))
}

/// Look for `glibc.malloc.mmap_threshold=<value>` among the colon-separated
/// tunables in `GLIBC_TUNABLES`. glibc's `parse_tunables_string` (elf/dl-tunables.c)
/// rejects the **entire** string (returns -1) if it reaches `\0` before finding
/// `=` in a name (last entry has no `=`), or if any entry's value contains a
/// duplicate `=`. When `parse_tunables_string` returns -1, `parse_tunables`
/// prints a warning and returns immediately without applying ANY tunable —
/// including ones already parsed into the tunables array. We match that by
/// returning `false` for the entire string on any of those conditions.
///
/// glibc parses tunable values with `_dl_strtoul`, which accepts decimal,
/// `0x` hex, `0` octal, an optional sign (negatives wrap to `unsigned long`),
/// and requires the entire value to be consumed; we match that with
/// `dl_strtoul_consumes_all`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn usable_glibc_tunable_threshold(tunables: Option<std::ffi::OsString>) -> bool {
    let s = match tunables.and_then(|v| v.into_string().ok()) {
        Some(s) => s,
        None => return false,
    };
    if s.is_empty() {
        return false;
    }

    // Parse the string character-by-character, matching glibc's
    // parse_tunables_string logic exactly. Using split(':') would lose the
    // distinction between an entry terminated by ':' (skip) and one terminated
    // by '\0' with no '=' (reject entire string).
    let bytes = s.as_bytes();
    let mut pos = 0;
    let mut found_threshold = false;

    loop {
        // Find where the name ends ('=', ':', or end of string).
        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != b'=' && bytes[pos] != b':' {
            pos += 1;
        }

        // End of string before '=' → glibc returns -1 (reject entire string).
        if pos >= bytes.len() {
            return false;
        }

        // ':' before '=' → glibc skips this entry and continues.
        if bytes[pos] == b':' {
            pos += 1;
            continue;
        }

        // Skip the '='.
        let name_end = pos;
        pos += 1;

        // Find where the value ends ('=', ':', or end of string).
        let val_start = pos;
        while pos < bytes.len() && bytes[pos] != b'=' && bytes[pos] != b':' {
            pos += 1;
        }

        // '=' in value → glibc returns -1 (reject entire string).
        if pos < bytes.len() && bytes[pos] == b'=' {
            return false;
        }

        let key = &s[name_start..name_end];
        let val = &s[val_start..pos];

        if key == MMAP_THRESHOLD_TUNABLE && dl_strtoul_consumes_all(val) {
            found_threshold = true;
        }

        // End of string → done.
        if pos >= bytes.len() {
            break;
        }

        // Skip the ':'.
        pos += 1;
    }

    found_threshold
}

/// Replicate glibc's `_dl_strtoul` (elf/dl-misc.c) just enough to determine
/// whether it would consume the entire string — which is what
/// `tunable_parse_num` checks (`endptr == strval + len`). Returns `true` if
/// glibc would accept the value and apply it.
///
/// `_dl_strtoul` skips leading spaces/tabs, accepts an optional `+`/`-` sign,
/// and parses `0x`-prefixed hex, `0`-prefixed octal, or plain decimal. A
/// negative result wraps to `unsigned long` (`-1` → `SIZE_MAX`). If no digit is
/// found after the sign, the end pointer stays at the current position — which
/// still counts as "consumed" when the string is empty or whitespace-only
/// (value 0). On overflow, `_dl_strtoul` stops at the overflowing digit (endptr
/// does not reach the end), so `tunable_parse_num` rejects the value.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn dl_strtoul_consumes_all(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut pos = 0;

    // Skip leading whitespace (spaces and tabs, matching _dl_strtoul).
    while pos < bytes.len() && (bytes[pos] == b' ' || bytes[pos] == b'\t') {
        pos += 1;
    }

    // Optional sign.
    if pos < bytes.len() && (bytes[pos] == b'-' || bytes[pos] == b'+') {
        pos += 1;
    }

    // Must have at least one digit (0-9) to start parsing, unless we're already
    // at the end (empty / whitespace-only / sign-only → value 0, consumed).
    if pos >= bytes.len() {
        return true;
    }
    if bytes[pos] < b'0' || bytes[pos] > b'9' {
        return false;
    }

    // Determine base: 0x → hex, 0 → octal, else decimal. _dl_strtoul unconditionally
    // advances past "0x"/"0X" when the first char is '0' and the next is 'x'/'X',
    // even if no hex digit follows — in that case the digit loop breaks immediately,
    // endptr reaches the end, and the value is 0.
    let base: u32 = if bytes[pos] == b'0'
        && pos + 1 < bytes.len()
        && (bytes[pos + 1] == b'x' || bytes[pos + 1] == b'X')
    {
        pos += 2; // skip "0x"
        16
    } else if bytes[pos] == b'0' {
        8
    } else {
        10
    };

    // Parse digits with overflow detection, matching _dl_strtoul's cutoff/cutlim
    // logic. On overflow, _dl_strtoul sets endptr to the overflowing digit and
    // returns UINT64_MAX — so the value is NOT fully consumed and
    // tunable_parse_num rejects it.
    let mut result: u64 = 0;
    let cutoff = u64::MAX / base as u64;
    let cutlim = u64::MAX % base as u64;

    while pos < bytes.len() {
        let b = bytes[pos];
        let digval: u32 = match digit_value(b, base) {
            Some(v) => v,
            None => break,
        };
        if result > cutoff || (result == cutoff && digval as u64 > cutlim) {
            // Overflow: _dl_strtoul stops here, endptr points at this digit.
            return false;
        }
        result *= base as u64;
        result += digval as u64;
        pos += 1;
    }

    // The entire string must be consumed (matching tunable_parse_num's check).
    pos == bytes.len()
}

/// Returns the numeric value of a digit byte in the given base, or `None` if
/// the byte is not a valid digit in that base.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn digit_value(b: u8, base: u32) -> Option<u32> {
    if (b'0'..=b'0' + (base - 1).min(9) as u8).contains(&b) {
        return Some((b - b'0') as u32);
    }
    if base == 16 {
        if (b'a'..=b'f').contains(&b) {
            return Some((b - b'a' + 10) as u32);
        }
        if (b'A'..=b'F').contains(&b) {
            return Some((b - b'A' + 10) as u32);
        }
    }
    None
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
    fn dl_strtoul_consumes_all_matches_glibc_parser() {
        // Decimal — any non-empty decimal integer is accepted, including
        // negative (wraps to unsigned) and zero.
        assert!(dl_strtoul_consumes_all("131072"));
        assert!(dl_strtoul_consumes_all("0"));
        assert!(dl_strtoul_consumes_all("-1"));
        assert!(dl_strtoul_consumes_all("-131072"));
        // Values above i64::MAX are valid for glibc's unsigned parser.
        assert!(dl_strtoul_consumes_all("9223372036854775808"));
        // Hex with 0x prefix.
        assert!(dl_strtoul_consumes_all("0x20000"));
        assert!(dl_strtoul_consumes_all("0X20000"));
        assert!(dl_strtoul_consumes_all("0x0"));
        // Octal with leading 0.
        assert!(dl_strtoul_consumes_all("010"));
        // Leading whitespace (spaces and tabs) is skipped.
        assert!(dl_strtoul_consumes_all("  131072"));
        assert!(dl_strtoul_consumes_all("\t0x20000"));
        // Empty and whitespace-only strings are accepted (value 0).
        assert!(dl_strtoul_consumes_all(""));
        assert!(dl_strtoul_consumes_all("  "));
        assert!(dl_strtoul_consumes_all("\t"));
        // Sign-only strings are accepted: _dl_strtoul skips the sign, finds no
        // digit, sets endptr to the position after the sign (== end of string),
        // and returns 0. tunable_parse_num sees endptr == strval + len → true.
        assert!(dl_strtoul_consumes_all("-"));
        assert!(dl_strtoul_consumes_all("+"));

        // Trailing garbage is rejected — _dl_strtoul stops at the first
        // non-digit and tunable_parse_num requires the entire string consumed.
        assert!(!dl_strtoul_consumes_all("131072abc"));
        // In hex mode, a-f are digits, so "0x20000abc" is a valid hex number.
        // Use a non-hex character like 'g' to test trailing garbage in hex.
        assert!(!dl_strtoul_consumes_all("0x20000g"));
        assert!(!dl_strtoul_consumes_all("128K"));
        // Non-numeric strings are rejected.
        assert!(!dl_strtoul_consumes_all("abc"));
        // "0x" with no hex digits: _dl_strtoul advances past "0x", the digit loop
        // breaks immediately (no hex digit), endptr reaches the end, value is 0.
        // tunable_parse_num accepts it.
        assert!(dl_strtoul_consumes_all("0x"));
        assert!(dl_strtoul_consumes_all("0X"));

        // Overflow: _dl_strtoul stops at the overflowing digit (endptr points
        // there, not at the end), so tunable_parse_num rejects the value.
        assert!(!dl_strtoul_consumes_all("18446744073709551616")); // u64::MAX + 1
        assert!(!dl_strtoul_consumes_all("99999999999999999999")); // 20 nines
        assert!(!dl_strtoul_consumes_all("0x10000000000000000")); // 2^64
        // u64::MAX itself is accepted: the last digit (5) equals cutlim (=5),
        // so the overflow check (digval > cutlim) is false.
        assert!(dl_strtoul_consumes_all("18446744073709551615")); // u64::MAX
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn usable_glibc_tunable_threshold_detects_mmap_threshold() {
        // Decimal, hex, octal, negative, and zero values are all accepted by
        // glibc's _dl_strtoul and cause the threshold to be pinned.
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=131072".into()
        )));
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=0x20000".into()
        )));
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=0".into()
        )));
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=-1".into()
        )));
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=9223372036854775808".into()
        )));
        // u64::MAX is accepted by _dl_strtoul (last digit == cutlim, no overflow).
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=18446744073709551615".into()
        )));
        // Appears alongside other tunables.
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.cpu.x=1:glibc.malloc.mmap_threshold=131072".into()
        )));
        // Leading ':' is accepted — glibc skips the empty entry and continues.
        assert!(usable_glibc_tunable_threshold(Some(
            ":glibc.malloc.mmap_threshold=131072".into()
        )));
        // Empty value is accepted by _dl_strtoul (value 0).
        assert!(usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=".into()
        )));

        // Non-numeric values are rejected by _dl_strtoul.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=abc".into()
        )));
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=128K".into()
        )));
        // A malformed sibling entry (duplicate '=') makes glibc reject the
        // entire string, so we must not accept the threshold entry either.
        // This applies regardless of whether the threshold is before or after
        // the malformed entry — parse_tunables_string returns -1, and
        // parse_tunables discards all tunables without applying any.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.check=2=2:glibc.malloc.mmap_threshold=131072".into()
        )));
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=262144:glibc.malloc.check=2=2".into()
        )));
        // A trailing entry with no '=' makes glibc reject the entire string
        // (parse_tunables_string hits '\0' before '=' and returns -1).
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=262144:glibc.cpu.x".into()
        )));
        // A trailing ':' makes glibc reject the entire string (the empty entry
        // after ':' hits '\0' before '=' and returns -1).
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.malloc.mmap_threshold=262144:".into()
        )));
        // Unrelated tunables do not count.
        assert!(!usable_glibc_tunable_threshold(Some(
            "glibc.cpu.x=1".into()
        )));
        assert!(!usable_glibc_tunable_threshold(None));
    }
}
