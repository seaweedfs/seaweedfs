//! Shared I/O-error tracking for `Volume` and `EcVolume`.
//!
//! Both volume kinds count consecutive storage-media errors and quarantine
//! themselves once the count is sustained, so the tracker lives here once
//! rather than once per volume kind. Mirrors Go's `weed/storage/io_error.go`,
//! which keeps `IoErrorTracker` and `IoErrorTolerance` in their own file and
//! embeds the tracker in `Volume`. Go's `EcVolume` re-implements the fields
//! and methods because they are unexported and EC lives in another package,
//! and its copy tests EIO directly, so it misses the Windows codes;
//! `pub(crate)` lets both volume kinds share one tracker — and one
//! `is_storage_io_error` — here.

use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// Consecutive storage-media errors a volume is allowed before it is
/// quarantined and stops being reported to the master.
pub(crate) const IO_ERROR_TOLERANCE: i32 = 3;

/// Returns true for I/O errors that indicate faulty storage media, not
/// transient/network failures. On Unix this is EIO; on Windows it covers
/// ERROR_CRC and ERROR_IO_DEVICE, which the kernel returns for failing disks.
pub(crate) fn is_storage_io_error(e: &io::Error) -> bool {
    #[cfg(unix)]
    {
        e.raw_os_error() == Some(libc::EIO)
    }
    #[cfg(windows)]
    {
        const ERROR_CRC: i32 = 23;
        const ERROR_IO_DEVICE: i32 = 1117;
        return e.raw_os_error() == Some(ERROR_CRC) || e.raw_os_error() == Some(ERROR_IO_DEVICE);
    }
    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

/// Consecutive storage-media error state for one volume.
///
/// `record` is the only writer of `last`/`count`: an error the platform
/// attributes to failing media bumps the count, anything else — including a
/// success — clears it, so a single transient failure never accumulates.
/// `quarantined` is sticky on purpose: once set it survives later successful
/// I/O and is lifted only by an explicit `reset` (Go's `markIoQuarantined`).
#[derive(Default)]
pub(crate) struct IoErrorTracker {
    last: Mutex<Option<String>>,
    count: AtomicI32,
    quarantined: AtomicBool,
}

impl IoErrorTracker {
    /// Record the outcome of an I/O operation: `Some(e)` for a failure,
    /// `None` for a success. Only storage-media failures count; every other
    /// outcome clears the running count and the last recorded error.
    pub(crate) fn record(&self, err: Option<&io::Error>) {
        if let Some(e) = err
            && is_storage_io_error(e)
        {
            self.count.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut guard) = self.last.lock() {
                *guard = Some(e.to_string());
            }
            crate::metrics::STORAGE_IO_ERROR_COUNTER.inc();
            return;
        }
        self.count.store(0, Ordering::Relaxed);
        if let Ok(mut guard) = self.last.lock()
            && guard.is_some()
        {
            *guard = None;
        }
    }

    /// The last recorded error, the consecutive count, and the quarantine flag.
    pub(crate) fn state(&self) -> (Option<String>, i32, bool) {
        let err = self.last.lock().ok().and_then(|g| g.clone());
        let count = self.count.load(Ordering::Relaxed);
        let quarantined = self.quarantined.load(Ordering::Relaxed);
        (err, count, quarantined)
    }

    /// Whether the volume has to be quarantined: either it already is, or the
    /// consecutive error count has reached the tolerance.
    pub(crate) fn should_quarantine(&self) -> bool {
        self.quarantined.load(Ordering::Relaxed)
            || self.count.load(Ordering::Relaxed) >= IO_ERROR_TOLERANCE
    }

    pub(crate) fn mark_quarantined(&self) {
        self.quarantined.store(true, Ordering::Relaxed);
    }

    /// Clear everything, including the sticky quarantine flag.
    pub(crate) fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.quarantined.store(false, Ordering::Relaxed);
        if let Ok(mut guard) = self.last.lock() {
            *guard = None;
        }
    }

    #[cfg(test)]
    pub(crate) fn set_last_io_error_for_test(&self, err: Option<&str>) {
        if let Ok(mut guard) = self.last.lock() {
            *guard = err.map(|value| value.to_string());
        }
        // Set the count at the tolerance so the helper reflects a sustained
        // error, not a single transient one.
        if err.is_some() {
            self.count.store(IO_ERROR_TOLERANCE, Ordering::Relaxed);
        } else {
            self.count.store(0, Ordering::Relaxed);
        }
    }
}

// The tracker only reacts to errors `is_storage_io_error` recognises, which is
// nothing at all on a platform that is neither Unix nor Windows.
#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    /// An OS error the platform reports for failing storage media.
    #[cfg(unix)]
    fn media_error() -> io::Error {
        io::Error::from_raw_os_error(libc::EIO)
    }

    /// An OS error the platform reports for failing storage media.
    #[cfg(windows)]
    fn media_error() -> io::Error {
        const ERROR_IO_DEVICE: i32 = 1117;
        io::Error::from_raw_os_error(ERROR_IO_DEVICE)
    }

    #[test]
    fn record_counts_consecutive_media_errors() {
        let tracker = IoErrorTracker::default();
        tracker.record(Some(&media_error()));
        tracker.record(Some(&media_error()));

        let (last, count, quarantined) = tracker.state();
        assert_eq!(last, Some(media_error().to_string()));
        assert_eq!(count, 2);
        assert!(!quarantined);
    }

    #[test]
    fn record_success_clears_the_count_and_the_last_error() {
        let tracker = IoErrorTracker::default();
        tracker.record(Some(&media_error()));
        tracker.record(None);

        assert_eq!(tracker.state(), (None, 0, false));
    }

    #[test]
    fn record_non_media_error_clears_the_count() {
        let tracker = IoErrorTracker::default();
        tracker.record(Some(&media_error()));
        tracker.record(Some(&io::Error::new(
            io::ErrorKind::NotFound,
            "no such file",
        )));

        assert_eq!(tracker.state(), (None, 0, false));
    }

    #[test]
    fn should_quarantine_only_once_the_tolerance_is_reached() {
        let tracker = IoErrorTracker::default();
        for _ in 1..IO_ERROR_TOLERANCE {
            tracker.record(Some(&media_error()));
            assert!(!tracker.should_quarantine());
        }
        tracker.record(Some(&media_error()));
        assert!(tracker.should_quarantine());
    }

    #[test]
    fn quarantine_survives_later_successful_io() {
        let tracker = IoErrorTracker::default();
        tracker.mark_quarantined();
        tracker.record(None);

        // The count and the last error clear, but the quarantine is sticky:
        // only explicit recovery lifts it.
        assert_eq!(tracker.state(), (None, 0, true));
        assert!(tracker.should_quarantine());
    }

    #[test]
    fn reset_lifts_the_quarantine() {
        let tracker = IoErrorTracker::default();
        tracker.record(Some(&media_error()));
        tracker.mark_quarantined();
        tracker.reset();

        assert_eq!(tracker.state(), (None, 0, false));
        assert!(!tracker.should_quarantine());
    }

    #[test]
    fn test_helper_arms_a_sustained_error() {
        let tracker = IoErrorTracker::default();
        tracker.set_last_io_error_for_test(Some("input/output error"));
        assert!(tracker.should_quarantine());
        assert_eq!(
            tracker.state(),
            (
                Some("input/output error".to_string()),
                IO_ERROR_TOLERANCE,
                false
            )
        );

        tracker.set_last_io_error_for_test(None);
        assert_eq!(tracker.state(), (None, 0, false));
    }
}
