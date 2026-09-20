//! Consecutive storage-media error tracking shared by `Volume` and
//! `EcVolume`. Mirrors Go's `weed/storage/io_error.go`.

use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// Consecutive storage-media errors allowed before the volume is quarantined.
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

/// Consecutive storage-media error state for one volume. `quarantined` is
/// sticky: once set it survives later successful I/O and is lifted only by
/// `reset_io_error_state`.
#[derive(Default)]
pub(crate) struct IoErrorTracker {
    last: Mutex<Option<String>>,
    count: AtomicI32,
    quarantined: AtomicBool,
}

impl IoErrorTracker {
    /// `Some(e)` records a failure, `None` a success. Only storage-media
    /// failures count; every other outcome clears the count and last error.
    pub(crate) fn check_read_write_error(&self, err: Option<&io::Error>) {
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
    pub(crate) fn get_io_error_state(&self) -> (Option<String>, i32, bool) {
        let err = self.last.lock().ok().and_then(|g| g.clone());
        let count = self.count.load(Ordering::Relaxed);
        let quarantined = self.quarantined.load(Ordering::Relaxed);
        (err, count, quarantined)
    }

    pub(crate) fn should_quarantine(&self) -> bool {
        self.quarantined.load(Ordering::Relaxed)
            || self.count.load(Ordering::Relaxed) >= IO_ERROR_TOLERANCE
    }

    pub(crate) fn mark_io_quarantined(&self) {
        self.quarantined.store(true, Ordering::Relaxed);
    }

    pub(crate) fn reset_io_error_state(&self) {
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
    fn check_read_write_error_counts_consecutive_media_errors() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.check_read_write_error(Some(&media_error()));

        let (last, count, quarantined) = tracker.get_io_error_state();
        assert_eq!(last, Some(media_error().to_string()));
        assert_eq!(count, 2);
        assert!(!quarantined);
    }

    #[test]
    fn success_clears_the_count_and_the_last_error() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.check_read_write_error(None);

        assert_eq!(tracker.get_io_error_state(), (None, 0, false));
    }

    #[test]
    fn non_media_error_clears_the_count() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.check_read_write_error(Some(&io::Error::new(
            io::ErrorKind::NotFound,
            "no such file",
        )));

        assert_eq!(tracker.get_io_error_state(), (None, 0, false));
    }

    #[test]
    fn should_quarantine_only_once_the_tolerance_is_reached() {
        let tracker = IoErrorTracker::default();
        for _ in 1..IO_ERROR_TOLERANCE {
            tracker.check_read_write_error(Some(&media_error()));
            assert!(!tracker.should_quarantine());
        }
        tracker.check_read_write_error(Some(&media_error()));
        assert!(tracker.should_quarantine());
    }

    #[test]
    fn quarantine_survives_later_successful_io() {
        let tracker = IoErrorTracker::default();
        tracker.mark_io_quarantined();
        tracker.check_read_write_error(None);

        assert_eq!(tracker.get_io_error_state(), (None, 0, true));
        assert!(tracker.should_quarantine());
    }

    #[test]
    fn reset_io_error_state_lifts_the_quarantine() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.mark_io_quarantined();
        tracker.reset_io_error_state();

        assert_eq!(tracker.get_io_error_state(), (None, 0, false));
        assert!(!tracker.should_quarantine());
    }

    #[test]
    fn test_helper_arms_a_sustained_error() {
        let tracker = IoErrorTracker::default();
        tracker.set_last_io_error_for_test(Some("input/output error"));
        assert!(tracker.should_quarantine());
        assert_eq!(
            tracker.get_io_error_state(),
            (
                Some("input/output error".to_string()),
                IO_ERROR_TOLERANCE,
                false
            )
        );

        tracker.set_last_io_error_for_test(None);
        assert_eq!(tracker.get_io_error_state(), (None, 0, false));
    }
}
