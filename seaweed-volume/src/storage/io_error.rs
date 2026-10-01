//! Consecutive storage-media error tracking shared by `Volume` and
//! `EcVolume`. Mirrors Go's `weed/storage/io_error.go`.

use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
    /// The consecutive error count in the low 32 bits and, in the high 32,
    /// how many times it has been cleared. They share one word so that
    /// `record_success_at` updates both in one step: reads record their
    /// outcomes here without the volume's write lock.
    streak: AtomicU64,
    quarantined: AtomicBool,
}

const STREAK_COUNT_BITS: u64 = 0xffff_ffff;

fn streak_count(streak: u64) -> i32 {
    (streak & STREAK_COUNT_BITS) as i32
}

/// `streak` with its count cleared and one more clear on record.
fn streak_cleared(streak: u64) -> u64 {
    (streak >> 32).wrapping_add(1) << 32
}

/// A point in the error streak, taken where a write landed whose success
/// is only recorded later. See `IoErrorTracker::record_success_at`.
#[derive(Clone, Copy)]
pub(crate) struct StreakMark(u64);

impl IoErrorTracker {
    /// `Some(e)` records a failure, `None` a success. Only storage-media
    /// failures count; every other outcome clears the count and last error.
    pub(crate) fn check_read_write_error(&self, err: Option<&io::Error>) {
        if let Some(e) = err
            && is_storage_io_error(e)
        {
            self.streak.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut guard) = self.last.lock() {
                *guard = Some(e.to_string());
            }
            crate::metrics::STORAGE_IO_ERROR_COUNTER.inc();
            return;
        }
        self.clear_count();
        self.clear_last();
    }

    fn clear_count(&self) {
        self.update_streak(|streak| Some(streak_cleared(streak)));
    }

    fn clear_last(&self) {
        if let Ok(mut guard) = self.last.lock()
            && guard.is_some()
        {
            *guard = None;
        }
    }

    /// Apply `f` to the streak atomically; `None` leaves it as it is.
    /// Returns the streak `f` produced, if any.
    fn update_streak(&self, mut f: impl FnMut(u64) -> Option<u64>) -> Option<u64> {
        let mut updated = None;
        let _ = self
            .streak
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |streak| {
                updated = f(streak);
                updated
            });
        updated
    }

    pub(crate) fn mark(&self) -> StreakMark {
        StreakMark(self.streak.load(Ordering::Relaxed))
    }

    /// Record a success as if it had come at `mark`: the errors counted
    /// before the mark are cleared and the ones counted since still stand,
    /// as they would had each outcome been recorded in order. A streak
    /// cleared since the mark is left as it is.
    pub(crate) fn record_success_at(&self, mark: StreakMark) {
        let before = streak_count(mark.0);
        let updated = self.update_streak(|streak| {
            if streak >> 32 != mark.0 >> 32 {
                return None;
            }
            if streak_count(streak) <= before {
                return Some(streak_cleared(streak));
            }
            Some(streak - before as u64)
        });
        // The last error stays when one counted since the mark is left.
        if updated.is_some_and(|streak| streak_count(streak) == 0) {
            self.clear_last();
        }
    }

    /// The last recorded error, the consecutive count, and the quarantine flag.
    pub(crate) fn get_io_error_state(&self) -> (Option<String>, i32, bool) {
        let err = self.last.lock().ok().and_then(|g| g.clone());
        let count = self.count();
        let quarantined = self.quarantined.load(Ordering::Relaxed);
        (err, count, quarantined)
    }

    fn count(&self) -> i32 {
        streak_count(self.streak.load(Ordering::Relaxed))
    }

    pub(crate) fn should_quarantine(&self) -> bool {
        self.quarantined.load(Ordering::Relaxed) || self.count() >= IO_ERROR_TOLERANCE
    }

    pub(crate) fn mark_io_quarantined(&self) {
        self.quarantined.store(true, Ordering::Relaxed);
    }

    pub(crate) fn reset_io_error_state(&self) {
        self.clear_count();
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
            self.update_streak(|streak| {
                Some((streak & !STREAK_COUNT_BITS) | IO_ERROR_TOLERANCE as u64)
            });
        } else {
            self.clear_count();
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
    fn success_at_a_mark_keeps_only_the_errors_after_it() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.check_read_write_error(Some(&media_error()));
        let mark = tracker.mark();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.record_success_at(mark);

        assert_eq!(
            tracker.get_io_error_state(),
            (Some(media_error().to_string()), 1, false)
        );
    }

    #[test]
    fn success_at_a_mark_with_nothing_after_it_clears_the_streak() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        let mark = tracker.mark();
        tracker.record_success_at(mark);

        assert_eq!(tracker.get_io_error_state(), (None, 0, false));
    }

    #[test]
    fn success_at_a_mark_leaves_a_streak_cleared_since() {
        let tracker = IoErrorTracker::default();
        tracker.check_read_write_error(Some(&media_error()));
        tracker.check_read_write_error(Some(&media_error()));
        let mark = tracker.mark();
        tracker.check_read_write_error(None);
        tracker.check_read_write_error(Some(&media_error()));
        tracker.record_success_at(mark);

        assert_eq!(
            tracker.get_io_error_state(),
            (Some(media_error().to_string()), 1, false)
        );
    }

    /// Reads update the tracker without the volume's write lock, so a
    /// success replayed at a mark must not lose the errors they record
    /// while it runs.
    #[test]
    fn success_at_a_mark_keeps_concurrent_errors() {
        use std::sync::{Arc, Barrier};
        const READERS: i32 = 4;
        const ERRORS: i32 = 200;

        for _ in 0..500 {
            let tracker = Arc::new(IoErrorTracker::default());
            tracker.check_read_write_error(Some(&media_error()));
            tracker.check_read_write_error(Some(&media_error()));
            let mark = tracker.mark();
            let start = Arc::new(Barrier::new(READERS as usize + 1));
            let readers: Vec<_> = (0..READERS)
                .map(|_| {
                    let (tracker, start) = (tracker.clone(), start.clone());
                    std::thread::spawn(move || {
                        start.wait();
                        for _ in 0..ERRORS {
                            tracker.check_read_write_error(Some(&media_error()));
                        }
                    })
                })
                .collect();
            start.wait();
            while tracker.get_io_error_state().1 < 2 + READERS * ERRORS / 2 {
                std::hint::spin_loop();
            }
            tracker.record_success_at(mark);
            for reader in readers {
                reader.join().unwrap();
            }
            // The two errors before the mark are cleared; every error the
            // readers recorded after it stands.
            assert_eq!(tracker.get_io_error_state().1, READERS * ERRORS);
        }
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
