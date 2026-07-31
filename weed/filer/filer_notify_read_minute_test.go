package filer

import (
	"testing"
	"time"
)

// TestPersistedLogScanStartCoversSpanningWindow pins the file-selection window.
// A log file is named for the start of the window it holds, and a window spans
// up to LogFlushInterval, so a cursor inside the window's later minutes must
// still open the file named for its earlier one -- otherwise the read reports
// "nothing on disk" for entries that are sitting in it, and the gap resolvers
// take that miss as proof the range is empty.
func TestPersistedLogScanStartCoversSpanningWindow(t *testing.T) {
	// A window sealed at 12:30:59 ending 12:31:58 is written to "12-30".
	sealed := time.Date(2026, 6, 29, 12, 30, 59, 0, time.UTC)
	fileMinute := sealed.Format("15-04")

	// A subscriber resuming mid-window must not sort past that file.
	for _, cursor := range []time.Time{
		sealed.Add(11 * time.Second), // 12:31:10, the reported case
		sealed.Add(59 * time.Second), // 12:31:58, the window's last entry
		sealed,                       // exactly the window start
	} {
		scanMinute := persistedLogScanStart(cursor).Format("15-04")
		if scanMinute > fileMinute {
			t.Fatalf("cursor %v scans from %q, which sorts past the file %q holding it",
				cursor.Format("15:04:05"), scanMinute, fileMinute)
		}
	}
}

// A cursor just after midnight has to reach back into the previous day.
func TestPersistedLogScanStartCrossesMidnight(t *testing.T) {
	cursor := time.Date(2026, 6, 29, 0, 0, 30, 0, time.UTC)
	scanFrom := persistedLogScanStart(cursor)
	if got, want := scanFrom.Format("2006-01-02"), "2026-06-28"; got != want {
		t.Fatalf("scan date = %q, want %q so the previous day's last file is listed", got, want)
	}
}
