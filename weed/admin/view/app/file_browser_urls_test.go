package app

import "testing"

func TestFileBrowserPathURLEscapesQueryPath(t *testing.T) {
	path := "/buckets/airflow-logs/logs/run_id=backfill__2025-01-31T15:00:00+00:00"
	want := "/files?path=%2Fbuckets%2Fairflow-logs%2Flogs%2Frun_id%3Dbackfill__2025-01-31T15%3A00%3A00%2B00%3A00"

	if got := fileBrowserPathURL(path); got != want {
		t.Fatalf("fileBrowserPathURL() = %q, want %q", got, want)
	}
}

func TestFileBrowserPageURLEscapesCursor(t *testing.T) {
	got := fileBrowserPageURL("/buckets/airflow-logs/logs/run_id=a+b", "task:1+next", 50)
	want := "/files?path=%2Fbuckets%2Fairflow-logs%2Flogs%2Frun_id%3Da%2Bb&lastFileName=task%3A1%2Bnext&limit=50"

	if got != want {
		t.Fatalf("fileBrowserPageURL() = %q, want %q", got, want)
	}
}
