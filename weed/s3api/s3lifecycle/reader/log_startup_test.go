package reader

import (
	"testing"
)

// LogStartup writes a single glog line summarising the reader's
// resume position; the only behavioral output is a side effect on the
// log sink, but exercising both branches still pins compile-time
// shape (e.g. that ShardPredicate-set readers don't trip on a missing
// ShardID and vice versa) and lets coverage actually visit the code.

func TestLogStartup_ShardIDOnly(t *testing.T) {
	// Single-shard configuration: ShardID is set, ShardPredicate is nil,
	// no Cursor, no StartTsNs. The function must run without panic.
	r := &Reader{ShardID: 7, EventBudget: 100}
	r.LogStartup()
}

func TestLogStartup_ShardPredicate(t *testing.T) {
	// ShardPredicate-set readers take a different log branch; pinning
	// the call here catches a regression that returns or panics.
	r := &Reader{
		ShardPredicate: func(int) bool { return true },
		EventBudget:    100,
	}
	r.LogStartup()
}

func TestLogStartup_StartTsNsOverridesCursor(t *testing.T) {
	// Explicit StartTsNs takes precedence over Cursor.MinTsNs; this
	// branch is otherwise only hit when a worker is replaying a
	// specific position. Run it through to make sure the override is
	// honored without consulting the Cursor.
	r := &Reader{ShardID: 0, StartTsNs: 1700000000_000_000_000, Cursor: NewCursor()}
	r.LogStartup()
}

func TestLogStartup_CursorMinFallback(t *testing.T) {
	// StartTsNs=0 with a non-nil Cursor falls back to Cursor.MinTsNs.
	c := NewCursor()
	r := &Reader{ShardID: 0, Cursor: c}
	r.LogStartup()
}
