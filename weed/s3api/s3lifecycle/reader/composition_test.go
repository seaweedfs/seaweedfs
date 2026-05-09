package reader

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Layer 2 contracts the dispatcher pipeline relies on. These augment
// reader_test.go (extract + dispatch) and cursor_test.go (Advance +
// Freeze) by pinning the cursor composition surface that
// dispatcher.Pipeline depends on for resume-point selection,
// checkpoint persistence, and Run-time input validation.

func TestCursorMinTsNsWithFrozenKeysIncluded(t *testing.T) {
	// MinTsNs is the resume point Reader.Run feeds to SubscribeMetadata
	// when StartTsNs is zero. A freeze pins a key at its current TsNs;
	// the min must still see that pinned position so the subscription
	// doesn't rewind past it. Mixing frozen + non-frozen keys exercises
	// the path where the smallest position belongs to a frozen key.
	c := NewCursor()
	frozenKey := key("a", s3lifecycle.ActionKindExpirationDays)
	movingKey := key("b", s3lifecycle.ActionKindNoncurrentDays)
	otherKey := key("c", s3lifecycle.ActionKindAbortMPU)

	c.Advance(frozenKey, 100)
	c.Advance(movingKey, 500)
	c.Advance(otherKey, 300)
	c.Freeze(frozenKey, 100)

	// Frozen key holds at 100; Advance must not move it.
	c.Advance(frozenKey, 9_999)
	require.Equal(t, int64(100), c.Get(frozenKey))

	// MinTsNs reflects the frozen key because it is still the smallest.
	assert.Equal(t, int64(100), c.MinTsNs())

	// Non-frozen keys keep advancing; min stays anchored to the freeze.
	c.Advance(movingKey, 700)
	c.Advance(otherKey, 600)
	assert.Equal(t, int64(100), c.MinTsNs())
}

func TestCursorSnapshotIsDeepCopy(t *testing.T) {
	// Snapshot is persisted via the persister; callers must be able to
	// hold the returned map without observing live cursor mutations,
	// and the cursor must not observe caller-side edits to the map.
	c := NewCursor()
	a := key("a", s3lifecycle.ActionKindExpirationDays)
	b := key("b", s3lifecycle.ActionKindNoncurrentDays)
	c.Advance(a, 100)
	c.Advance(b, 200)

	snap := c.Snapshot()
	require.Equal(t, int64(100), snap[a])
	require.Equal(t, int64(200), snap[b])

	// Mutating the returned map must not bleed into the cursor.
	snap[a] = 9_999
	delete(snap, b)
	snap[key("z", s3lifecycle.ActionKindAbortMPU)] = 42
	assert.Equal(t, int64(100), c.Get(a), "cursor must be insulated from snapshot writes")
	assert.Equal(t, int64(200), c.Get(b), "cursor must retain key the caller deleted from snapshot")
	assert.Equal(t, int64(0), c.Get(key("z", s3lifecycle.ActionKindAbortMPU)), "cursor must not see keys added to snapshot")

	// Mutating the cursor after Snapshot must not bleed into the map
	// the caller is still holding (e.g., between Snapshot and Save).
	snap2 := c.Snapshot()
	c.Advance(a, 1_000)
	c.Advance(key("d", s3lifecycle.ActionKindAbortMPU), 50)
	assert.Equal(t, int64(100), snap2[a], "snapshot must not see post-snapshot Advance")
	_, hasNew := snap2[key("d", s3lifecycle.ActionKindAbortMPU)]
	assert.False(t, hasNew, "snapshot must not see post-snapshot key insertions")
}

func TestCursorRestoreReplacesNotMerges(t *testing.T) {
	// On startup the persister loads the last checkpoint and Restores
	// the cursor. A subsequent Restore (e.g., after a re-bootstrap)
	// must fully replace the in-memory map: keys present before but
	// absent in the new map must vanish, otherwise stale resume
	// points would survive across restores.
	c := NewCursor()
	a := key("a", s3lifecycle.ActionKindExpirationDays)
	b := key("b", s3lifecycle.ActionKindNoncurrentDays)
	cc := key("c", s3lifecycle.ActionKindAbortMPU)

	c.Restore(map[s3lifecycle.ActionKey]int64{a: 100, b: 200, cc: 300})
	require.Equal(t, int64(100), c.Get(a))
	require.Equal(t, int64(200), c.Get(b))
	require.Equal(t, int64(300), c.Get(cc))

	// Freeze a key before second Restore; Restore must clear frozen state
	// alongside the value map so a stale freeze doesn't survive a reload.
	c.Freeze(a, 100)
	require.True(t, c.IsFrozen(a))

	// Second Restore drops keys b and c entirely; a is replaced.
	c.Restore(map[s3lifecycle.ActionKey]int64{a: 50})
	assert.Equal(t, int64(50), c.Get(a))
	assert.False(t, c.IsFrozen(a), "Restore must clear frozen state")
	assert.Equal(t, int64(0), c.Get(b), "key absent from second Restore must be removed")
	assert.Equal(t, int64(0), c.Get(cc), "key absent from second Restore must be removed")

	// MinTsNs reflects only the surviving key.
	assert.Equal(t, int64(50), c.MinTsNs())

	// Restoring an empty map clears all state.
	c.Restore(map[s3lifecycle.ActionKey]int64{})
	assert.Equal(t, int64(0), c.Get(a))
	assert.Equal(t, int64(0), c.MinTsNs())
}

func TestReaderRunValidatesInputsBeforeSubscribing(t *testing.T) {
	// Run guards: out-of-range ShardID (no predicate), nil Events
	// channel, empty BucketsPath. Each error path must fire before any
	// client call, so a nil client suffices to drive the assertions.
	out := make(chan *Event, 1)

	cases := []struct {
		name    string
		reader  *Reader
		wantSub string
	}{
		{
			name:    "shard id below zero",
			reader:  &Reader{ShardID: -1, BucketsPath: "/buckets", Events: out},
			wantSub: "shard_id",
		},
		{
			name:    "shard id at or above ShardCount",
			reader:  &Reader{ShardID: s3lifecycle.ShardCount, BucketsPath: "/buckets", Events: out},
			wantSub: "shard_id",
		},
		{
			name:    "nil events channel",
			reader:  &Reader{ShardID: 0, BucketsPath: "/buckets", Events: nil},
			wantSub: "nil Events",
		},
		{
			name:    "empty buckets path",
			reader:  &Reader{ShardID: 0, BucketsPath: "", Events: out},
			wantSub: "empty BucketsPath",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Bound the call so a regression that lets Run reach the
			// nil client's blocking subscribe surfaces as a test failure
			// instead of hanging the suite.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := tc.reader.Run(ctx, nil, "test", 0)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantSub)
		})
	}
}
