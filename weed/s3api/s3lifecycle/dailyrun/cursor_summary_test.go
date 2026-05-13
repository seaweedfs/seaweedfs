package dailyrun

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSummarizeShardCursorLag_AllColdStart pins the marker tokens for
// the worker's very first heartbeat — no cursor file exists yet, no
// walker has fired, the line must say "cold" rather than "0s" so
// operators can tell the difference between "I'm caught up" and
// "I haven't started." Both states would be 0s otherwise.
func TestSummarizeShardCursorLag_AllColdStart(t *testing.T) {
	p := newMemPersister()
	cfg := Config{Persister: p, Shards: []int{0, 1, 2}}
	runNow := time.Unix(1_700_000_000, 0).UTC()

	got := summarizeShardCursorLag(context.Background(), cfg, runNow)
	assert.Equal(t, "cursor_lag_max=cold walked_max_age=cold", got)
}

// TestSummarizeShardCursorLag_PicksMaxAcrossShards confirms the worst-
// case lag wins, not the average. Operators alert on the worst shard;
// the cluster is only as healthy as its slowest one.
func TestSummarizeShardCursorLag_PicksMaxAcrossShards(t *testing.T) {
	p := newMemPersister()
	runNow := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, p.Save(context.Background(), 0, Cursor{
		TsNs:         runNow.Add(-5 * time.Minute).UnixNano(),
		LastWalkedNs: runNow.Add(-30 * time.Minute).UnixNano(),
	}))
	require.NoError(t, p.Save(context.Background(), 1, Cursor{
		TsNs:         runNow.Add(-3 * time.Hour).UnixNano(), // worst cursor
		LastWalkedNs: runNow.Add(-10 * time.Minute).UnixNano(),
	}))
	require.NoError(t, p.Save(context.Background(), 2, Cursor{
		TsNs:         runNow.Add(-30 * time.Minute).UnixNano(),
		LastWalkedNs: runNow.Add(-2 * time.Hour).UnixNano(), // worst walker
	}))
	cfg := Config{Persister: p, Shards: []int{0, 1, 2}}

	got := summarizeShardCursorLag(context.Background(), cfg, runNow)
	assert.Contains(t, got, "cursor_lag_max=3h0m0s")
	assert.Contains(t, got, "walked_max_age=2h0m0s")
}

// TestSummarizeShardCursorLag_PartialFill — some shards have cursors,
// some don't. Don't let the unwalked-yet shards contaminate the worst-
// case calculation; just compute the max over what we have.
func TestSummarizeShardCursorLag_PartialFill(t *testing.T) {
	p := newMemPersister()
	runNow := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, p.Save(context.Background(), 0, Cursor{
		TsNs: runNow.Add(-15 * time.Minute).UnixNano(),
		// LastWalkedNs not set — shard 0 hasn't walked yet
	}))
	require.NoError(t, p.Save(context.Background(), 1, Cursor{
		// TsNs not set — shard 1 hasn't drained yet
		LastWalkedNs: runNow.Add(-45 * time.Minute).UnixNano(),
	}))
	cfg := Config{Persister: p, Shards: []int{0, 1, 2}}

	got := summarizeShardCursorLag(context.Background(), cfg, runNow)
	// Only shard 0 contributes to cursor_lag_max; only shard 1 to walked_max_age.
	assert.Contains(t, got, "cursor_lag_max=15m0s")
	assert.Contains(t, got, "walked_max_age=45m0s")
	// Shard 2 wasn't saved at all — it doesn't show up as either.
	assert.False(t, strings.Contains(got, "cold"), "any-fill case should not emit cold markers")
}
