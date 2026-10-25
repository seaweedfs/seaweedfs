package reader

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// InMemoryPersister is the in-memory test double other lifecycle tests
// rely on for cursor checkpointing. The deep-copy contract documented
// on the Persister interface is what insulates each test's state from
// the others — these tests pin every direction of that contract so a
// regression here can't silently corrupt downstream tests.

func TestInMemoryPersister_LoadOnUnknownShardReturnsEmptyMap(t *testing.T) {
	// Contract: Load returns an empty map (not an error) when no prior
	// state exists. The dispatcher startup path relies on this so a
	// fresh worker doesn't have to special-case the "no checkpoint yet"
	// branch.
	p := NewInMemoryPersister()
	got, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestInMemoryPersister_SaveLoadRoundTrip(t *testing.T) {
	p := NewInMemoryPersister()
	want := map[s3lifecycle.ActionKey]int64{
		key("a", s3lifecycle.ActionKindExpirationDays): 100,
		key("b", s3lifecycle.ActionKindNoncurrentDays): 200,
	}
	require.NoError(t, p.Save(context.Background(), 7, want))

	got, err := p.Load(context.Background(), 7)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestInMemoryPersister_SaveCopiesInput(t *testing.T) {
	// Caller mutating the map after Save must not bleed into stored
	// state — the next Load must return what was saved.
	p := NewInMemoryPersister()
	in := map[s3lifecycle.ActionKey]int64{
		key("a", s3lifecycle.ActionKindExpirationDays): 100,
	}
	require.NoError(t, p.Save(context.Background(), 0, in))

	in[key("a", s3lifecycle.ActionKindExpirationDays)] = 9_999
	in[key("z", s3lifecycle.ActionKindAbortMPU)] = 42
	delete(in, key("a", s3lifecycle.ActionKindExpirationDays))

	got, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(100), got[key("a", s3lifecycle.ActionKindExpirationDays)],
		"caller-side mutation must not bleed into stored state")
	assert.NotContains(t, got, key("z", s3lifecycle.ActionKindAbortMPU),
		"caller-side insertion must not bleed into stored state")
}

func TestInMemoryPersister_LoadReturnsACopy(t *testing.T) {
	// Caller mutating the returned map must not bleed back; otherwise
	// a test reusing the persister across cases could see drift.
	p := NewInMemoryPersister()
	want := map[s3lifecycle.ActionKey]int64{
		key("a", s3lifecycle.ActionKindExpirationDays): 100,
	}
	require.NoError(t, p.Save(context.Background(), 0, want))

	first, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	first[key("a", s3lifecycle.ActionKindExpirationDays)] = 9_999
	first[key("z", s3lifecycle.ActionKindAbortMPU)] = 42

	second, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(100), second[key("a", s3lifecycle.ActionKindExpirationDays)],
		"caller-side mutation of the first snapshot must not bleed back")
	assert.NotContains(t, second, key("z", s3lifecycle.ActionKindAbortMPU),
		"caller-side insertion into the first snapshot must not bleed back")
}

func TestInMemoryPersister_SaveReplacesNotMerges(t *testing.T) {
	// Documented contract: Save replaces prior state atomically. A
	// merge would silently keep stale resume points across restarts.
	p := NewInMemoryPersister()
	require.NoError(t, p.Save(context.Background(), 0, map[s3lifecycle.ActionKey]int64{
		key("a", s3lifecycle.ActionKindExpirationDays): 100,
		key("b", s3lifecycle.ActionKindNoncurrentDays): 200,
	}))
	require.NoError(t, p.Save(context.Background(), 0, map[s3lifecycle.ActionKey]int64{
		key("c", s3lifecycle.ActionKindAbortMPU): 300,
	}))

	got, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, map[s3lifecycle.ActionKey]int64{
		key("c", s3lifecycle.ActionKindAbortMPU): 300,
	}, got, "second Save must replace prior state, not merge")
}

func TestInMemoryPersister_DifferentShardsIsolated(t *testing.T) {
	// State for shardID=1 must not leak into Load(shardID=2).
	p := NewInMemoryPersister()
	require.NoError(t, p.Save(context.Background(), 1, map[s3lifecycle.ActionKey]int64{
		key("a", s3lifecycle.ActionKindExpirationDays): 100,
	}))
	require.NoError(t, p.Save(context.Background(), 2, map[s3lifecycle.ActionKey]int64{
		key("b", s3lifecycle.ActionKindNoncurrentDays): 200,
	}))

	got1, err := p.Load(context.Background(), 1)
	require.NoError(t, err)
	got2, err := p.Load(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, int64(100), got1[key("a", s3lifecycle.ActionKindExpirationDays)])
	assert.NotContains(t, got1, key("b", s3lifecycle.ActionKindNoncurrentDays))
	assert.Equal(t, int64(200), got2[key("b", s3lifecycle.ActionKindNoncurrentDays)])
	assert.NotContains(t, got2, key("a", s3lifecycle.ActionKindExpirationDays))
}

func TestInMemoryPersister_SaveEmptyMapClearsState(t *testing.T) {
	// An empty map is valid input; Save must accept it and a subsequent
	// Load must return an empty map (not the prior contents).
	p := NewInMemoryPersister()
	require.NoError(t, p.Save(context.Background(), 0, map[s3lifecycle.ActionKey]int64{
		key("a", s3lifecycle.ActionKindExpirationDays): 100,
	}))
	require.NoError(t, p.Save(context.Background(), 0, map[s3lifecycle.ActionKey]int64{}))
	got, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestInMemoryPersister_ConcurrentSaveLoadDoesNotRace(t *testing.T) {
	// The dispatcher fans cursor reads/writes across many goroutines;
	// the persister must hold under load without livelocking or
	// data-racing (-race detector enforces that).
	p := NewInMemoryPersister()
	var wg sync.WaitGroup
	const N = 64
	wg.Add(N * 2)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = p.Save(context.Background(), i%4, map[s3lifecycle.ActionKey]int64{
				key("k", s3lifecycle.ActionKindExpirationDays): int64(i),
			})
		}()
		go func() {
			defer wg.Done()
			_, _ = p.Load(context.Background(), i%4)
		}()
	}
	wg.Wait()
}
