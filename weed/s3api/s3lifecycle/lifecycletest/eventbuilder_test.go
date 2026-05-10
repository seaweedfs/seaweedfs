package lifecycletest

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCreate_PopulatesNewEntryOnly(t *testing.T) {
	t0 := time.Unix(1700000000, 123)
	e := NewCreate("bk", "obj.txt", t0)
	require.NotNil(t, e.NewEntry)
	assert.Nil(t, e.OldEntry)
	assert.True(t, e.IsCreate())
	assert.Equal(t, t0.UnixNano(), e.TsNs)
	assert.Equal(t, "bk", e.Bucket)
	assert.Equal(t, "obj.txt", e.Key)
	assert.Equal(t, "obj.txt", e.NewEntry.Name)
	// Mtime defaults to the event timestamp; tests that don't care can
	// rely on this rather than threading time through every call.
	assert.Equal(t, t0.Unix(), e.NewEntry.Attributes.Mtime)
	assert.Equal(t, int32(t0.Nanosecond()), e.NewEntry.Attributes.MtimeNs)
}

func TestNewDelete_PopulatesOldEntryOnly(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	e := NewDelete("bk", "obj.txt", t0)
	require.NotNil(t, e.OldEntry)
	assert.Nil(t, e.NewEntry)
	assert.True(t, e.IsDelete())
	assert.Equal(t, "obj.txt", e.OldEntry.Name)
}

func TestNewUpdate_PopulatesBothEntries(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	e := NewUpdate("bk", "obj.txt", t0)
	require.NotNil(t, e.OldEntry)
	require.NotNil(t, e.NewEntry)
	assert.False(t, e.IsCreate())
	assert.False(t, e.IsDelete())
	// OldEntry's Mtime defaults to the event ts so its mirror of pre-
	// update state reflects the same wall-clock origin as NewEntry —
	// a downstream router that compares mtimes won't see a synthetic
	// 1970 epoch.
	assert.Equal(t, t0.Unix(), e.OldEntry.Attributes.Mtime)
	assert.Equal(t, t0.Unix(), e.NewEntry.Attributes.Mtime)
}

func TestNewCreate_NestedKeyUsesLeafName(t *testing.T) {
	// Filer entries store only the leaf name; mirroring that in the
	// fixture keeps router/dispatcher tests realistic.
	e := NewCreate("bk", "a/b/c.txt", time.Unix(0, 0))
	assert.Equal(t, "c.txt", e.NewEntry.Name)
}

func TestNewCreate_DirectoryKeyUsesSlashlessLeaf(t *testing.T) {
	// Directory-marker objects (S3 keys ending in "/") store the leaf
	// name without the trailing slash on the filer side. A pre-fix
	// regression returned "" here.
	e := NewCreate("bk", "folder/", time.Unix(0, 0))
	assert.Equal(t, "folder", e.NewEntry.Name)

	// Nested directory key strips both the trailing slash AND the
	// parent prefix.
	e2 := NewCreate("bk", "a/b/folder/", time.Unix(0, 0))
	assert.Equal(t, "folder", e2.NewEntry.Name)

	// Multiple trailing slashes collapse.
	e3 := NewCreate("bk", "folder///", time.Unix(0, 0))
	assert.Equal(t, "folder", e3.NewEntry.Name)
}

func TestNewCreate_ShardIDDerivedFromKey(t *testing.T) {
	// ShardID matches s3lifecycle.ShardID(bucket, key) so the event
	// routes through the same shard the production reader would
	// classify it under.
	e := NewCreate("bk", "obj.txt", time.Unix(0, 0))
	assert.Equal(t, s3lifecycle.ShardID("bk", "obj.txt"), e.ShardID)
}

func TestEventOption_WithSize(t *testing.T) {
	e := NewCreate("bk", "k", time.Unix(0, 0), WithSize(4096))
	assert.Equal(t, uint64(4096), e.NewEntry.Attributes.FileSize)
}

func TestEventOption_WithSizeAppliesToOldEntryOnDelete(t *testing.T) {
	// Delete events have OldEntry only; WithSize must apply there.
	e := NewDelete("bk", "k", time.Unix(0, 0), WithSize(8192))
	assert.Equal(t, uint64(8192), e.OldEntry.Attributes.FileSize)
}

func TestEventOption_WithModTimeOverridesDefault(t *testing.T) {
	// Default Mtime is event ts; explicit WithModTime overrides.
	t0 := time.Unix(1700000000, 0)
	override := time.Unix(1700000123, 456)
	e := NewCreate("bk", "k", t0, WithModTime(override))
	assert.Equal(t, override.Unix(), e.NewEntry.Attributes.Mtime)
	assert.Equal(t, int32(override.Nanosecond()), e.NewEntry.Attributes.MtimeNs)
}

func TestEventOption_WithTtlSec(t *testing.T) {
	// WithTtlSec drives the lifecycle metadata-only delete path; the
	// gate fires when the live entry's TtlSec > 0.
	e := NewCreate("bk", "k", time.Unix(0, 0), WithTtlSec(300))
	assert.Equal(t, int32(300), e.NewEntry.Attributes.TtlSec)
}

func TestEventOption_WithVersionID(t *testing.T) {
	e := NewCreate("bk", "k", time.Unix(0, 0), WithVersionID("v_abc"))
	assert.Equal(t, []byte("v_abc"), e.NewEntry.Extended[s3_constants.ExtVersionIdKey])
}

func TestEventOption_WithExtendedKeyValue(t *testing.T) {
	e := NewCreate("bk", "k", time.Unix(0, 0), WithExtended("Custom-Tag", []byte("v1")))
	assert.Equal(t, []byte("v1"), e.NewEntry.Extended["Custom-Tag"])
}

func TestEventOption_WithChunks(t *testing.T) {
	c1 := &filer_pb.FileChunk{FileId: "1,abc"}
	c2 := &filer_pb.FileChunk{FileId: "1,def"}
	e := NewCreate("bk", "k", time.Unix(0, 0), WithChunks(c1, c2))
	require.Len(t, e.NewEntry.Chunks, 2)
	assert.Equal(t, "1,abc", e.NewEntry.Chunks[0].FileId)
	assert.Equal(t, "1,def", e.NewEntry.Chunks[1].FileId)
}

func TestEventOption_WithShardIDOverrides(t *testing.T) {
	e := NewCreate("bk", "obj.txt", time.Unix(0, 0), WithShardID(7))
	assert.Equal(t, 7, e.ShardID)
}

func TestEventOption_WithOldSizeTargetsOldEntryOnUpdate(t *testing.T) {
	// On Update events, WithSize lands on NewEntry; WithOldSize lets
	// the caller configure pre-update state independently. Both options
	// in a single call should produce distinct sizes on the two entries.
	t0 := time.Unix(1700000000, 0)
	e := NewUpdate("bk", "k", t0, WithSize(200), WithOldSize(100))
	assert.Equal(t, uint64(200), e.NewEntry.Attributes.FileSize, "WithSize lands on NewEntry")
	assert.Equal(t, uint64(100), e.OldEntry.Attributes.FileSize, "WithOldSize lands on OldEntry")
}

func TestEventOption_WithOldChunksTargetsOldEntryOnUpdate(t *testing.T) {
	newChunk := &filer_pb.FileChunk{FileId: "1,new"}
	oldChunk := &filer_pb.FileChunk{FileId: "1,old"}
	t0 := time.Unix(1700000000, 0)
	e := NewUpdate("bk", "k", t0, WithChunks(newChunk), WithOldChunks(oldChunk))
	require.Len(t, e.NewEntry.Chunks, 1)
	require.Len(t, e.OldEntry.Chunks, 1)
	assert.Equal(t, "1,new", e.NewEntry.Chunks[0].FileId)
	assert.Equal(t, "1,old", e.OldEntry.Chunks[0].FileId)
}

func TestEventOption_WithOldModTimeTargetsOldEntryOnUpdate(t *testing.T) {
	// Pre-update mtime can lag the event timestamp; WithOldModTime lets
	// a router test pin the noncurrent-clock origin.
	t0 := time.Unix(1700000000, 0)
	older := time.Unix(1699000000, 500)
	e := NewUpdate("bk", "k", t0, WithOldModTime(older))
	assert.Equal(t, older.Unix(), e.OldEntry.Attributes.Mtime)
	assert.Equal(t, int32(500), e.OldEntry.Attributes.MtimeNs)
	// NewEntry's mtime stays at the default ts.
	assert.Equal(t, t0.Unix(), e.NewEntry.Attributes.Mtime)
}

func TestEventOption_WithOldOptionsAreNoOpsOnCreate(t *testing.T) {
	// Create events have no OldEntry; the WithOld* options must not
	// panic and must not leak fields into NewEntry.
	t0 := time.Unix(1700000000, 0)
	e := NewCreate("bk", "k", t0,
		WithOldSize(999),
		WithOldChunks(&filer_pb.FileChunk{FileId: "phantom"}),
		WithOldModTime(time.Unix(1, 0)),
	)
	assert.Nil(t, e.OldEntry)
	assert.Equal(t, uint64(0), e.NewEntry.Attributes.FileSize, "WithOldSize must not bleed to NewEntry")
	assert.Empty(t, e.NewEntry.Chunks, "WithOldChunks must not bleed to NewEntry")
	assert.Equal(t, t0.Unix(), e.NewEntry.Attributes.Mtime, "WithOldModTime must not bleed to NewEntry")
}

func TestEventOption_LaterOverridesEarlier(t *testing.T) {
	// Apply order matters: later options win on the same field. Pins
	// the documented ordering so tests can compose default+override
	// patterns without surprises.
	e := NewCreate("bk", "k", time.Unix(0, 0),
		WithSize(100),
		WithSize(200),
	)
	assert.Equal(t, uint64(200), e.NewEntry.Attributes.FileSize)
}

func TestMetaLogClock_DefaultStepIsOneSecond(t *testing.T) {
	c := NewMetaLogClock(time.Unix(1000, 0), 0)
	t1 := c.Next()
	t2 := c.Next()
	assert.Equal(t, time.Second, t2.Sub(t1))
}

func TestMetaLogClock_CustomStep(t *testing.T) {
	c := NewMetaLogClock(time.Unix(1000, 0), 250*time.Millisecond)
	t1 := c.Next()
	t2 := c.Next()
	assert.Equal(t, 250*time.Millisecond, t2.Sub(t1))
}

func TestMetaLogClock_PeekDoesNotAdvance(t *testing.T) {
	c := NewMetaLogClock(time.Unix(1000, 0), time.Second)
	first := c.Peek()
	again := c.Peek()
	assert.True(t, first.Equal(again), "Peek must not advance the clock")
	advanced := c.Next()
	assert.True(t, first.Equal(advanced), "Next returns what Peek returned")
}

func TestMetaLogClock_ConcurrentNextNoRace(t *testing.T) {
	// Tests that produce events from many goroutines (e.g. fan-out
	// fixtures) need the clock to serialize without deadlock or
	// duplicate timestamps. -race catches a regression that drops
	// the lock.
	c := NewMetaLogClock(time.Unix(0, 0), time.Microsecond)
	const N = 64
	seen := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			seen.Store(c.Next(), struct{}{})
		}()
	}
	wg.Wait()
	count := 0
	seen.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, N, count, "every Next must produce a unique timestamp")
}
