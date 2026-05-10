package lifecycletest

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Coverage gap-fillers for the With* options' OldEntry-only branches
// (NewEntry==nil, OldEntry populated — i.e. Delete events). The
// existing eventbuilder_test.go covers Create events; these tests
// exercise the alternate fall-through path on each option.

func TestEventOption_WithModTime_AppliesToOldEntryOnDelete(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	override := time.Unix(1699000000, 250)
	e := NewDelete("bk", "k", t0, WithModTime(override))
	require.Nil(t, e.NewEntry)
	require.NotNil(t, e.OldEntry)
	assert.Equal(t, override.Unix(), e.OldEntry.Attributes.Mtime)
	assert.Equal(t, int32(250), e.OldEntry.Attributes.MtimeNs)
}

func TestEventOption_WithTtlSec_AppliesToOldEntryOnDelete(t *testing.T) {
	e := NewDelete("bk", "k", time.Unix(0, 0), WithTtlSec(7200))
	require.NotNil(t, e.OldEntry)
	assert.Equal(t, int32(7200), e.OldEntry.Attributes.TtlSec)
}

func TestEventOption_WithVersionID_AppliesToOldEntryOnDelete(t *testing.T) {
	// VersionID-on-delete: the bootstrapper synthesizes deletes for
	// noncurrent-version sweeps and needs the version-id stamp.
	e := NewDelete("bk", "k", time.Unix(0, 0), WithVersionID("v_old"))
	require.NotNil(t, e.OldEntry)
	require.NotNil(t, e.OldEntry.Extended)
	assert.Equal(t, []byte("v_old"), e.OldEntry.Extended[s3_constants.ExtVersionIdKey])
}

func TestEventOption_WithExtended_AppliesToOldEntryOnDelete(t *testing.T) {
	e := NewDelete("bk", "k", time.Unix(0, 0), WithExtended("Custom-Tag", []byte("v")))
	require.NotNil(t, e.OldEntry)
	require.NotNil(t, e.OldEntry.Extended)
	assert.Equal(t, []byte("v"), e.OldEntry.Extended["Custom-Tag"])
}

func TestEventOption_WithChunks_AppliesToOldEntryOnDelete(t *testing.T) {
	c := &filer_pb.FileChunk{FileId: "1,old"}
	e := NewDelete("bk", "k", time.Unix(0, 0), WithChunks(c))
	require.NotNil(t, e.OldEntry)
	require.Len(t, e.OldEntry.Chunks, 1)
	assert.Equal(t, "1,old", e.OldEntry.Chunks[0].FileId)
}

func TestEventOption_WithBootstrapVersion(t *testing.T) {
	// WithBootstrapVersion attaches a BootstrapVersion to the event;
	// the bootstrap walker uses this for per-version state the live
	// meta-log doesn't carry. Pin both create and delete shapes.
	bv := &reader.BootstrapVersion{
		LogicalKey:      "obj.txt",
		VersionID:       "v_aaa",
		IsLatest:        true,
		IsDeleteMarker:  false,
		NumVersions:     3,
		NoncurrentIndex: 2,
	}

	create := NewCreate("bk", "obj.txt", time.Unix(0, 0), WithBootstrapVersion(bv))
	require.Same(t, bv, create.BootstrapVersion)

	del := NewDelete("bk", "obj.txt", time.Unix(0, 0), WithBootstrapVersion(bv))
	require.Same(t, bv, del.BootstrapVersion)

	update := NewUpdate("bk", "obj.txt", time.Unix(0, 0), WithBootstrapVersion(bv))
	require.Same(t, bv, update.BootstrapVersion)
}

func TestEventOption_NoPanicOnEmptyEvent(t *testing.T) {
	// Defense: an event with neither NewEntry nor OldEntry must not
	// panic on any With* option, even though no constructor produces
	// this shape today. Entry-targeting options fall through silently
	// (NewEntry/OldEntry stay nil); WithBootstrapVersion targets the
	// event itself, not an entry, so it does set BootstrapVersion —
	// included here for panic safety.
	e := &reader.Event{}
	WithModTime(time.Unix(1, 0))(e)
	WithTtlSec(60)(e)
	WithVersionID("v")(e)
	WithExtended("k", []byte("v"))(e)
	WithChunks(&filer_pb.FileChunk{FileId: "x"})(e)
	WithSize(1024)(e)
	bv := &reader.BootstrapVersion{VersionID: "v"}
	WithBootstrapVersion(bv)(e)

	assert.Nil(t, e.NewEntry, "entry-targeting options must not allocate NewEntry")
	assert.Nil(t, e.OldEntry, "entry-targeting options must not allocate OldEntry")
	assert.Same(t, bv, e.BootstrapVersion, "WithBootstrapVersion sets the field regardless of entry state")
}
