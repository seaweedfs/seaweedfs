package storage

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// A needle larger than one EC block is split over consecutive blocks, which
// live on different shards. The intervals are read concurrently, so check they
// still come back in order.
func TestReadEcShardNeedleSpanningBlocks(t *testing.T) {
	const vid = needle.VolumeId(7)
	store, ecVolume, n := newLocalEcVolume(t, vid)

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	if len(intervals) < 2 {
		t.Fatalf("needle covers %d interval(s), want it split over several", len(intervals))
	}

	got := new(needle.Needle)
	got.Id = n.Id
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		t.Fatalf("read ec needle: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Fatalf("read back %d bytes, want the %d written", len(got.Data), len(n.Data))
	}
}
