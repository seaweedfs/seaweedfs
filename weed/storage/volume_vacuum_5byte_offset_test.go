//go:build 5BytesOffset

package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A write replayed by makeupDiff must record all offset bytes, including when
// the new offset lands in a different 32 GiB range than the old one.
func TestConcurrentWriteCrossesOffsetBoundary(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 784, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.Version3, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	write := func(key uint64, size int) *needle.Needle {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(key), Data: bytes.Repeat([]byte("x"), size)}
		n.Checksum = needle.NewCRC(n.Data)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatal(err)
		}
		return n
	}
	write(1, 32)
	if err := v.DataBackend.Truncate(64 << 30); err != nil {
		t.Fatal(err)
	}
	if err := v.CompactByIndex(nil); err != nil {
		t.Fatal(err)
	}
	written := write(342511246, 113)
	before := newEmptyNeedle(uint64(written.Id))
	if _, err := v.readNeedle(before, nil, nil); err != nil || !bytes.Equal(before.Data, written.Data) {
		t.Fatalf("read before commit: %v", err)
	}
	cpd, err := os.Stat(v.FileName(".cpd"))
	if err != nil {
		t.Fatal(err)
	}
	expectedOffset := cpd.Size()
	if err := v.CommitCompact(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	indexFile, err := os.Open(v.FileName(".idx"))
	if err != nil {
		t.Fatal(err)
	}
	defer indexFile.Close()
	var actualOffset int64
	if err := idx.WalkIndexFile(indexFile, 0, func(key types.NeedleId, offset types.Offset, _ types.Size) error {
		if key == written.Id {
			actualOffset = offset.ToActualOffset()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := newEmptyNeedle(uint64(written.Id))
	_, readErr := v.readNeedle(after, nil, nil)
	t.Logf("stored offset=%d, correct offset=%d, excess=%d, post-commit read=%v", actualOffset, expectedOffset, actualOffset-expectedOffset, readErr)
	if actualOffset != expectedOffset || readErr != nil || !bytes.Equal(after.Data, written.Data) {
		physical := new(needle.Needle)
		if err := physical.ReadData(v.DataBackend, expectedOffset, written.Size, v.Version()); err == nil && bytes.Equal(physical.Data, written.Data) {
			t.Logf("body intact at offset %d despite the bad index entry", expectedOffset)
		}
		t.Fatalf("replayed write lost: stored offset=%d, want=%d, read=%v", actualOffset, expectedOffset, readErr)
	}

	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	if err := v.CommitCompact(); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if _, err := v.readNeedle(newEmptyNeedle(uint64(written.Id)), nil, nil); err != nil {
		t.Fatalf("needle lost after second vacuum: %v", err)
	}
	if _, present := v.nm.Get(written.Id); !present {
		t.Fatal("needle missing from index after second vacuum")
	}
}
