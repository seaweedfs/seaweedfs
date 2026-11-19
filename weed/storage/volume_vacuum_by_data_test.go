package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// A write or delete that lands between CompactByVolumeData and CommitCompact
// must be carried into the compacted volume by a successful commit.
// CompactByVolumeData never builds the temporary needle map that makeupDiff
// loads the incremental index into, so the commit used to dereference a nil
// v.tmpNm and panic.
func TestCommitCompactAfterCompactByVolumeDataWithConcurrentChange(t *testing.T) {
	kinds := []struct {
		name string
		kind NeedleMapKind
	}{{"memory", NeedleMapInMemory}, {"leveldb", NeedleMapLevelDb}}
	for _, k := range kinds {
		for _, change := range []string{"write", "delete"} {
			kind := k.kind
			t.Run(fmt.Sprintf("%s/%s", k.name, change), func(t *testing.T) {
				testCommitAfterCompactByVolumeData(t, kind, change)
			})
		}
	}
}

func testCommitAfterCompactByVolumeData(t *testing.T, kind NeedleMapKind, change string) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, kind, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer func() { v.Close() }()

	want := map[uint64][]byte{}
	for i := uint64(1); i <= 5; i++ {
		n := newRandomNeedle(i)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		want[i] = n.Data
	}

	if err := v.CompactByVolumeData(nil); err != nil {
		t.Fatalf("CompactByVolumeData: %v", err)
	}

	// The change in the commit window: the index grows past lastCompactIndexOffset.
	switch change {
	case "write":
		n := newRandomNeedle(6)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write 6: %v", err)
		}
		want[6] = n.Data
	case "delete":
		if _, err := v.deleteNeedle2(newEmptyNeedle(3), false); err != nil {
			t.Fatalf("delete 3: %v", err)
		}
		delete(want, 3)
	}

	var commitErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("CommitCompact panicked after CompactByVolumeData with a concurrent %s: %v", change, r)
			}
		}()
		commitErr = v.CommitCompact()
	}()
	if commitErr != nil {
		t.Fatalf("CommitCompact after CompactByVolumeData with a concurrent %s: %v", change, commitErr)
	}

	for i := uint64(1); i <= 6; i++ {
		n := newEmptyNeedle(i)
		_, err := v.readNeedle(n, nil, nil)
		data, live := want[i]
		switch {
		case live && err != nil:
			t.Errorf("needle %d: read after commit: %v", i, err)
		case live && !bytes.Equal(n.Data, data):
			t.Errorf("needle %d: data differs after commit", i)
		case !live && err == nil && i <= 5:
			t.Errorf("needle %d: deleted in the commit window but still readable", i)
		}
	}
}
