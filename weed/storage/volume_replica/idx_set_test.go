package volume_replica

import (
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func TestEqualLiveNeedleSets_SameFileCountDifferentIds(t *testing.T) {
	a := needle_map.NewMemDb()
	b := needle_map.NewMemDb()
	defer a.Close()
	defer b.Close()

	// Equal cardinality, different ids — FileCount would match, live sets must not.
	a.Set(types.NeedleId(1), types.ToOffset(8), types.Size(100))
	a.Set(types.NeedleId(2), types.ToOffset(16), types.Size(100))
	a.Set(types.NeedleId(3), types.ToOffset(24), types.Size(100))
	b.Set(types.NeedleId(1), types.ToOffset(8), types.Size(100))
	b.Set(types.NeedleId(2), types.ToOffset(16), types.Size(100))
	b.Set(types.NeedleId(4), types.ToOffset(24), types.Size(100))

	la, err := LiveNeedlesFromIndex(a)
	if err != nil {
		t.Fatal(err)
	}
	lb, err := LiveNeedlesFromIndex(b)
	if err != nil {
		t.Fatal(err)
	}
	if EqualLiveNeedleSets(la, lb) {
		t.Fatalf("equal FileCount with different ids must not match: a=%v b=%v", la, lb)
	}
}

func TestEqualLiveNeedleSets_VacuumOffsetsIgnored(t *testing.T) {
	a := needle_map.NewMemDb()
	b := needle_map.NewMemDb()
	defer a.Close()
	defer b.Close()

	a.Set(types.NeedleId(10), types.ToOffset(8), types.Size(50))
	a.Set(types.NeedleId(11), types.ToOffset(64), types.Size(80))
	// Vacuum rewrote the same live needles to new offsets.
	b.Set(types.NeedleId(10), types.ToOffset(8), types.Size(50))
	b.Set(types.NeedleId(11), types.ToOffset(200), types.Size(80))

	la, err := LiveNeedlesFromIndex(a)
	if err != nil {
		t.Fatal(err)
	}
	lb, err := LiveNeedlesFromIndex(b)
	if err != nil {
		t.Fatal(err)
	}
	if !EqualLiveNeedleSets(la, lb) {
		t.Fatalf("same ids with different offsets must match: a=%v b=%v", la, lb)
	}
}

func TestEqualLiveNeedleSets_SizeMismatchIgnored(t *testing.T) {
	a := needle_map.NewMemDb()
	b := needle_map.NewMemDb()
	defer a.Close()
	defer b.Close()

	a.Set(types.NeedleId(10), types.ToOffset(8), types.Size(50))
	b.Set(types.NeedleId(10), types.ToOffset(8), types.Size(74))

	la, err := LiveNeedlesFromIndex(a)
	if err != nil {
		t.Fatal(err)
	}
	lb, err := LiveNeedlesFromIndex(b)
	if err != nil {
		t.Fatal(err)
	}
	if !EqualLiveNeedleSets(la, lb) {
		t.Fatalf("same NeedleId with different Size must match: a=%v b=%v", la, lb)
	}
}

func TestLiveNeedlesFromIndex_SkipsDeleted(t *testing.T) {
	db := needle_map.NewMemDb()
	defer db.Close()
	db.Set(types.NeedleId(1), types.ToOffset(8), types.Size(40))
	db.Set(types.NeedleId(2), types.ToOffset(16), types.TombstoneFileSize)
	db.Set(types.NeedleId(3), types.ToOffset(24), types.Size(0))

	live, err := LiveNeedlesFromIndex(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].Id != 1 {
		t.Fatalf("live=%v, want only id=1", live)
	}
}

func TestSelectSourceIfIndexesMatch_SingleReplica(t *testing.T) {
	loc := wdclient.Location{Url: "127.0.0.1:8080"}
	got, err := SelectSourceIfIndexesMatch(nil, needle.VolumeId(7), "", []wdclient.Location{loc}, "other", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != loc.Url {
		t.Fatalf("got %+v, want %+v", got, loc)
	}
}

func TestSelectSourceIfIndexesMatch_NoReplicas(t *testing.T) {
	_, err := SelectSourceIfIndexesMatch(nil, needle.VolumeId(7), "", nil, "", io.Discard)
	if err == nil {
		t.Fatal("expected error for empty replica list")
	}
}
