package volume_replica

import (
	"bytes"
	"io"
	"strings"
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
	if len(live) != 1 || live[0] != 1 {
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

func TestChooseSourceFromLiveNeedles_SuccessContainsConsistent(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	live := []types.NeedleId{1, 2}
	var buf bytes.Buffer
	got, err := chooseSourceFromLiveNeedles(7, []replicaLiveIdx{
		{loc: a, live: live},
		{loc: b, live: append([]types.NeedleId(nil), live...)},
	}, "", &buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != a.Url {
		t.Fatalf("got %+v, want %+v", got, a)
	}
	if !strings.Contains(buf.String(), "replicas are consistent") {
		t.Fatalf("success log %q must contain %q", buf.String(), "replicas are consistent")
	}
	wantLine := "volume 7: all 2 data replicas are consistent (matching live idx sets, 2 needles)"
	if !strings.Contains(buf.String(), wantLine) {
		t.Fatalf("success log %q must contain %q", buf.String(), wantLine)
	}
}

func TestChooseSourceFromLiveNeedles_EmptyVsLiveRefuses(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	_, err := chooseSourceFromLiveNeedles(7, []replicaLiveIdx{
		{loc: a, live: []types.NeedleId{1}},
		{loc: b, live: nil}, // empty / superblock stub
	}, "", io.Discard)
	if err == nil {
		t.Fatal("empty live-idx vs non-empty must error, not skip")
	}
	if strings.Contains(err.Error(), "skipping") {
		t.Fatalf("must refuse, not skip: %v", err)
	}
}

func TestChooseSourceFromLiveNeedles_AllEmpty(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	_, err := chooseSourceFromLiveNeedles(7, []replicaLiveIdx{
		{loc: a, live: nil},
		{loc: b, live: nil},
	}, "", io.Discard)
	if err == nil {
		t.Fatal("all empty live-idx must error")
	}
	if !strings.Contains(err.Error(), "no data-bearing replicas") {
		t.Fatalf("got %v, want no data-bearing replicas", err)
	}
}

func TestRequireCapacitySource_ThreeLocationsSourceFromCapacity(t *testing.T) {
	fat := wdclient.Location{Url: "fat:8080"}
	cap1 := wdclient.Location{Url: "cap1:8080"}
	cap2 := wdclient.Location{Url: "cap2:8080"}
	filtered := []wdclient.Location{cap1, cap2}

	got, err := RequireCapacitySource(1, cap1, filtered)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != cap1.Url {
		t.Fatalf("got %+v, want capacity source %+v", got, cap1)
	}

	_, err = RequireCapacitySource(1, fat, filtered)
	if err == nil {
		t.Fatal("must refuse encode from first fat replica")
	}
}
