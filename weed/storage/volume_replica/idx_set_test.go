package volume_replica

import (
	"bytes"
	"fmt"
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

type replicaRows struct {
	loc        wdclient.Location
	live       map[types.NeedleId]uint64
	tombstones map[types.NeedleId]uint64
}

// missedDeleteViews builds views and index maps from replicas, keeping each
// row's AppendAtNs for the fake meta reader.
func missedDeleteViews(t *testing.T, replicas []replicaRows) ([]replicaLiveIdx, []*needle_map.MemDb, func(int, needle_map.NeedleValue) (uint64, error)) {
	t.Helper()
	ts := make(map[string]uint64)
	views := make([]replicaLiveIdx, 0, len(replicas))
	dbs := make([]*needle_map.MemDb, 0, len(replicas))
	for _, r := range replicas {
		db := needle_map.NewMemDb()
		t.Cleanup(db.Close)
		offset := int64(8)
		for id, at := range r.live {
			db.Set(id, types.ToOffset(offset), types.Size(40))
			ts[fmt.Sprintf("%s/%d", r.loc.Url, id)] = at
			offset += 64
		}
		for id, at := range r.tombstones {
			db.Set(id, types.ToOffset(offset), types.TombstoneFileSize)
			ts[fmt.Sprintf("%s/%d", r.loc.Url, id)] = at
			offset += 64
		}
		live, err := LiveNeedlesFromIndex(db)
		if err != nil {
			t.Fatal(err)
		}
		views = append(views, replicaLiveIdx{loc: r.loc, live: live})
		dbs = append(dbs, db)
	}
	return views, dbs, func(i int, nv needle_map.NeedleValue) (uint64, error) {
		at, ok := ts[fmt.Sprintf("%s/%d", views[i].loc.Url, nv.Key)]
		if !ok {
			return 0, fmt.Errorf("no row for %d on %s", nv.Key, views[i].loc.Url)
		}
		return at, nil
	}
}

func findMissedDeletes(views []replicaLiveIdx, dbs []*needle_map.MemDb, appendAtNs func(int, needle_map.NeedleValue) (uint64, error)) error {
	lives := make([][]types.NeedleId, len(views))
	for i := range views {
		lives[i] = views[i].live
	}
	missed, err := MissedDeletes(lives, dbs, appendAtNs)
	if err != nil {
		return err
	}
	for i := range views {
		views[i].missed = missed[i]
	}
	return nil
}

func TestFindMissedDeletes_NewerTombstoneSkipsLaggingReplica(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	views, dbs, appendAtNs := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 10, 2: 20, 3: 30}},
		{loc: b, live: map[types.NeedleId]uint64{1: 11, 3: 31}, tombstones: map[types.NeedleId]uint64{2: 50}},
	})
	if err := findMissedDeletes(views, dbs, appendAtNs); err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 1 || views[0].missed[0] != 2 || len(views[1].missed) != 0 {
		t.Fatalf("missed a=%v b=%v, want a=[2] b=[]", views[0].missed, views[1].missed)
	}
	var buf bytes.Buffer
	got, err := chooseSourceFromLiveNeedles(7, views, a.Url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != b.Url {
		t.Fatalf("source %s, want %s: the preferred replica still holds a deleted needle", got.Url, b.Url)
	}
	if !strings.Contains(buf.String(), "missed 1 delete(s)") {
		t.Fatalf("log %q must report the missed delete", buf.String())
	}
}

func TestFindMissedDeletes_WriteAfterTombstoneStillRefuses(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	views, dbs, appendAtNs := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 10, 2: 90}},
		{loc: b, live: map[types.NeedleId]uint64{1: 11}, tombstones: map[types.NeedleId]uint64{2: 50}},
	})
	if err := findMissedDeletes(views, dbs, appendAtNs); err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 0 {
		t.Fatalf("a write newer than the tombstone is not a missed delete: %v", views[0].missed)
	}
	if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
		t.Fatalf("got %v, want a no-union refusal", err)
	}
}

func TestFindMissedDeletes_AbsentElsewhereStillRefuses(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	views, dbs, appendAtNs := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 10, 2: 20}},
		{loc: b, live: map[types.NeedleId]uint64{1: 11}},
	})
	if err := findMissedDeletes(views, dbs, appendAtNs); err != nil {
		t.Fatal(err)
	}
	if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
		t.Fatalf("got %v, want a no-union refusal", err)
	}
}

func TestFindMissedDeletes_NoAppendAtNsStillRefuses(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	views, dbs, appendAtNs := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 0, 2: 0}},
		{loc: b, live: map[types.NeedleId]uint64{1: 0}, tombstones: map[types.NeedleId]uint64{2: 0}},
	})
	if err := findMissedDeletes(views, dbs, appendAtNs); err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 0 {
		t.Fatalf("without AppendAtNs nothing is proven: %v", views[0].missed)
	}
}

func TestFindMissedDeletes_EveryReplicaMissedOneRefuses(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	views, dbs, appendAtNs := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 10, 2: 20}, tombstones: map[types.NeedleId]uint64{4: 60}},
		{loc: b, live: map[types.NeedleId]uint64{1: 11, 4: 41}, tombstones: map[types.NeedleId]uint64{2: 50}},
	})
	if err := findMissedDeletes(views, dbs, appendAtNs); err != nil {
		t.Fatal(err)
	}
	_, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "every replica missed a delete") {
		t.Fatalf("got %v, want refusal when no replica is clean", err)
	}
}

func TestFindMissedDeletes_ThreeReplicasPicksTheOneWithTombstones(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	c := wdclient.Location{Url: "127.0.0.1:8082"}
	views, dbs, appendAtNs := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 10, 2: 20}},
		{loc: b, live: map[types.NeedleId]uint64{1: 11, 2: 21}},
		{loc: c, live: map[types.NeedleId]uint64{1: 12}, tombstones: map[types.NeedleId]uint64{2: 50}},
	})
	if err := findMissedDeletes(views, dbs, appendAtNs); err != nil {
		t.Fatal(err)
	}
	got, err := chooseSourceFromLiveNeedles(7, views, b.Url, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != c.Url {
		t.Fatalf("source %s, want %s", got.Url, c.Url)
	}
}

func TestFindMissedDeletes_MetaReadErrorAborts(t *testing.T) {
	a := wdclient.Location{Url: "127.0.0.1:8080"}
	b := wdclient.Location{Url: "127.0.0.1:8081"}
	views, dbs, _ := missedDeleteViews(t, []replicaRows{
		{loc: a, live: map[types.NeedleId]uint64{1: 10, 2: 20}},
		{loc: b, live: map[types.NeedleId]uint64{1: 11}, tombstones: map[types.NeedleId]uint64{2: 50}},
	})
	failing := func(int, needle_map.NeedleValue) (uint64, error) {
		return 0, fmt.Errorf("unavailable")
	}
	if err := findMissedDeletes(views, dbs, failing); err == nil {
		t.Fatal("a meta read error must abort")
	}
}

func TestSubtractNeedleIds(t *testing.T) {
	got := subtractNeedleIds([]types.NeedleId{1, 2, 3, 5, 8}, []types.NeedleId{0, 2, 5, 6, 9})
	want := []types.NeedleId{1, 3, 8}
	if !EqualLiveNeedleSets(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
