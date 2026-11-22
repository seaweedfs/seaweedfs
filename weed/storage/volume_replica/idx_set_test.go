package volume_replica

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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

type writeMeta struct {
	cookie, crc  uint32
	lastModified uint64
	appendAtNs   uint64
}

var tombstone = writeMeta{appendAtNs: 200}

type replicaRows struct {
	loc         wdclient.Location
	live        map[types.NeedleId]writeMeta
	deleted     map[types.NeedleId]writeMeta // written, then tombstoned
	zeroDeleted map[types.NeedleId]writeMeta // written, then deleted by an offset-0 row
	bare        []types.NeedleId             // tombstone with no earlier write
	overwritten map[types.NeedleId]writeMeta // an earlier write the live row replaced
	tombstoneNs uint64                       // stamp of the deleted rows' tombstones, if not tombstone's
}

// missedDeleteViews writes each replica's rows as raw .idx bytes, loads them
// with loadIndexHistory and runs MissedDeletes with a fake meta reader.
func missedDeleteViews(t *testing.T, replicas []replicaRows, readErr error) ([]replicaLiveIdx, error) {
	t.Helper()
	metas := make([]map[int64]writeMeta, len(replicas))
	views := make([]replicaLiveIdx, len(replicas))
	var latest, replaced []*needle_map.MemDb
	for i, r := range replicas {
		metas[i] = make(map[int64]writeMeta)
		var data []byte
		offset := int64(8)
		row := func(id types.NeedleId, size types.Size, m writeMeta) {
			data = append(data, needle_map.ToBytes(id, types.ToOffset(offset), size)...)
			metas[i][offset] = m
			offset += 64
		}
		deletedTombstone := tombstone
		if r.tombstoneNs != 0 {
			deletedTombstone.appendAtNs = r.tombstoneNs
		}
		for id, m := range r.deleted {
			row(id, types.Size(40), m)
			row(id, types.TombstoneFileSize, deletedTombstone)
		}
		for id, m := range r.zeroDeleted {
			row(id, types.Size(40), m)
			data = append(data, needle_map.ToBytes(id, types.Offset{}, types.TombstoneFileSize)...)
		}
		for _, id := range r.bare {
			row(id, types.TombstoneFileSize, tombstone)
		}
		for id, m := range r.overwritten {
			row(id, types.Size(40), m)
		}
		for id, m := range r.live {
			row(id, types.Size(40), m)
		}
		db, earlier := needle_map.NewMemDb(), needle_map.NewMemDb()
		t.Cleanup(db.Close)
		t.Cleanup(earlier.Close)
		if err := loadIndexHistory(data, db, earlier); err != nil {
			t.Fatal(err)
		}
		live, err := LiveNeedlesFromIndex(db)
		if err != nil {
			t.Fatal(err)
		}
		views[i] = replicaLiveIdx{loc: r.loc, live: live}
		latest, replaced = append(latest, db), append(replaced, earlier)
	}
	lives := make([][]types.NeedleId, len(views))
	for i := range views {
		lives[i] = views[i].live
	}
	missed, err := MissedDeletes(lives, latest, replaced, func(i int, nv needle_map.NeedleValue) (*volume_server_pb.ReadNeedleMetaResponse, error) {
		if readErr != nil {
			return nil, readErr
		}
		m, ok := metas[i][nv.Offset.ToActualOffset()]
		if !ok {
			return nil, fmt.Errorf("no record at %d on replica %d", nv.Offset.ToActualOffset(), i)
		}
		return &volume_server_pb.ReadNeedleMetaResponse{Cookie: m.cookie, Crc: m.crc, LastModified: m.lastModified, AppendAtNs: m.appendAtNs}, nil
	})
	if err != nil {
		return nil, err
	}
	for i := range views {
		views[i].missed = missed[i]
	}
	return views, nil
}

var (
	replicaA = wdclient.Location{Url: "127.0.0.1:8080"}
	replicaB = wdclient.Location{Url: "127.0.0.1:8081"}
	replicaC = wdclient.Location{Url: "127.0.0.1:8082"}
	write1   = writeMeta{cookie: 0x11, crc: 0xa1, lastModified: 100}
	write2   = writeMeta{cookie: 0x22, crc: 0xa2, lastModified: 100}
	write3   = writeMeta{cookie: 0x33, crc: 0xa3, lastModified: 100}
)

func TestMissedDeletes_SameWriteThenTombstoneSkipsLaggingReplica(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2, 3: write3}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1, 3: write3}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 1 || views[0].missed[0] != 2 || len(views[1].missed) != 0 {
		t.Fatalf("missed a=%v b=%v, want a=[2] b=[]", views[0].missed, views[1].missed)
	}
	var buf bytes.Buffer
	got, err := chooseSourceFromLiveNeedles(7, views, replicaA.Url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != replicaB.Url {
		t.Fatalf("source %s, want %s: the preferred replica still holds a deleted needle", got.Url, replicaB.Url)
	}
	if !strings.Contains(buf.String(), "missed 1 delete(s)") {
		t.Fatalf("log %q must report the missed delete", buf.String())
	}
}

func TestMissedDeletes_DifferentWriteStillRefuses(t *testing.T) {
	for name, rewrite := range map[string]writeMeta{
		"content":      {cookie: 0x22, crc: 0xb2, lastModified: 100},
		"lastModified": {cookie: 0x22, crc: 0xa2, lastModified: 105},
		"cookie":       {cookie: 0x23, crc: 0xa2, lastModified: 100},
	} {
		views, err := missedDeleteViews(t, []replicaRows{
			{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: rewrite}},
			{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(views[0].missed) != 0 {
			t.Fatalf("%s: a write other than the deleted one is not a missed delete: %v", name, views[0].missed)
		}
		if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
			t.Fatalf("%s: got %v, want a no-union refusal", name, err)
		}
	}
}

func TestMissedDeletes_IdenticalRewriteAfterOwnDeleteStillRefuses(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}, deleted: map[types.NeedleId]writeMeta{2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 0 {
		t.Fatalf("A deleted the needle too, so its live copy is a rewrite: %v", views[0].missed)
	}
	if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
		t.Fatalf("got %v, want a no-union refusal", err)
	}
}

func TestMissedDeletes_IdenticalRewriteAfterOffsetZeroDeleteStillRefuses(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}, zeroDeleted: map[types.NeedleId]writeMeta{2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 0 {
		t.Fatalf("A deleted the needle too, so its live copy is a rewrite: %v", views[0].missed)
	}
	if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
		t.Fatalf("got %v, want a no-union refusal", err)
	}
}

func TestMissedDeletes_OverwriteWithSameMetaStillRefuses(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}, overwritten: map[types.NeedleId]writeMeta{2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 0 {
		t.Fatalf("A wrote the needle twice, so its live copy may postdate the delete: %v", views[0].missed)
	}
}

func TestMissedDeletes_LiveWriteStampedAfterTheTombstoneStillRefuses(t *testing.T) {
	for name, stamp := range map[string]uint64{"after": 300, "equal": 200} {
		rewrite := write2
		rewrite.appendAtNs = stamp
		views, err := missedDeleteViews(t, []replicaRows{
			{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: rewrite}},
			{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(views[0].missed) != 0 {
			t.Fatalf("%s: a compacted rewrite stamped no earlier than the delete is not a missed delete: %v", name, views[0].missed)
		}
		if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
			t.Fatalf("%s: got %v, want a no-union refusal", name, err)
		}
	}
}

func TestMissedDeletes_LiveWriteFarFromTheDeletedWriteStillRefuses(t *testing.T) {
	written := uint64(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	for name, tc := range map[string]struct{ rewriteAt, tombstoneAt time.Duration }{
		// A's clock runs an hour behind: the rewrite came 30 minutes after B's delete.
		"skewed clock": {rewriteAt: 30 * time.Minute, tombstoneAt: time.Hour},
		// B's compaction commit stamped a delete made before the rewrite.
		"compaction-stamped tombstone": {rewriteAt: 5 * time.Minute, tombstoneAt: 10 * time.Minute},
	} {
		deletedWrite, rewrite := write2, write2
		deletedWrite.appendAtNs = written
		rewrite.appendAtNs = written + uint64(tc.rewriteAt)
		views, err := missedDeleteViews(t, []replicaRows{
			{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: rewrite}},
			{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: deletedWrite},
				tombstoneNs: written + uint64(tc.tombstoneAt)},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(views[0].missed) != 0 {
			t.Fatalf("%s: a live write appended %v after the deleted one is not the same write: %v", name, tc.rewriteAt, views[0].missed)
		}
	}
}

func TestMissedDeletes_SameWriteStampedApartBySkewStillProves(t *testing.T) {
	written := uint64(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	for name, skew := range map[string]time.Duration{"behind": -20 * time.Second, "ahead": 20 * time.Second} {
		deletedWrite, liveWrite := write2, write2
		deletedWrite.appendAtNs = written
		liveWrite.appendAtNs = uint64(int64(written) + int64(skew))
		views, err := missedDeleteViews(t, []replicaRows{
			{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: liveWrite}},
			{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: deletedWrite},
				tombstoneNs: written + uint64(time.Hour)},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(views[0].missed) != 1 || views[0].missed[0] != 2 {
			t.Fatalf("%s: the same write stamped %v apart must count as a missed delete: %v", name, skew, views[0].missed)
		}
	}
}

func TestMissedDeletes_AbsentElsewhereStillRefuses(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chooseSourceFromLiveNeedles(7, views, "", io.Discard); err == nil || !strings.Contains(err.Error(), "no union") {
		t.Fatalf("got %v, want a no-union refusal", err)
	}
}

func TestMissedDeletes_TombstoneWithoutWriteStillRefuses(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, bare: []types.NeedleId{2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(views[0].missed) != 0 {
		t.Fatalf("a tombstone whose write is gone proves nothing: %v", views[0].missed)
	}
}

func TestMissedDeletes_EveryReplicaMissedOneRefuses(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}, deleted: map[types.NeedleId]writeMeta{3: write3}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1, 3: write3}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = chooseSourceFromLiveNeedles(7, views, "", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "every replica missed a delete") {
		t.Fatalf("got %v, want refusal when no replica is clean", err)
	}
}

func TestMissedDeletes_ThreeReplicasPicksTheOneThatDeleted(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}},
		{loc: replicaC, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := chooseSourceFromLiveNeedles(7, views, replicaB.Url, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got.Url != replicaC.Url {
		t.Fatalf("source %s, want %s", got.Url, replicaC.Url)
	}
}

func TestMissedDeletes_AbsentOnThirdReplicaStillProves(t *testing.T) {
	views, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
		{loc: replicaC, live: map[types.NeedleId]writeMeta{1: write1}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := chooseSourceFromLiveNeedles(7, views, replicaA.Url, io.Discard)
	if err != nil {
		t.Fatalf("B's write then tombstone proves the delete; C holding no row agrees with it: %v", err)
	}
	if got.Url != replicaB.Url {
		t.Fatalf("source %s, want %s", got.Url, replicaB.Url)
	}
}

func TestMissedDeletes_MetaReadErrorAborts(t *testing.T) {
	_, err := missedDeleteViews(t, []replicaRows{
		{loc: replicaA, live: map[types.NeedleId]writeMeta{1: write1, 2: write2}},
		{loc: replicaB, live: map[types.NeedleId]writeMeta{1: write1}, deleted: map[types.NeedleId]writeMeta{2: write2}},
	}, fmt.Errorf("unavailable"))
	if err == nil {
		t.Fatal("a meta read error must abort")
	}
}

func TestLoadIndexHistory(t *testing.T) {
	var data []byte
	row := func(id types.NeedleId, offset int64, size types.Size) {
		data = append(data, needle_map.ToBytes(id, types.ToOffset(offset), size)...)
	}
	row(1, 8, 40)
	row(1, 64, types.TombstoneFileSize)
	row(2, 128, 40)
	row(2, 192, types.TombstoneFileSize)
	row(2, 256, 50)
	row(3, 320, 40)
	row(3, 384, types.TombstoneFileSize)
	row(3, 448, types.TombstoneFileSize)
	row(4, 512, 40)
	row(4, 0, types.TombstoneFileSize)
	row(5, 576, 40)
	row(5, 640, 40)
	row(6, 704, 40)
	row(6, 704, 40)

	latest, replaced := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer latest.Close()
	defer replaced.Close()
	if err := loadIndexHistory(data, latest, replaced); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[types.NeedleId]int64{1: 8, 2: 128, 3: 320, 4: 512, 5: 576} {
		if nv, found := replaced.Get(id); !found || nv.Offset.ToActualOffset() != want {
			t.Fatalf("replaced row of %d = %+v, want offset %d", id, nv, want)
		}
	}
	if _, found := replaced.Get(6); found {
		t.Fatal("a repeated identical row replaces nothing")
	}
	if nv, found := latest.Get(2); !found || nv.Size != 50 {
		t.Fatalf("latest row of 2 = %+v, want the rewrite", nv)
	}
	if _, found := latest.Get(4); found {
		t.Fatal("an offset-0 row drops the key, as LoadFilterFromReaderAt does")
	}
}

func TestSubtractNeedleIds(t *testing.T) {
	got := subtractNeedleIds([]types.NeedleId{1, 2, 3, 5, 8}, []types.NeedleId{0, 2, 5, 6, 9})
	want := []types.NeedleId{1, 3, 8}
	if !EqualLiveNeedleSets(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
