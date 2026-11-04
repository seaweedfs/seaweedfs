package storage

import (
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

type shards = []erasure_coding.ShardId

// reconstructedIntervals reads ec_reconstructed_interval_total; this package
// has no parallel tests, so a test's before/after difference is its own.
func reconstructedIntervals() (success, failure float64) {
	c := stats.VolumeServerECReconstructedIntervalCounter
	return testutil.ToFloat64(c.WithLabelValues("success")), testutil.ToFloat64(c.WithLabelValues("failure"))
}

// readEcNeedle writes a one-needle EC volume, overwrites the needle's range on
// the corrupt shards, mounts every shard but missing (with no location for
// them, so the read reconstructs), reads the needle, and returns how far each
// counter moved.
func readEcNeedle(t *testing.T, vid needle.VolumeId, missing, corrupt shards) (n *needle.Needle, err error, dSuccess, dFailure float64) {
	t.Helper()
	store := newTestStore(t, 1)
	_, n = writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	skip := map[erasure_coding.ShardId]bool{}
	for _, sid := range missing {
		skip[sid] = true
	}
	for sid := erasure_coding.ShardId(0); sid < erasure_coding.TotalShardsCount; sid++ {
		if skip[sid] {
			continue
		}
		if err := store.MountEcShards("", vid, sid, ""); err != nil {
			t.Fatalf("mount shard %d: %v", sid, err)
		}
	}
	ecVolume, _ := store.Locations[0].FindEcVolume(vid)
	ecVolume.EncodeTsNs = 1700000000123456789
	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	_, off := ecVolume.IntervalToShardIdAndOffset(intervals[0])
	for _, sid := range corrupt {
		corruptEcShardRange(t, ecVolume, sid, off, int64(intervals[0].Size))
	}
	seedShardLocations(ecVolume, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1))
	ecVolume.ShardLocationsLock.Lock()
	for sid := range skip {
		delete(ecVolume.ShardLocations, sid)
	}
	ecVolume.ShardLocationsLock.Unlock()

	s0, f0 := reconstructedIntervals()
	got := &needle.Needle{Id: n.Id}
	if _, err = store.ReadEcShardNeedle(vid, got, nil); err == nil && string(got.Data) != string(n.Data) {
		t.Fatal("read returned different bytes")
	}
	s1, f1 := reconstructedIntervals()
	return n, err, s1 - s0, f1 - f0
}

// What the counter means, case by case. A direct read isn't counted, nor is a
// corrupt interval read directly (that goes to the repair path). A missing
// shard counts one success when the reconstruction produced data and one
// failure when it couldn't (5 shards left). success is not needle validation:
// rebuilt from corrupt parity, the interval counts success while the read fails.
func TestEcReconstructedIntervalCounter(t *testing.T) {
	parity := shards{10, 11, 12, 13}
	for _, c := range []struct {
		vid              needle.VolumeId
		missing, corrupt shards
		fails            bool
		s, f             float64
	}{
		{17, nil, nil, false, 0, 0},
		{18, shards{0}, nil, false, 1, 0},
		{19, shards{0, 6, 7, 8, 9, 10, 11, 12, 13}, nil, true, 0, 1},
		{20, nil, append(shards{0}, parity...), true, 0, 0},
		{21, shards{0}, parity, true, 1, 0},
	} {
		if _, err, s, f := readEcNeedle(t, c.vid, c.missing, c.corrupt); (err != nil) != c.fails || s != c.s || f != c.f {
			t.Errorf("missing %v, corrupt %v: err %v, success +%v, failure +%v; want failed=%v, +%v, +%v", c.missing, c.corrupt, err, s, f, c.fails, c.s, c.f)
		}
	}
}

// The line binds a reconstruction to the volume's generation and the needle,
// at V(3) and not below.
func TestEcReconstructedIntervalLogLine(t *testing.T) {
	flag.Set("logtostderr", "true")
	t.Cleanup(func() { flag.Set("vmodule", ""); flag.Set("logtostderr", "false") })
	markers := func(vmodule string, vid needle.VolumeId, missing shards) (n *needle.Needle, lines []string) {
		flag.Set("vmodule", vmodule)
		r, w, _ := os.Pipe()
		stderr, out := os.Stderr, make(chan []byte, 1)
		restore := sync.OnceFunc(func() { os.Stderr = stderr; w.Close() })
		t.Cleanup(restore)
		go func() { b, _ := io.ReadAll(r); r.Close(); out <- b }()
		os.Stderr = w
		n, _, _, _ = readEcNeedle(t, vid, missing, nil)
		restore()
		for _, line := range strings.Split(string(<-out), "\n") {
			if strings.Contains(line, "ec reconstructed interval:") {
				lines = append(lines, line)
			}
		}
		return n, lines
	}
	prefix := func(vid int, n *needle.Needle) string {
		return "ec reconstructed interval: volume=" + needle.VolumeId(vid).String() + " generation=1700000000123456789 needle=" + n.Id.String() + " shard=0 "
	}
	if n, lines := markers("store_ec=3", 22, shards{0}); len(lines) != 1 || !strings.HasSuffix(lines[0], prefix(22, n)+"result=success error=<nil>") {
		t.Errorf("success line %q", lines)
	}
	if n, lines := markers("store_ec=3", 23, shards{0, 6, 7, 8, 9, 10, 11, 12, 13}); len(lines) != 1 ||
		!strings.Contains(lines[0], prefix(23, n)+"result=failure error=") || strings.HasSuffix(lines[0], "error=<nil>") {
		t.Errorf("failure line %q", lines)
	}
	if _, lines := markers("store_ec=2", 24, shards{0}); len(lines) != 0 {
		t.Errorf("at V(2) the line was logged: %q", lines)
	}
}
