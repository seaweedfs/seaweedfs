package weed_server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ecVolumeWithDeletedNeedle mounts an EC volume holding one needle that has
// already been deleted. A runtime EC delete is recorded in the .ecj journal
// rather than by rewriting the sealed .ecx, and the load reads .ecj back into
// the in-memory deleted set, so locating the needle is enough to see it is
// gone -- no shard payload is needed.
func ecVolumeWithDeletedNeedle(t *testing.T, vid needle.VolumeId, id types.NeedleId) *storage.Store {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, vid.String())

	entry := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(entry[0:types.NeedleIdSize], id)
	types.OffsetToBytes(entry[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], types.Offset{})
	types.SizeToBytes(entry[types.NeedleIdSize+types.OffsetSize:], types.Size(100))

	journal := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(journal, id)

	for name, content := range map[string][]byte{
		base + ".ecx":  entry,
		base + ".ecj":  journal,
		base + ".ec00": make([]byte, 8),
		base + ".vif":  {},
	} {
		if err := os.WriteFile(name, content, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	location := storage.NewDiskLocation(dir, 100, util.MinFreeSpace{}, dir, types.HardDriveType, nil, stats.DiskIOProbeConfig{})
	// Close stops the location's disk-space goroutine and releases the mounted
	// EC volume's file handles. Registered before the mount so it also covers a
	// failure there.
	t.Cleanup(location.Close)
	if _, err := location.LoadEcShard("", vid, erasure_coding.ShardId(0)); err != nil {
		t.Fatalf("load ec shard: %v", err)
	}

	state, err := storage.NewState(dir)
	if err != nil {
		t.Fatalf("new store state: %v", err)
	}
	return &storage.Store{Locations: []*storage.DiskLocation{location}, State: state}
}

// Deleting a needle that is already gone is not a failure. The non-EC branch
// has always answered StatusNotModified; the EC branch used to answer 500,
// so a duplicate or replayed delete was booked as a server error.
func TestBatchDelete_AlreadyDeletedEcNeedleIsNotAnError(t *testing.T) {
	const vid = needle.VolumeId(7)
	const needleId = types.NeedleId(1)

	vs := &VolumeServer{
		store: ecVolumeWithDeletedNeedle(t, vid, needleId),
		guard: security.NewGuard(nil, "", 0, "", 0),
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})

	resp, err := vs.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{"7,0100000000"},
		SkipCookieCheck: true,
	})
	if err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("got %d results, want 1: %+v", len(resp.Results), resp.Results)
	}

	got := resp.Results[0]
	if got.Status != http.StatusNotModified {
		t.Fatalf("status = %d (error %q), want %d — an already-deleted needle is not a server error",
			got.Status, got.Error, http.StatusNotModified)
	}
	if got.Error != "" {
		t.Fatalf("error = %q, want empty", got.Error)
	}
}

// The HTTP delete handler has the same asymmetry, and one extra consequence:
// writeDeleteResult counts a failed delete in VolumeServerFileWriteFailures, so
// a replayed delete of an EC needle used to inflate a failure metric. The
// non-EC path answers 404 from its ReadVolumeNeedle pre-check instead.
func TestDeleteHandler_AlreadyDeletedEcNeedleIsNotAWriteFailure(t *testing.T) {
	const vid = needle.VolumeId(8)
	const needleId = types.NeedleId(1)

	vs := &VolumeServer{
		store: ecVolumeWithDeletedNeedle(t, vid, needleId),
		guard: security.NewGuard(nil, "", 0, "", 0),
	}

	before := testutil.ToFloat64(stats.VolumeServerFileWriteFailures)

	w := httptest.NewRecorder()
	vs.DeleteHandler(w, httptest.NewRequest(http.MethodDelete, "/8,0100000000", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d (%s), want %d — an already-deleted needle is not a server error",
			w.Code, w.Body.String(), http.StatusNotFound)
	}
	if after := testutil.ToFloat64(stats.VolumeServerFileWriteFailures); after != before {
		t.Fatalf("VolumeServerFileWriteFailures moved %v -> %v; a delete that found nothing to do is not a write failure", before, after)
	}
}
