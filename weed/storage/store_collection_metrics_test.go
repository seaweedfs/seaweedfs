package storage

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// The per-collection gauges are only ever set for collections the heartbeat
// still finds on this server. A volume.balance that moves a collection's last
// volume off a server used to leave its read-only count - marked read-only for
// the move, moments before it went - standing on that server until a restart,
// with nothing in volume.list to match it.
func TestCollectHeartbeatClearsMetricsOfDepartedCollection(t *testing.T) {
	stats.VolumeServerReadOnlyVolumeGauge.Reset()
	stats.VolumeServerDiskSizeGauge.Reset()
	t.Cleanup(stats.VolumeServerReadOnlyVolumeGauge.Reset)
	t.Cleanup(stats.VolumeServerDiskSizeGauge.Reset)

	store := newTestStore(t, 1)
	mountTestVolume(t, store.Locations[0], 1, "pics")
	v := store.findVolume(1)
	if v == nil {
		t.Fatal("volume 1 not mounted")
	}
	v.noWriteOrDelete = true

	store.CollectHeartbeat()
	if got := testutil.ToFloat64(stats.VolumeServerReadOnlyVolumeGauge.WithLabelValues("pics", stats.IsReadOnly)); got != 1 {
		t.Fatalf("read-only volumes of pics = %v, want 1", got)
	}

	if err := store.UnmountVolume(1); err != nil {
		t.Fatalf("UnmountVolume: %v", err)
	}
	store.CollectHeartbeat()

	if n := testutil.CollectAndCount(stats.VolumeServerReadOnlyVolumeGauge); n != 0 {
		t.Errorf("%d read-only series left after the collection left the server", n)
	}
	if n := testutil.CollectAndCount(stats.VolumeServerDiskSizeGauge); n != 0 {
		t.Errorf("%d disk size series left after the collection left the server", n)
	}
}

// The counts are per collection and per server, and a server holds far more
// than 255 volumes of one collection.
func TestCollectHeartbeatCountsPast255ReadOnlyVolumes(t *testing.T) {
	stats.VolumeServerReadOnlyVolumeGauge.Reset()
	t.Cleanup(stats.VolumeServerReadOnlyVolumeGauge.Reset)

	store := newTestStore(t, 1)
	const readOnlyVolumes = 300
	for i := 1; i <= readOnlyVolumes; i++ {
		mountTestVolume(t, store.Locations[0], needle.VolumeId(i), "pics")
		store.findVolume(needle.VolumeId(i)).noWriteOrDelete = true
	}

	store.CollectHeartbeat()
	if got := testutil.ToFloat64(stats.VolumeServerReadOnlyVolumeGauge.WithLabelValues("pics", stats.IsReadOnly)); got != readOnlyVolumes {
		t.Errorf("read-only volumes of pics = %v, want %d", got, readOnlyVolumes)
	}
}

func TestCollectErasureCodingHeartbeatClearsMetricsOfDepartedCollection(t *testing.T) {
	stats.VolumeServerDiskSizeGauge.Reset()
	t.Cleanup(stats.VolumeServerDiskSizeGauge.Reset)

	store, _, vid, collection, plant := setupECStoreWithMixedDisks(t)
	plant()
	if err := store.MountEcShards(collection, vid, 0, ""); err != nil {
		t.Fatalf("MountEcShards: %v", err)
	}

	store.CollectErasureCodingHeartbeat()
	if got := testutil.ToFloat64(stats.VolumeServerDiskSizeGauge.WithLabelValues(collection, "ec")); got == 0 {
		t.Fatalf("ec disk size of %s = 0, want the mounted shard's size", collection)
	}

	if err := store.UnmountEcShards(vid, erasure_coding.ShardId(0), 0); err != nil {
		t.Fatalf("UnmountEcShards: %v", err)
	}
	store.CollectErasureCodingHeartbeat()

	if n := testutil.CollectAndCount(stats.VolumeServerDiskSizeGauge); n != 0 {
		t.Errorf("%d disk size series left after the collection's shards left the server", n)
	}
}
