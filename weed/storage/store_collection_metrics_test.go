package storage

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// fillTestVolume writes one needle and dates the volume now. A volume with no
// content never expires, and the needle carries no append time of its own,
// which would leave the volume dated to the epoch.
func fillTestVolume(t *testing.T, v *Volume) {
	t.Helper()
	if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(v.Id)), true, false, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	v.lastModifiedTsSeconds = uint64(time.Now().Unix())
}

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
	mountTestVolume(t, store.Locations[0], 1, "pics").noWriteOrDelete = true

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

// A volume being deleted for expiry is already gone as far as the gauges are
// concerned, so the heartbeat that drops the collection's last one has to take
// its series along rather than leave them standing for another pass.
func TestCollectHeartbeatClearsMetricsWhenTheLastVolumeExpires(t *testing.T) {
	stats.VolumeServerReadOnlyVolumeGauge.Reset()
	stats.VolumeServerDiskSizeGauge.Reset()
	t.Cleanup(stats.VolumeServerReadOnlyVolumeGauge.Reset)
	t.Cleanup(stats.VolumeServerDiskSizeGauge.Reset)

	store := newTestStore(t, 1)
	store.SetVolumeSizeLimit(30 << 30)
	v := mountTestVolume(t, store.Locations[0], 1, "pics")
	v.Ttl = &needle.TTL{Count: 1, Unit: needle.Minute}
	fillTestVolume(t, v)

	store.CollectHeartbeat()
	if got := testutil.ToFloat64(stats.VolumeServerDiskSizeGauge.WithLabelValues("pics", "normal")); got == 0 {
		t.Fatal("disk size of pics = 0, want the written needle")
	}

	v.lastModifiedTsSeconds = uint64(time.Now().Add(-time.Hour).Unix())
	store.CollectHeartbeat()

	if store.findVolume(1) != nil {
		t.Fatal("the expired volume outlived the heartbeat")
	}
	if n := testutil.CollectAndCount(stats.VolumeServerReadOnlyVolumeGauge); n != 0 {
		t.Errorf("%d read-only series left after the last volume expired", n)
	}
	if n := testutil.CollectAndCount(stats.VolumeServerDiskSizeGauge); n != 0 {
		t.Errorf("%d disk size series left after the last volume expired", n)
	}
}

// A collection that loses one volume to expiry and keeps another must report
// what is left, not what is left minus what went.
func TestCollectHeartbeatSizesOnlySurvivingVolumes(t *testing.T) {
	stats.VolumeServerDiskSizeGauge.Reset()
	t.Cleanup(stats.VolumeServerDiskSizeGauge.Reset)

	store := newTestStore(t, 2)
	store.SetVolumeSizeLimit(30 << 30)
	// One volume per location, so the surviving one is always scanned first.
	surviving := mountTestVolume(t, store.Locations[0], 1, "pics")
	fillTestVolume(t, surviving)
	expiring := mountTestVolume(t, store.Locations[1], 2, "pics")
	expiring.Ttl = &needle.TTL{Count: 1, Unit: needle.Minute}
	fillTestVolume(t, expiring)
	// Both volumes carry deleted bytes, which are totalled the same way.
	for _, v := range []*Volume{surviving, expiring} {
		n := newRandomNeedle(uint64(v.Id) + 100)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle: %v", err)
		}
		if _, err := v.deleteNeedle2(n); err != nil {
			t.Fatalf("delete needle: %v", err)
		}
	}

	heartbeat := store.CollectHeartbeat()
	var survivingSize, survivingDeleted uint64
	for _, m := range heartbeat.Volumes {
		if m.Id == 1 {
			survivingSize, survivingDeleted = m.Size, m.DeletedByteCount
		}
	}
	if survivingSize == 0 || survivingDeleted == 0 {
		t.Fatalf("the surviving volume reported size %d and %d deleted bytes, want both", survivingSize, survivingDeleted)
	}

	expiring.lastModifiedTsSeconds = uint64(time.Now().Add(-time.Hour).Unix())
	store.CollectHeartbeat()

	if store.findVolume(1) == nil {
		t.Fatal("the volume without a ttl was deleted")
	}
	if got := testutil.ToFloat64(stats.VolumeServerDiskSizeGauge.WithLabelValues("pics", "normal")); got != float64(survivingSize) {
		t.Errorf("disk size of pics = %v, want %d, the volume still here", got, survivingSize)
	}
	if got := testutil.ToFloat64(stats.VolumeServerDiskSizeGauge.WithLabelValues("pics", "deleted_bytes")); got != float64(survivingDeleted) {
		t.Errorf("deleted bytes of pics = %v, want %d, the volume still here", got, survivingDeleted)
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
