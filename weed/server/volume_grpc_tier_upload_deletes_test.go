package weed_server

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// tierUploadForDeletes writes 10 needles into volume 1 of a new store, marks it
// read-only as volume.tier.upload does, and moves its .dat to the tier.
func tierUploadForDeletes(t *testing.T, replication string) *storage.Store {
	t.Helper()
	backend.BackendStorages[tierTimestampTestBackendName] = &tierTimestampTestBackend{root: t.TempDir()}
	t.Cleanup(func() { delete(backend.BackendStorages, tierTimestampTestBackendName) })
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	if err := store.AddVolume(1, "", storage.NeedleMapInMemory, replication, "", 0, needle.Version3, 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}
	for i := 1; i <= 10; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(uint64(i)), Cookie: 7, Data: bytes.Repeat([]byte{byte(i)}, 1024)}
		n.Checksum = needle.NewCRC(n.Data)
		if _, err := store.WriteVolumeNeedle(1, n, false, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := store.MarkVolumeReadonly(1, false, false); err != nil {
		t.Fatalf("mark read-only: %v", err)
	}
	vs := &VolumeServer{store: store}
	if err := vs.VolumeTierMoveDatToRemote(&volume_server_pb.VolumeTierMoveDatToRemoteRequest{
		VolumeId: 1, DestinationBackendName: tierTimestampTestBackendName,
	}, &discardServerStream[volume_server_pb.VolumeTierMoveDatToRemoteResponse]{}); err != nil {
		t.Fatalf("tier upload: %v", err)
	}
	return store
}

// A single-copy volume takes deletes as soon as it is tiered, as it would
// after a restart. Before, they were refused ("volume 1 is read only") until
// then, leaving the needles live in the index.
func TestTierUploadAcceptsDeletesOnASingleCopyVolume(t *testing.T) {
	store := tierUploadForDeletes(t, "000")
	if _, err := store.DeleteVolumeNeedle(1, &needle.Needle{Id: 2}); err != nil {
		t.Fatalf("delete on the freshly tiered volume: %v", err)
	}
	if _, err := store.ReadVolumeNeedle(1, &needle.Needle{Id: 2}, nil, nil); err == nil {
		t.Fatalf("the deleted needle still reads")
	}
	if _, err := store.ReadVolumeNeedle(1, &needle.Needle{Id: 3}, nil, nil); err != nil {
		t.Fatalf("a live needle does not read: %v", err)
	}
	n := &needle.Needle{Id: 99, Cookie: 7, Data: []byte("x")}
	n.Checksum = needle.NewCRC(n.Data)
	if _, err := store.WriteVolumeNeedle(1, n, false, false); err == nil {
		t.Fatalf("a write to a tiered volume succeeded")
	}
}

// A replicated volume keeps refusing deletes: the shell copies the uploaded
// replica's .idx and .vif to the others after the upload, and a tombstone
// written before that copy would reach one replica only.
func TestTierUploadKeepsRefusingDeletesOnAReplicatedVolume(t *testing.T) {
	store := tierUploadForDeletes(t, "001")
	if _, err := store.DeleteVolumeNeedle(1, &needle.Needle{Id: 2}); err == nil {
		t.Fatalf("a replicated volume accepted a delete right after the tier upload")
	}
}
