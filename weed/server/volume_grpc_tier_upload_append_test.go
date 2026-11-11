package weed_server

import (
	"bytes"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A write acknowledged after the tier upload copied the .dat is not in the
// copy. The upload must not switch the volume to that copy or remove the local
// .dat that holds the write.
func TestTierUploadRefusesADatWrittenDuringTheCopy(t *testing.T) {
	root := t.TempDir()
	b := &afterCopyBackend{tierTimestampTestBackend: tierTimestampTestBackend{root: root}}
	backend.BackendStorages[tierTimestampTestBackendName] = b
	t.Cleanup(func() { delete(backend.BackendStorages, tierTimestampTestBackendName) })

	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	if err := store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}
	written := map[uint64][]byte{}
	write := func(i uint64) error {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(i), Cookie: 7, Data: bytes.Repeat([]byte{byte(i)}, 1024)}
		n.Checksum = needle.NewCRC(n.Data)
		if _, err := store.WriteVolumeNeedle(1, n, false, false); err != nil {
			return err
		}
		written[i] = n.Data
		return nil
	}
	for i := uint64(1); i <= 5; i++ {
		if err := write(i); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	v := store.GetVolume(1)

	appended := false
	b.afterCopy = func() {
		if err := write(6); err != nil {
			t.Errorf("append after the copy: %v", err)
			return
		}
		appended = true
	}
	vs := &VolumeServer{store: store}
	err := vs.VolumeTierMoveDatToRemote(&volume_server_pb.VolumeTierMoveDatToRemoteRequest{
		VolumeId: 1, DestinationBackendName: tierTimestampTestBackendName,
	}, &discardServerStream[volume_server_pb.VolumeTierMoveDatToRemoteResponse]{})
	if !appended {
		t.Fatal("no write was acknowledged between the copy and the attach") // positive control
	}
	if err == nil {
		t.Errorf("the upload attached a remote copy that misses an acknowledged write")
	}
	if v.HasRemoteFile() {
		t.Errorf("the volume is tiered to the stale remote copy")
	}
	if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Errorf("the abandoned remote copy was left behind: %v", entries)
	}
	if !util.FileExists(v.FileName(".dat")) {
		t.Fatalf("the local .dat is gone")
	}
	for i := uint64(1); i <= 6; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(i), Cookie: 7}
		if _, err := store.ReadVolumeNeedle(1, n, nil, nil); err != nil {
			t.Errorf("needle %d does not read after the upload: %v", i, err)
		} else if !bytes.Equal(n.Data, written[i]) {
			t.Errorf("needle %d reads different data", i)
		}
	}
}
