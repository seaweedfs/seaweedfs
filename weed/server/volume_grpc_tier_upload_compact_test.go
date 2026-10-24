package weed_server

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// afterCopyBackend runs a hook once the .dat has been copied, before the
// upload handler records the remote file.
type afterCopyBackend struct {
	tierTimestampTestBackend
	afterCopy func()
}

func (b *afterCopyBackend) CopyFile(file *os.File, fn func(progressed int64, percentage float32) error, concurrency int) (string, int64, error) {
	key, size, err := b.tierTimestampTestBackend.CopyFile(file, fn, concurrency)
	if err == nil && b.afterCopy != nil {
		b.afterCopy()
	}
	return key, size, err
}

// A compaction committed after the tier upload copied the .dat, but before the
// upload switched the volume to the remote object, leaves that object with the
// old layout and the volume with the compacted index. The upload must not
// attach it.
func TestTierUploadRefusesADatReplacedByACompactionCommit(t *testing.T) {
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
	for i := uint64(1); i <= 10; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(i), Cookie: 7, Data: bytes.Repeat([]byte{byte(i)}, 1024)}
		n.Checksum = needle.NewCRC(n.Data)
		if _, err := store.WriteVolumeNeedle(1, n, false, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		written[i] = n.Data
	}
	for i := uint64(1); i <= 5; i++ {
		if _, err := store.DeleteVolumeNeedle(1, &needle.Needle{Id: types.Uint64ToNeedleId(i)}); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	v := store.GetVolume(1)
	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := store.MarkVolumeReadonly(1, false, false); err != nil {
		t.Fatalf("mark read-only: %v", err)
	}

	committed := false
	b.afterCopy = func() {
		if err := v.CommitCompact(); err != nil {
			t.Errorf("commit: %v", err)
			return
		}
		committed = true
	}
	vs := &VolumeServer{store: store}
	err := vs.VolumeTierMoveDatToRemote(&volume_server_pb.VolumeTierMoveDatToRemoteRequest{
		VolumeId: 1, DestinationBackendName: tierTimestampTestBackendName,
	}, &discardServerStream[volume_server_pb.VolumeTierMoveDatToRemoteResponse]{})
	if !committed {
		t.Fatal("the compaction did not commit between the copy and the attach") // positive control
	}
	if err == nil {
		t.Errorf("the upload attached a remote copy of a .dat that a compaction had replaced")
	}
	if v.HasRemoteFile() {
		t.Errorf("the volume is tiered to the stale remote copy")
	}
	if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Errorf("the abandoned remote copy was left behind: %v", entries)
	}
	if !util.FileExists(filepath.Join(v.FileName(".dat"))) {
		t.Fatalf("the local .dat is gone")
	}
	for i := uint64(6); i <= 10; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(i), Cookie: 7}
		if _, err := store.ReadVolumeNeedle(1, n, nil, nil); err != nil {
			t.Errorf("needle %d does not read after the upload: %v", i, err)
		} else if !bytes.Equal(n.Data, written[i]) {
			t.Errorf("needle %d reads different data", i)
		}
	}
}
