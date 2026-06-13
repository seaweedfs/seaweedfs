package weed_server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc"
)

const tierTestBackendName = "tier_test_local_dir.default"

// tierTestBackend is a fake BackendStorage backed by files in a temp dir. It
// records DeleteFile calls so a test can assert that a shared remote object is
// (or is not) removed.
type tierTestBackend struct {
	root string

	mu      sync.Mutex
	deletes []string
}

func (b *tierTestBackend) ToProperties() map[string]string { return map[string]string{"root": b.root} }

func (b *tierTestBackend) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	return &tierTestBackendFile{backend: b, key: key, tierInfo: tierInfo}
}

func (b *tierTestBackend) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	key = fmt.Sprintf("obj-%d", time.Now().UnixNano())
	dst := filepath.Join(b.root, key)
	out, err := os.Create(dst)
	if err != nil {
		return "", 0, err
	}
	defer out.Close()
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", 0, err
	}
	written, err := io.Copy(out, f)
	if err != nil {
		return "", 0, err
	}
	return key, written, nil
}

func (b *tierTestBackend) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	in, err := os.Open(filepath.Join(b.root, key))
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, err
	}
	written, err := io.Copy(out, in)
	if err != nil {
		out.Close()
		return 0, err
	}
	// mirror the real backends: fsync the .dat before close so the caller can
	// trim the .vif and delete the remote object without risking a torn write.
	if syncErr := out.Sync(); syncErr != nil {
		out.Close()
		return 0, syncErr
	}
	if closeErr := out.Close(); closeErr != nil {
		return 0, closeErr
	}
	return written, nil
}

func (b *tierTestBackend) DeleteFile(key string) error {
	b.mu.Lock()
	b.deletes = append(b.deletes, key)
	b.mu.Unlock()
	return os.Remove(filepath.Join(b.root, key))
}

func (b *tierTestBackend) deleteHistory() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.deletes))
	copy(out, b.deletes)
	return out
}

func (b *tierTestBackend) objectExists(key string) bool {
	_, err := os.Stat(filepath.Join(b.root, key))
	return err == nil
}

type tierTestBackendFile struct {
	backend  *tierTestBackend
	key      string
	tierInfo *volume_server_pb.VolumeInfo
}

func (f *tierTestBackendFile) ReadAt(p []byte, off int64) (int, error) {
	in, err := os.Open(filepath.Join(f.backend.root, f.key))
	if err != nil {
		return 0, err
	}
	defer in.Close()
	return in.ReadAt(p, off)
}
func (f *tierTestBackendFile) WriteAt(p []byte, off int64) (int, error) { panic("not implemented") }
func (f *tierTestBackendFile) Truncate(off int64) error                 { panic("not implemented") }
func (f *tierTestBackendFile) Close() error                             { return nil }
func (f *tierTestBackendFile) Name() string                            { return f.key }
func (f *tierTestBackendFile) Sync() error                             { return nil }
func (f *tierTestBackendFile) GetStat() (int64, time.Time, error) {
	files := f.tierInfo.GetFiles()
	if len(files) == 0 {
		return 0, time.Time{}, fmt.Errorf("remote file info not found")
	}
	return int64(files[0].FileSize), time.Unix(int64(files[0].ModifiedTime), 0), nil
}

// fakeTierStream is a no-op server stream for the tier-download RPC.
type fakeTierStream struct {
	grpc.ServerStream
}

func (s *fakeTierStream) Send(*volume_server_pb.VolumeTierMoveDatFromRemoteResponse) error { return nil }

func newTierTestStore(t *testing.T, dir string) *storage.Store {
	t.Helper()
	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, diskIOProbeConfig)

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	return store
}

// tierUpVolumeOnDisk creates a real local volume in dir, writes a few needles,
// uploads the .dat to the fake backend, rewrites the .vif to remote mode, and
// removes the local .dat — mirroring volume_grpc_tier_upload.go. It returns the
// remote object key and the original local .dat bytes for later comparison.
func tierUpVolumeOnDisk(t *testing.T, dir string, vid needle.VolumeId, b *tierTestBackend) (key string, localDat []byte) {
	t.Helper()
	store := newTierTestStore(t, dir)
	if err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}
	for i := 1; i <= 5; i++ {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(uint64(i))
		n.Data = []byte(fmt.Sprintf("payload-%d-localdisk", i))
		n.Checksum = needle.NewCRC(n.Data)
		if _, err := store.WriteVolumeNeedle(vid, n, true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	v := store.GetVolume(vid)
	if v == nil {
		t.Fatal("volume not found after add")
	}
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	if !ok {
		t.Fatalf("expected on-disk backend before tier-up, got %T", v.DataBackend)
	}
	datPath := v.FileName(".dat")
	key, size, err := b.CopyFile(diskFile.File, nil)
	if err != nil {
		t.Fatalf("upload to fake backend: %v", err)
	}

	bType, bId := backend.BackendNameToTypeId(tierTestBackendName)
	v.GetVolumeInfo().Files = append(v.GetVolumeInfo().GetFiles(), &volume_server_pb.RemoteFile{
		BackendType:  bType,
		BackendId:    bId,
		Key:          key,
		FileSize:     uint64(size),
		ModifiedTime: uint64(time.Now().Unix()),
		Extension:    ".dat",
	})
	if err := v.SaveVolumeInfo(); err != nil {
		t.Fatalf("save volume info: %v", err)
	}

	localDat, err = os.ReadFile(datPath)
	if err != nil {
		t.Fatalf("read local dat: %v", err)
	}
	if err := store.UnmountVolume(vid); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	if err := os.Remove(datPath); err != nil {
		t.Fatalf("remove local dat: %v", err)
	}
	return key, localDat
}

// TestTierMoveDatFromRemote_KeepRemote_LeavesReplicaLocal reproduces the
// multi-replica tier-download data loss: when KeepRemoteDatFile=true (every
// replica except the last), the replica must end up served from its local .dat
// with the shared remote object intact. Before the fix the keep-remote path
// returned right after the download without trimming the .vif or swapping the
// data backend, so the replica stayed remote-backed and a subsequent delete of
// the shared object on the final replica bricked it.
func TestTierMoveDatFromRemote_KeepRemote_LeavesReplicaLocal(t *testing.T) {
	b := &tierTestBackend{root: t.TempDir()}
	backend.BackendStorages[tierTestBackendName] = b
	t.Cleanup(func() { delete(backend.BackendStorages, tierTestBackendName) })

	dir := t.TempDir()
	const vid = needle.VolumeId(71)
	key, localDat := tierUpVolumeOnDisk(t, dir, vid, b)

	if !b.objectExists(key) {
		t.Fatal("remote object missing after tier-up")
	}

	// Mount the tiered volume the way a volume server does at startup.
	store := newTierTestStore(t, dir)
	v := store.GetVolume(vid)
	if v == nil {
		t.Fatal("tiered volume not loaded by store")
	}
	if !v.HasRemoteFile() {
		t.Fatal("volume should load in remote mode before download")
	}

	vs := &VolumeServer{store: store}
	req := &volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
		VolumeId:          uint32(vid),
		Collection:        "",
		KeepRemoteDatFile: true,
	}
	if err := vs.VolumeTierMoveDatFromRemote(req, &fakeTierStream{}); err != nil {
		t.Fatalf("VolumeTierMoveDatFromRemote: %v", err)
	}

	// keepRemote: the shared object must survive untouched.
	if len(b.deleteHistory()) != 0 {
		t.Fatalf("KeepRemoteDatFile=true must not delete the shared remote object, deletes: %v", b.deleteHistory())
	}
	if !b.objectExists(key) {
		t.Fatal("remote object must still exist after a keep-remote download")
	}

	// The in-memory volume must now be served from a local DiskFile, not the
	// remote backend — this is the assertion that fails on the pre-fix code.
	if _, ok := v.DataBackend.(*backend.DiskFile); !ok {
		t.Fatalf("after keep-remote download the data backend must be local DiskFile, got %T", v.DataBackend)
	}

	// Remount and confirm the volume is no longer remote-backed and reads come
	// from the local .dat (matching the bytes uploaded before tiering).
	if err := store.UnmountVolume(vid); err != nil {
		t.Fatalf("unmount after download: %v", err)
	}
	if err := store.MountVolume(vid); err != nil {
		t.Fatalf("remount after download: %v", err)
	}
	v2 := store.GetVolume(vid)
	if v2 == nil {
		t.Fatal("volume missing after remount")
	}
	if v2.HasRemoteFile() {
		t.Fatal("after a keep-remote download the remounted replica must not be remote-backed")
	}
	if _, ok := v2.DataBackend.(*backend.DiskFile); !ok {
		t.Fatalf("remounted replica must read from local DiskFile, got %T", v2.DataBackend)
	}

	// The .vif must no longer reference the remote object.
	storageName, storageKey := v2.RemoteStorageNameKey()
	if storageName != "" || storageKey != "" {
		t.Fatalf("trimmed .vif must not reference remote object, got %q/%q", storageName, storageKey)
	}

	// The local .dat must hold the downloaded content (the original bytes).
	gotDat, err := os.ReadFile(v2.FileName(".dat"))
	if err != nil {
		t.Fatalf("read local dat after download: %v", err)
	}
	if len(gotDat) != len(localDat) {
		t.Fatalf("local .dat size mismatch: got %d want %d", len(gotDat), len(localDat))
	}
}
