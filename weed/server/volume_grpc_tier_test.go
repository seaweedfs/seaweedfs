package weed_server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const tierTimestampTestBackendName = "tier_timestamp_test.default"

type discardServerStream[T any] struct {
	grpc.ServerStream
}

func (s *discardServerStream[T]) Send(*T) error {
	return nil
}

type tierTimestampTestBackend struct {
	root string
}

func (b *tierTimestampTestBackend) ToProperties() map[string]string {
	return map[string]string{"root": b.root}
}

func (b *tierTimestampTestBackend) NewStorageFile(key string, volumeInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	return &tierTimestampTestBackendFile{
		path:       filepath.Join(b.root, key),
		volumeInfo: volumeInfo,
	}
}

func (b *tierTimestampTestBackend) CopyFile(file *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	key = "remote.dat"
	fileInfo, err := file.Stat()
	if err != nil {
		return "", 0, err
	}

	output, err := os.Create(filepath.Join(b.root, key))
	if err != nil {
		return "", 0, err
	}
	defer output.Close()

	size, err = io.Copy(output, io.NewSectionReader(file, 0, fileInfo.Size()))
	if err == nil && fn != nil {
		err = fn(size, 100)
	}
	return key, size, err
}

func (b *tierTimestampTestBackend) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	input, err := os.Open(filepath.Join(b.root, key))
	if err != nil {
		return 0, err
	}
	defer input.Close()

	output, err := os.Create(fileName)
	if err != nil {
		return 0, err
	}
	defer output.Close()

	size, err = io.Copy(output, input)
	if err == nil && fn != nil {
		err = fn(size, 100)
	}
	return size, err
}

func (b *tierTimestampTestBackend) DeleteFile(key string) error {
	return os.Remove(filepath.Join(b.root, key))
}

type tierTimestampTestBackendFile struct {
	path       string
	volumeInfo *volume_server_pb.VolumeInfo
}

func (f *tierTimestampTestBackendFile) ReadAt(p []byte, off int64) (int, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return file.ReadAt(p, off)
}

func (f *tierTimestampTestBackendFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, fmt.Errorf("remote test file is read-only")
}

func (f *tierTimestampTestBackendFile) Truncate(off int64) error {
	return fmt.Errorf("remote test file is read-only")
}

func (f *tierTimestampTestBackendFile) Close() error {
	return nil
}

func (f *tierTimestampTestBackendFile) GetStat() (datSize int64, modTime time.Time, err error) {
	files := f.volumeInfo.GetFiles()
	if len(files) == 0 {
		return 0, time.Time{}, fmt.Errorf("remote file info not found")
	}
	return int64(files[0].GetFileSize()), time.Unix(int64(files[0].GetModifiedTime()), 0), nil
}

func (f *tierTimestampTestBackendFile) Name() string {
	return f.path
}

func (f *tierTimestampTestBackendFile) Sync() error {
	return nil
}

func TestVolumeTierMoveDatPreservesModifiedTime(t *testing.T) {
	dataDir := t.TempDir()
	remoteDir := t.TempDir()
	testBackend := &tierTimestampTestBackend{root: remoteDir}
	backend.BackendStorages[tierTimestampTestBackendName] = testBackend
	t.Cleanup(func() {
		delete(backend.BackendStorages, tierTimestampTestBackendName)
	})

	store := storage.NewStore(
		nil,
		"localhost",
		8080,
		18080,
		"http://localhost:8080",
		"store-id",
		[]string{dataDir},
		[]int32{10},
		[]util.MinFreeSpace{{}},
		"",
		storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType},
		nil,
		0,
		stats.DefaultDiskIOProbeConfig(),
	)
	t.Cleanup(store.Close)

	const volumeId = needle.VolumeId(1)
	if err := store.AddVolume(volumeId, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}

	volume := store.GetVolume(volumeId)
	dataFileName := volume.FileName(".dat")
	sourceModifiedTime := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(dataFileName, sourceModifiedTime, sourceModifiedTime); err != nil {
		t.Fatalf("set source modified time: %v", err)
	}

	// Re-open the data backend so the DiskFile caches the on-disk mtime, the way
	// a volume freshly loaded from disk does.
	volume.DataBackend.Close()
	reopened, err := os.OpenFile(dataFileName, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("reopen data file: %v", err)
	}
	volume.DataBackend = backend.NewDiskFile(reopened)

	volumeServer := &VolumeServer{store: store}
	if err := volumeServer.VolumeTierMoveDatToRemote(
		&volume_server_pb.VolumeTierMoveDatToRemoteRequest{
			VolumeId:               uint32(volumeId),
			DestinationBackendName: tierTimestampTestBackendName,
		},
		&discardServerStream[volume_server_pb.VolumeTierMoveDatToRemoteResponse]{},
	); err != nil {
		t.Fatalf("move data to remote: %v", err)
	}

	remoteFiles := volume.GetVolumeInfo().GetFiles()
	if len(remoteFiles) != 1 {
		t.Fatalf("remote file count = %d, want 1", len(remoteFiles))
	}
	if got := remoteFiles[0].GetModifiedTime(); got != uint64(sourceModifiedTime.Unix()) {
		t.Fatalf("remote modified time = %d, want %d", got, sourceModifiedTime.Unix())
	}
	if _, err := os.Stat(dataFileName); !os.IsNotExist(err) {
		t.Fatalf("local data file still exists after upload: %v", err)
	}

	if err := volumeServer.VolumeTierMoveDatFromRemote(
		&volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
			VolumeId: uint32(volumeId),
		},
		&discardServerStream[volume_server_pb.VolumeTierMoveDatFromRemoteResponse]{},
	); err != nil {
		t.Fatalf("move data from remote: %v", err)
	}

	fileInfo, err := os.Stat(dataFileName)
	if err != nil {
		t.Fatalf("stat downloaded data file: %v", err)
	}
	if got := fileInfo.ModTime().Unix(); got != sourceModifiedTime.Unix() {
		t.Fatalf("downloaded modified time = %d, want %d", got, sourceModifiedTime.Unix())
	}
}
