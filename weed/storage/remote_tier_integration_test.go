package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// In-process integration tests for cloud-tiered ("remote") volumes.
//
// Cover the operations the user is likely to schedule against a tiered
// volume — balance/move, vacuum, EC encode, EC decode — exercising the real
// Volume / DiskLocation / Store code paths against a fake BackendStorage that
// stores objects in a temp dir. The fake stands in for S3/rclone/etc. so the
// tests stay hermetic and fast.

// localDirBackend is a BackendStorage that stores objects as files in a
// temp directory. It deletes from / writes to the dir under a mutex so the
// tests can observe ordering (e.g. that a remote object survives a move).
type localDirBackend struct {
	root string

	mu      sync.Mutex
	deletes []string // history of DeleteFile keys, for assertions
}

func newLocalDirBackend(t *testing.T) *localDirBackend {
	t.Helper()
	root := t.TempDir()
	return &localDirBackend{root: root}
}

func (b *localDirBackend) ToProperties() map[string]string {
	return map[string]string{"root": b.root}
}

func (b *localDirBackend) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	return &localDirBackendFile{backend: b, key: key, tierInfo: tierInfo}
}

func (b *localDirBackend) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	key = fmt.Sprintf("obj-%d-%d", time.Now().UnixNano(), os.Getpid())
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
	if fn != nil {
		_ = fn(written, 100)
	}
	return key, written, nil
}

func (b *localDirBackend) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	src := filepath.Join(b.root, key)
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.Create(fileName)
	if err != nil {
		return 0, err
	}
	defer out.Close()
	written, err := io.Copy(out, in)
	if err != nil {
		return 0, err
	}
	if fn != nil {
		_ = fn(written, 100)
	}
	return written, nil
}

func (b *localDirBackend) DeleteFile(key string) error {
	b.mu.Lock()
	b.deletes = append(b.deletes, key)
	b.mu.Unlock()
	return os.Remove(filepath.Join(b.root, key))
}

func (b *localDirBackend) deleteHistory() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.deletes))
	copy(out, b.deletes)
	return out
}

func (b *localDirBackend) objectExists(key string) bool {
	_, err := os.Stat(filepath.Join(b.root, key))
	return err == nil
}

// localDirBackendFile satisfies BackendStorageFile by reading/writing through
// the file in the temp dir keyed by the .vif's stored object key. Size and
// modtime come from the cached tierInfo, mirroring the S3 backend's
// behavior of returning .vif metadata from GetStat.
type localDirBackendFile struct {
	backend  *localDirBackend
	key      string
	tierInfo *volume_server_pb.VolumeInfo
}

func (f *localDirBackendFile) ReadAt(p []byte, off int64) (int, error) {
	in, err := os.Open(filepath.Join(f.backend.root, f.key))
	if err != nil {
		return 0, err
	}
	defer in.Close()
	return in.ReadAt(p, off)
}

func (f *localDirBackendFile) WriteAt(p []byte, off int64) (int, error) {
	out, err := os.OpenFile(filepath.Join(f.backend.root, f.key), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return 0, err
	}
	defer out.Close()
	return out.WriteAt(p, off)
}

func (f *localDirBackendFile) Truncate(off int64) error {
	return os.Truncate(filepath.Join(f.backend.root, f.key), off)
}

func (f *localDirBackendFile) Close() error                                 { return nil }
func (f *localDirBackendFile) Name() string                                 { return f.key }
func (f *localDirBackendFile) Sync() error                                  { return nil }
func (f *localDirBackendFile) GetStat() (int64, time.Time, error) {
	files := f.tierInfo.GetFiles()
	if len(files) == 0 {
		return 0, time.Time{}, fmt.Errorf("remote file info not found")
	}
	return int64(files[0].FileSize), time.Unix(int64(files[0].ModifiedTime), 0), nil
}

const (
	testBackendName = "test_local_dir.default"
)

// registerTestBackend installs the fake backend in the global registry under
// testBackendName for the duration of one test. Volume.Destroy and tier
// upload look this map up by name, so the registration must outlive the
// volume operations exercised below.
func registerTestBackend(t *testing.T, b *localDirBackend) {
	t.Helper()
	backend.BackendStorages[testBackendName] = b
	t.Cleanup(func() {
		delete(backend.BackendStorages, testBackendName)
	})
}

// tierUpVolume creates a real on-disk volume, writes a few needles, then
// uploads the .dat to the fake backend and rewrites the volume in remote
// mode (mirrors the production flow in volume_grpc_tier_upload.go but
// in-process).
func tierUpVolume(t *testing.T, dir string, vid needle.VolumeId, b *localDirBackend) (collection string, key string) {
	t.Helper()
	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		_, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false)
		require.NoError(t, err)
	}

	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	require.True(t, ok, "expected on-disk backend before tier-up")

	uploadKey, size, err := b.CopyFile(diskFile.File, nil)
	require.NoError(t, err)

	bType, bId := backend.BackendNameToTypeId(testBackendName)
	v.GetVolumeInfo().Files = append(v.GetVolumeInfo().GetFiles(), &volume_server_pb.RemoteFile{
		BackendType:  bType,
		BackendId:    bId,
		Key:          uploadKey,
		Offset:       0,
		FileSize:     uint64(size),
		ModifiedTime: uint64(time.Now().Unix()),
		Extension:    ".dat",
	})
	require.NoError(t, v.SaveVolumeInfo())
	require.NoError(t, v.LoadRemoteFile())
	require.NoError(t, os.Remove(v.FileName(".dat")))

	// Close the volume cleanly. Tests below reload it from disk to mirror
	// what a volume server does on restart with a tiered volume.
	v.Close()

	return v.Collection, uploadKey
}

// reloadVolume loads an existing volume from disk, the way a volume server
// does at startup. Returns the live volume with its async write worker
// running, so Destroy's channel close is valid.
func reloadVolume(t *testing.T, dir string, vid needle.VolumeId) *Volume {
	t.Helper()
	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	return v
}

// TestRemoteTier_Move_KeepsRemoteObject simulates the move-on-source-after-copy
// step of a balance: Destroy(onlyEmpty=false, keepRemoteData=true). The remote
// object must survive — the destination's freshly-copied .vif points at it.
func TestRemoteTier_Move_KeepsRemoteObject(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	const vid = needle.VolumeId(31)
	_, key := tierUpVolume(t, dir, vid, b)
	require.True(t, b.objectExists(key), "remote object missing after tier-up")

	v := reloadVolume(t, dir, vid)
	require.True(t, v.HasRemoteFile())

	require.NoError(t, v.Destroy(false, true))

	require.True(t, b.objectExists(key), "Destroy(keepRemoteData=true) must not delete remote object")
	require.Empty(t, b.deleteHistory(), "no DeleteFile call expected on a move-style destroy")
}

// TestRemoteTier_RealDelete_RemovesRemoteObject is the inverse: a true delete
// (keepRemoteData=false) must clean up the remote object. Locks in that we
// did not accidentally turn the new flag into a global skip.
func TestRemoteTier_RealDelete_RemovesRemoteObject(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	const vid = needle.VolumeId(32)
	_, key := tierUpVolume(t, dir, vid, b)

	v := reloadVolume(t, dir, vid)
	require.True(t, v.HasRemoteFile())

	require.NoError(t, v.Destroy(false, false))

	require.False(t, b.objectExists(key), "Destroy(keepRemoteData=false) must delete remote object")
	require.Equal(t, []string{key}, b.deleteHistory())
}

// TestRemoteTier_Vacuum_DoesNotDeleteRemote runs the compact paths against a
// remote-tier volume and asserts the safety property: regardless of whether
// compact succeeds, errors, or no-ops, it must not delete the cloud object.
func TestRemoteTier_Vacuum_DoesNotDeleteRemote(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	const vid = needle.VolumeId(33)
	_, key := tierUpVolume(t, dir, vid, b)
	require.True(t, b.objectExists(key))

	v := reloadVolume(t, dir, vid)
	defer v.Close()
	require.True(t, v.HasRemoteFile())

	_ = v.CompactByVolumeData(nil)
	_ = v.CompactByIndex(nil)
	require.True(t, b.objectExists(key), "Compact must not delete remote object")
	require.Empty(t, b.deleteHistory())
}

// TestRemoteTier_ECEncode_RequiresLocalDat confirms the EC encoder runs
// against the local .dat path. For a remote-tier volume the .dat is gone,
// so encoding is expected to fail with a missing-file error — locks in that
// callers must download (tier_move_dat_from_remote) before encoding.
func TestRemoteTier_ECEncode_RequiresLocalDat(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	const vid = needle.VolumeId(34)
	tierUpVolume(t, dir, vid, b)

	baseFileName := filepath.Join(dir, fmt.Sprintf("%d", uint32(vid)))
	err := erasure_coding.WriteEcFiles(baseFileName)
	require.Error(t, err, "EC encoder must not run with .dat missing — caller is expected to download first")
	require.Contains(t, err.Error(), ".dat")
}

// TestRemoteTier_ECEncodeDecode_AfterDownload exercises the encode/decode
// round-trip on a tiered volume after pulling the .dat back to local disk
// (the production sequence used by `volume.tier.move -dest=local`).
func TestRemoteTier_ECEncodeDecode_AfterDownload(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	const vid = needle.VolumeId(35)
	_, key := tierUpVolume(t, dir, vid, b)

	baseFileName := filepath.Join(dir, fmt.Sprintf("%d", uint32(vid)))
	datPath := baseFileName + ".dat"
	_, err := b.DownloadFile(datPath, key, nil)
	require.NoError(t, err)

	require.NoError(t, erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx"))
	require.NoError(t, erasure_coding.WriteEcFiles(baseFileName))

	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardPath := fmt.Sprintf("%s.ec%02d", baseFileName, i)
		_, statErr := os.Stat(shardPath)
		require.NoError(t, statErr, "shard %d missing after encode", i)
	}

	// Drop the parity-range shards (indices DataShardsCount..TotalShardsCount-1)
	// and rebuild — exercises the recover-from-missing-parity path.
	for i := erasure_coding.DataShardsCount; i < erasure_coding.TotalShardsCount; i++ {
		shardPath := fmt.Sprintf("%s.ec%02d", baseFileName, i)
		require.NoError(t, os.Remove(shardPath))
	}
	rebuilt, err := erasure_coding.RebuildEcFiles(baseFileName)
	require.NoError(t, err)
	require.NotEmpty(t, rebuilt, "rebuild should report which parity shards were regenerated")
	for i := erasure_coding.DataShardsCount; i < erasure_coding.TotalShardsCount; i++ {
		shardPath := fmt.Sprintf("%s.ec%02d", baseFileName, i)
		_, statErr := os.Stat(shardPath)
		require.NoError(t, statErr, "parity shard %d missing after rebuild", i)
	}
}
