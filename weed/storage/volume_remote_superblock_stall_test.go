package storage

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// stallingBackend serves localDirBackend's objects, but every read waits
// until release is closed: an endpoint that accepts requests and never answers.
type stallingBackend struct {
	*localDirBackend
	release chan struct{}
	reads   atomic.Int32
}

func (b *stallingBackend) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	return &stallingBackendFile{BackendStorageFile: b.localDirBackend.NewStorageFile(key, tierInfo), backend: b}
}

type stallingBackendFile struct {
	backend.BackendStorageFile
	backend *stallingBackend
}

func (f *stallingBackendFile) ReadAt(p []byte, off int64) (int, error) {
	f.backend.reads.Add(1)
	<-f.backend.release
	return f.BackendStorageFile.ReadAt(p, off)
}

func shortenRemoteSuperBlockWait(t *testing.T, timeout, stall time.Duration) {
	t.Helper()
	oldTimeout, oldStall := remoteSuperBlockTimeout, remoteSuperBlockStallPeriod
	remoteSuperBlockTimeout, remoteSuperBlockStallPeriod = timeout, stall
	remoteSuperBlockStalls.Delete(testBackendName)
	t.Cleanup(func() {
		remoteSuperBlockTimeout, remoteSuperBlockStallPeriod = oldTimeout, oldStall
		remoteSuperBlockStalls.Delete(testBackendName)
	})
}

func TestReadRemoteSuperBlockStallSkipsBackend(t *testing.T) {
	shortenRemoteSuperBlockWait(t, 50*time.Millisecond, time.Hour)
	b := &stallingBackend{localDirBackend: newLocalDirBackend(t), release: make(chan struct{})}
	key, _, err := b.CopyFile(writeSuperBlockFile(t), nil, 0)
	require.NoError(t, err)
	f := b.NewStorageFile(key, &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{{FileSize: super_block.SuperBlockSize}}})

	start := time.Now()
	_, err = readRemoteSuperBlock(testBackendName, f)
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.EqualValues(t, 1, b.reads.Load())

	_, err = readRemoteSuperBlock(testBackendName, f)
	require.Error(t, err)
	require.EqualValues(t, 1, b.reads.Load(), "a stalled backend must not be asked again within the stall period")

	close(b.release)
	remoteSuperBlockStalls.Store(testBackendName, time.Now().Add(-time.Second))
	sb, err := readRemoteSuperBlock(testBackendName, f)
	require.NoError(t, err)
	require.Equal(t, needle.GetCurrentVersion(), sb.Version)
	_, stalled := remoteSuperBlockStalls.Load(testBackendName)
	require.False(t, stalled, "a successful read clears the stall")
}

func writeSuperBlockFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "sb.dat"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	sb := super_block.SuperBlock{Version: needle.GetCurrentVersion(), ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: &needle.TTL{}}
	_, err = f.Write(sb.Bytes())
	require.NoError(t, err)
	return f
}

// A backend that never answers must not hold up the disk scan, local volumes
// included, and the tiered volumes must serve reads once it answers again.
func TestRemoteTier_DiskScanDoesNotWaitOnStalledBackend(t *testing.T) {
	shortenRemoteSuperBlockWait(t, 200*time.Millisecond, time.Hour)
	local := newLocalDirBackend(t)
	registerTestBackend(t, local)

	dir := t.TempDir()
	remoteVids := []needle.VolumeId{61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72}
	for _, vid := range remoteVids {
		tierUpVolume(t, dir, vid, local)
	}
	lv, err := NewVolume(dir, dir, "", 60, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	lv.Close()

	stalling := &stallingBackend{localDirBackend: local, release: make(chan struct{})}
	backend.BackendStorages[testBackendName] = stalling
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(stalling.release) }) }
	t.Cleanup(release)

	loc := &DiskLocation{
		Directory:              dir,
		DirectoryUuid:          "test-uuid",
		IdxDirectory:           dir,
		DiskType:               types.HddType,
		MaxVolumeCount:         100,
		OriginalMaxVolumeCount: 100,
		MinFreeSpace:           util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"},
	}
	loc.volumes = make(map[needle.VolumeId]*Volume)
	loc.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)

	start := time.Now()
	loc.concurrentLoadingVolumes(NeedleMapInMemory, 2, 0, 0)
	require.Less(t, time.Since(start), 5*time.Second, "the disk scan waited on the stalled backend")
	defer func() {
		for _, v := range loc.volumes {
			v.Close()
		}
	}()

	_, ok := loc.FindVolume(60)
	require.True(t, ok, "local volume not loaded")
	for _, vid := range remoteVids {
		v, ok := loc.FindVolume(vid)
		require.True(t, ok, "tiered volume %d not loaded", vid)
		require.True(t, v.HasRemoteFile())
	}
	require.Less(t, int(stalling.reads.Load()), len(remoteVids), "every tiered volume waited on the stalled backend")

	release()
	v, _ := loc.FindVolume(remoteVids[0])
	_, err = v.readNeedle(newEmptyNeedle(1), nil, nil)
	require.NoError(t, err, "a tiered volume must serve reads once the backend answers")
}

// The .idx head repair reads the remote .dat, so it must wait for a load that
// can reach the backend instead of stalling this one.
func TestRemoteTier_IdxHeadRepairWaitsForReachableBackend(t *testing.T) {
	shortenRemoteSuperBlockWait(t, 200*time.Millisecond, time.Hour)
	local := newLocalDirBackend(t)
	registerTestBackend(t, local)

	dir := t.TempDir()
	const vid = needle.VolumeId(80)
	tierUpVolume(t, dir, vid, local)
	idxPath := filepath.Join(dir, "80.idx")
	clobberIdxHead(t, idxPath, []uint64{4, 5})
	sizeBefore := fileSize(t, idxPath)

	stalling := &stallingBackend{localDirBackend: local, release: make(chan struct{})}
	backend.BackendStorages[testBackendName] = stalling
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(stalling.release) }) }
	time.AfterFunc(10*time.Second, release)
	t.Cleanup(release)

	start := time.Now()
	v := reloadVolume(t, dir, vid)
	require.Less(t, time.Since(start), 5*time.Second, "the load waited on the stalled backend")
	v.Close()
	require.Equal(t, sizeBefore, fileSize(t, idxPath), "the repair ran against an unreachable backend")

	backend.BackendStorages[testBackendName] = local
	remoteSuperBlockStalls.Delete(testBackendName)
	v = reloadVolume(t, dir, vid)
	defer v.Close()
	require.Greater(t, fileSize(t, idxPath), sizeBefore, "the repair did not run once the backend answered")
	_, err := v.readNeedle(newEmptyNeedle(1), nil, nil)
	require.NoError(t, err)
}
