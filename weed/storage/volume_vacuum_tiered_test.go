package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A tiered volume that accepted deletes has garbage, but its .dat is remote:
// the compaction must be refused before any copy, and the store must not
// report the volume as a vacuum candidate.
func TestCompactRefusesTieredVolume(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)
	dir := t.TempDir()
	const vid = needle.VolumeId(34)
	v, _ := tierUpVolumeLive(t, dir, vid, b)
	defer v.Close()
	require.True(t, v.HasRemoteFile())
	for i := 1; i <= 4; i++ {
		_, err := v.deleteNeedle2(newRandomNeedle(uint64(i)))
		require.NoError(t, err)
	}
	require.Greater(t, v.garbageLevel(), 0.3, "precondition: a ratio a vacuum would select")

	require.ErrorContains(t, v.CompactByIndex(nil), "tiered")
	require.ErrorContains(t, v.CompactByVolumeData(nil), "tiered")
	require.False(t, util.FileExists(v.FileName(".cpd")))
	require.False(t, util.FileExists(v.FileName(".cpx")))

	loc := &DiskLocation{Directory: dir, volumes: map[needle.VolumeId]*Volume{}}
	loc.SetVolume(vid, v)
	s := &Store{Locations: []*DiskLocation{loc}}
	ratio, _, err := s.CheckCompactVolume(vid)
	require.NoError(t, err)
	require.Zero(t, ratio, "a tiered volume reports nothing to reclaim")
}

// A volume tiered while its compaction copy ran (keeping the local .dat) must
// not commit: the reload would read the remote object, which has the old
// layout, through the compacted index.
func TestCommitCompactRefusesVolumeTieredDuringCopy(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 35, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	defer v.Close()

	written := map[uint64][]byte{}
	for i := uint64(1); i <= 6; i++ {
		n := newRandomNeedle(i)
		n.Cookie = 7
		_, _, _, err := v.writeNeedle2(n, true, false, false)
		require.NoError(t, err)
		written[i] = n.Data
	}
	for i := uint64(1); i <= 3; i++ {
		_, err := v.deleteNeedle2(newEmptyNeedle(i))
		require.NoError(t, err)
	}
	require.NoError(t, v.CompactByIndex(nil))
	require.True(t, util.FileExists(v.FileName(".cpd")))

	// tier the volume up, keeping the local .dat
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	require.True(t, ok)
	key, size, err := b.CopyFile(diskFile.File, nil, 0)
	require.NoError(t, err)
	bType, bId := backend.BackendNameToTypeId(testBackendName)
	v.GetVolumeInfo().Files = append(v.GetVolumeInfo().GetFiles(), &volume_server_pb.RemoteFile{
		BackendType: bType, BackendId: bId, Key: key, FileSize: uint64(size),
		ModifiedTime: uint64(time.Now().Unix()), Extension: ".dat",
	})
	require.NoError(t, v.SaveVolumeInfo())
	require.NoError(t, v.LoadRemoteFile())

	require.ErrorContains(t, v.CommitCompact(), "tiered")
	require.False(t, util.FileExists(v.FileName(".cpd")))
	require.False(t, util.FileExists(v.FileName(".cpx")))
	for i := uint64(4); i <= 6; i++ {
		got := &needle.Needle{Id: types.Uint64ToNeedleId(i), Cookie: 7}
		_, err := v.readNeedle(got, nil, nil)
		require.NoError(t, err, "needle %d", i)
		require.True(t, bytes.Equal(written[i], got.Data), "needle %d data", i)
	}
}
