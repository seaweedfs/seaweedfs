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
	garbage := v.garbageLevel()
	require.Greater(t, garbage, 0.0, "precondition: the deletes left garbage")

	require.ErrorContains(t, v.CompactByIndex(nil), "tiered")
	require.ErrorContains(t, v.CompactByVolumeData(nil), "tiered")
	require.False(t, util.FileExists(v.FileName(".cpd")))
	require.False(t, util.FileExists(v.FileName(".cpx")))

	loc := &DiskLocation{Directory: dir, volumes: map[needle.VolumeId]*Volume{}}
	loc.SetVolume(vid, v)
	s := &Store{Locations: []*DiskLocation{loc}}
	ratio, _, err := s.CheckCompactVolume(vid)
	require.NoError(t, err)
	// volume.tier.compact reads this ratio while the volume is still remote to
	// decide whether to download and compact it; vacuum selection skips tiered
	// volumes by their remote storage name instead.
	require.Equal(t, garbage, ratio, "a tiered volume reports its real garbage ratio")
}

// A volume tiered while its compaction copy ran (keeping the local .dat) must
// not commit: the reload would read the remote object, which has the old
// layout, through the compacted index.
func TestCommitCompactRefusesVolumeTieredDuringCopy(t *testing.T) {
	t.Run("memory", func(t *testing.T) { testCommitCompactRefusesVolumeTieredDuringCopy(t, NeedleMapInMemory) })
	t.Run("leveldb", func(t *testing.T) { testCommitCompactRefusesVolumeTieredDuringCopy(t, NeedleMapLevelDb) })
}

func testCommitCompactRefusesVolumeTieredDuringCopy(t *testing.T, kind NeedleMapKind) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 35, kind, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
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
	require.NotNil(t, v.tmpNm, "precondition: the copy left its temporary needle map")
	if kind == NeedleMapLevelDb {
		require.True(t, util.FileExists(v.FileName(".cpldb")), "precondition: the copy left its temporary LevelDB")
	}

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
	require.False(t, util.FileExists(v.FileName(".cpldb")), "the temporary LevelDB is left on disk")
	require.Nil(t, v.tmpNm, "the temporary needle map is kept")
	for i := uint64(4); i <= 6; i++ {
		got := &needle.Needle{Id: types.Uint64ToNeedleId(i), Cookie: 7}
		_, err := v.readNeedle(got, nil, nil)
		require.NoError(t, err, "needle %d", i)
		require.True(t, bytes.Equal(written[i], got.Data), "needle %d data", i)
	}
}
