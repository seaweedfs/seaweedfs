package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// volume.configure must leave the live superblock, the .dat header, the
// in-memory .vif copy and the .vif on disk all on the new placement.
func TestConfigureVolumeUpdatesSuperblock(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{10}, []util.MinFreeSpace{{}}, "", NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	require.NoError(t, store.AddVolume(1, "", NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	require.NoError(t, store.ConfigureVolume(1, "010"))

	v := store.GetVolume(1)
	require.NotNil(t, v)
	require.Equal(t, "010", v.ReplicaPlacement.String())
	require.Equal(t, "010", v.GetVolumeInfo().GetReplication())

	datFile, err := os.Open(v.FileName(".dat"))
	require.NoError(t, err)
	sb, err := super_block.ReadSuperBlock(backend.NewDiskFile(datFile))
	require.NoError(t, err)
	datFile.Close()
	require.Equal(t, "010", sb.ReplicaPlacement.String())

	volumeInfo, _, _, err := volume_info.MaybeLoadVolumeInfo(v.FileName(".vif"))
	require.NoError(t, err)
	require.Equal(t, "010", volumeInfo.GetReplication())

	require.Error(t, store.ConfigureVolume(1, "bogus"))
}
