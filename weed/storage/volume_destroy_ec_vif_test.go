package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// A regular volume and an EC volume for the same id share <base>.vif. Deleting
// the regular volume must drop its .dat/.idx but keep the .vif so the
// coexisting EC volume's info file survives. This is the same-disk case that
// arises when EC shards are distributed onto a source/replica server before
// the original volume is deleted.
func TestDestroyKeepsVifWhenEcCoexists(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	v.location = newTestDiskLocation(dir)
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	require.NoError(t, err)

	base := VolumeFileName(dir, "", 1)
	vifPath := base + ".vif"
	require.NoError(t, os.WriteFile(vifPath, []byte("ec-volume-info"), 0o644))
	// An on-disk .ecx marks a coexisting EC volume for the same id.
	ecxPath := erasure_coding.EcShardFileName("", dir, 1) + ".ecx"
	require.NoError(t, os.WriteFile(ecxPath, []byte("ec-index"), 0o644))

	require.NoError(t, v.Destroy(false, false))

	assertFileExist(t, false, base+".dat")
	assertFileExist(t, false, base+".idx")
	assertFileExist(t, true, vifPath) // shared with the EC volume, must survive
	assertFileExist(t, true, ecxPath) // EC sidecars are never touched here
}

// With no coexisting EC volume the .vif is a plain regular-volume file and is
// removed with the rest.
func TestDestroyRemovesVifWhenNoEc(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	v.location = newTestDiskLocation(dir)
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	require.NoError(t, err)

	base := VolumeFileName(dir, "", 1)
	vifPath := base + ".vif"
	require.NoError(t, os.WriteFile(vifPath, []byte("regular-volume-info"), 0o644))

	require.NoError(t, v.Destroy(false, false))

	assertFileExist(t, false, base+".dat")
	assertFileExist(t, false, base+".idx")
	assertFileExist(t, false, vifPath) // removed along with the regular volume
}

func newTestDiskLocation(dir string) *DiskLocation {
	loc := &DiskLocation{
		Directory:      dir,
		IdxDirectory:   dir,
		DiskType:       types.HddType,
		MaxVolumeCount: 100,
		MinFreeSpace:   util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"},
	}
	loc.volumes = make(map[needle.VolumeId]*Volume)
	loc.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	return loc
}
