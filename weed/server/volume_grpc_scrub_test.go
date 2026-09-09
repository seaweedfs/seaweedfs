package weed_server

import (
	"context"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node-wide scrub reads the volume list without holding anything across the
// scan, so the set is free to change under it: the heartbeat drops a volume
// that reported an I/O error, and a delete or an unmount can land between two
// volumes. Failing the RPC there discards every result gathered so far and
// leaves every later volume unscrubbed, so an id that came from the node's own
// listing is skipped instead.
func TestScrubVolumeSkipsVanishedVolumeAndScrubsTheRest(t *testing.T) {
	present := needle.VolumeId(1)
	vs, _ := newMaintenanceModeServer(t, present, "")

	// Absent first: if the handler still aborted, the present volume behind it
	// would never be scrubbed.
	res, err := vs.scrubVolumes(context.Background(),
		&volume_server_pb.ScrubVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{needle.VolumeId(17), present}, false)
	require.NoError(t, err, "a volume that disappeared from the node's own listing must not fail the scrub")
	assert.Equal(t, uint64(1), res.GetTotalVolumes(), "the volume behind the vanished one must still be scrubbed")
}

// A caller who names a volume that is not here gets told, unchanged.
func TestScrubVolumeFailsOnExplicitlyRequestedMissingVolume(t *testing.T) {
	vs, _ := newMaintenanceModeServer(t, needle.VolumeId(1), "")

	_, err := vs.scrubVolumes(context.Background(),
		&volume_server_pb.ScrubVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{needle.VolumeId(17)}, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "volume id 17 not found")
}

// newEcScrubServer mounts one EC shard (plus the shared index) so FindEcVolume
// resolves, which is all an INDEX-mode scrub needs.
func newEcScrubServer(t *testing.T, vid needle.VolumeId, collection string) *VolumeServer {
	t.Helper()
	dir := t.TempDir()
	store := newTraversalTestStore(dir)
	t.Cleanup(store.Close)

	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ecx", make([]byte, types.NeedleMapEntrySize), 0o644))
	require.NoError(t, os.WriteFile(base+".ecj", nil, 0o644))
	require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(0), []byte("s"), 0o644))
	require.NoError(t, store.MountEcShards(collection, vid, 0, types.HardDriveType.String()))

	return &VolumeServer{store: store}
}

// Same rule for EC volumes, which the heartbeat also expires under a store
// write: delete_expired_ec_volumes destroys a volume whose destroy time has
// passed, and volume_ec_shards_delete unmounts one on demand.
func TestScrubEcVolumeSkipsVanishedVolumeAndScrubsTheRest(t *testing.T) {
	present := needle.VolumeId(77)
	vs := newEcScrubServer(t, present, "ec-scrub")

	res, err := vs.scrubEcVolumes(
		&volume_server_pb.ScrubEcVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{needle.VolumeId(17), present}, false)
	require.NoError(t, err, "an EC volume that disappeared from the node's own listing must not fail the scrub")
	assert.Equal(t, uint64(1), res.GetTotalVolumes(), "the EC volume behind the vanished one must still be scrubbed")
}

func TestScrubEcVolumeFailsOnExplicitlyRequestedMissingVolume(t *testing.T) {
	vs := newEcScrubServer(t, needle.VolumeId(77), "ec-scrub")

	_, err := vs.scrubEcVolumes(
		&volume_server_pb.ScrubEcVolumeRequest{Mode: volume_server_pb.VolumeScrubMode_INDEX},
		[]needle.VolumeId{needle.VolumeId(17)}, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EC volume id 17 not found")
}
