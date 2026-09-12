package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestMountVolumeValidatorAnnouncesOnlyAfterValidation(t *testing.T) {
	dir := t.TempDir()
	const vid = needle.VolumeId(17)

	volume, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	volume.Close()

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3,
		stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)

	validationErr := errors.New("copy counts differ")
	err = store.MountVolume(vid, nil, func(*Volume) error {
		select {
		case <-store.NewVolumesChan:
			t.Fatal("volume was announced before validation completed")
		default:
		}
		return validationErr
	})

	require.ErrorIs(t, err, validationErr)
	require.Nil(t, store.GetVolume(vid))
	select {
	case message := <-store.NewVolumesChan:
		t.Fatalf("volume was announced after validation failed: %+v", message)
	default:
	}
}
