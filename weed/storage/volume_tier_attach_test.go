package storage

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// When the .vif rename commits but the directory fsync fails, the saved .vif
// names the remote copy. AttachRemoteDatFile must keep that entry (the upload
// handler deletes a copy the volume info does not name) and serve from the
// copy the .vif points at, while still reporting the error.
func TestAttachRemoteDatFileKeepsACommittedVifEntry(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 86, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	defer v.Close()
	for i := 1; i <= 5; i++ {
		_, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false)
		require.NoError(t, err)
	}
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	require.True(t, ok)
	key, size, err := b.CopyFile(diskFile.File, nil, 0)
	require.NoError(t, err)

	saved := saveVolumeInfoFile
	saveVolumeInfoFile = func(fileName string, info *volume_server_pb.VolumeInfo) error {
		if err := saved(fileName, info); err != nil {
			return err
		}
		return &volume_info.NotCrashDurableError{FileName: fileName, Err: syscall.EIO}
	}
	defer func() { saveVolumeInfoFile = saved }()

	bType, bId := backend.BackendNameToTypeId(testBackendName)
	err = v.AttachRemoteDatFile(diskFile, &volume_server_pb.RemoteFile{
		BackendType: bType, BackendId: bId, Key: key, FileSize: uint64(size),
		ModifiedTime: uint64(time.Now().Unix()), Extension: ".dat",
	})
	var ndErr *volume_info.NotCrashDurableError
	require.True(t, errors.As(err, &ndErr), "the fsync failure is still reported: %v", err)

	onDisk, _, _, err := volume_info.MaybeLoadVolumeInfo(v.FileName(".vif"))
	require.NoError(t, err)
	require.Len(t, onDisk.GetFiles(), 1)
	require.Equal(t, key, onDisk.GetFiles()[0].Key)
	require.Len(t, v.GetVolumeInfo().GetFiles(), 1, "the in-memory entry was rolled back while the .vif names the copy")
	require.Equal(t, key, v.GetVolumeInfo().GetFiles()[0].Key)
	require.True(t, v.HasRemoteFile())
	_, isDisk := v.DataBackend.(*backend.DiskFile)
	require.False(t, isDisk, "the live backend must be the copy the .vif names")

	n := &needle.Needle{Id: types.Uint64ToNeedleId(3)}
	_, err = v.readNeedle(n, &ReadOption{}, nil)
	require.NoError(t, err)
}
