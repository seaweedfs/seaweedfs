package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A read-only load keeps an unaligned .dat tail, and the data backend pads its
// size past it. The copy holds the physical bytes, so AttachRemoteDatFile must
// not read the padding as a write that landed during the copy.
func TestAttachRemoteDatFileAcceptsAnUnalignedDatTail(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	dir := t.TempDir()
	open := func() *Volume {
		v, err := NewVolume(dir, dir, "", 88, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
		require.NoError(t, err)
		return v
	}
	v := open()
	for i := 1; i <= 3; i++ {
		_, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false)
		require.NoError(t, err)
	}
	require.NoError(t, v.PersistReadOnly(true, false))
	datName := v.FileName(".dat")
	v.Close()
	f, err := os.OpenFile(datName, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	v = open()
	defer v.Close()
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	require.True(t, ok)
	key, size, err := b.CopyFile(diskFile.File, nil, 0)
	require.NoError(t, err)
	require.NotZero(t, size%types.NeedlePaddingSize, "the .dat must end unaligned") // positive control

	bType, bId := backend.BackendNameToTypeId(testBackendName)
	require.NoError(t, v.AttachRemoteDatFile(diskFile, &volume_server_pb.RemoteFile{
		BackendType: bType, BackendId: bId, Key: key, FileSize: uint64(size),
		ModifiedTime: uint64(time.Now().Unix()), Extension: ".dat",
	}))
	require.True(t, v.HasRemoteFile())
}
