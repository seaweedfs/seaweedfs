package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"github.com/stretchr/testify/assert"
)

func TestIsEmptyVolumeDeleteCandidate(t *testing.T) {
	matcher, err := wildcard.CompileCollectionMatcher("")
	assert.NoError(t, err)

	const quietSeconds = int64(60)
	const now = int64(1_000_000)
	old := now - quietSeconds - 1

	tests := []struct {
		name string
		v    *master_pb.VolumeInformationMessage
		want bool
	}{
		{"small .dat is empty", &master_pb.VolumeInformationMessage{Size: super_block.SuperBlockSize, ModifiedAtSecond: old}, true},
		{"all needles deleted is empty", &master_pb.VolumeInformationMessage{Size: 1 << 30, FileCount: 100, DeleteCount: 100, ModifiedAtSecond: old}, true},
		{"live needles keep the volume", &master_pb.VolumeInformationMessage{Size: 1 << 30, FileCount: 100, DeleteCount: 99, ModifiedAtSecond: old}, false},
		{"overwrites alone are not empty", &master_pb.VolumeInformationMessage{Size: 1 << 30, FileCount: 200, DeleteCount: 100, ModifiedAtSecond: old}, false},
		{"recent all-deleted volume is kept", &master_pb.VolumeInformationMessage{Size: 1 << 30, FileCount: 100, DeleteCount: 100, ModifiedAtSecond: now}, false},
		{"never written volume is kept", &master_pb.VolumeInformationMessage{Size: super_block.SuperBlockSize, ModifiedAtSecond: 0}, false},
		{"remote-backed replica is kept", &master_pb.VolumeInformationMessage{Size: super_block.SuperBlockSize, RemoteStorageName: "s3", RemoteStorageKey: "v", ModifiedAtSecond: old}, false},
		{"remote-backed garbage is kept", &master_pb.VolumeInformationMessage{Size: 1 << 30, FileCount: 100, DeleteCount: 100, RemoteStorageName: "s3", ModifiedAtSecond: old}, false},
		{"protected read-only volume is kept", &master_pb.VolumeInformationMessage{Size: super_block.SuperBlockSize, ReadOnly: true, ModifiedAtSecond: old}, false},
		{"deletable read-only volume can go", &master_pb.VolumeInformationMessage{Size: super_block.SuperBlockSize, ReadOnly: true, ReadOnlyCanDelete: true, ModifiedAtSecond: old}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isEmptyVolumeDeleteCandidate(tc.v, quietSeconds, now, matcher))
		})
	}
}
