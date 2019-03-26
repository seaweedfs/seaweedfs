package storage

import (
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

func (v *Volume) GetVolumeSyncStatus() *volume_server_pb.VolumeSyncStatusResponse {
	var syncStatus = &volume_server_pb.VolumeSyncStatusResponse{}
	if stat, err := v.dataFile.Stat(); err == nil {
		syncStatus.TailOffset = uint64(stat.Size())
	}
	syncStatus.Collection = v.Collection
	syncStatus.IdxFileSize = v.nm.IndexFileSize()
	syncStatus.CompactRevision = uint32(v.SuperBlock.CompactRevision)
	syncStatus.Ttl = v.SuperBlock.Ttl.String()
	syncStatus.Replication = v.SuperBlock.ReplicaPlacement.String()
	return syncStatus
}