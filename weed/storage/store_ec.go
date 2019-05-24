package storage

import (
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func (s *Store) CollectErasureCodingHeartbeat() *master_pb.Heartbeat {
	var ecShardMessages []*master_pb.VolumeEcShardInformationMessage
	for _, location := range s.Locations {
		location.ecShardsLock.RLock()
		for _, ecShards := range location.ecShards {
			ecShardMessages = append(ecShardMessages, ecShards.ToVolumeEcShardInformationMessage()...)
		}
		location.ecShardsLock.RUnlock()
	}

	return &master_pb.Heartbeat{
		EcShards: ecShardMessages,
	}

}
