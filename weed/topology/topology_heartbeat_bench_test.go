package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func benchHeartbeatMessages(count int) []*master_pb.VolumeInformationMessage {
	messages := make([]*master_pb.VolumeInformationMessage, 0, count)
	for i := 0; i < count; i++ {
		messages = append(messages, &master_pb.VolumeInformationMessage{
			Id:               uint32(i),
			Size:             1024 * 1024,
			Collection:       "benchcollection",
			FileCount:        100,
			DeleteCount:      1,
			DeletedByteCount: 1024,
			ReplicaPlacement: 0,
			Version:          3,
			CompactRevision:  1,
			ModifiedAtSecond: 1700000000,
		})
	}
	return messages
}

// A volume server re-sends its whole volume list on every heartbeat, so this is
// the master's steady-state cost per volume server every VolumePulsePeriod.
func benchSyncDataNodeRegistration(b *testing.B, count int) {
	topo := NewTopology("bench", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": uint32(count) * 2})
	topo.SyncDataNodeRegistration(benchHeartbeatMessages(count), dn)

	messages := benchHeartbeatMessages(count)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		topo.SyncDataNodeRegistration(messages, dn)
	}
}

func BenchmarkSyncDataNodeRegistration(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("%dVolumes", count), func(b *testing.B) {
			benchSyncDataNodeRegistration(b, count)
		})
	}
}
