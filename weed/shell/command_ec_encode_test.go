package shell

import (
	"regexp"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
)

func TestSelectVolumeIdsFromTopology(t *testing.T) {
	// Topology volumeSizeLimit:1000 MB hdd(volume:28/30 active:28 free:2 remote:0)
	//   DataCenter DefaultDataCenter hdd(volume:28/30 active:28 free:2 remote:0)
	//     Rack DefaultRack hdd(volume:28/30 active:28 free:2 remote:0)
	//       DataNode seaweedfs-volume-0.seaweedfs-volume.sea:8080 hdd(volume:14/15 active:14 free:1 remote:0)
	//         Disk hdd(volume:14/15 active:14 free:1 remote:0) id:0
	//           volume Id:1, Size:23520, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:3, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770366011
	//           volume Id:2, Size:28536, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:1, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770365291
	//           volume Id:3, Size:100752, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:2, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364931
	//           volume Id:4, Size:3112, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:4, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300850
	//           volume Id:5, Size:145208, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:2, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770365652
	//           volume Id:6, Size:146456, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:4, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364812
	//           volume Id:7, Size:8, ReplicaPlacement:001, Collection:encrypt-data, Version:3, Ttl:, FileCount:0, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300824
	//           volume Id:8, Size:808, ReplicaPlacement:001, Collection:encrypt-data, Version:3, Ttl:, FileCount:2, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300796
	//           volume Id:9, Size:8, ReplicaPlacement:001, Collection:encrypt-data, Version:3, Ttl:, FileCount:0, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300814
	//           volume Id:10, Size:1048582008, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:125, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364737
	//           volume Id:11, Size:1048582008, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:125, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364796
	//           volume Id:12, Size:1048582008, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:125, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364829
	//           volume Id:13, Size:570428616, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:68, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770366062
	//           volume Id:14, Size:478153400, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:57, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770366020
	//         Disk hdd {Size:4194776448 FileCount:518 DeletedFileCount:0 DeletedBytes:0}
	//       DataNode seaweedfs-volume-0.seaweedfs-volume.sea:8080 {Size:4194776448 FileCount:518 DeletedFileCount:0 DeletedBytes:0}
	//   DataCenter DefaultDataCenter hdd(volume:28/30 active:28 free:2 remote:0)
	//     Rack DefaultRack hdd(volume:28/30 active:28 free:2 remote:0)
	//       DataNode seaweedfs-volume-1.seaweedfs-volume.sea:8080 hdd(volume:14/15 active:14 free:1 remote:0)
	//         Disk hdd(volume:14/15 active:14 free:1 remote:0) id:0
	//           volume Id:1, Size:23520, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:3, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770366012
	//           volume Id:2, Size:28536, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:1, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770365292
	//           volume Id:3, Size:100752, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:2, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364931
	//           volume Id:4, Size:3112, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:4, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300850
	//           volume Id:5, Size:145208, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:2, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770365653
	//           volume Id:6, Size:146456, ReplicaPlacement:001, Collection:, Version:3, Ttl:, FileCount:4, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364812
	//           volume Id:7, Size:8, ReplicaPlacement:001, Collection:encrypt-data, Version:3, Ttl:, FileCount:0, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300824
	//           volume Id:8, Size:808, ReplicaPlacement:001, Collection:encrypt-data, Version:3, Ttl:, FileCount:2, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300796
	//           volume Id:9, Size:8, ReplicaPlacement:001, Collection:encrypt-data, Version:3, Ttl:, FileCount:0, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770300814
	//           volume Id:10, Size:1048582008, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:125, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364737
	//           volume Id:11, Size:1048582008, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:125, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364796
	//           volume Id:12, Size:1048582008, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:125, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770364828
	//           volume Id:13, Size:570428616, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:68, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770366062
	//           volume Id:14, Size:478153400, ReplicaPlacement:001, Collection:test, Version:3, Ttl:, FileCount:57, DeleteCount:0, DeletedByteCount:0, ReadOnly:false, ModifiedAtSecond:1770366020
	//         Disk hdd {Size:4194776448 FileCount:518 DeletedFileCount:0 DeletedBytes:0}
	//       DataNode seaweedfs-volume-1.seaweedfs-volume.sea:8080 {Size:4194776448 FileCount:518 DeletedFileCount:0 DeletedBytes:0}
	//     Rack DefaultRack {Size:8389552896 FileCount:1036 DeletedFileCount:0 DeletedBytes:0}
	//   DataCenter DefaultDataCenter {Size:8389552896 FileCount:1036 DeletedFileCount:0 DeletedBytes:0}
	// total size:8389552896 file_count:1036

	topologyInfo := &master_pb.TopologyInfo{
		Id: "DefaultDataCenter",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "DefaultDataCenter",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "DefaultRack",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "seaweedfs-volume-0.seaweedfs-volume.sea:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										Type:            "hdd",
										FreeVolumeCount: 1,
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 1, Size: 23520, Collection: "", ModifiedAtSecond: 1770366011},
											{Id: 2, Size: 28536, Collection: "", ModifiedAtSecond: 1770365291},
											{Id: 3, Size: 100752, Collection: "", ModifiedAtSecond: 1770364931},
											{Id: 4, Size: 3112, Collection: "", ModifiedAtSecond: 1770300850},
											{Id: 5, Size: 145208, Collection: "", ModifiedAtSecond: 1770365652},
											{Id: 6, Size: 146456, Collection: "", ModifiedAtSecond: 1770364812},
											{Id: 7, Size: 8, Collection: "encrypt-data", ModifiedAtSecond: 1770300824},
											{Id: 8, Size: 808, Collection: "encrypt-data", ModifiedAtSecond: 1770300796},
											{Id: 9, Size: 8, Collection: "encrypt-data", ModifiedAtSecond: 1770300814},
											{Id: 10, Size: 1048582008, Collection: "test", ModifiedAtSecond: 1770364737},
											{Id: 11, Size: 1048582008, Collection: "test", ModifiedAtSecond: 1770364796},
											{Id: 12, Size: 1048582008, Collection: "test", ModifiedAtSecond: 1770364829},
											{Id: 13, Size: 570428616, Collection: "test", ModifiedAtSecond: 1770366062},
											{Id: 14, Size: 478153400, Collection: "test", ModifiedAtSecond: 1770366020},
										},
									},
								},
							},
							{
								Id: "seaweedfs-volume-1.seaweedfs-volume.sea:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										Type:            "hdd",
										FreeVolumeCount: 1,
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 1, Size: 23520, Collection: "", ModifiedAtSecond: 1770366012},
											{Id: 2, Size: 28536, Collection: "", ModifiedAtSecond: 1770365292},
											{Id: 3, Size: 100752, Collection: "", ModifiedAtSecond: 1770364931},
											{Id: 4, Size: 3112, Collection: "", ModifiedAtSecond: 1770300850},
											{Id: 5, Size: 145208, Collection: "", ModifiedAtSecond: 1770365653},
											{Id: 6, Size: 146456, Collection: "", ModifiedAtSecond: 1770364812},
											{Id: 7, Size: 8, Collection: "encrypt-data", ModifiedAtSecond: 1770300824},
											{Id: 8, Size: 808, Collection: "encrypt-data", ModifiedAtSecond: 1770300796},
											{Id: 9, Size: 8, Collection: "encrypt-data", ModifiedAtSecond: 1770300814},
											{Id: 10, Size: 1048582008, Collection: "test", ModifiedAtSecond: 1770364737},
											{Id: 11, Size: 1048582008, Collection: "test", ModifiedAtSecond: 1770364796},
											{Id: 12, Size: 1048582008, Collection: "test", ModifiedAtSecond: 1770364828},
											{Id: 13, Size: 570428616, Collection: "test", ModifiedAtSecond: 1770366062},
											{Id: 14, Size: 478153400, Collection: "test", ModifiedAtSecond: 1770366020},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	volumeSizeLimitMb := uint64(1000)
	collectionPattern := ".*"
	collectionRegex, _ := regexp.Compile(collectionPattern)

	// ec.encode -force
	// force means we ignore the check for 4+ volume servers.
	// But in this unit test we are testing selectVolumeIdsFromTopology, which selects volumes.
	// The -force flag is handled in the caller of this function.

	// Default values
	quietPeriod := time.Hour
	fullPercentage := 95.0
	verbose := true

	// Mock time to be slightly after the latest modification time in the test data
	// The latest ModifiedAtSecond in the test data is 1770366062 (Volume 13)
	// Let's set now to be 1770366062 + quietPeriod.Seconds() + 100
	nowUnixSeconds := int64(1770366062) + int64(quietPeriod.Seconds()) + 100
	quietSeconds := int64(quietPeriod.Seconds())

	// Test case 1: Select all volumes with sufficient size
	// Note: The logic requires (FreeVolumeCount >= 2) unless the volume is already selected?
	// Wait, distinct volume IDs.
	// Logic:
	// if diskInfo.FreeVolumeCount < 2 { skip }
	// In the test data, FreeVolumeCount is 1. So it should skip all volumes basically?
	// Let's check the code:
	/*
		// check free disk space
		if good, found := vidMap[v.Id]; found {
			if good {
				if diskInfo.FreeVolumeCount < 2 {
					// ...
					vidMap[v.Id] = false
					noFreeDisk++
				}
			}
		} else {
			if diskInfo.FreeVolumeCount < 2 {
				// ...
				vidMap[v.Id] = false
				noFreeDisk++
			} else {
				// ...
				vidMap[v.Id] = true
			}
		}
	*/
	// Yes, if FreeVolumeCount < 2, it marks the volume as bad (false in vidMap).
	// In the provided topology, FreeVolumeCount is 1 ("free:1"), so it is < 2.
	// So ALL volumes should be skipped due to insufficient free disk space.

	vids, _ := selectVolumeIdsFromTopology(topologyInfo, volumeSizeLimitMb, collectionRegex, nil, quietSeconds, nowUnixSeconds, fullPercentage, verbose)

	assert.Equal(t, 0, len(vids), "Should select 0 volumes because FreeVolumeCount is 1 (less than 2)")

	// Test case 2: If we had enough free space
	topologyInfo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos["hdd"].FreeVolumeCount = 2
	topologyInfo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[1].DiskInfos["hdd"].FreeVolumeCount = 2

	// Update expected selection based on size and other filters
	// Size threshold: 95% of 1000MB = 950MB = 996147200 bytes
	// Volumes >= 950MB:
	// Vol 10: 1048582008 > 950MB
	// Vol 11: 1048582008 > 950MB
	// Vol 12: 1048582008 > 950MB
	// Vol 13: 570428616 < 950MB
	// Vol 14: 478153400 < 950MB
	// ... others are small

	// So expected volumes: 10, 11, 12.

	vids, _ = selectVolumeIdsFromTopology(topologyInfo, volumeSizeLimitMb, collectionRegex, nil, quietSeconds, nowUnixSeconds, fullPercentage, verbose)

	expectedVids := []needle.VolumeId{10, 11, 12}
	assert.Equal(t, len(expectedVids), len(vids), "Should select 3 volumes")

	// Check content
	vidMap := make(map[needle.VolumeId]bool)
	for _, vid := range vids {
		vidMap[vid] = true
	}
	for _, vid := range expectedVids {
		assert.True(t, vidMap[vid], "Volume %d should be selected", vid)
	}
}

func TestEcEncodeNodeCountCheck(t *testing.T) {
	topologyInfo := &master_pb.TopologyInfo{
		Id: "DefaultDataCenter",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "DefaultDataCenter",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "DefaultRack",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{Id: "node1"},
							{Id: "node2"},
						},
					},
				},
			},
		},
	}

	nodeCount := 0
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		nodeCount++
	})

	// Default parity shards count is 4
	minNodeCount := 4

	// Case 1: Without -force
	forceChanges := false
	willProceed := forceChanges || nodeCount >= minNodeCount
	assert.False(t, willProceed, "Should NOT proceed with %d nodes (min %d) without force", nodeCount, minNodeCount)

	// Case 2: With -force
	forceChanges = true
	willProceed = forceChanges || nodeCount >= minNodeCount
	assert.True(t, willProceed, "Should proceed with -force even with %d nodes", nodeCount)
}

func TestParseEcEncodeVolumeIds(t *testing.T) {
	vids, err := parseEcEncodeVolumeIds("101, 102,101, 103")
	assert.NoError(t, err)
	assert.Equal(t, []needle.VolumeId{101, 102, 103}, vids)

	_, err = parseEcEncodeVolumeIds("101,abc")
	assert.Error(t, err)

	_, err = parseEcEncodeVolumeIds(" , ")
	assert.Error(t, err)
}

func TestChunkEcEncodeVolumeIds(t *testing.T) {
	vids := []needle.VolumeId{101, 102, 103, 104, 105}

	assert.Equal(t, [][]needle.VolumeId{
		{101, 102},
		{103, 104},
		{105},
	}, chunkEcEncodeVolumeIds(vids, 2))

	assert.Equal(t, [][]needle.VolumeId{vids}, chunkEcEncodeVolumeIds(vids, 0))
}

// allFourteenBits is EcIndexBits for shards 0..13 (standard 10+4).
const allFourteenBits = uint32((1 << 14) - 1)

// hddKey is the DiskInfos map key for the default HDD disk type: the master
// normalizes disk type strings, so HDD is keyed by the empty string.
const hddKey = ""

// topoWithClumpedEcVolume models the failure mode: all 14 shards of vid on
// nodeA, with a second empty node that could have received them.
func topoWithClumpedEcVolume(vid uint32, collection string) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{
						Id: "nodeA:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							hddKey: {
								Type:            hddKey,
								MaxVolumeCount:  100,
								FreeVolumeCount: 50,
								EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
									Id:          vid,
									Collection:  collection,
									EcIndexBits: allFourteenBits,
								}},
							},
						},
					},
					{
						Id: "nodeB:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							hddKey: {
								Type:            hddKey,
								MaxVolumeCount:  100,
								FreeVolumeCount: 50,
							},
						},
					},
				},
			}},
		}},
	}
}

func TestEcShardSpread_Clumped(t *testing.T) {
	topo := topoWithClumpedEcVolume(2059, "ec1c")
	visible, maxOnOneNode := ecShardSpread(topo, needle.VolumeId(2059), types.HardDriveType)
	assert.Equal(t, 14, visible)
	assert.Equal(t, 14, maxOnOneNode)
	visible, maxOnOneNode = ecShardSpread(topo, needle.VolumeId(9999), types.HardDriveType)
	assert.Equal(t, 0, visible)
	assert.Equal(t, 0, maxOnOneNode)
}

func TestEcShardSpread_MultiDiskNodeUnionsBits(t *testing.T) {
	// One node, two physical disks, with shard 0 reported by both entries
	// mid-move: bits must union, not sum.
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id: "nodeA:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						hddKey: {Type: hddKey, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "c", EcIndexBits: uint32(0b0011)}, // shards 0,1
							{Id: 1, Collection: "c", EcIndexBits: uint32(0b0101)}, // shards 0,2
						}},
					},
				}},
			}},
		}},
	}
	visible, maxOnOneNode := ecShardSpread(topo, needle.VolumeId(1), types.HardDriveType)
	assert.Equal(t, 3, visible)
	assert.Equal(t, 3, maxOnOneNode)
}

func TestEcShardSpread_Spread(t *testing.T) {
	// 7 shards on A, 7 on B — not a single-node clump.
	halfA := uint32((1 << 7) - 1)            // shards 0..6
	halfB := uint32(((1 << 14) - 1) ^ halfA) // shards 7..13
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{Id: "nodeA:8080", DiskInfos: map[string]*master_pb.DiskInfo{
						hddKey: {Type: hddKey, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "c", EcIndexBits: halfA},
						}},
					}},
					{Id: "nodeB:8080", DiskInfos: map[string]*master_pb.DiskInfo{
						hddKey: {Type: hddKey, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "c", EcIndexBits: halfB},
						}},
					}},
				},
			}},
		}},
	}
	visible, maxOnOneNode := ecShardSpread(topo, needle.VolumeId(1), types.HardDriveType)
	assert.Equal(t, 14, visible)
	assert.Equal(t, 7, maxOnOneNode)
}

func TestAssertEcShardsNotSingleNode_RefusesClump(t *testing.T) {
	topo := topoWithClumpedEcVolume(2059, "ec1c")
	err := assertEcShardsNotSingleNode(topo, []needle.VolumeId{2059}, types.HardDriveType)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all 14 visible EC shards still sit on one node")
	assert.Contains(t, err.Error(), "refusing to delete")
}

func TestAssertEcShardsNotSingleNode_RefusesDegradedClump(t *testing.T) {
	// Only 12 of 14 shards visible (heartbeat lag), all on nodeA: still a
	// clump. The gate must judge against the visible set, not TotalShardsCount.
	topo := topoWithClumpedEcVolume(2059, "ec1c")
	topo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos[hddKey].EcShardInfos[0].EcIndexBits = uint32((1 << 12) - 1)
	err := assertEcShardsNotSingleNode(topo, []needle.VolumeId{2059}, types.HardDriveType)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all 12 visible EC shards still sit on one node")
}

func TestAssertEcShardsNotSingleNode_AllowsSpread(t *testing.T) {
	halfA := uint32((1 << 7) - 1)
	halfB := uint32(((1 << 14) - 1) ^ halfA)
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{Id: "nodeA:8080", DiskInfos: map[string]*master_pb.DiskInfo{
						hddKey: {Type: hddKey, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "c", EcIndexBits: halfA},
						}},
					}},
					{Id: "nodeB:8080", DiskInfos: map[string]*master_pb.DiskInfo{
						hddKey: {Type: hddKey, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "c", EcIndexBits: halfB},
						}},
					}},
				},
			}},
		}},
	}
	assert.NoError(t, assertEcShardsNotSingleNode(topo, []needle.VolumeId{1}, types.HardDriveType))
}

func TestAssertEcShardsNotSingleNode_SkipsSingleNodeCluster(t *testing.T) {
	// True single-node install: rebalance cannot spread; do not refuse delete.
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id: "only:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						hddKey: {
							Type: hddKey,
							EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
								Id: 1, Collection: "c", EcIndexBits: allFourteenBits,
							}},
						},
					},
				}},
			}},
		}},
	}
	assert.NoError(t, assertEcShardsNotSingleNode(topo, []needle.VolumeId{1}, types.HardDriveType))
}
