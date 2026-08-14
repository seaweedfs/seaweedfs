package shell

import (
	"context"
	"regexp"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// The EC orchestration logic lives in weed/ec so the shell commands and the
// maintenance workers share it. The aliases below keep the shell-internal
// names working; command files migrate to the ec package incrementally.
type DataCenterId = ec.DataCenterId
type RackId = ec.RackId
type EcNode = ec.EcNode
type EcDisk = ec.EcDisk

type nodeLiveness = ec.NodeLiveness

const (
	nodeUp              = ec.NodeUp
	nodeDown            = ec.NodeDown
	nodeLivenessUnknown = ec.NodeLivenessUnknown
)

var (
	ecBalanceAlgorithmDescription    = ec.BalanceAlgorithmDescription
	eachDataNode                     = ec.EachDataNode
	sortEcNodesByFreeslotsDescending = ec.SortEcNodesByFreeslotsDescending
	sortEcNodesByFreeslotsAscending  = ec.SortEcNodesByFreeslotsAscending
	countShards                      = ec.CountShards
	countFreeShardSlots              = ec.CountFreeShardSlots
	collectEcVolumeServersByDc       = ec.CollectEcVolumeServersByDc
	sourceServerDeleteEcShards       = ec.SourceServerDeleteEcShards
	pingVolumeServer                 = ec.PingVolumeServer
	isNodeUnreachable                = ec.IsNodeUnreachable
	classifyNodeLiveness             = ec.ClassifyNodeLiveness
	unmountAndDeleteEcShardsQuiet    = ec.UnmountAndDeleteEcShardsQuiet
	unmountEcShards                  = ec.UnmountEcShards
	mountEcShards                    = ec.MountEcShards
	ceilDivide                       = ec.CeilDivide
	findEcVolumeShardsInfo           = ec.FindEcVolumeShardsInfo
	pickBestDiskOnNode               = ec.PickBestDiskOnNode
	assertEncodableRegularVolumes    = ec.AssertEncodableRegularVolumes
	collectVolumeIdToCollection      = ec.CollectVolumeIdToCollection
	collectCollectionsForVolumeIds   = ec.CollectCollectionsForVolumeIds
	parseVolumeIdsFlag               = ec.ParseVolumeIdsFlag
	chunkVolumeIds                   = ec.ChunkVolumeIds
	errFullTeardownNotAcked          = ec.ErrFullTeardownNotAcked
)

// ecEnv adapts a CommandEnv to the cluster access hooks the ec package needs.
func (ce *CommandEnv) ecEnv() *ec.Env {
	return &ec.Env{
		GrpcDialOption: ce.option.GrpcDialOption,
		FetchTopology: func(delay time.Duration) (*master_pb.TopologyInfo, uint64, error) {
			return collectTopologyInfo(ce, delay)
		},
		GetVolumeLocations: func(vid uint32) ([]wdclient.Location, bool) {
			return ce.MasterClient.GetLocationsClone(vid)
		},
		IsLocked: ce.isLocked,
	}
}

// Overridable functions for testing.
var getDefaultReplicaPlacement = _getDefaultReplicaPlacement

func _getDefaultReplicaPlacement(commandEnv *CommandEnv) (*super_block.ReplicaPlacement, error) {
	var resp *master_pb.GetMasterConfigurationResponse
	var err error

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		return err
	})
	if err != nil {
		return nil, err
	}

	return super_block.NewReplicaPlacementFromString(resp.DefaultReplication)
}

func parseReplicaPlacementArg(commandEnv *CommandEnv, replicaStr string) (*super_block.ReplicaPlacement, error) {
	var rp *super_block.ReplicaPlacement
	var err error

	if replicaStr != "" {
		rp, err = super_block.NewReplicaPlacementFromString(replicaStr)
		if err != nil {
			return rp, err
		}
		glog.V(1).Infof("using replica placement %q for EC volumes\n", rp.String())
	} else {
		// No replica placement argument provided, resolve from master default settings.
		rp, err = getDefaultReplicaPlacement(commandEnv)
		if err != nil {
			return rp, err
		}
		glog.V(1).Infof("using master default replica placement %q for EC volumes\n", rp.String())
	}

	return rp, nil
}

func collectTopologyInfo(commandEnv *CommandEnv, delayBeforeCollecting time.Duration) (topoInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, err error) {
	if delayBeforeCollecting > 0 {
		time.Sleep(delayBeforeCollecting)
	}

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = pb.CollectVolumeList(context.Background(), client, &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return
	}

	return resp.TopologyInfo, resp.VolumeSizeLimitMb, nil
}

func collectDataNodes(commandEnv *CommandEnv, delayBeforeCollecting time.Duration) ([]*master_pb.DataNodeInfo, error) {
	dataNodes := []*master_pb.DataNodeInfo{}

	topo, _, err := collectTopologyInfo(commandEnv, delayBeforeCollecting)
	if err != nil {
		return nil, err
	}

	for _, dci := range topo.GetDataCenterInfos() {
		for _, r := range dci.GetRackInfos() {
			for _, dn := range r.GetDataNodeInfos() {
				dataNodes = append(dataNodes, dn)
			}
		}
	}

	return dataNodes, nil
}

func collectEcNodesForDC(commandEnv *CommandEnv, selectedDataCenter string, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	return ec.CollectEcNodesForDC(commandEnv.ecEnv(), selectedDataCenter, diskType)
}

func collectEcNodes(commandEnv *CommandEnv, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	return ec.CollectEcNodes(commandEnv.ecEnv(), diskType)
}

func moveMountedShardToEcNode(commandEnv *CommandEnv, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destinationEcNode *EcNode, destDiskId uint32, applyBalancing bool, diskType types.DiskType) (err error) {
	return ec.MoveMountedShardToEcNode(commandEnv.ecEnv(), existingLocation, collection, vid, shardId, destinationEcNode, destDiskId, applyBalancing, diskType)
}

// EcBalance balances EC shards across the cluster; see ec.EcBalance for the
// excludeNodes and volumeIds semantics.
func EcBalance(commandEnv *CommandEnv, collections []string, dc string, ecReplicaPlacement *super_block.ReplicaPlacement, diskType types.DiskType, maxParallelization int, ioBytePerSecond int64, applyBalancing bool, excludeNodes map[pb.ServerAddress]struct{}, volumeIds []needle.VolumeId) (err error) {
	return ec.EcBalance(commandEnv.ecEnv(), collections, dc, ecReplicaPlacement, diskType, maxParallelization, ioBytePerSecond, applyBalancing, excludeNodes, volumeIds)
}

// compileCollectionPattern compiles a regex pattern for collection matching.
// Empty patterns match empty collections only.
// The special keyword CollectionDefault ("_default") matches empty collections.
func compileCollectionPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		// empty pattern matches empty collection
		return regexp.Compile("^$")
	}
	if pattern == CollectionDefault {
		// CollectionDefault keyword matches empty collection
		return regexp.Compile("^$")
	}
	return regexp.Compile(pattern)
}
