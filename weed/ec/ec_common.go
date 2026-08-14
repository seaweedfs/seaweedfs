package ec

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataCenterId string
type EcNodeId string
type RackId string

// EcDisk represents a single disk on a volume server
type EcDisk struct {
	DiskId       uint32
	DiskType     string
	FreeEcSlots  int
	EcShardCount int // Total EC shards on this disk
	// Map of volumeId -> ShardsInfo for shards on this disk
	EcShards map[needle.VolumeId]*erasure_coding.ShardsInfo
}

type EcNode struct {
	Info       *master_pb.DataNodeInfo
	DC         DataCenterId
	Rack       RackId
	FreeEcSlot int
	// Disks maps diskId -> EcDisk for disk-level balancing
	Disks map[uint32]*EcDisk
}

var BalanceAlgorithmDescription = `
	func EcBalance() {
		for each collection:
			balanceEcVolumes(collectionName)
		for each rack:
			balanceEcRack(rack)
	}

	func balanceEcVolumes(collectionName){
		for each volume:
			doDeduplicateEcShards(volumeId)

		tracks rack~shardCount mapping
		for each volume:
			doBalanceEcShardsAcrossRacks(volumeId)

		for each volume:
			doBalanceEcShardsWithinRacks(volumeId)
	}

	// spread ec shards into more racks
	func doBalanceEcShardsAcrossRacks(volumeId){
		tracks rack~volumeIdShardCount mapping
		averageShardsPerEcRack = totalShardNumber / numRacks  // totalShardNumber is 14 for now, later could varies for each dc
		ecShardsToMove = select overflown ec shards from racks with ec shard counts > averageShardsPerEcRack
		for each ecShardsToMove {
			destRack = pickOneRack(rack~shardCount, rack~volumeIdShardCount, ecShardReplicaPlacement)
			destVolumeServers = volume servers on the destRack
			pickOneEcNodeAndMoveOneShard(destVolumeServers)
		}
	}

	func doBalanceEcShardsWithinRacks(volumeId){
		racks = collect all racks that the volume id is on
		for rack, shards := range racks
			doBalanceEcShardsWithinOneRack(volumeId, shards, rack)
	}

	// move ec shards
	func doBalanceEcShardsWithinOneRack(volumeId, shards, rackId){
		tracks volumeServer~volumeIdShardCount mapping
		averageShardCount = len(shards) / numVolumeServers
		volumeServersOverAverage = volume servers with volumeId's ec shard counts > averageShardsPerEcRack
		ecShardsToMove = select overflown ec shards from volumeServersOverAverage
		for each ecShardsToMove {
			destVolumeServer = pickOneVolumeServer(volumeServer~shardCount, volumeServer~volumeIdShardCount, ecShardReplicaPlacement)
			pickOneEcNodeAndMoveOneShard(destVolumeServers)
		}
	}

	// move ec shards while keeping shard distribution for the same volume unchanged or more even
	func balanceEcRack(rack){
		averageShardCount = total shards / numVolumeServers
		for hasMovedOneEcShard {
			sort all volume servers ordered by the number of local ec shards
			pick the volume server A with the lowest number of ec shards x
			pick the volume server B with the highest number of ec shards y
			if y > averageShardCount and x +1 <= averageShardCount {
				if B has a ec shard with volume id v that A does not have {
					move one ec shard v from B to A
					hasMovedOneEcShard = true
				}
			}
		}
	}
	`

func CollectEcNodesForDC(env *Env, selectedDataCenter string, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	if env == nil || env.FetchTopology == nil {
		return nil, 0, fmt.Errorf("no topology source configured")
	}
	// list all possible locations
	// collect topology information
	topologyInfo, _, err := env.FetchTopology(0)
	if err != nil {
		return
	}

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots = CollectEcVolumeServersByDc(topologyInfo, selectedDataCenter, diskType)

	SortEcNodesByFreeslotsDescending(ecNodes)

	return
}

func CollectEcNodes(env *Env, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	return CollectEcNodesForDC(env, "", diskType)
}

// AssertEncodableRegularVolumes rejects volume ids that are not encodable
// regular volumes in the topology snapshot: an already-EC volume (present only
// as EC shards, with no .dat) or an id absent from the cluster. Encoding an
// already-EC volume would clear its shards before failing, destroying the only
// copy. A volume present as BOTH a regular .dat and stale orphan shards (a
// failed-encode retry) passes, so the retry + orphan sweep still works.
func AssertEncodableRegularVolumes(t *master_pb.TopologyInfo, vids []needle.VolumeId) error {
	want := make(map[needle.VolumeId]bool, len(vids))
	for _, vid := range vids {
		want[vid] = true
	}
	regular := make(map[needle.VolumeId]bool)
	hasEcShards := make(map[needle.VolumeId]bool)
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					if diskInfo == nil {
						continue
					}
					for _, vi := range diskInfo.VolumeInfos {
						if want[needle.VolumeId(vi.Id)] {
							regular[needle.VolumeId(vi.Id)] = true
						}
					}
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						if want[needle.VolumeId(ecShardInfo.Id)] {
							hasEcShards[needle.VolumeId(ecShardInfo.Id)] = true
						}
					}
				}
			}
		}
	}
	for _, vid := range vids {
		if regular[vid] {
			continue
		}
		if hasEcShards[vid] {
			return fmt.Errorf("volume %d is already EC-encoded (no .dat replica); refusing to re-encode, which would destroy its shards", vid)
		}
		return fmt.Errorf("volume %d not found as a regular volume in the cluster; refusing to encode", vid)
	}
	return nil
}

// CollectVolumeIdToCollection returns a map from volume ID to its collection name
func CollectVolumeIdToCollection(t *master_pb.TopologyInfo, vids []needle.VolumeId) map[needle.VolumeId]string {
	result := make(map[needle.VolumeId]string)
	if len(vids) == 0 {
		return result
	}

	vidSet := make(map[needle.VolumeId]bool)
	for _, vid := range vids {
		vidSet[vid] = true
	}

	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					if diskInfo == nil {
						continue
					}
					for _, vi := range diskInfo.VolumeInfos {
						vid := needle.VolumeId(vi.Id)
						if vidSet[vid] {
							result[vid] = vi.Collection
						}
					}
				}
			}
		}
	}
	return result
}

func CollectCollectionsForVolumeIds(t *master_pb.TopologyInfo, vids []needle.VolumeId) []string {
	if len(vids) == 0 {
		return nil
	}

	found := map[string]bool{}
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						for _, vid := range vids {
							if needle.VolumeId(vi.Id) == vid {
								found[vi.Collection] = true
							}
						}
					}
					for _, ecs := range diskInfo.EcShardInfos {
						for _, vid := range vids {
							if needle.VolumeId(ecs.Id) == vid {
								found[ecs.Collection] = true
							}
						}
					}
				}
			}
		}
	}
	if len(found) == 0 {
		return nil
	}

	collections := []string{}
	for k := range found {
		collections = append(collections, k)
	}
	sort.Strings(collections)
	return collections
}

func MoveMountedShardToEcNode(env *Env, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destinationEcNode *EcNode, destDiskId uint32, applyBalancing bool, diskType types.DiskType) (err error) {

	if !env.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	copiedShardIds := []erasure_coding.ShardId{shardId}

	if applyBalancing {

		existingServerAddress := pb.NewServerAddressFromDataNode(existingLocation.Info)

		// ask destination node to copy shard and the ecx file from source node, and mount it
		copiedShardIds, err = OneServerCopyAndMountEcShardsFromSource(env.GrpcDialOption, destinationEcNode, []erasure_coding.ShardId{shardId}, vid, collection, existingServerAddress, destDiskId)
		if err != nil {
			return err
		}

		// unmount the to be deleted shards
		err = UnmountEcShards(env.GrpcDialOption, vid, existingServerAddress, copiedShardIds)
		if err != nil {
			return err
		}

		// ask source node to delete the shard, and maybe the ecx file
		err = SourceServerDeleteEcShards(env.GrpcDialOption, collection, vid, existingServerAddress, copiedShardIds)
		if err != nil {
			return err
		}

		if destDiskId > 0 {
			fmt.Printf("moved ec shard %d.%d %s => %s (disk %d)\n", vid, shardId, existingLocation.Info.Id, destinationEcNode.Info.Id, destDiskId)
		} else {
			fmt.Printf("moved ec shard %d.%d %s => %s\n", vid, shardId, existingLocation.Info.Id, destinationEcNode.Info.Id)
		}

	}

	destinationEcNode.AddEcVolumeShards(vid, collection, copiedShardIds, diskType)
	existingLocation.DeleteEcVolumeShards(vid, copiedShardIds)

	return nil

}

func OneServerCopyAndMountEcShardsFromSource(grpcDialOption grpc.DialOption,
	targetServer *EcNode, shardIdsToCopy []erasure_coding.ShardId,
	volumeId needle.VolumeId, collection string, existingLocation pb.ServerAddress, destDiskId uint32) (copiedShardIds []erasure_coding.ShardId, err error) {

	fmt.Printf("allocate %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.Info.Id)

	targetAddress := pb.NewServerAddressFromDataNode(targetServer.Info)
	err = volume_move.NewMover(grpcDialOption).CopyAndMountEcShards(context.Background(), volumeId, collection, shardIdsToCopy, existingLocation, targetAddress, destDiskId, 0, os.Stdout)
	if err != nil {
		return
	}

	// SameServer, not ==: a representation mismatch here would report a
	// same-server mount-in-place as a copy and have the caller delete the
	// shards it kept.
	if !volume_move.SameServer(targetAddress, existingLocation) {
		copiedShardIds = shardIdsToCopy
		glog.V(0).Infof("%s ec volume %d deletes shards %+v", existingLocation, volumeId, copiedShardIds)
	}

	return
}

func EachDataNode(topo *master_pb.TopologyInfo, fn func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(DataCenterId(dc.Id), RackId(rack.Id), dn)
			}
		}
	}
}

func SortEcNodesByFreeslotsDescending(ecNodes []*EcNode) {
	slices.SortFunc(ecNodes, func(a, b *EcNode) int {
		return b.FreeEcSlot - a.FreeEcSlot
	})
}

func SortEcNodesByFreeslotsAscending(ecNodes []*EcNode) {
	slices.SortFunc(ecNodes, func(a, b *EcNode) int {
		return a.FreeEcSlot - b.FreeEcSlot
	})
}

func CountShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, eci := range ecShardInfos {
		count += erasure_coding.GetShardCount(eci)
	}
	return
}

func CountFreeShardSlots(dn *master_pb.DataNodeInfo, diskType types.DiskType) (count int) {
	if dn.DiskInfos == nil {
		return 0
	}
	diskInfo := dn.DiskInfos[string(diskType)]
	if diskInfo == nil {
		return 0
	}

	// A physically near-full disk has no room for more EC shards regardless of
	// slot math (an over-set maxVolumeCount hides real fullness; statfs free bytes
	// already include EC shard files). No-opinion when the server reports no bytes.
	if balancer.DiskTooFullAfter(diskInfo.DiskTotalBytes, diskInfo.DiskFreeBytes, 0, balancer.DefaultMaxDiskUsagePercent) {
		return 0
	}

	slots := int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - CountShards(diskInfo.EcShardInfos)
	if slots < 0 {
		return 0
	}

	return slots
}

func (ecNode *EcNode) LocalShardIdCount(vid uint32) int {
	for _, diskInfo := range ecNode.Info.DiskInfos {
		for _, eci := range diskInfo.EcShardInfos {
			if vid == eci.Id {
				return erasure_coding.GetShardCount(eci)
			}
		}
	}
	return 0
}

func CollectEcVolumeServersByDc(topo *master_pb.TopologyInfo, selectedDataCenter string, diskType types.DiskType) (ecNodes []*EcNode, totalFreeEcSlots int) {
	EachDataNode(topo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if selectedDataCenter != "" && selectedDataCenter != string(dc) {
			return
		}

		freeEcSlots := CountFreeShardSlots(dn, diskType)
		ecNode := &EcNode{
			Info:       dn,
			DC:         dc,
			Rack:       rack,
			FreeEcSlot: int(freeEcSlots),
			Disks:      make(map[uint32]*EcDisk),
		}

		// Build disk-level information from volumes and EC shards
		// First, discover all unique disk IDs from VolumeInfos (includes empty disks)
		allDiskIds := make(map[uint32]string) // diskId -> diskType
		for diskTypeKey, diskInfo := range dn.DiskInfos {
			if diskInfo == nil {
				continue
			}
			// Get all disk IDs from volumes
			for _, vi := range diskInfo.VolumeInfos {
				allDiskIds[vi.DiskId] = diskTypeKey
			}
			// Also get disk IDs from EC shards
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				allDiskIds[ecShardInfo.DiskId] = diskTypeKey
			}
		}

		// Group EC shards by disk_id
		diskShards := make(map[uint32]map[needle.VolumeId]*erasure_coding.ShardsInfo)
		for _, diskInfo := range dn.DiskInfos {
			if diskInfo == nil {
				continue
			}
			for _, eci := range diskInfo.EcShardInfos {
				diskId := eci.DiskId
				if diskShards[diskId] == nil {
					diskShards[diskId] = make(map[needle.VolumeId]*erasure_coding.ShardsInfo)
				}
				vid := needle.VolumeId(eci.Id)
				diskShards[diskId][vid] = erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
			}
		}

		// Create EcDisk for each discovered disk
		diskCount := len(allDiskIds)
		if diskCount == 0 {
			diskCount = 1
		}
		freePerDisk := int(freeEcSlots) / diskCount

		for diskId, diskTypeStr := range allDiskIds {
			shards := diskShards[diskId]
			if shards == nil {
				shards = make(map[needle.VolumeId]*erasure_coding.ShardsInfo)
			}
			totalShardCount := 0
			for _, shardsInfo := range shards {
				totalShardCount += shardsInfo.Count()
			}

			ecNode.Disks[diskId] = &EcDisk{
				DiskId:       diskId,
				DiskType:     diskTypeStr,
				FreeEcSlots:  freePerDisk,
				EcShardCount: totalShardCount,
				EcShards:     shards,
			}
		}

		ecNodes = append(ecNodes, ecNode)
		totalFreeEcSlots += freeEcSlots
	})
	return
}

func SourceServerDeleteEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeDeletedShardIds []erasure_coding.ShardId) error {

	fmt.Printf("delete %d.%v from %s\n", volumeId, toBeDeletedShardIds, sourceLocation)

	return volume_move.NewMover(grpcDialOption).DeleteEcShards(context.Background(), volumeId, collection, sourceLocation, toBeDeletedShardIds)
}

// ErrFullTeardownNotAcked marks a reachable server that completed the delete RPC
// but did not report full_teardown_done (a pre-upgrade volume server). The orphan
// sweep must treat this as fatal: the node may still hold an orphan that a later
// copy would re-stamp into the new generation. Aliased to the shared sentinel so
// the shell and the plugin-worker EC task agree on the teardown-not-acked signal.
var ErrFullTeardownNotAcked = erasure_coding.ErrFullTeardownNotAcked

// PingVolumeServer probes node liveness with an empty-target Ping, which is never
// maintenance-gated, and returns the raw Ping error (nil on success). It lets the
// orphan sweep disambiguate a delete codes.Unavailable: a Rust volume server in
// maintenance mode fails the maintenance-gated delete with Unavailable yet answers
// Ping, whereas a genuinely-down node fails Ping with a transport Unavailable too.
// A Go server returns Unknown for maintenance, which IsNodeUnreachable already
// treats as fatal. The caller classifies the result with ClassifyNodeLiveness:
// only a Ping that itself transport-failed (codes.Unavailable) confirms the node
// is down; a nil error (reachable) or any other Ping error (inconclusive — e.g. a
// pre-Ping server returning Unimplemented, which means the node is up) is fatal.
func PingVolumeServer(grpcDialOption grpc.DialOption, location pb.ServerAddress) error {
	return operation.WithVolumeServerClient(false, location, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, pingErr := client.Ping(context.Background(), &volume_server_pb.PingRequest{})
		return pingErr
	})
}

// IsNodeUnreachable reports whether err means the volume server could not be
// reached at all, as opposed to an RPC that reached the node and failed. Only an
// unreachable node is safe to skip in the orphan sweep. A dead peer surfaces as
// a gRPC codes.Unavailable from the RPC (the dial is lazy, so it never fails at
// connect time); any non-status error reached node logic and is treated as
// reachable, so the sweep stays fatal rather than silently leaving stale state.
func IsNodeUnreachable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

// NodeLiveness is the tri-state result of a PingVolumeServer probe.
type NodeLiveness int

const (
	// NodeUp: Ping succeeded — the node is reachable (e.g. a Rust volume server
	// in maintenance mode that fails the delete but answers Ping).
	NodeUp NodeLiveness = iota
	// NodeDown: Ping itself transport-failed with codes.Unavailable — the node is
	// confirmed unreachable. The only state the orphan sweep may skip.
	NodeDown
	// NodeLivenessUnknown: Ping reached failing logic with any non-Unavailable
	// code (Internal, ResourceExhausted, Unimplemented from a pre-Ping server, …)
	// or a non-status error. This does NOT prove the node is down, so it is fatal.
	NodeLivenessUnknown
)

// ClassifyNodeLiveness maps a PingVolumeServer error into the tri-state. A nil
// error is NodeUp, a transport codes.Unavailable is NodeDown (reusing the same
// rule as IsNodeUnreachable), and every other Ping failure is NodeLivenessUnknown.
func ClassifyNodeLiveness(pingErr error) NodeLiveness {
	if pingErr == nil {
		return NodeUp
	}
	if IsNodeUnreachable(pingErr) {
		return NodeDown
	}
	return NodeLivenessUnknown
}

// UnmountAndDeleteEcShardsQuiet unmounts then deletes shards on one server in a
// single connection, without the per-call logging the interactive helpers emit.
// Used by the orphan sweep, which fans out to every node x volume and would
// otherwise flood the shell with no-op lines.
func UnmountAndDeleteEcShardsQuiet(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, location pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	return erasure_coding.UnmountAndDeleteEcShards(context.Background(), grpcDialOption, location, collection,
		uint32(volumeId), erasure_coding.ShardIdsToUint32(shardIds), 0)
}

func UnmountEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeUnmountedShardIds []erasure_coding.ShardId) error {

	fmt.Printf("unmount %d.%v from %s\n", volumeId, toBeUnmountedShardIds, sourceLocation)

	return volume_move.NewMover(grpcDialOption).UnmountEcShards(context.Background(), volumeId, sourceLocation, toBeUnmountedShardIds)
}

func MountEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeMountedShardIds []erasure_coding.ShardId) error {

	fmt.Printf("mount %d.%v on %s\n", volumeId, toBeMountedShardIds, sourceLocation)

	return volume_move.NewMover(grpcDialOption).MountEcShards(context.Background(), volumeId, collection, sourceLocation, toBeMountedShardIds)
}

func CeilDivide(a, b int) int {
	var r int
	if (a % b) != 0 {
		r = 1
	}
	return (a / b) + r
}

func FindEcVolumeShardsInfo(ecNode *EcNode, vid needle.VolumeId, diskType types.DiskType) *erasure_coding.ShardsInfo {
	if diskInfo, found := ecNode.Info.DiskInfos[string(diskType)]; found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				return erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo)
			}
		}
	}

	// Returns an empty ShardsInfo struct on failure, to avoid potential nil dereferences.
	return erasure_coding.NewShardsInfo()
}

// TODO: simplify me
func (ecNode *EcNode) AddEcVolumeShards(vid needle.VolumeId, collection string, shardIds []erasure_coding.ShardId, diskType types.DiskType) *EcNode {

	foundVolume := false
	diskInfo, found := ecNode.Info.DiskInfos[string(diskType)]
	if found {
		for _, ecsi := range diskInfo.EcShardInfos {
			if needle.VolumeId(ecsi.Id) == vid {
				si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecsi)
				oldShardCount := si.Count()
				for _, shardId := range shardIds {
					si.Set(erasure_coding.NewShardInfo(shardId, 0))
				}
				ecsi.EcIndexBits = si.Bitmap()
				ecsi.ShardSizes = si.SizesInt64()
				ecNode.FreeEcSlot -= si.Count() - oldShardCount
				foundVolume = true
				break
			}
		}
	} else {
		diskInfo = &master_pb.DiskInfo{
			Type: string(diskType),
		}
		ecNode.Info.DiskInfos[string(diskType)] = diskInfo
	}

	if !foundVolume {
		si := erasure_coding.NewShardsInfo()
		for _, id := range shardIds {
			si.Set(erasure_coding.NewShardInfo(id, 0))
		}
		diskInfo.EcShardInfos = append(diskInfo.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{
			Id:          uint32(vid),
			Collection:  collection,
			EcIndexBits: si.Bitmap(),
			ShardSizes:  si.SizesInt64(),
			DiskType:    string(diskType),
		})
		ecNode.FreeEcSlot -= si.Count()
	}

	return ecNode
}

// DeleteEcVolumeShards removes the shards from the node model wherever they
// sit. A node holds a given shard in exactly one disk-type bucket, but which
// bucket is not the caller's to know: a mid-migration (cross-tier encode)
// volume keeps its fresh shards in the SOURCE disk's bucket while the balance
// runs against the target type, so a bucket-scoped delete would miss them and
// the dry-run model would count a moved shard twice.
func (ecNode *EcNode) DeleteEcVolumeShards(vid needle.VolumeId, shardIds []erasure_coding.ShardId) *EcNode {

	for _, diskInfo := range ecNode.Info.DiskInfos {
		if diskInfo == nil {
			continue
		}
		for _, eci := range diskInfo.EcShardInfos {
			if needle.VolumeId(eci.Id) == vid {
				si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
				oldCount := si.Count()
				for _, shardId := range shardIds {
					si.Delete(shardId)
				}
				eci.EcIndexBits = si.Bitmap()
				eci.ShardSizes = si.SizesInt64()
				ecNode.FreeEcSlot -= si.Count() - oldCount
			}
		}
	}

	return ecNode
}

// PickBestDiskOnNode selects the best disk on a node for placing a new EC shard
// It prefers disks of the specified type with fewer shards and more free slots
// When shardId is provided and dataShardCount > 0, it applies anti-affinity:
// - For data shards (shardId < dataShardCount): prefer disks without parity shards
// - For parity shards (shardId >= dataShardCount): prefer disks without data shards
// If strictDiskType is false, it will fall back to other disk types if no matching disk is found
func PickBestDiskOnNode(ecNode *EcNode, vid needle.VolumeId, diskType types.DiskType, strictDiskType bool, shardId erasure_coding.ShardId, dataShardCount int) uint32 {
	if len(ecNode.Disks) == 0 {
		return 0 // No disk info available, let the server decide
	}

	var bestDiskId uint32
	bestScore := -1
	var fallbackDiskId uint32
	fallbackScore := -1

	// Determine if we're placing a data or parity shard
	isDataShard := dataShardCount > 0 && int(shardId) < dataShardCount

	for diskId, disk := range ecNode.Disks {
		if disk.FreeEcSlots <= 0 {
			continue
		}

		// Check existing shards on this disk for this volume
		existingShards := 0
		hasDataShards := false
		hasParityShards := false
		if si, ok := disk.EcShards[vid]; ok {
			existingShards = si.Count()
			// Check what type of shards are on this disk
			if dataShardCount > 0 {
				for _, existingShardId := range si.Ids() {
					if int(existingShardId) < dataShardCount {
						hasDataShards = true
					} else {
						hasParityShards = true
					}
				}
			}
		}

		// Score: prefer disks with fewer total shards and fewer shards of this volume
		// Lower score is better
		score := disk.EcShardCount*10 + existingShards*100

		// Apply anti-affinity penalty if applicable
		if dataShardCount > 0 {
			if isDataShard && hasParityShards {
				// Penalize placing data shard on disk with parity shards
				score += 1000
			} else if !isDataShard && hasDataShards {
				// Penalize placing parity shard on disk with data shards
				score += 1000
			}
		}

		if disk.DiskType == string(diskType) {
			// Matching disk type - this is preferred
			if bestScore == -1 || score < bestScore {
				bestScore = score
				bestDiskId = diskId
			}
		} else if !strictDiskType {
			// Non-matching disk type - use as fallback if allowed
			if fallbackScore == -1 || score < fallbackScore {
				fallbackScore = score
				fallbackDiskId = diskId
			}
		}
	}

	// Return matching disk type if found, otherwise fallback. Gate on bestScore,
	// not bestDiskId: physical disk 0 is a valid target and 0 is also the "no
	// match" zero value, so testing bestDiskId would never select disk 0.
	if bestScore != -1 {
		return bestDiskId
	}
	return fallbackDiskId
}

// ecBalancer drives an EC balance run: it collects the cluster's EC nodes, hands
// them to the shared ecbalancer planner, and executes the planned shard moves.
// The balancing policy lives in weed/storage/erasure_coding/ecbalancer, shared
// with the EC balance worker so the two cannot drift.
type ecBalancer struct {
	env                *Env
	ecNodes            []*EcNode
	replicaPlacement   *super_block.ReplicaPlacement
	applyBalancing     bool
	maxParallelization int
	ioBytePerSecond    int64
	diskType           types.DiskType
	// volumeIds narrows the plan to these ec volume ids; nil balances every volume
	// of the selected collections.
	volumeIds map[uint32]bool
	// migratingVolumeIds are mid-encode volumes whose shards are ingested from
	// every disk-type bucket; see EcBalance.
	migratingVolumeIds map[uint32]bool
}

// excludeNodes is a set of server addresses kept out of the balance as copy/move
// targets and sources. ec.encode passes the nodes its orphan sweep could not
// reach: such a node may still hold a stale-generation shard orphan, and pairing
// it with a new-generation shard from a balance copy would mix generations on one
// node. The standalone ec.balance command passes nil.
//
// volumeIds, when non-empty, restricts the plan to those ec volume ids; empty
// balances every volume of the given collections.
//
// migratingVolumeIds names volumes whose shards are ingested from EVERY
// disk-type bucket, not just the diskType one. ec.encode passes its batch:
// shard generation writes beside the source .dat, so a cross-tier encode
// (source on hdd, -diskType=ssd) leaves the fresh shards in the source bucket,
// where a target-bucket-only balance cannot see them — it plans no moves and
// the encode's spread guard aborts. Everything else keeps the bucket filter,
// so a plain ec.balance -diskType=X never drags deliberately tiered shards of
// other types onto X disks.
func EcBalance(env *Env, collections []string, dc string, ecReplicaPlacement *super_block.ReplicaPlacement, diskType types.DiskType, maxParallelization int, ioBytePerSecond int64, applyBalancing bool, excludeNodes map[pb.ServerAddress]struct{}, volumeIds []needle.VolumeId, migratingVolumeIds []needle.VolumeId) (err error) {
	// collect all ec nodes
	allEcNodes, totalFreeEcSlots, err := CollectEcNodesForDC(env, dc, diskType)
	if err != nil {
		return err
	}

	// Drop excluded nodes (and the slots they contribute) before planning so they
	// can be neither a target nor a source for any move this balance plans.
	if len(excludeNodes) > 0 {
		kept := allEcNodes[:0]
		var excludedFreeSlots int
		for _, en := range allEcNodes {
			if _, skip := excludeNodes[pb.NewServerAddressFromDataNode(en.Info)]; skip {
				excludedFreeSlots += en.FreeEcSlot
				glog.V(0).Infof("EC balance excluding node %s: skipped as unreachable by the encode orphan sweep", en.Info.Id)
				continue
			}
			kept = append(kept, en)
		}
		allEcNodes = kept
		totalFreeEcSlots -= excludedFreeSlots
	}

	if totalFreeEcSlots < 1 {
		return fmt.Errorf("no free ec shard slots. only %d left", totalFreeEcSlots)
	}

	var volumeIdFilter map[uint32]bool
	if len(volumeIds) > 0 {
		volumeIdFilter = make(map[uint32]bool, len(volumeIds))
		for _, vid := range volumeIds {
			volumeIdFilter[uint32(vid)] = true
		}
	}
	var migrating map[uint32]bool
	if len(migratingVolumeIds) > 0 {
		migrating = make(map[uint32]bool, len(migratingVolumeIds))
		for _, vid := range migratingVolumeIds {
			migrating[uint32(vid)] = true
		}
	}

	ecb := &ecBalancer{
		env:                env,
		ecNodes:            allEcNodes,
		replicaPlacement:   ecReplicaPlacement,
		applyBalancing:     applyBalancing,
		maxParallelization: maxParallelization,
		ioBytePerSecond:    ioBytePerSecond,
		diskType:           diskType,
		volumeIds:          volumeIdFilter,
		migratingVolumeIds: migrating,
	}

	if len(collections) == 0 {
		glog.V(1).Infof("WARNING: No collections to balance EC volumes across.\n")
	}
	return ecb.balance(collections)
}

// defaultECRatio resolves a collection's EC data/parity counts, defaulting to
// the standard scheme. This is the admin-side plug-in point for custom ratios.
func defaultECRatio(_ string) (int, int) {
	// Custom EC ratios are an enterprise feature; OSS uses the standard scheme.
	return erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
}

// balance plans EC shard moves with the shared planner and executes them. When
// collections is empty all collections present are balanced.
func (ecb *ecBalancer) balance(collections []string) error {
	topo, volumeRatio, selected := toBalancerTopology(ecb.ecNodes, collections, ecb.diskType, ecb.volumeIds, ecb.migratingVolumeIds)
	if len(ecb.volumeIds) > 0 {
		requested := make([]uint32, 0, len(ecb.volumeIds))
		for vid := range ecb.volumeIds {
			requested = append(requested, vid)
		}
		slices.Sort(requested)
		var missing []uint32
		for _, vid := range requested {
			if !selected[vid] {
				missing = append(missing, vid)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("no ec shards found for volume(s) %v: not an ec volume, or outside the selected collection, dataCenter or diskType", missing)
		}
		fmt.Printf("balancing ec volume(s) %v\n", requested)
	}
	moves := ecbalancer.Plan(topo, ecbalancer.Options{
		DiskType:           string(ecb.diskType),
		ImbalanceThreshold: 0, // the shell balances to an even distribution
		ReplicaPlacement:   ecb.replicaPlacement,
		Ratio:              defaultECRatio,
		// Prefer each volume's own heartbeat-reported ratio over the collection
		// default so a mixed-ratio collection is spread per volume; 0 defers to
		// defaultECRatio (and is the always-0 OSS case).
		VolumeRatio: volumeRatio,
		// Balance the global phase by fractional fullness so heterogeneous-capacity
		// nodes fill proportionally (matching the worker). This is identical to raw
		// shard count when capacities are uniform.
		GlobalUtilizationBased: true,
	})
	if len(ecb.volumeIds) > 0 {
		var deletions int
		for _, m := range moves {
			if m.Phase == "dedup" {
				deletions++
			}
		}
		fmt.Printf("planned %d ec shard move(s) and %d ec shard deletion(s)\n", len(moves)-deletions, deletions)
	}
	return ecb.executeMoves(moves)
}

// toBalancerTopology builds an ecbalancer.Topology from the EcNode model,
// including the shards of the requested collections (all collections when empty)
// and, when volumeIds is non-nil, only those volume ids. Volumes left out here are
// invisible to the planner, so no phase - dedup included - can plan against them.
// It also returns a per-volume ratio lookup built from each shard's heartbeat
// (0,0 when unreported, e.g. always in OSS), which Plan prefers over the
// collection ratio for mixed-ratio clusters, and the set of volume ids that made
// it into the topology.
func toBalancerTopology(ecNodes []*EcNode, collections []string, diskType types.DiskType, volumeIds map[uint32]bool, migratingVolumeIds map[uint32]bool) (*ecbalancer.Topology, func(collection string, vid uint32) (int, int), map[uint32]bool) {
	allowed := make(map[string]bool, len(collections))
	for _, c := range collections {
		allowed[c] = true
	}

	type volRatioKey struct {
		collection string
		vid        uint32
	}
	volRatios := make(map[volRatioKey][2]int)
	selected := make(map[uint32]bool)

	topo := ecbalancer.NewTopology()
	for _, en := range ecNodes {
		rackKey := string(en.DC) + ":" + string(en.Rack)
		node := topo.AddNode(en.Info.Id, string(en.DC), rackKey, en.FreeEcSlot)
		// Group by physical machine (host) so shards spread across machines, not just
		// nodes; the id stays the node identity used for moves.
		node.SetHost(pb.NewServerAddressFromDataNode(en.Info).ToHost())
		for diskId, d := range en.Disks {
			node.AddDisk(diskId, d.DiskType, d.FreeEcSlots, d.EcShardCount)
		}
		for diskTypeKey, diskInfo := range en.Info.DiskInfos {
			if diskInfo == nil {
				continue
			}
			for _, eci := range diskInfo.EcShardInfos {
				// A migrating (mid-encode) volume's fresh shards sit beside the
				// source .dat, in whatever bucket that disk belongs to; ingest
				// them regardless so the balance can move them onto the target
				// disk type. All other volumes keep the bucket filter.
				if diskTypeKey != string(diskType) && !migratingVolumeIds[eci.Id] {
					continue
				}
				if len(allowed) > 0 && !allowed[eci.Collection] {
					continue
				}
				if volumeIds != nil && !volumeIds[eci.Id] {
					continue
				}
				selected[eci.Id] = true
				node.AddShards(eci.Id, eci.Collection, eci.DiskId, erasure_coding.ShardBits(eci.EcIndexBits))
				if d, p := ecbalancer.VolumeShardRatio(eci); d > 0 || p > 0 {
					volRatios[volRatioKey{eci.Collection, eci.Id}] = [2]int{d, p}
				}
			}
		}
	}

	volumeRatio := func(collection string, vid uint32) (int, int) {
		r := volRatios[volRatioKey{collection, vid}]
		return r[0], r[1]
	}
	return topo, volumeRatio, selected
}

// executeMoves carries out the planned moves. Phases run in order (a within-rack
// move can depend on a cross-rack move's result), and the independent moves
// within a phase run with up to maxParallelization concurrency. Apply mode does
// only the RPCs; dry-run mode runs sequentially and mutates the in-memory EcNode
// model so callers/tests can inspect the planned end state.
func (ecb *ecBalancer) executeMoves(moves []ecbalancer.Move) error {
	byID := make(map[string]*EcNode, len(ecb.ecNodes))
	for _, en := range ecb.ecNodes {
		byID[en.Info.Id] = en
	}

	// Plan emits moves grouped by phase; run each contiguous same-phase group
	// together, waiting before the next so cross-phase dependencies hold.
	for i := 0; i < len(moves); {
		j := i
		for j < len(moves) && moves[j].Phase == moves[i].Phase {
			j++
		}
		if err := ecb.executePhase(byID, moves[i:j]); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func (ecb *ecBalancer) executePhase(byID map[string]*EcNode, moves []ecbalancer.Move) error {
	if !ecb.applyBalancing {
		// Dry-run: sequential so the in-memory model updates are race-free and
		// reflect the full plan for inspection.
		for _, m := range moves {
			if err := ecb.executeMove(byID, m); err != nil {
				return err
			}
		}
		return nil
	}
	// Apply mode: parallelize across volumes, but run one volume's moves within a
	// phase sequentially. Concurrent moves of the same volume to a node can race
	// on its shared .ecx/.ecj/.vif sidecar files.
	var order []uint32
	byVol := make(map[uint32][]ecbalancer.Move)
	for _, m := range moves {
		if _, ok := byVol[m.VolumeID]; !ok {
			order = append(order, m.VolumeID)
		}
		byVol[m.VolumeID] = append(byVol[m.VolumeID], m)
	}
	taskGroups := make([][]util.ErrorWaitGroupTask, 0, len(order))
	for _, vid := range order {
		movesForVolume := byVol[vid]
		taskGroup := make([]util.ErrorWaitGroupTask, 0, len(movesForVolume))
		for _, move := range movesForVolume {
			move := move
			taskGroup = append(taskGroup, func() error {
				return ecb.executeMove(byID, move)
			})
		}
		taskGroups = append(taskGroups, taskGroup)
	}
	return util.ExecuteParallelTaskGroups(ecb.maxParallelization, taskGroups)
}

// verifyEcShardOnKeepNode confirms the node a dedup move chose to keep actually
// holds the shard, so a duplicate is only removed when a real copy remains. An
// unreachable keep node is unknown, not confirmed, and blocks the delete — as
// does one that answers too slowly to be waited on, which is why this is
// bounded rather than left to hang the whole balance run.
// ecShardVerifyTimeout bounds the keep-node inventory query. A node that accepts
// the connection but never answers must not stall the whole balance run.
const ecShardVerifyTimeout = 30 * time.Second

func verifyEcShardOnKeepNode(grpcDialOption grpc.DialOption, collection string, vid needle.VolumeId, keepNode string, shardId erasure_coding.ShardId) error {
	if keepNode == "" {
		return fmt.Errorf("refusing dedup delete of %d.%d: no keep node recorded", vid, shardId)
	}
	ctx, cancel := context.WithTimeout(context.Background(), ecShardVerifyTimeout)
	defer cancel()
	if err := erasure_coding.VerifyShardsOnServer(ctx, collection, uint32(vid), keepNode,
		[]uint32{uint32(shardId)}, grpcDialOption); err != nil {
		return fmt.Errorf("refusing dedup delete: %w", err)
	}
	return nil
}

func (ecb *ecBalancer) executeMove(byID map[string]*EcNode, m ecbalancer.Move) error {
	src := byID[m.SourceNode]
	if src == nil {
		return nil
	}
	vid := needle.VolumeId(m.VolumeID)
	shardId := erasure_coding.ShardId(m.ShardID)
	shardIds := []erasure_coding.ShardId{shardId}

	if m.Phase == "dedup" {
		fmt.Printf("dedup: delete ec shard %d.%d on %s\n", vid, shardId, m.SourceNode)
		if !ecb.applyBalancing {
			src.DeleteEcVolumeShards(vid, shardIds)
			return nil
		}
		grpcDialOption := ecb.env.GrpcDialOption
		// Nothing is copied first, so the shard surviving elsewhere is the only
		// thing making this safe -- and the plan saying so is not evidence. A
		// topology entry can name a location holding nothing, and deleting on
		// that basis removes the last copy. Confirm the keep node has it.
		if err := verifyEcShardOnKeepNode(grpcDialOption, m.Collection, vid, m.KeepNode, shardId); err != nil {
			return err
		}
		addr := pb.NewServerAddressFromDataNode(src.Info)
		if err := UnmountEcShards(grpcDialOption, vid, addr, shardIds); err != nil {
			return err
		}
		return SourceServerDeleteEcShards(grpcDialOption, m.Collection, vid, addr, shardIds)
	}

	dst := byID[m.TargetNode]
	if dst == nil {
		return nil
	}
	if m.TargetDisk > 0 {
		fmt.Printf("%s moves ec shard %d.%d to %s (disk %d)\n", m.SourceNode, vid, shardId, m.TargetNode, m.TargetDisk)
	} else {
		fmt.Printf("%s moves ec shard %d.%d to %s\n", m.SourceNode, vid, shardId, m.TargetNode)
	}
	if !ecb.applyBalancing {
		// Dry-run: update the in-memory model only.
		return MoveMountedShardToEcNode(ecb.env, src, m.Collection, vid, shardId, dst, m.TargetDisk, false, ecb.diskType)
	}
	return ecb.applyShardMoveRPC(src, dst, m.Collection, vid, shardId, m.TargetDisk)
}

// applyShardMoveRPC copies a shard to the destination disk, verifies the
// destination registered it, then unmounts and deletes it on the source. It
// does not touch the in-memory model, so it is safe to run concurrently across
// the moves of a phase.
func (ecb *ecBalancer) applyShardMoveRPC(src, dst *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destDiskId uint32) error {
	srcAddr := pb.NewServerAddressFromDataNode(src.Info)
	dstAddr := pb.NewServerAddressFromDataNode(dst.Info)
	if volume_move.SameServer(srcAddr, dstAddr) {
		// A same-server (cross-disk) move cannot be expressed with these RPCs;
		// leave the shard where it is.
		return nil
	}
	return volume_move.NewMover(ecb.env.GrpcDialOption).MoveEcShards(context.Background(), volume_move.EcShardMove{
		VolumeId:   vid,
		Collection: collection,
		ShardIds:   []erasure_coding.ShardId{shardId},
		Source:     srcAddr,
		Target:     dstAddr,
		TargetDisk: destDiskId,
	}, volume_move.EcMoveOptions{IoBytePerSecond: ecb.ioBytePerSecond, Writer: os.Stdout})
}

// CountExistingEcShardsForVolume returns the number of distinct EC shard IDs
// for (volumeID, collection) present in the topology, counting only the single
// largest encode generation. Shards are grouped by encode_ts_ns (the per-encode
// identity from .vif), so two interrupted encode runs whose shard sets overlap
// are never unioned into a false-complete set that would wrongly trigger the
// orphaned-source delete. Walks every disk's EcIndexBits bitmap rather than
// trusting len(EcShardInfos), because a single info entry can carry multiple
// shards. Shards reporting encode_ts_ns==0 (pre-upgrade servers) form their own
// generation bucket.
//
// Limitation: the heartbeat carries one encode_ts_ns per (volume, disk), so this
// separates generations living on different disks; same-disk mixing is prevented
// upstream by the pre-encode artifact wipe and the cross-run read guard.
func CountExistingEcShardsForVolume(topologyInfo *master_pb.TopologyInfo, volumeID uint32, collection string) int {
	if topologyInfo == nil {
		return 0
	}
	perGeneration := make(map[int64]erasure_coding.ShardBits)
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, diskInfo := range node.DiskInfos {
					if diskInfo == nil {
						continue
					}
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						if ecShardInfo == nil {
							continue
						}
						if ecShardInfo.Id != volumeID || ecShardInfo.Collection != collection {
							continue
						}
						perGeneration[ecShardInfo.EncodeTsNs] |= erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
					}
				}
			}
		}
	}
	best := 0
	for _, bits := range perGeneration {
		if c := bits.Count(); c > best {
			best = c
		}
	}
	return best
}

// ParseVolumeIdsFlag parses a comma-separated -volumeIds flag value, dropping
// duplicates and keeping the given order.
func ParseVolumeIdsFlag(volumeIdsStr string) ([]needle.VolumeId, error) {
	var volumeIds []needle.VolumeId
	seen := make(map[needle.VolumeId]bool)
	for _, part := range strings.Split(volumeIdsStr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		vidValue, err := strconv.ParseUint(part, 10, 32)
		if err != nil || vidValue == 0 {
			return nil, fmt.Errorf("invalid volume id %q in -volumeIds", part)
		}
		// ParseUint with bitSize 32 bounds the value; convert through uint32
		// (matching the rest of the codebase) so the narrowing is provably safe.
		vid := needle.VolumeId(uint32(vidValue))
		if seen[vid] {
			continue
		}
		seen[vid] = true
		volumeIds = append(volumeIds, vid)
	}
	if len(volumeIds) == 0 {
		return nil, fmt.Errorf("-volumeIds does not contain any valid volume id")
	}
	return volumeIds, nil
}
