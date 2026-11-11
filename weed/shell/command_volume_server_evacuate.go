package shell

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandVolumeServerEvacuate{})
}

type commandVolumeServerEvacuate struct {
	topologyInfo       *master_pb.TopologyInfo
	targetServer       *string
	volumeRack         *string
	maxParallelization int
	movesPerExec       int
	movedCount         int
	limitReported      bool
}

func (c *commandVolumeServerEvacuate) Name() string {
	return "volumeServer.evacuate"
}

func (c *commandVolumeServerEvacuate) Help() string {
	return `move out all data on a volume server

	volumeServer.evacuate -node <host:port> [-skipNonMoveable] [-maxParallelization 1] [-movesPerExec 0] [-noLock] [-apply]

	This command moves all data away from the volume server.
	The volumes on the volume servers will be redistributed.

	Usually this is used to prepare to shutdown or upgrade the volume server.

	Sometimes a volume can not be moved because there are no
	good destination to meet the replication requirement. 
	E.g. a volume replication 001 in a cluster with 2 volume servers can not be moved.
	You can use "-skipNonMoveable" to move the rest volumes.
	It also skips EC shards that fail to move (a read error on a failing disk, for
	example) and goes on with the rest, the other shards of the same volume included.

	EC shards are moved up to -maxParallelization at a time (default 1), never two
	shards of one volume at once. Normal volumes are moved one at a time.

	-movesPerExec limits how many volumes and EC shards one run moves (default 0 for
	unlimited), so a big server can be emptied in several runs.

	You can use "-noLock" to run without cluster-wide lock.

`
}

func (c *commandVolumeServerEvacuate) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeServerEvacuate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	vsEvacuateCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeServer := vsEvacuateCommand.String("node", "", "<host>:<port> of the volume server")
	c.volumeRack = vsEvacuateCommand.String("rack", "", "source rack for the volume servers")
	c.targetServer = vsEvacuateCommand.String("target", "", "<host>:<port> of target volume")
	skipNonMoveable := vsEvacuateCommand.Bool("skipNonMoveable", false, "skip volumes that can not be moved")
	applyChange := vsEvacuateCommand.Bool("apply", false, "actually apply the changes")
	// TODO: remove this alias
	applyChangeAlias := vsEvacuateCommand.Bool("force", false, "actually apply the changes (alias for -apply)")
	retryCount := vsEvacuateCommand.Int("retry", 0, "how many times to retry")
	noLock := vsEvacuateCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")
	maxParallelization := vsEvacuateCommand.Int("maxParallelization", 1, "run up to X EC shard moves in parallel")
	movesPerExec := vsEvacuateCommand.Int("movesPerExec", 0, "how many volumes and EC shards to move in one run (default is 0 for unlimited)")
	if err = vsEvacuateCommand.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *maxParallelization < 1 {
		return fmt.Errorf("maxParallelization must be >= 1")
	}
	if *movesPerExec < 0 {
		return fmt.Errorf("movesPerExec must be >= 0")
	}
	c.maxParallelization = *maxParallelization
	c.movesPerExec = *movesPerExec
	c.movedCount = 0
	c.limitReported = false

	handleDeprecatedForceFlag(writer, vsEvacuateCommand, applyChangeAlias, applyChange)
	infoAboutSimulationMode(writer, *applyChange, "-apply")

	if *noLock {
		commandEnv.SetNoLock(true)
	} else if err = commandEnv.confirmIsLocked(args); err != nil && *applyChange {
		return
	}
	if *volumeServer == "" && *c.volumeRack == "" {
		return fmt.Errorf("need to specify volume server by -node=<host>:<port> or source rack")
	}

	for i := 0; i < *retryCount+1; i++ {
		if err = c.volumeServerEvacuate(commandEnv, *volumeServer, *skipNonMoveable, *applyChange, writer); err == nil {
			return nil
		}
	}

	return

}

func (c *commandVolumeServerEvacuate) volumeServerEvacuate(commandEnv *CommandEnv, volumeServer string, skipNonMoveable, applyChange bool, writer io.Writer) (err error) {
	// 1. confirm the volume server is part of the cluster
	// 2. collect all other volume servers, sort by empty slots
	// 3. move to any other volume server as long as it satisfy the replication requirements

	// list all the volumes
	// collect topology information
	c.topologyInfo, _, err = collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	defer func() {
		c.topologyInfo = nil
	}()

	if err := c.evacuateNormalVolumes(commandEnv, volumeServer, skipNonMoveable, applyChange, writer); err != nil {
		return err
	}

	if err := c.evacuateEcVolumes(commandEnv, volumeServer, skipNonMoveable, applyChange, writer); err != nil {
		return err
	}

	return nil
}

func (c *commandVolumeServerEvacuate) evacuateNormalVolumes(commandEnv *CommandEnv, volumeServer string, skipNonMoveable, applyChange bool, writer io.Writer) error {
	// find this volume server
	volumeServers := collectVolumeServersByDcRackNode(c.topologyInfo, "", "", "")
	thisNodes, otherNodes := c.nodesOtherThan(volumeServers, volumeServer)
	if len(thisNodes) == 0 {
		return fmt.Errorf("%s is not found in this cluster", volumeServer)
	}

	// move away normal volumes
	for _, thisNode := range thisNodes {
		for _, diskInfo := range thisNode.info.DiskInfos {
			if applyChange {
				if topologyInfo, _, err := collectTopologyInfo(commandEnv, 0); err != nil {
					fmt.Fprintf(writer, "update topologyInfo %v", err)
				} else {
					_, otherNodesNew := c.nodesOtherThan(
						collectVolumeServersByDcRackNode(topologyInfo, "", "", ""), volumeServer)
					if len(otherNodesNew) > 0 {
						otherNodes = otherNodesNew
						c.topologyInfo = topologyInfo
						fmt.Fprintf(writer, "topologyInfo updated %v\n", len(otherNodes))
					}
				}
			}
			volumeReplicas, _ := collectVolumeReplicaLocations(c.topologyInfo)
			// A successful move calls adjustAfterMove, which removes the volume from
			// this disk's VolumeInfos in place. Iterate over a snapshot so removals
			// don't leave nil holes in the slice we are ranging over.
			for _, vol := range slices.Clone(diskInfo.VolumeInfos) {
				if c.limitReached(writer) {
					return nil
				}
				hasMoved, err := moveAwayOneNormalVolume(commandEnv, volumeReplicas, vol, thisNode, otherNodes, applyChange)
				if err != nil {
					fmt.Fprintf(writer, "move away volume %d from %s: %v\n", vol.Id, volumeServer, err)
				}
				if hasMoved {
					c.movedCount++
				}
				if !hasMoved {
					if skipNonMoveable {
						replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(vol.ReplicaPlacement))
						fmt.Fprintf(writer, "skipping non moveable volume %d replication:%s\n", vol.Id, replicaPlacement.String())
					} else {
						return fmt.Errorf("failed to move volume %d from %s", vol.Id, volumeServer)
					}
				}
			}
		}
	}
	return nil
}

func (c *commandVolumeServerEvacuate) evacuateEcVolumes(commandEnv *CommandEnv, volumeServer string, skipNonMoveable, applyChange bool, writer io.Writer) error {
	// Evacuate EC volumes for all disk types discovered from topology
	// Disk types are free-form tags (e.g., "", "hdd", "ssd", "nvme", etc.)
	// We need to handle each disk type separately because shards should be moved to nodes with the same disk type
	// We collect topology once at the start and track capacity changes ourselves
	// (via freeEcSlot decrement after each move) rather than repeatedly refreshing,
	// which would give a false sense of correctness since topology could be stale.
	//
	// All moves are planned first, updating the in-memory model as the old
	// one-move-at-a-time loop did, then applied up to -maxParallelization at a time.
	diskTypes := collectVolumeDiskTypes(c.topologyInfo)

	var moves []ecbalancer.Move
	var allNodes []*EcNode
	var planErr error
planning:
	for _, diskType := range diskTypes {
		ecNodes, _ := collectEcVolumeServersByDc(c.topologyInfo, "", diskType)
		allNodes = append(allNodes, ecNodes...)
		thisNodes, otherNodes := c.ecNodesOtherThan(ecNodes, volumeServer)
		if len(thisNodes) == 0 {
			// This server doesn't have EC shards for this disk type, skip
			continue
		}

		// plan moving away ec volumes for this disk type
		for _, thisNode := range thisNodes {
			diskInfo, found := thisNode.Info.DiskInfos[string(diskType)]
			if !found {
				continue
			}
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				limit := 0
				if c.movesPerExec > 0 {
					limit = c.movesPerExec - c.movedCount - len(moves)
					if limit <= 0 {
						break
					}
				}
				volumeMoves, planned := c.planMoveAwayOneEcVolume(ecShardInfo, thisNode, otherNodes, diskType, limit, writer)
				moves = append(moves, volumeMoves...)
				if !planned {
					if skipNonMoveable {
						fmt.Fprintf(writer, "failed to move away ec volume %d from %s\n", ecShardInfo.Id, volumeServer)
					} else {
						// Like the one-at-a-time loop did, still move what comes before it.
						planErr = fmt.Errorf("failed to move away ec volume %d from %s", ecShardInfo.Id, volumeServer)
						break planning
					}
				}
			}
		}
	}

	if !applyChange {
		c.movedCount += len(moves)
		c.limitReached(writer)
		return planErr
	}
	if len(moves) == 0 {
		return planErr
	}
	source := volumeServer
	if source == "" {
		source = "rack " + *c.volumeRack
	}
	fmt.Fprintf(writer, "moving %d ec shard(s) from %s, up to %d at a time\n", len(moves), source, c.maxParallelization)
	result, err := ec.RunEcShardMoves(commandEnv.ecEnv(), allNodes, moves, c.maxParallelization, 0, !skipNonMoveable, writer)
	c.movedCount += result.Moved
	fmt.Fprintf(writer, "ec shards from %s: %d moved, %d failed, %d not started\n", source, result.Moved, result.Failed, result.NotStarted)
	if err != nil {
		if skipNonMoveable {
			fmt.Fprintf(writer, "skipped ec shards that failed to move: %v\n", err)
		} else {
			return err
		}
	}
	c.limitReached(writer)
	return planErr
}

// planMoveAwayOneEcVolume picks a destination for each shard of this EC volume on
// thisNode, at most limit shards when limit > 0, and records the moves in the
// in-memory model. planned is false when a shard has no destination; the shards
// planned before it are still returned.
func (c *commandVolumeServerEvacuate) planMoveAwayOneEcVolume(ecShardInfo *master_pb.VolumeEcShardInformationMessage, thisNode *EcNode, otherNodes []*EcNode, diskType types.DiskType, limit int, writer io.Writer) (moves []ecbalancer.Move, planned bool) {
	si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecShardInfo)
	for _, shardId := range si.Ids() {
		if limit > 0 && len(moves) >= limit {
			break
		}
		// Sort by: 1) fewest shards of this volume, 2) most free EC slots
		// This ensures we prefer nodes with capacity and balanced shard distribution
		slices.SortFunc(otherNodes, func(a, b *EcNode) int {
			aShards := a.LocalShardIdCount(ecShardInfo.Id)
			bShards := b.LocalShardIdCount(ecShardInfo.Id)
			if aShards != bShards {
				return aShards - bShards // Prefer fewer shards
			}
			return b.FreeEcSlot - a.FreeEcSlot // Then prefer more free slots
		})

		shardPlanned := false
		skippedNodes := 0
		for i := 0; i < len(otherNodes); i++ {
			emptyNode := otherNodes[i]

			// Skip nodes with no free EC slots
			if emptyNode.FreeEcSlot <= 0 {
				skippedNodes++
				continue
			}

			collectionPrefix := ""
			if ecShardInfo.Collection != "" {
				collectionPrefix = ecShardInfo.Collection + "_"
			}
			vid := needle.VolumeId(ecShardInfo.Id)
			// For evacuation, prefer same disk type but allow fallback to other types
			// No anti-affinity needed for evacuation (dataShardCount=0)
			destDiskId := pickBestDiskOnNode(emptyNode, vid, diskType, false, shardId, 0)
			if destDiskId > 0 {
				fmt.Fprintf(writer, "moving ec volume %s%d.%d %s => %s (disk %d)\n", collectionPrefix, ecShardInfo.Id, shardId, thisNode.Info.Id, emptyNode.Info.Id, destDiskId)
			} else {
				fmt.Fprintf(writer, "moving ec volume %s%d.%d %s => %s\n", collectionPrefix, ecShardInfo.Id, shardId, thisNode.Info.Id, emptyNode.Info.Id)
			}
			shardIds := []erasure_coding.ShardId{shardId}
			emptyNode.AddEcVolumeShards(vid, ecShardInfo.Collection, shardIds, diskType)
			thisNode.DeleteEcVolumeShards(vid, shardIds)
			moves = append(moves, ecbalancer.Move{
				VolumeID:   ecShardInfo.Id,
				ShardID:    int(shardId),
				Collection: ecShardInfo.Collection,
				SourceNode: thisNode.Info.Id,
				TargetNode: emptyNode.Info.Id,
				TargetDisk: destDiskId,
			})
			shardPlanned = true
			// Update the node's free slot count after the planned move
			emptyNode.FreeEcSlot--
			break
		}
		if !shardPlanned {
			if skippedNodes > 0 {
				fmt.Fprintf(writer, "no available destination for ec shard %d.%d: %d nodes have no free slots\n",
					ecShardInfo.Id, shardId, skippedNodes)
			}
			// Ensure partial moves are reported as failures to prevent data loss
			return moves, false
		}
	}

	return moves, true
}

// limitReached reports whether -movesPerExec moves have been made, and says so once.
func (c *commandVolumeServerEvacuate) limitReached(writer io.Writer) bool {
	if c.movesPerExec == 0 || c.movedCount < c.movesPerExec {
		return false
	}
	if !c.limitReported {
		c.limitReported = true
		fmt.Fprintf(writer, "reached -movesPerExec=%d, stopping; run the command again to move the rest\n", c.movesPerExec)
	}
	return true
}

func moveAwayOneNormalVolume(commandEnv *CommandEnv, volumeReplicas map[uint32][]*VolumeReplica, vol *master_pb.VolumeInformationMessage, thisNode *Node, otherNodes []*Node, applyChange bool) (hasMoved bool, err error) {
	freeVolumeCountfn := capacityByFreeVolumeCount(types.ToDiskType(vol.DiskType))
	maxVolumeCountFn := capacityByMaxVolumeCount(types.ToDiskType(vol.DiskType))
	for _, n := range otherNodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			return v.DiskType == vol.DiskType
		})
	}
	// most empty one is in the front
	slices.SortFunc(otherNodes, func(a, b *Node) int {
		return cmp.Compare(a.localVolumeRatio(maxVolumeCountFn), b.localVolumeRatio(maxVolumeCountFn))
	})
	for i := 0; i < len(otherNodes); i++ {
		emptyNode := otherNodes[i]
		if freeVolumeCountfn(emptyNode.info) <= 0 {
			continue
		}
		hasMoved, err = maybeMoveOneVolume(commandEnv, volumeReplicas, thisNode, vol, emptyNode, applyChange)
		if err != nil {
			return
		}
		if hasMoved {
			break
		}
	}
	return
}

func (c *commandVolumeServerEvacuate) nodesOtherThan(volumeServers []*Node, thisServer string) (thisNodes []*Node, otherNodes []*Node) {
	for _, node := range volumeServers {
		if node.info.Id == thisServer || (*c.volumeRack != "" && node.rack == *c.volumeRack) {
			thisNodes = append(thisNodes, node)
			continue
		}
		if *c.volumeRack != "" && *c.volumeRack == node.rack {
			continue
		}
		if *c.targetServer != "" && *c.targetServer != node.info.Id {
			continue
		}
		otherNodes = append(otherNodes, node)
	}
	return
}

func (c *commandVolumeServerEvacuate) ecNodesOtherThan(volumeServers []*EcNode, thisServer string) (thisNodes []*EcNode, otherNodes []*EcNode) {
	for _, node := range volumeServers {
		if node.Info.Id == thisServer || (*c.volumeRack != "" && string(node.Rack) == *c.volumeRack) {
			thisNodes = append(thisNodes, node)
			continue
		}
		if *c.volumeRack != "" && *c.volumeRack == string(node.Rack) {
			continue
		}
		if *c.targetServer != "" && *c.targetServer != node.Info.Id {
			continue
		}
		otherNodes = append(otherNodes, node)
	}
	return
}
