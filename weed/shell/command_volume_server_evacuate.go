package shell

import (
	"flag"
	"fmt"
	"io"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandVolumeServerEvacuate{})
}

type commandVolumeServerEvacuate struct {
	topologyInfo *master_pb.TopologyInfo
	targetServer *string
	volumeRack   *string
}

func (c *commandVolumeServerEvacuate) Name() string {
	return "volumeServer.evacuate"
}

func (c *commandVolumeServerEvacuate) Help() string {
	return `move out all data on a volume server

	volumeServer.evacuate -node <host:port>

	This command moves all data away from the volume server.
	The volumes on the volume servers will be redistributed.

	Usually this is used to prepare to shutdown or upgrade the volume server.

	Sometimes a volume can not be moved because there are no
	good destination to meet the replication requirement. 
	E.g. a volume replication 001 in a cluster with 2 volume servers can not be moved.
	You can use "-skipNonMoveable" to move the rest volumes.

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
	if err = vsEvacuateCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, vsEvacuateCommand, applyChangeAlias, applyChange)
	infoAboutSimulationMode(writer, *applyChange, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil && *applyChange {
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
			for _, vol := range diskInfo.VolumeInfos {
				hasMoved, err := moveAwayOneNormalVolume(commandEnv, volumeReplicas, vol, thisNode, otherNodes, applyChange)
				if err != nil {
					fmt.Fprintf(writer, "move away volume %d from %s: %v\n", vol.Id, volumeServer, err)
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
	// find this ec volume server
	// We collect topology once at the start and track capacity changes ourselves
	// (via freeEcSlot decrement after each move) rather than repeatedly refreshing,
	// which would give a false sense of correctness since topology could be stale.
	ecNodes, _ := collectEcVolumeServersByDc(c.topologyInfo, "")
	thisNodes, otherNodes := c.ecNodesOtherThan(ecNodes, volumeServer)
	if len(thisNodes) == 0 {
		return fmt.Errorf("%s is not found in this cluster\n", volumeServer)
	}

	// move away ec volumes
	for _, thisNode := range thisNodes {
		for _, diskInfo := range thisNode.info.DiskInfos {
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				hasMoved, err := c.moveAwayOneEcVolume(commandEnv, ecShardInfo, thisNode, otherNodes, applyChange, writer)
				if err != nil {
					fmt.Fprintf(writer, "move away volume %d from %s: %v\n", ecShardInfo.Id, volumeServer, err)
				}
				if !hasMoved {
					if skipNonMoveable {
						fmt.Fprintf(writer, "failed to move away ec volume %d from %s\n", ecShardInfo.Id, volumeServer)
					} else {
						return fmt.Errorf("failed to move away ec volume %d from %s", ecShardInfo.Id, volumeServer)
					}
				}
			}
		}
	}
	return nil
}

func (c *commandVolumeServerEvacuate) moveAwayOneEcVolume(commandEnv *CommandEnv, ecShardInfo *master_pb.VolumeEcShardInformationMessage, thisNode *EcNode, otherNodes []*EcNode, applyChange bool, writer io.Writer) (hasMoved bool, err error) {

	for _, shardId := range erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIds() {
		// Sort by: 1) fewest shards of this volume, 2) most free EC slots
		// This ensures we prefer nodes with capacity and balanced shard distribution
		slices.SortFunc(otherNodes, func(a, b *EcNode) int {
			aShards := a.localShardIdCount(ecShardInfo.Id)
			bShards := b.localShardIdCount(ecShardInfo.Id)
			if aShards != bShards {
				return aShards - bShards // Prefer fewer shards
			}
			return b.freeEcSlot - a.freeEcSlot // Then prefer more free slots
		})

		shardMoved := false
		skippedNodes := 0
		for i := 0; i < len(otherNodes); i++ {
			emptyNode := otherNodes[i]

			// Skip nodes with no free EC slots
			if emptyNode.freeEcSlot <= 0 {
				skippedNodes++
				continue
			}

			collectionPrefix := ""
			if ecShardInfo.Collection != "" {
				collectionPrefix = ecShardInfo.Collection + "_"
			}
			vid := needle.VolumeId(ecShardInfo.Id)
			destDiskId := pickBestDiskOnNode(emptyNode, vid)
			if destDiskId > 0 {
				fmt.Fprintf(writer, "moving ec volume %s%d.%d %s => %s (disk %d)\n", collectionPrefix, ecShardInfo.Id, shardId, thisNode.info.Id, emptyNode.info.Id, destDiskId)
			} else {
				fmt.Fprintf(writer, "moving ec volume %s%d.%d %s => %s\n", collectionPrefix, ecShardInfo.Id, shardId, thisNode.info.Id, emptyNode.info.Id)
			}
			err = moveMountedShardToEcNode(commandEnv, thisNode, ecShardInfo.Collection, vid, shardId, emptyNode, destDiskId, applyChange)
			if err != nil {
				hasMoved = false
				return
			} else {
				hasMoved = true
				shardMoved = true
				// Update the node's free slot count after successful move
				emptyNode.freeEcSlot--
				break
			}
		}
		if !shardMoved {
			if skippedNodes > 0 {
				fmt.Fprintf(writer, "no available destination for ec shard %d.%d: %d nodes have no free slots\n",
					ecShardInfo.Id, shardId, skippedNodes)
			}
			// Ensure partial moves are reported as failures to prevent data loss
			hasMoved = false
			return
		}
	}

	return
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
		return int(a.localVolumeRatio(maxVolumeCountFn) - b.localVolumeRatio(maxVolumeCountFn))
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
		if node.info.Id == thisServer || (*c.volumeRack != "" && string(node.rack) == *c.volumeRack) {
			thisNodes = append(thisNodes, node)
			continue
		}
		if *c.volumeRack != "" && *c.volumeRack == string(node.rack) {
			continue
		}
		if *c.targetServer != "" && *c.targetServer != node.info.Id {
			continue
		}
		otherNodes = append(otherNodes, node)
	}
	return
}
