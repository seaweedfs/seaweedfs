package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"io"
	"os"
	"sort"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-collection ALL|EACH_COLLECTION|<collection_name>] [-force] [-dataCenter=<data_center_name>]

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			balanceWritableVolumes()
			balanceReadOnlyVolumes()
		}
	}

	func balanceWritableVolumes(){
		idealWritableVolumeRatio = totalWritableVolumes / totalNumberOfMaxVolumes
		for hasMovedOneVolume {
			sort all volume servers ordered by the localWritableVolumeRatio = localWritableVolumes to localVolumeMax
			pick the volume server B with the highest localWritableVolumeRatio y
			for any the volume server A with the number of writable volumes x + 1 <= idealWritableVolumeRatio * localVolumeMax {
				if y > localWritableVolumeRatio {
					if B has a writable volume id v that A does not have, and satisfy v replication requirements {
						move writable volume v from A to B
					}
				}
			}
		}
	}
	func balanceReadOnlyVolumes(){
		//similar to balanceWritableVolumes
	}

`
}

func (c *commandVolumeBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or use \"ALL_COLLECTIONS\" across collections, \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan.")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 15*time.Second)
	if err != nil {
		return err
	}

	volumeServers := collectVolumeServersByDc(topologyInfo, *dc)
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	diskTypes := collectVolumeDiskTypes(topologyInfo)

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, true, false)
		if err != nil {
			return err
		}
		for _, c := range collections {
			if err = balanceVolumeServers(commandEnv, diskTypes, volumeReplicas, volumeServers, volumeSizeLimitMb*1024*1024, c, *applyBalancing); err != nil {
				return err
			}
		}
	} else if *collection == "ALL_COLLECTIONS" {
		if err = balanceVolumeServers(commandEnv, diskTypes, volumeReplicas, volumeServers, volumeSizeLimitMb*1024*1024, "ALL_COLLECTIONS", *applyBalancing); err != nil {
			return err
		}
	} else {
		if err = balanceVolumeServers(commandEnv, diskTypes, volumeReplicas, volumeServers, volumeSizeLimitMb*1024*1024, *collection, *applyBalancing); err != nil {
			return err
		}
	}

	return nil
}

func balanceVolumeServers(commandEnv *CommandEnv, diskTypes []types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, volumeSizeLimit uint64, collection string, applyBalancing bool) error {

	for _, diskType := range diskTypes {
		if err := balanceVolumeServersByDiskType(commandEnv, diskType, volumeReplicas, nodes, volumeSizeLimit, collection, applyBalancing); err != nil {
			return err
		}
	}
	return nil

}

func balanceVolumeServersByDiskType(commandEnv *CommandEnv, diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, volumeSizeLimit uint64, collection string, applyBalancing bool) error {

	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL_COLLECTIONS" {
				if v.Collection != collection {
					return false
				}
			}
			return v.DiskType == string(diskType)
		})
	}
	if err := balanceSelectedVolume(commandEnv, diskType, volumeReplicas, nodes, capacityByMaxVolumeCount(diskType), sortWritableVolumes, applyBalancing); err != nil {
		return err
	}

	return nil
}

func collectVolumeServersByDc(t *master_pb.TopologyInfo, selectedDataCenter string) (nodes []*Node) {
	for _, dc := range t.DataCenterInfos {
		if selectedDataCenter != "" && dc.Id != selectedDataCenter {
			continue
		}
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				nodes = append(nodes, &Node{
					info: dn,
					dc:   dc.Id,
					rack: r.Id,
				})
			}
		}
	}
	return
}

func collectVolumeDiskTypes(t *master_pb.TopologyInfo) (diskTypes []types.DiskType) {
	knownTypes := make(map[string]bool)
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for diskType, _ := range dn.DiskInfos {
					if _, found := knownTypes[diskType]; !found {
						knownTypes[diskType] = true
					}
				}
			}
		}
	}
	for diskType, _ := range knownTypes {
		diskTypes = append(diskTypes, types.ToDiskType(diskType))
	}
	return
}

type Node struct {
	info            *master_pb.DataNodeInfo
	selectedVolumes map[uint32]*master_pb.VolumeInformationMessage
	dc              string
	rack            string
}

type CapacityFunc func(*master_pb.DataNodeInfo) int

func capacityByMaxVolumeCount(diskType types.DiskType) CapacityFunc {
	return func(info *master_pb.DataNodeInfo) int {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found {
			return 0
		}
		return int(diskInfo.MaxVolumeCount)
	}
}

func capacityByFreeVolumeCount(diskType types.DiskType) CapacityFunc {
	return func(info *master_pb.DataNodeInfo) int {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found {
			return 0
		}
		return int(diskInfo.MaxVolumeCount - diskInfo.VolumeCount)
	}
}

func (n *Node) localVolumeRatio(capacityFunc CapacityFunc) float64 {
	return divide(len(n.selectedVolumes), capacityFunc(n.info))
}

func (n *Node) localVolumeNextRatio(capacityFunc CapacityFunc) float64 {
	return divide(len(n.selectedVolumes)+1, capacityFunc(n.info))
}

func (n *Node) selectVolumes(fn func(v *master_pb.VolumeInformationMessage) bool) {
	n.selectedVolumes = make(map[uint32]*master_pb.VolumeInformationMessage)
	for _, diskInfo := range n.info.DiskInfos {
		for _, v := range diskInfo.VolumeInfos {
			if fn(v) {
				n.selectedVolumes[v.Id] = v
			}
		}
	}
}

func sortWritableVolumes(volumes []*master_pb.VolumeInformationMessage) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Size < volumes[j].Size
	})
}

func sortReadOnlyVolumes(volumes []*master_pb.VolumeInformationMessage) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Id < volumes[j].Id
	})
}

func balanceSelectedVolume(commandEnv *CommandEnv, diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, capacityFunc CapacityFunc, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage), applyBalancing bool) (err error) {
	selectedVolumeCount, volumeMaxCount := 0, 0
	var nodesWithCapacity []*Node
	for _, dn := range nodes {
		selectedVolumeCount += len(dn.selectedVolumes)
		capacity := capacityFunc(dn.info)
		if capacity > 0 {
			nodesWithCapacity = append(nodesWithCapacity, dn)
		}
		volumeMaxCount += capacity
	}

	idealVolumeRatio := divide(selectedVolumeCount, volumeMaxCount)

	hasMoved := true

	// fmt.Fprintf(os.Stdout, " total %d volumes, max %d volumes, idealVolumeRatio %f\n", selectedVolumeCount, volumeMaxCount, idealVolumeRatio)

	for hasMoved {
		hasMoved = false
		sort.Slice(nodesWithCapacity, func(i, j int) bool {
			return nodesWithCapacity[i].localVolumeRatio(capacityFunc) < nodesWithCapacity[j].localVolumeRatio(capacityFunc)
		})

		fullNode := nodesWithCapacity[len(nodesWithCapacity)-1]
		var candidateVolumes []*master_pb.VolumeInformationMessage
		for _, v := range fullNode.selectedVolumes {
			candidateVolumes = append(candidateVolumes, v)
		}
		sortCandidatesFn(candidateVolumes)

		for i := 0; i < len(nodesWithCapacity)-1; i++ {
			emptyNode := nodesWithCapacity[i]
			if !(fullNode.localVolumeRatio(capacityFunc) > idealVolumeRatio && emptyNode.localVolumeNextRatio(capacityFunc) <= idealVolumeRatio) {
				// no more volume servers with empty slots
				break
			}
			fmt.Fprintf(os.Stdout, "%s %.2f %.2f:%.2f\t", diskType.ReadableString(), idealVolumeRatio, fullNode.localVolumeRatio(capacityFunc), emptyNode.localVolumeNextRatio(capacityFunc))
			hasMoved, err = attemptToMoveOneVolume(commandEnv, volumeReplicas, fullNode, candidateVolumes, emptyNode, applyBalancing)
			if err != nil {
				return
			}
			if hasMoved {
				// moved one volume
				break
			}
		}
	}
	return nil
}

func attemptToMoveOneVolume(commandEnv *CommandEnv, volumeReplicas map[uint32][]*VolumeReplica, fullNode *Node, candidateVolumes []*master_pb.VolumeInformationMessage, emptyNode *Node, applyBalancing bool) (hasMoved bool, err error) {

	for _, v := range candidateVolumes {
		hasMoved, err = maybeMoveOneVolume(commandEnv, volumeReplicas, fullNode, v, emptyNode, applyBalancing)
		if err != nil {
			return
		}
		if hasMoved {
			break
		}
	}
	return
}

func maybeMoveOneVolume(commandEnv *CommandEnv, volumeReplicas map[uint32][]*VolumeReplica, fullNode *Node, candidateVolume *master_pb.VolumeInformationMessage, emptyNode *Node, applyChange bool) (hasMoved bool, err error) {

	if candidateVolume.ReplicaPlacement > 0 {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(candidateVolume.ReplicaPlacement))
		if !isGoodMove(replicaPlacement, volumeReplicas[candidateVolume.Id], fullNode, emptyNode) {
			return false, nil
		}
	}
	if _, found := emptyNode.selectedVolumes[candidateVolume.Id]; !found {
		if err = moveVolume(commandEnv, candidateVolume, fullNode, emptyNode, applyChange); err == nil {
			adjustAfterMove(candidateVolume, volumeReplicas, fullNode, emptyNode)
			return true, nil
		} else {
			return
		}
	}
	return
}

func moveVolume(commandEnv *CommandEnv, v *master_pb.VolumeInformationMessage, fullNode *Node, emptyNode *Node, applyChange bool) error {
	collectionPrefix := v.Collection + "_"
	if v.Collection == "" {
		collectionPrefix = ""
	}
	fmt.Fprintf(os.Stdout, "  moving %s volume %s%d %s => %s\n", v.DiskType, collectionPrefix, v.Id, fullNode.info.Id, emptyNode.info.Id)
	if applyChange {
		return LiveMoveVolume(commandEnv.option.GrpcDialOption, os.Stderr, needle.VolumeId(v.Id), pb.NewServerAddressFromDataNode(fullNode.info), pb.NewServerAddressFromDataNode(emptyNode.info), 5*time.Second, v.DiskType, false)
	}
	return nil
}

func isGoodMove(placement *super_block.ReplicaPlacement, existingReplicas []*VolumeReplica, sourceNode, targetNode *Node) bool {
	for _, replica := range existingReplicas {
		if replica.location.dataNode.Id == targetNode.info.Id &&
			replica.location.rack == targetNode.rack &&
			replica.location.dc == targetNode.dc {
			// never move to existing nodes
			return false
		}
	}
	dcs, racks := make(map[string]bool), make(map[string]int)
	for _, replica := range existingReplicas {
		if replica.location.dataNode.Id != sourceNode.info.Id {
			dcs[replica.location.DataCenter()] = true
			racks[replica.location.Rack()]++
		}
	}

	dcs[targetNode.dc] = true
	racks[fmt.Sprintf("%s %s", targetNode.dc, targetNode.rack)]++

	if len(dcs) != placement.DiffDataCenterCount+1 {
		return false
	}

	if len(racks) != placement.DiffRackCount+placement.DiffDataCenterCount+1 {
		return false
	}

	for _, sameRackCount := range racks {
		if sameRackCount != placement.SameRackCount+1 {
			return false
		}
	}

	return true

}

func adjustAfterMove(v *master_pb.VolumeInformationMessage, volumeReplicas map[uint32][]*VolumeReplica, fullNode *Node, emptyNode *Node) {
	delete(fullNode.selectedVolumes, v.Id)
	if emptyNode.selectedVolumes != nil {
		emptyNode.selectedVolumes[v.Id] = v
	}
	existingReplicas := volumeReplicas[v.Id]
	for _, replica := range existingReplicas {
		if replica.location.dataNode.Id == fullNode.info.Id &&
			replica.location.rack == fullNode.rack &&
			replica.location.dc == fullNode.dc {
			loc := newLocation(emptyNode.dc, emptyNode.rack, emptyNode.info)
			replica.location = &loc
			return
		}
	}
}
