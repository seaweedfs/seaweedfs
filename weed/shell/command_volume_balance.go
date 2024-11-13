package shell

import (
	"cmp"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"golang.org/x/exp/slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
	commandEnv        *CommandEnv
	applyBalancing    *bool
	parallelBalancing *bool
	volumeServers     []*Node
	volumeReplicas    map[uint32][]*VolumeReplica
	diskTypes         []types.DiskType
	lock              sync.RWMutex
	wg                sync.WaitGroup
	chErrc            chan error
	chDone            chan struct{}
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-collection ALL_COLLECTIONS|EACH_COLLECTION|<collection_name>] [-force] [-dataCenter=<data_center_name>] [-racks=rack_name_one,rack_name_two] [-nodes=192.168.0.1:8080,192.168.0.2:8080]

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

func (c *commandVolumeBalance) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "ALL_COLLECTIONS", "collection name, or use \"ALL_COLLECTIONS\" across collections, \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	racks := balanceCommand.String("racks", "", "only apply the balancing for this racks")
	nodes := balanceCommand.String("nodes", "", "only apply the balancing for this nodes")
	noLock := balanceCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")
	c.applyBalancing = balanceCommand.Bool("force", false, "apply the balancing plan.")
	c.parallelBalancing = balanceCommand.Bool("parallel", false, "do parallel balancing")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *c.applyBalancing, "-force")

	if *noLock {
		commandEnv.noLock = true
	} else {
		if err = commandEnv.confirmIsLocked(args); err != nil {
			return
		}
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 5*time.Second)
	if err != nil {
		return err
	}

	c.commandEnv = commandEnv
	c.volumeServers = collectVolumeServersByDcRackNode(topologyInfo, *dc, *racks, *nodes)
	c.volumeReplicas, _ = collectVolumeReplicaLocations(topologyInfo)
	c.diskTypes = collectVolumeDiskTypes(topologyInfo)

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, true, false)
		if err != nil {
			return err
		}
		for _, eachCollection := range collections {
			if err = c.balanceVolumeServers(eachCollection); err != nil {
				return err
			}
		}
	} else {
		if err = c.balanceVolumeServers(*collection); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandVolumeBalance) balanceVolumeServers(collection string) error {
	for _, diskType := range c.diskTypes {
		if err := c.balanceVolumeServersByDiskType(diskType, collection); err != nil {
			return err
		}
	}
	return nil

}

func (c *commandVolumeBalance) balanceVolumeServersByDiskType(diskType types.DiskType, collection string) error {
	for _, n := range c.volumeServers {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL_COLLECTIONS" {
				if v.Collection != collection {
					return false
				}
			}
			return v.DiskType == string(diskType)
		})
	}
	if err := c.balanceSelectedVolume(diskType); err != nil {
		return err
	}

	return nil
}

func collectVolumeServersByDcRackNode(t *master_pb.TopologyInfo, selectedDataCenter string, selectedRacks string, selectedNodes string) (nodes []*Node) {
	for _, dc := range t.DataCenterInfos {
		if selectedDataCenter != "" && dc.Id != selectedDataCenter {
			continue
		}
		for _, r := range dc.RackInfos {
			if selectedRacks != "" && !strings.Contains(selectedRacks, r.Id) {
				continue
			}
			for _, dn := range r.DataNodeInfos {
				if selectedNodes != "" && !strings.Contains(selectedNodes, dn.Id) {
					continue
				}
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
				for diskType := range dn.DiskInfos {
					if _, found := knownTypes[diskType]; !found {
						knownTypes[diskType] = true
					}
				}
			}
		}
	}
	for diskType := range knownTypes {
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

type CapacityFunc func(*master_pb.DataNodeInfo) float64

func capacityByMaxVolumeCount(diskType types.DiskType) CapacityFunc {
	return func(info *master_pb.DataNodeInfo) float64 {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found {
			return 0
		}
		return float64(diskInfo.MaxVolumeCount)
	}
}

func capacityByFreeVolumeCount(diskType types.DiskType) CapacityFunc {
	return func(info *master_pb.DataNodeInfo) float64 {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found {
			return 0
		}
		var ecShardCount int
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			ecShardCount += erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIdCount()
		}
		return float64(diskInfo.MaxVolumeCount-diskInfo.VolumeCount) - float64(ecShardCount)/erasure_coding.DataShardsCount
	}
}

func (n *Node) localVolumeRatio(capacityFunc CapacityFunc) float64 {
	return float64(len(n.selectedVolumes)) / capacityFunc(n.info)
}

func (n *Node) localVolumeNextRatio(capacityFunc CapacityFunc) float64 {
	return float64(len(n.selectedVolumes)+1) / capacityFunc(n.info)
}

func (n *Node) isOneVolumeOnly() bool {
	if len(n.selectedVolumes) != 1 {
		return false
	}
	for _, disk := range n.info.DiskInfos {
		if disk.VolumeCount == 1 && disk.MaxVolumeCount == 1 {
			return true
		}
	}
	return false
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
	slices.SortFunc(volumes, func(a, b *master_pb.VolumeInformationMessage) int {
		return cmp.Compare(a.Size, b.Size)
	})
}

func (c *commandVolumeBalance) balanceSelectedVolume(diskType types.DiskType) (err error) {
	selectedVolumeCount, volumeMaxCount := 0, float64(0)
	var nodesWithCapacity []*Node
	capacityFunc := capacityByMaxVolumeCount(diskType)
	if *c.parallelBalancing {
		c.chErrc = make(chan error, 1)
		c.chDone = make(chan struct{})
		defer close(c.chErrc)
		defer close(c.chDone)
	}

	for _, dn := range c.volumeServers {
		selectedVolumeCount += len(dn.selectedVolumes)
		capacity := capacityFunc(dn.info)
		if capacity > 0 {
			nodesWithCapacity = append(nodesWithCapacity, dn)
		}
		volumeMaxCount += capacity
	}

	idealVolumeRatio := float64(selectedVolumeCount) / volumeMaxCount

	hasMoved := true

	// fmt.Fprintf(os.Stdout, " total %d volumes, max %d volumes, idealVolumeRatio %f\n", selectedVolumeCount, volumeMaxCount, idealVolumeRatio)
	for hasMoved {
		hasMoved = false
		slices.SortFunc(nodesWithCapacity, func(a, b *Node) int {
			return cmp.Compare(a.localVolumeRatio(capacityFunc), b.localVolumeRatio(capacityFunc))
		})
		if len(nodesWithCapacity) == 0 {
			fmt.Printf("no volume server found with capacity for %s", diskType.ReadableString())
			return nil
		}

		var fullNode *Node
		var fullNodeIndex int
		for fullNodeIndex = len(nodesWithCapacity) - 1; fullNodeIndex >= 0; fullNodeIndex-- {
			fullNode = nodesWithCapacity[fullNodeIndex]
			if !fullNode.isOneVolumeOnly() {
				break
			}
		}
		var candidateVolumes []*master_pb.VolumeInformationMessage
		for _, v := range fullNode.selectedVolumes {
			candidateVolumes = append(candidateVolumes, v)
		}
		sortWritableVolumes(candidateVolumes)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for _, emptyNode := range nodesWithCapacity[:fullNodeIndex] {
				if !(fullNode.localVolumeRatio(capacityFunc) > idealVolumeRatio && emptyNode.localVolumeNextRatio(capacityFunc) <= idealVolumeRatio) {
					// no more volume servers with empty slots
					break
				}
				fmt.Fprintf(os.Stdout, "%s %.2f %.2f:%.2f\t", diskType.ReadableString(), idealVolumeRatio, fullNode.localVolumeRatio(capacityFunc), emptyNode.localVolumeNextRatio(capacityFunc))
				hasMoved, err = c.attemptToMoveOneVolume(fullNode, candidateVolumes, emptyNode)
				if err != nil {
					return
				}
				if hasMoved {
					// moved one volume
					break
				}
			}
		}()

		if !*c.parallelBalancing {
			c.wg.Wait()
		}
	}

	if *c.parallelBalancing {
		c.wg.Wait()
	}

	return nil
}

func (c *commandVolumeBalance) attemptToMoveOneVolume(fullNode *Node, candidateVolumes []*master_pb.VolumeInformationMessage, emptyNode *Node) (hasMoved bool, err error) {

	for _, v := range candidateVolumes {
		hasMoved, err = maybeMoveOneVolume(c.commandEnv, c.volumeReplicas, fullNode, v, emptyNode, *c.applyBalancing)
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
	if !commandEnv.isLocked() {
		return false, fmt.Errorf("lock is lost")
	}

	if candidateVolume.RemoteStorageName != "" {
		return false, fmt.Errorf("does not move volume in remove storage")
	}

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
		return LiveMoveVolume(commandEnv.option.GrpcDialOption, os.Stderr, needle.VolumeId(v.Id), pb.NewServerAddressFromDataNode(fullNode.info), pb.NewServerAddressFromDataNode(emptyNode.info), 5*time.Second, v.DiskType, 0, false)
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

	// existing replicas except the one on sourceNode
	existingReplicasExceptSourceNode := make([]*VolumeReplica, 0)
	for _, replica := range existingReplicas {
		if replica.location.dataNode.Id != sourceNode.info.Id {
			existingReplicasExceptSourceNode = append(existingReplicasExceptSourceNode, replica)
		}
	}

	// target location
	targetLocation := location{
		dc:       targetNode.dc,
		rack:     targetNode.rack,
		dataNode: targetNode.info,
	}

	// check if this satisfies replication requirements
	return satisfyReplicaPlacement(placement, existingReplicasExceptSourceNode, targetLocation)
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
			for diskType, diskInfo := range fullNode.info.DiskInfos {
				if diskType == v.DiskType {
					addVolumeCount(diskInfo, -1)
				}
			}
			for diskType, diskInfo := range emptyNode.info.DiskInfos {
				if diskType == v.DiskType {
					addVolumeCount(diskInfo, 1)
				}
			}
			return
		}
	}
}
