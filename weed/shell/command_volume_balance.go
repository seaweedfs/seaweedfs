package shell

import (
	"cmp"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

const (
	thresholdVolumeSize      = 1.01
	countZeroSelectedVolumes = 0.5
)

func init() {
	Commands = append(Commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
	volumeSizeLimitMb uint64
	commandEnv        *CommandEnv
	volumeByActive    *bool
	applyBalancing    bool
	volumesPerExec    int
	movedCount        int
	byDiskUsage       bool

	// diskUsageHighWaterPercent skips a move target whose physical disk used%
	// is at or above this mark. 0 or >=100 disables the gate.
	diskUsageHighWaterPercent int
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-collection ALL_COLLECTIONS|EACH_COLLECTION|<collection_name>] [-apply] [-dataCenter=<data_center_name>] [-racks=rack_name_one,rack_name_two] [-nodes=192.168.0.1:8080,192.168.0.2:8080] [-volumesPerExec=5] [-byDiskUsage] [-maxDiskUsagePercent=90]

	The -collection parameter supports:
	  - ALL_COLLECTIONS: balance across all collections
	  - EACH_COLLECTION: balance each collection separately
	  - Regular expressions for pattern matching:
	    * Use exact match: volume.balance -collection="^mybucket$"
	    * Match multiple buckets: volume.balance -collection="bucket.*"
	    * Match all user collections: volume.balance -collection="user-.*"

	The -volumesPerExec parameter limits the maximum number of volume moves in one command execution.
	If unset - the command will try to balance all volumes at once.
	It might be beneficial to set, if your cluster has lots of volumes growing and topology changes faster than balancing can occur.

	The -maxDiskUsagePercent flag (default 90) skips any move target whose physical disk is already used at
	or above that percentage, using the real filesystem capacity each volume server reports. This is the
	default guard against an over-configured maxVolumeCount making a physically full disk look empty: such
	a server is never chosen as a move target, judged per server against its own disk so heterogeneous disk
	sizes are handled correctly. Set it to 0 (or >=100) to disable. Servers running an older build that does
	not report disk bytes are not gated, and balancing falls back to slot-only behavior for them.

	The -byDiskUsage flag ranks servers by the actual data they hold (sum of volume sizes) instead of the
	default slot-density metric. The default metric normalizes by maxVolumeCount, so a server whose
	maxVolumeCount is configured too high for its disk looks nearly empty even when its disk is physically
	full, and balancing can drain less-full servers onto it. Use -byDiskUsage to balance actual data
	distribution instead. It assumes comparable disk sizes across servers of the same disk type.

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
	allowedVolumeBy := map[string]*bool{
		"ALL":    nil,
		"ACTIVE": new(bool),
		"FULL":   new(bool),
	}
	*allowedVolumeBy["ACTIVE"] = true
	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := balanceCommand.Bool("v", false, "verbose mode")
	collection := balanceCommand.String("collection", "ALL_COLLECTIONS", "collection name, or use \"ALL_COLLECTIONS\" across collections, \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	racks := balanceCommand.String("racks", "", "only apply the balancing for this racks")
	nodes := balanceCommand.String("nodes", "", "only apply the balancing for this nodes")
	noLock := balanceCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")
	applyBalancing := balanceCommand.Bool("apply", false, "apply the balancing plan.")
	// TODO: remove this alias
	applyBalancingAlias := balanceCommand.Bool("force", false, "apply the balancing plan (alias for -apply)")
	volumesPerExec := balanceCommand.Int("volumesPerExec", 0, "how many volumes to move in one run (default is 0 for unlimited)")
	byDiskUsage := balanceCommand.Bool("byDiskUsage", false, "rank servers by actual data held (sum of volume sizes) instead of slot density; use when maxVolumeCount is set too high for the disk. Assumes comparable disk sizes per disk type.")
	maxDiskUsagePercent := balanceCommand.Int("maxDiskUsagePercent", balancer.DefaultMaxDiskUsagePercent, "skip a move target whose physical disk used%% is at/above this; judged per server against its own disk, so heterogeneous disk sizes are fine. 0 or >=100 disables. Auto-skipped for servers that do not report disk bytes.")

	balanceCommand.Func("volumeBy", "only apply the balancing for ALL volumes and ACTIVE or FULL", func(flagValue string) error {
		if flagValue == "" {
			return nil
		}
		for allowed, volumeBy := range allowedVolumeBy {
			if flagValue == allowed {
				c.volumeByActive = volumeBy
				return nil
			}
		}
		return fmt.Errorf("use \"ALL\", \"ACTIVE\" or \"FULL\"")
	})
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}
	handleDeprecatedForceFlag(writer, balanceCommand, applyBalancingAlias, applyBalancing)
	c.applyBalancing = *applyBalancing
	if *volumesPerExec < 0 {
		return fmt.Errorf("volumesPerExec must be >= 0")
	}
	c.volumesPerExec = *volumesPerExec
	c.movedCount = 0
	c.byDiskUsage = *byDiskUsage
	c.diskUsageHighWaterPercent = *maxDiskUsagePercent

	infoAboutSimulationMode(writer, c.applyBalancing, "-apply")

	if *noLock {
		commandEnv.noLock = true
	} else {
		if err = commandEnv.confirmIsLocked(args); err != nil {
			return
		}
	}
	commandEnv.verbose = *verbose
	c.commandEnv = commandEnv

	// collect topology information
	var topologyInfo *master_pb.TopologyInfo
	topologyInfo, c.volumeSizeLimitMb, err = collectTopologyInfo(commandEnv, 5*time.Second)
	if err != nil {
		return err
	}

	volumeServers := collectVolumeServersByDcRackNode(topologyInfo, *dc, *racks, *nodes)
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	diskTypes := collectVolumeDiskTypes(topologyInfo)

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, true, false)
		if err != nil {
			return err
		}
		for _, col := range collections {
			if c.volumesPerExec > 0 && c.movedCount >= c.volumesPerExec {
				break
			}
			// Use direct string comparison for exact match (more efficient than regex)
			if err = c.balanceVolumeServers(diskTypes, volumeReplicas, volumeServers, nil, col); err != nil {
				return err
			}
		}
	} else if *collection == "ALL_COLLECTIONS" {
		// Pass nil pattern for all collections
		if err = c.balanceVolumeServers(diskTypes, volumeReplicas, volumeServers, nil, *collection); err != nil {
			return err
		}
	} else {
		// Compile user-provided pattern
		collectionPattern, err := compileCollectionPattern(*collection)
		if err != nil {
			return fmt.Errorf("invalid collection pattern '%s': %v", *collection, err)
		}
		if err = c.balanceVolumeServers(diskTypes, volumeReplicas, volumeServers, collectionPattern, *collection); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandVolumeBalance) balanceVolumeServers(diskTypes []types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, collectionPattern *regexp.Regexp, collectionName string) error {
	for _, diskType := range diskTypes {
		if c.volumesPerExec > 0 && c.movedCount >= c.volumesPerExec {
			break
		}
		if err := c.balanceVolumeServersByDiskType(diskType, volumeReplicas, nodes, collectionPattern, collectionName); err != nil {
			return err
		}
	}
	return nil
}

func (c *commandVolumeBalance) balanceVolumeServersByDiskType(diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, collectionPattern *regexp.Regexp, collectionName string) error {
	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collectionName != "ALL_COLLECTIONS" {
				if collectionPattern != nil {
					// Use regex pattern matching
					if !collectionPattern.MatchString(v.Collection) {
						return false
					}
				} else {
					// Use exact string matching (for EACH_COLLECTION)
					if v.Collection != collectionName {
						return false
					}
				}
			}
			if v.DiskType != string(diskType) {
				return false
			}
			return selectVolumesByActive(v.Size, c.volumeByActive, c.volumeSizeLimitMb)
		})
	}
	if err := c.balanceSelectedVolume(diskType, volumeReplicas, nodes, sortWritableVolumes); err != nil {
		return err
	}

	return nil
}

// splitCSVSet parses a comma-separated list into a set for exact-match filtering.
// Whitespace around items is trimmed and empty items are skipped, so callers
// can use len(set) > 0 to test whether any filter was specified.
func splitCSVSet(csv string) map[string]bool {
	set := make(map[string]bool)
	for _, item := range strings.Split(csv, ",") {
		if item = strings.TrimSpace(item); item != "" {
			set[item] = true
		}
	}
	return set
}

func collectVolumeServersByDcRackNode(t *master_pb.TopologyInfo, selectedDataCenter string, selectedRacks string, selectedNodes string) (nodes []*Node) {
	rackSet := splitCSVSet(selectedRacks)
	nodeSet := splitCSVSet(selectedNodes)
	for _, dc := range t.DataCenterInfos {
		if selectedDataCenter != "" && dc.Id != selectedDataCenter {
			continue
		}
		for _, r := range dc.RackInfos {
			if len(rackSet) > 0 && !rackSet[r.Id] {
				continue
			}
			for _, dn := range r.DataNodeInfos {
				if len(nodeSet) > 0 && !nodeSet[dn.Id] {
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
type DensityFunc func(*master_pb.DataNodeInfo) (float64, uint64)

func capacityByMinVolumeDensity(diskType types.DiskType, volumeSizeLimitMb uint64) DensityFunc {
	return func(info *master_pb.DataNodeInfo) (float64, uint64) {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found {
			return 0, 0
		}
		var volumeSizes uint64
		for _, volumeInfo := range diskInfo.VolumeInfos {
			volumeSizes += volumeInfo.Size
		}
		if volumeSizeLimitMb == 0 {
			volumeSizeLimitMb = util.VolumeSizeLimitGB * util.KiByte
		}
		usedVolumeCount := volumeSizes / (volumeSizeLimitMb * util.MiByte)
		return float64(diskInfo.MaxVolumeCount - int64(usedVolumeCount)), usedVolumeCount
	}
}

// capacityByActualDataUsage ranks servers purely by how much actual data they
// hold (sum of volume sizes), ignoring MaxVolumeCount. The slot-density metric
// divides by MaxVolumeCount, so a server whose MaxVolumeCount was configured too
// high for its disk looks nearly empty even when its disk is physically full and
// gets picked as a move target. This function keeps the fullest-by-data server
// ranked as full so balancing drains it instead of piling onto it. It assumes
// comparable disk sizes across servers of the same disk type. Capacity is a
// uniform constant so the density ratio is proportional to actual data; the
// constant cancels out of every ratio comparison in balanceSelectedVolume.
func capacityByActualDataUsage(diskType types.DiskType, volumeSizeLimitMb uint64) DensityFunc {
	return func(info *master_pb.DataNodeInfo) (float64, uint64) {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found || diskInfo == nil {
			return 0, 0
		}
		var volumeSizes uint64
		for _, volumeInfo := range diskInfo.VolumeInfos {
			volumeSizes += volumeInfo.Size
		}
		if volumeSizeLimitMb == 0 {
			volumeSizeLimitMb = util.VolumeSizeLimitGB * util.KiByte
		}
		usedVolumeCount := volumeSizes / (volumeSizeLimitMb * util.MiByte)
		return 1, usedVolumeCount
	}
}

func capacityByMaxVolumeCount(diskType types.DiskType) CapacityFunc {
	return func(info *master_pb.DataNodeInfo) float64 {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found {
			return 0
		}
		var ecShardCount int
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			ecShardCount += erasure_coding.GetShardCount(ecShardInfo)
		}
		return float64(diskInfo.MaxVolumeCount) - float64(ecShardCount)/erasure_coding.DataShardsCount
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
			ecShardCount += erasure_coding.GetShardCount(ecShardInfo)
		}
		return float64(diskInfo.MaxVolumeCount-diskInfo.VolumeCount) - float64(ecShardCount)/erasure_coding.DataShardsCount
	}
}

func (n *Node) localVolumeDensityRatio(capacityFunc DensityFunc) float64 {
	capacity, selectedVolumes := capacityFunc(n.info)
	if capacity == 0 {
		return 0
	}
	if selectedVolumes == 0 {
		return countZeroSelectedVolumes / capacity
	}
	return float64(selectedVolumes) / capacity
}

func (n *Node) localVolumeDensityNextRatio(capacityFunc DensityFunc) float64 {
	capacity, selectedVolumes := capacityFunc(n.info)
	if capacity == 0 {
		return 0
	}
	return float64(selectedVolumes+1) / capacity
}

func (n *Node) localVolumeRatio(capacityFunc CapacityFunc) float64 {
	return float64(len(n.selectedVolumes)) / capacityFunc(n.info)
}

func (n *Node) hasFreeVolumeSlot(diskType types.DiskType) bool {
	diskInfo, found := n.info.DiskInfos[string(diskType)]
	if !found || diskInfo == nil {
		return false
	}
	return diskInfo.VolumeCount < diskInfo.MaxVolumeCount
}

// diskBytes returns the node's physical disk capacity and free bytes for a disk
// type. ok is false when the volume server did not report it (DiskTotalBytes==0),
// which makes callers fall back to slot-only behavior.
func (n *Node) diskBytes(diskType types.DiskType) (total, free uint64, ok bool) {
	diskInfo, found := n.info.DiskInfos[string(diskType)]
	if !found || diskInfo == nil || diskInfo.DiskTotalBytes == 0 {
		return 0, 0, false
	}
	return diskInfo.DiskTotalBytes, diskInfo.DiskFreeBytes, true
}

// targetDiskTooFull reports whether moving one more volume onto node would push
// its physical disk used% at/above the high-water mark. It judges each server
// against its own disk, so a larger disk holding more bytes is not unfairly
// excluded. Returns false (no opinion) when the gate is disabled or the server
// does not report disk bytes.
func (c *commandVolumeBalance) targetDiskTooFull(node *Node, diskType types.DiskType, volumeSizeLimitMb uint64) bool {
	total, free, ok := node.diskBytes(diskType)
	if !ok {
		return false
	}
	return balancer.DiskTooFullAfter(total, free, volumeSizeLimitMb*util.MiByte, c.diskUsageHighWaterPercent)
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

func selectVolumesByActive(volumeSize uint64, volumeByActive *bool, volumeSizeLimitMb uint64) bool {
	if volumeByActive == nil {
		return true
	}
	if uint64(float64(volumeSize)*thresholdVolumeSize) < volumeSizeLimitMb*util.MiByte {
		return *volumeByActive
	} else {
		return !(*volumeByActive)
	}
}

func (c *commandVolumeBalance) balanceSelectedVolume(diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage)) (err error) {
	selectedVolumeCount, volumeCapacities := uint64(0), float64(0)
	var nodesWithCapacity []*Node
	volumeSizeLimitMb := c.volumeSizeLimitMb
	if volumeSizeLimitMb == 0 {
		volumeSizeLimitMb = util.VolumeSizeLimitGB * util.KiByte
	}
	capacityFunc := capacityByMinVolumeDensity(diskType, volumeSizeLimitMb)
	if c.byDiskUsage {
		capacityFunc = capacityByActualDataUsage(diskType, volumeSizeLimitMb)
	}
	for _, dn := range nodes {
		capacity, volumeCount := capacityFunc(dn.info)
		if capacity > 0 {
			nodesWithCapacity = append(nodesWithCapacity, dn)
		}
		volumeCapacities += capacity
		selectedVolumeCount += volumeCount
	}
	if volumeCapacities == 0 {
		return nil
	}
	idealVolumeRatio := float64(selectedVolumeCount) / volumeCapacities

	hasMoved := true

	if c.commandEnv != nil && c.commandEnv.verbose {
		fmt.Fprintf(os.Stdout, "selected nodes %d, volumes:%d, cap:%d, idealVolumeRatio %f\n", len(nodesWithCapacity), selectedVolumeCount, int64(volumeCapacities), idealVolumeRatio*100)
	}
	for hasMoved {
		hasMoved = false
		if c.volumesPerExec > 0 && c.movedCount >= c.volumesPerExec {
			break
		}
		slices.SortFunc(nodesWithCapacity, func(a, b *Node) int {
			return cmp.Compare(a.localVolumeDensityRatio(capacityFunc), b.localVolumeDensityRatio(capacityFunc))
		})
		if len(nodesWithCapacity) == 0 {
			if c.commandEnv != nil && c.commandEnv.verbose {
				fmt.Fprintf(os.Stdout, "no volume server found with capacity for %s", diskType.ReadableString())
			}
			return nil
		}

		var fullNode *Node
		var fullNodeIndex int
		for fullNodeIndex = len(nodesWithCapacity) - 1; fullNodeIndex >= 0; fullNodeIndex-- {
			fullNode = nodesWithCapacity[fullNodeIndex]
			if len(fullNode.selectedVolumes) == 0 {
				continue
			}
			if !fullNode.isOneVolumeOnly() {
				break
			}
		}
		var candidateVolumes []*master_pb.VolumeInformationMessage
		for _, v := range fullNode.selectedVolumes {
			candidateVolumes = append(candidateVolumes, v)
		}
		if fullNodeIndex == -1 {
			if c.commandEnv != nil && c.commandEnv.verbose {
				fmt.Fprintf(os.Stdout, "no nodes with capacity found for %s, nodes %d", diskType.ReadableString(), len(nodesWithCapacity))
			}
			return nil
		}
		sortCandidatesFn(candidateVolumes)
		for _, emptyNode := range nodesWithCapacity[:fullNodeIndex] {
			// In byte-usage mode capacity is a uniform constant, so a target's
			// free volume slots aren't reflected in its ranking; skip targets that
			// are already at MaxVolumeCount so balancing never exceeds the slot limit.
			if c.byDiskUsage && !emptyNode.hasFreeVolumeSlot(diskType) {
				continue
			}
			// Never move onto a server whose physical disk is already near full,
			// even if the slot-density metric ranks it as the emptiest node. This is
			// the root-cause guard for an over-configured maxVolumeCount making a
			// full disk look empty; it is judged per server against its own disk.
			if c.targetDiskTooFull(emptyNode, diskType, volumeSizeLimitMb) {
				if c.commandEnv != nil && c.commandEnv.verbose {
					fmt.Fprintf(os.Stdout, "skip target %s: disk used%% >= %d%%\n", emptyNode.info.Id, c.diskUsageHighWaterPercent)
				}
				continue
			}
			if !(fullNode.localVolumeDensityNextRatio(capacityFunc) > idealVolumeRatio && emptyNode.localVolumeDensityNextRatio(capacityFunc) <= idealVolumeRatio) {
				if c.commandEnv != nil && c.commandEnv.verbose {
					fmt.Printf("no more volume servers with empty slots %s, idealVolumeRatio %f\n", emptyNode.info.Id, idealVolumeRatio)
				}
				break
			}
			fmt.Fprintf(os.Stdout, "%s %.2f %.2f:%.2f\t", diskType.ReadableString(), idealVolumeRatio,
				fullNode.localVolumeDensityRatio(capacityFunc), emptyNode.localVolumeDensityNextRatio(capacityFunc))
			if c.commandEnv != nil && c.commandEnv.verbose {
				fmt.Fprintf(os.Stdout, "%s %.1f %.1f:%.1f\t", diskType.ReadableString(), idealVolumeRatio*100,
					fullNode.localVolumeDensityRatio(capacityFunc)*100, emptyNode.localVolumeDensityNextRatio(capacityFunc)*100)
			}
			hasMoved, err = attemptToMoveOneVolume(c.commandEnv, volumeReplicas, fullNode, candidateVolumes, emptyNode, c.applyBalancing)
			if err != nil {
				if c.commandEnv != nil && c.commandEnv.verbose {
					fmt.Fprintf(os.Stdout, "attempt to move one volume error %+v\n", err)
				}
				if strings.Contains(err.Error(), util.ErrVolumeNoSpaceLeft) {
					continue
				}
				return
			}
			if hasMoved {
				c.movedCount++
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
	if !commandEnv.isLocked() {
		return false, fmt.Errorf("lock is lost")
	}

	if candidateVolume.RemoteStorageName != "" {
		return false, fmt.Errorf("does not move volume in remote storage")
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
		return LiveMoveVolume(commandEnv.option.GrpcDialOption, os.Stderr, needle.VolumeId(v.Id), pb.NewServerAddressFromDataNode(fullNode.info), pb.NewServerAddressFromDataNode(emptyNode.info), 5*time.Second, v.DiskType, 0, v.ReadOnly)
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

	// Don't move a replica onto a machine (host) that already holds one of this
	// volume's replicas: servers sharing a host are one fault domain, so both would
	// die together. Best-effort -- skip and let balancing try the next target.
	targetHost := pb.NewServerAddressFromDataNode(targetNode.info).ToHost()
	for _, replica := range existingReplicasExceptSourceNode {
		if pb.NewServerAddressFromDataNode(replica.location.dataNode).ToHost() == targetHost {
			return false
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

// addDiskFreeBytes adjusts a disk's reported free bytes by delta (negative when a
// volume lands on it), so the physical-fullness gate stays consistent as volumes
// move within a single balance run. No-op when the disk reports no physical
// capacity (DiskTotalBytes==0); clamps to [0, DiskTotalBytes].
func addDiskFreeBytes(diskInfo *master_pb.DiskInfo, delta int64) {
	if diskInfo.DiskTotalBytes == 0 {
		return
	}
	free := int64(diskInfo.DiskFreeBytes) + delta
	if free < 0 {
		free = 0
	}
	if uint64(free) > diskInfo.DiskTotalBytes {
		free = int64(diskInfo.DiskTotalBytes)
	}
	diskInfo.DiskFreeBytes = uint64(free)
}

func removeVolumeInfo(diskInfo *master_pb.DiskInfo, volumeId uint32) {
	for i, volumeInfo := range diskInfo.VolumeInfos {
		if volumeInfo.Id == volumeId {
			// order does not matter here, so swap with the last and truncate
			last := len(diskInfo.VolumeInfos) - 1
			diskInfo.VolumeInfos[i] = diskInfo.VolumeInfos[last]
			diskInfo.VolumeInfos[last] = nil
			diskInfo.VolumeInfos = diskInfo.VolumeInfos[:last]
			return
		}
	}
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
			// Move the volume's size accounting between disks so that
			// capacityByMinVolumeDensity recomputes ratios correctly on the next
			// iteration. Without this the density view stays stale and the planner
			// keeps draining the same node, moving every volume onto one server.
			if fullDisk, found := fullNode.info.DiskInfos[v.DiskType]; found {
				removeVolumeInfo(fullDisk, v.Id)
				addVolumeCount(fullDisk, -1)
				addDiskFreeBytes(fullDisk, int64(v.Size))
			}
			if emptyDisk, found := emptyNode.info.DiskInfos[v.DiskType]; found {
				emptyDisk.VolumeInfos = append(emptyDisk.VolumeInfos, v)
				addVolumeCount(emptyDisk, 1)
				addDiskFreeBytes(emptyDisk, -int64(v.Size))
			}
			return
		}
	}
}
