package shell

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

const thresholdVolumeSize = 1.01

func init() {
	Commands = append(Commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
	volumeSizeLimitMb  uint64
	commandEnv         *CommandEnv
	volumeByActive     *bool
	applyBalancing     bool
	volumesPerExec     int
	ioBytePerSecond    int64
	maxParallelization int
	movedCount         int
	byDiskUsage        bool
	balanceMu          sync.Mutex

	// diskUsageHighWaterPercent skips a move target whose physical disk used%
	// is at or above this mark. 0 or >=100 disables the gate.
	diskUsageHighWaterPercent int
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-collection ALL_COLLECTIONS|EACH_COLLECTION|<collection_name>] [-apply] [-dataCenter=<data_center_name>] [-racks=rack_name_one,rack_name_two] [-nodes=192.168.0.1:8080,192.168.0.2:8080] [-volumesPerExec=5] [-maxParallelization=1] [-byDiskUsage] [-maxDiskUsagePercent=90]

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
	The -maxParallelization parameter limits the number of volume moves running at the same time.
	The default value of 1 keeps the original sequential behavior.

	The -maxDiskUsagePercent flag (default 90) skips any move target whose physical disk is already used at
	or above that percentage, using the real filesystem capacity each volume server reports. This is the
	default guard against an over-configured maxVolumeCount making a physically full disk look empty: such
	a server is never chosen as a move target, judged per server against its own disk so heterogeneous disk
	sizes are handled correctly. Set it to 0 (or >=100) to disable. Servers running an older build that does
	not report disk bytes are not gated, and balancing falls back to slot-only behavior for them.

	The -byDiskUsage flag ranks servers by their reported physical disk used percentage instead of the
	default slot-density metric. If any server does not report physical disk bytes (older build), ranking
	falls back to the sum of volume sizes for all servers, since the two scales are not comparable. The
	default metric normalizes by maxVolumeCount, so a server whose maxVolumeCount is configured too high
	for its disk looks nearly empty even when its disk is physically full, and balancing can drain
	less-full servers onto it. Use -byDiskUsage to balance actual disk usage instead.

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
	ioBytePerSecond := balanceCommand.Int64("ioBytePerSecond", 0, "limit the speed of move")
	noLock := balanceCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")
	applyBalancing := balanceCommand.Bool("apply", false, "apply the balancing plan.")
	// TODO: remove this alias
	applyBalancingAlias := balanceCommand.Bool("force", false, "apply the balancing plan (alias for -apply)")
	volumesPerExec := balanceCommand.Int("volumesPerExec", 0, "how many volumes to move in one run (default is 0 for unlimited)")
	maxParallelization := balanceCommand.Int("maxParallelization", 1, "run up to X volume moves in parallel, whenever possible")
	byDiskUsage := balanceCommand.Bool("byDiskUsage", false, "rank servers by reported physical disk used percent instead of slot density; falls back to sum of volume sizes for all servers when any server does not report disk bytes. Use when maxVolumeCount is set too high for the disk.")
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
	if *maxParallelization < 1 {
		return fmt.Errorf("maxParallelization must be >= 1")
	}
	c.ioBytePerSecond = *ioBytePerSecond
	c.volumesPerExec = *volumesPerExec
	c.maxParallelization = *maxParallelization
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

type volumeBalanceMove struct {
	volume *master_pb.VolumeInformationMessage
	source *Node
	target *Node
}

// volumeBalancePlan is the common source/target decision used by sequential
// and parallel execution. The caller decides when to mutate the in-memory
// topology: sequential execution updates it after a successful RPC, while
// parallel execution reserves it before starting the RPC.
type volumeBalancePlan struct {
	source           *Node
	target           *Node
	candidateVolumes []*master_pb.VolumeInformationMessage
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
		return balancer.VolumeDensity(diskInfo.MaxVolumeCount, volumeSizes, volumeSizeLimitMb*util.MiByte)
	}
}

// capacityByDiskUsage ranks servers by reported physical disk used percentage.
// This makes a physically full disk rank as a move source, even if the regular
// SeaweedFS volumes in topology do not make it look like the largest data holder.
// The percent scale is only comparable when every server reports disk bytes, so
// if any node lacks DiskTotalBytes (older build), all nodes fall back to the
// previous ranking by summed volume sizes with a uniform capacity: mixing the two
// scales would rank non-reporting servers as orders of magnitude fuller, and
// normalizing the fallback by MaxVolumeCount instead would reintroduce the
// over-configured-maxVolumeCount distortion this flag exists to avoid.
func capacityByDiskUsage(diskType types.DiskType, volumeSizeLimitMb uint64, nodes []*Node) DensityFunc {
	if volumeSizeLimitMb == 0 {
		volumeSizeLimitMb = util.VolumeSizeLimitGB * util.KiByte
	}
	volumeSizeLimitBytes := volumeSizeLimitMb * util.MiByte
	allReportDiskBytes := true
	for _, n := range nodes {
		if diskInfo, found := n.info.DiskInfos[string(diskType)]; found && diskInfo != nil && diskInfo.DiskTotalBytes == 0 {
			allReportDiskBytes = false
			break
		}
	}
	return func(info *master_pb.DataNodeInfo) (float64, uint64) {
		diskInfo, found := info.DiskInfos[string(diskType)]
		if !found || diskInfo == nil {
			return 0, 0
		}
		if allReportDiskBytes && diskInfo.DiskTotalBytes > 0 {
			usedBytes := uint64(0)
			if diskInfo.DiskFreeBytes < diskInfo.DiskTotalBytes {
				usedBytes = diskInfo.DiskTotalBytes - diskInfo.DiskFreeBytes
			}
			return float64(diskInfo.DiskTotalBytes) / float64(volumeSizeLimitBytes),
				balancer.UsedVolumeEquivalents(usedBytes, volumeSizeLimitBytes)
		}
		var volumeSizes uint64
		for _, volumeInfo := range diskInfo.VolumeInfos {
			volumeSizes += volumeInfo.Size
		}
		return 1, balancer.UsedVolumeEquivalents(volumeSizes, volumeSizeLimitBytes)
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
	return balancer.DensityRatio(capacityFunc(n.info))
}

func (n *Node) localVolumeDensityNextRatio(capacityFunc DensityFunc) float64 {
	return balancer.DensityNextRatio(capacityFunc(n.info))
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

// planBalance computes the state shared by both the sequential and parallel
// balancing paths: the nodes that have spare capacity, the density function
// used to rank them, and the ideal volume ratio this pass is aiming for.
// ok is false when there is no capacity to balance against (volumeCapacities
// == 0); callers should return nil without doing any work in that case.
func (c *commandVolumeBalance) planBalance(diskType types.DiskType, nodes []*Node) (nodesWithCapacity []*Node, capacityFunc DensityFunc, idealVolumeRatio float64, volumeSizeLimitMb uint64, ok bool) {
	volumeSizeLimitMb = c.volumeSizeLimitMb
	if volumeSizeLimitMb == 0 {
		volumeSizeLimitMb = util.VolumeSizeLimitGB * util.KiByte
	}
	capacityFunc = capacityByMinVolumeDensity(diskType, volumeSizeLimitMb)
	if c.byDiskUsage {
		capacityFunc = capacityByDiskUsage(diskType, volumeSizeLimitMb, nodes)
	}

	selectedVolumeCount, volumeCapacities := uint64(0), float64(0)
	for _, dn := range nodes {
		capacity, volumeCount := capacityFunc(dn.info)
		if capacity > 0 {
			nodesWithCapacity = append(nodesWithCapacity, dn)
		}
		volumeCapacities += capacity
		selectedVolumeCount += volumeCount
	}
	if volumeCapacities == 0 {
		return nil, nil, 0, volumeSizeLimitMb, false
	}
	idealVolumeRatio = float64(selectedVolumeCount) / volumeCapacities

	if c.commandEnv != nil && c.commandEnv.verbose {
		fmt.Fprintf(os.Stdout, "selected nodes %d, volumes:%d, cap:%d, idealVolumeRatio %f\n",
			len(nodesWithCapacity), selectedVolumeCount, int64(volumeCapacities), idealVolumeRatio*100)
	}
	return nodesWithCapacity, capacityFunc, idealVolumeRatio, volumeSizeLimitMb, true
}

// planBalanceMove applies the same source, target, and replica-placement
// policy for both execution modes. It only reads the current planner state;
// reserveParallelMove is the caller that turns the returned move into a
// reservation before starting a concurrent RPC.
//
// sourceFound distinguishes "all eligible nodes contain only one volume" from
// "there is a source, but no eligible target/volume pair" so the sequential
// path can retain its existing diagnostic output.
func (c *commandVolumeBalance) planBalanceMove(diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodesWithCapacity []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage), capacityFunc DensityFunc, idealVolumeRatio float64, volumeSizeLimitMb uint64, failedTargets map[string]struct{}) (plan *volumeBalancePlan, sourceFound bool) {
	if len(nodesWithCapacity) == 0 {
		return nil, false
	}

	sourceCandidates := make([]balancer.NodeLoad, 0, len(nodesWithCapacity))
	for _, node := range nodesWithCapacity {
		if len(node.selectedVolumes) == 0 || node.isOneVolumeOnly() {
			continue
		}
		sourceCandidates = append(sourceCandidates, balancer.NodeLoad{
			ID:    node.info.Id,
			Score: node.localVolumeDensityRatio(capacityFunc),
		})
	}
	if len(sourceCandidates) == 0 {
		return nil, false
	}
	sourceIndex := balancer.PickMostLoaded(sourceCandidates)
	source := nodeByID(nodesWithCapacity, sourceCandidates[sourceIndex].ID)
	if source == nil {
		return nil, false
	}
	if source.localVolumeDensityNextRatio(capacityFunc) <= idealVolumeRatio {
		return nil, true
	}

	candidateVolumes := make([]*master_pb.VolumeInformationMessage, 0, len(source.selectedVolumes))
	for _, volume := range source.selectedVolumes {
		if volume.RemoteStorageName != "" {
			continue
		}
		candidateVolumes = append(candidateVolumes, volume)
	}
	sortCandidatesFn(candidateVolumes)

	targetCandidates := make([]balancer.NodeLoad, 0, len(nodesWithCapacity))
	volumesByTarget := make(map[string][]*master_pb.VolumeInformationMessage)
	for _, target := range nodesWithCapacity {
		if target.info.Id == source.info.Id {
			continue
		}
		if _, failed := failedTargets[target.info.Id]; failed {
			continue
		}
		// In byte-usage mode the ranking ignores volume slots, so skip targets
		// that are already at MaxVolumeCount so balancing never exceeds the
		// slot limit.
		if c.byDiskUsage && !target.hasFreeVolumeSlot(diskType) {
			continue
		}
		// Never move onto a server whose physical disk is already near full,
		// even if the density metric ranks it as the emptiest node.
		if c.targetDiskTooFull(target, diskType, volumeSizeLimitMb) {
			if c.commandEnv != nil && c.commandEnv.verbose {
				fmt.Fprintf(os.Stdout, "skip target %s: disk used%% >= %d%%\n", target.info.Id, c.diskUsageHighWaterPercent)
			}
			continue
		}
		if target.localVolumeDensityNextRatio(capacityFunc) > idealVolumeRatio {
			continue
		}

		eligibleVolumes := make([]*master_pb.VolumeInformationMessage, 0, len(candidateVolumes))
		for _, volume := range candidateVolumes {
			if _, found := target.selectedVolumes[volume.Id]; found {
				continue
			}
			if volume.ReplicaPlacement > 0 {
				replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(volume.ReplicaPlacement))
				if !isGoodMove(replicaPlacement, volumeReplicas[volume.Id], source, target) {
					continue
				}
			}
			eligibleVolumes = append(eligibleVolumes, volume)
		}
		if len(eligibleVolumes) == 0 {
			continue
		}
		targetCandidates = append(targetCandidates, balancer.NodeLoad{
			ID:    target.info.Id,
			Score: target.localVolumeDensityRatio(capacityFunc),
		})
		volumesByTarget[target.info.Id] = eligibleVolumes
	}

	targetIndex := balancer.PickLeastLoaded(targetCandidates)
	if targetIndex < 0 {
		return nil, true
	}
	target := nodeByID(nodesWithCapacity, targetCandidates[targetIndex].ID)
	if target == nil {
		return nil, true
	}
	return &volumeBalancePlan{
		source:           source,
		target:           target,
		candidateVolumes: volumesByTarget[target.info.Id],
	}, true
}

func nodeByID(nodes []*Node, id string) *Node {
	for _, node := range nodes {
		if node.info.Id == id {
			return node
		}
	}
	return nil
}

func (c *commandVolumeBalance) printBalancePlan(diskType types.DiskType, plan *volumeBalancePlan, capacityFunc DensityFunc, idealVolumeRatio float64) {
	if c.commandEnv != nil && c.commandEnv.verbose {
		fullRatio := plan.source.localVolumeDensityRatio(capacityFunc)
		emptyNextRatio := plan.target.localVolumeDensityNextRatio(capacityFunc)
		fmt.Fprintf(os.Stdout, "%s %.2f %.2f:%.2f\t", diskType.ReadableString(), idealVolumeRatio, fullRatio, emptyNextRatio)
	}
}

func (c *commandVolumeBalance) balanceSelectedVolume(diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage)) (err error) {
	if c.maxParallelization > 1 {
		return c.balanceSelectedVolumeParallel(diskType, volumeReplicas, nodes, sortCandidatesFn)
	}

	nodesWithCapacity, capacityFunc, idealVolumeRatio, volumeSizeLimitMb, ok := c.planBalance(diskType, nodes)
	if !ok {
		return nil
	}

	hasMoved := true
	for hasMoved {
		hasMoved = false
		if c.volumesPerExec > 0 && c.movedCount >= c.volumesPerExec {
			break
		}
		if len(nodesWithCapacity) == 0 {
			if c.commandEnv != nil && c.commandEnv.verbose {
				fmt.Fprintf(os.Stdout, "no volume server found with capacity for %s", diskType.ReadableString())
			}
			return nil
		}

		plan, sourceFound := c.planBalanceMove(diskType, volumeReplicas, nodesWithCapacity, sortCandidatesFn,
			capacityFunc, idealVolumeRatio, volumeSizeLimitMb, nil)
		if !sourceFound {
			if c.commandEnv != nil && c.commandEnv.verbose {
				fmt.Fprintf(os.Stdout, "no nodes with capacity found for %s, nodes %d", diskType.ReadableString(), len(nodesWithCapacity))
			}
			return nil
		}
		if plan == nil {
			return nil
		}

		c.printBalancePlan(diskType, plan, capacityFunc, idealVolumeRatio)
		hasMoved, err = attemptToMoveOneVolume(c.commandEnv, volumeReplicas, plan.source,
			plan.candidateVolumes, plan.target, c.ioBytePerSecond, c.applyBalancing)
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
		}
	}
	return nil
}

func (c *commandVolumeBalance) balanceSelectedVolumeParallel(diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodes []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage)) error {
	nodesWithCapacity, capacityFunc, idealVolumeRatio, volumeSizeLimitMb, ok := c.planBalance(diskType, nodes)
	if !ok {
		return nil
	}
	failedTargets := make(map[string]struct{})

	for {
		c.balanceMu.Lock()
		moves := make([]volumeBalanceMove, 0, c.maxParallelization)
		for len(moves) < c.maxParallelization {
			if c.volumesPerExec > 0 && c.movedCount >= c.volumesPerExec {
				break
			}
			move, ok := c.reserveParallelMove(diskType, volumeReplicas, nodesWithCapacity, sortCandidatesFn, capacityFunc, idealVolumeRatio, volumeSizeLimitMb, failedTargets)
			if !ok {
				break
			}
			moves = append(moves, *move)
		}
		c.balanceMu.Unlock()

		if len(moves) == 0 {
			return nil
		}

		ewg := NewErrorWaitGroup(c.maxParallelization)
		for _, move := range moves {
			move := move
			ewg.Add(func() error {
				return c.executeParallelMove(move, volumeReplicas, failedTargets)
			})
		}
		if err := ewg.Wait(); err != nil {
			return err
		}
	}
}

func (c *commandVolumeBalance) reserveParallelMove(diskType types.DiskType, volumeReplicas map[uint32][]*VolumeReplica, nodesWithCapacity []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage), capacityFunc DensityFunc, idealVolumeRatio float64, volumeSizeLimitMb uint64, failedTargets map[string]struct{}) (*volumeBalanceMove, bool) {
	plan, _ := c.planBalanceMove(diskType, volumeReplicas, nodesWithCapacity, sortCandidatesFn,
		capacityFunc, idealVolumeRatio, volumeSizeLimitMb, failedTargets)
	if plan == nil || len(plan.candidateVolumes) == 0 {
		return nil, false
	}

	c.printBalancePlan(diskType, plan, capacityFunc, idealVolumeRatio)
	volume := plan.candidateVolumes[0]
	// Reserve the move before releasing the planner lock. This makes the next
	// reservation observe the updated source, target, replica, and disk
	// accounting while the network operation runs in parallel.
	adjustAfterMove(volume, volumeReplicas, plan.source, plan.target)
	c.movedCount++
	return &volumeBalanceMove{volume: volume, source: plan.source, target: plan.target}, true
}

func (c *commandVolumeBalance) executeParallelMove(move volumeBalanceMove, volumeReplicas map[uint32][]*VolumeReplica, failedTargets map[string]struct{}) error {
	if !c.commandEnv.isLocked() {
		c.balanceMu.Lock()
		rollbackParallelMove(move, volumeReplicas)
		c.movedCount--
		c.balanceMu.Unlock()
		return fmt.Errorf("lock is lost")
	}

	if err := moveVolume(c.commandEnv, move.volume, move.source, move.target, c.ioBytePerSecond, c.applyBalancing); err != nil {
		if c.commandEnv != nil && c.commandEnv.verbose {
			fmt.Fprintf(os.Stdout, "attempt to parallel move volume error %+v\n", err)
		}
		c.balanceMu.Lock()
		rollbackParallelMove(move, volumeReplicas)
		c.movedCount--
		if strings.Contains(err.Error(), util.ErrVolumeNoSpaceLeft) {
			failedTargets[move.target.info.Id] = struct{}{}
			c.balanceMu.Unlock()
			return nil
		}
		c.balanceMu.Unlock()
		return err
	}
	return nil
}

func rollbackParallelMove(move volumeBalanceMove, volumeReplicas map[uint32][]*VolumeReplica) {
	delete(move.target.selectedVolumes, move.volume.Id)
	if move.source.selectedVolumes == nil {
		move.source.selectedVolumes = make(map[uint32]*master_pb.VolumeInformationMessage)
	}
	move.source.selectedVolumes[move.volume.Id] = move.volume

	for _, replica := range volumeReplicas[move.volume.Id] {
		if replica.location.dataNode.Id != move.target.info.Id || replica.location.rack != move.target.rack || replica.location.dc != move.target.dc {
			continue
		}
		loc := newLocation(move.source.dc, move.source.rack, move.source.info)
		replica.location = &loc
		if targetDisk, found := move.target.info.DiskInfos[move.volume.DiskType]; found {
			removeVolumeInfo(targetDisk, move.volume.Id)
			addVolumeCount(targetDisk, -1)
			addDiskFreeBytes(targetDisk, int64(move.volume.Size))
		}
		if sourceDisk, found := move.source.info.DiskInfos[move.volume.DiskType]; found {
			sourceDisk.VolumeInfos = append(sourceDisk.VolumeInfos, move.volume)
			addVolumeCount(sourceDisk, 1)
			addDiskFreeBytes(sourceDisk, -int64(move.volume.Size))
		}
		return
	}
}

func attemptToMoveOneVolume(commandEnv *CommandEnv, volumeReplicas map[uint32][]*VolumeReplica, fullNode *Node, candidateVolumes []*master_pb.VolumeInformationMessage, emptyNode *Node, ioBytePerSecond int64, applyBalancing bool) (hasMoved bool, err error) {

	for _, v := range candidateVolumes {
		hasMoved, err = maybeMoveOneVolume(commandEnv, volumeReplicas, fullNode, v, emptyNode, ioBytePerSecond, applyBalancing)
		if err != nil {
			return
		}
		if hasMoved {
			break
		}
	}
	return
}

func maybeMoveOneVolume(commandEnv *CommandEnv, volumeReplicas map[uint32][]*VolumeReplica, fullNode *Node, candidateVolume *master_pb.VolumeInformationMessage, emptyNode *Node, ioBytePerSecond int64, applyChange bool) (hasMoved bool, err error) {
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
		if err = moveVolume(commandEnv, candidateVolume, fullNode, emptyNode, ioBytePerSecond, applyChange); err == nil {
			adjustAfterMove(candidateVolume, volumeReplicas, fullNode, emptyNode)
			return true, nil
		} else {
			return
		}
	}
	return
}

func moveVolume(commandEnv *CommandEnv, v *master_pb.VolumeInformationMessage, fullNode *Node, emptyNode *Node, ioBytePerSecond int64, applyChange bool) error {
	collectionPrefix := v.Collection + "_"
	if v.Collection == "" {
		collectionPrefix = ""
	}
	fmt.Fprintf(os.Stdout, "  moving %s volume %s%d %s => %s\n", v.DiskType, collectionPrefix, v.Id, fullNode.info.Id, emptyNode.info.Id)
	if applyChange {
		return LiveMoveVolume(context.Background(), commandEnv.option.GrpcDialOption, os.Stderr, needle.VolumeId(v.Id), pb.NewServerAddressFromDataNode(fullNode.info), pb.NewServerAddressFromDataNode(emptyNode.info), 5*time.Second, v.DiskType, ioBytePerSecond, v.ReadOnly)
	}
	return nil
}

// toBalancerLocation converts a shell replica location to the shared placement
// abstraction, resolving the physical host for machine anti-affinity.
func toBalancerLocation(loc *location) balancer.Location {
	return balancer.Location{
		DataCenter: loc.dc,
		Rack:       loc.rack,
		NodeID:     loc.dataNode.Id,
		Host:       pb.NewServerAddressFromDataNode(loc.dataNode).ToHost(),
	}
}

func isGoodMove(placement *super_block.ReplicaPlacement, existingReplicas []*VolumeReplica, sourceNode, targetNode *Node) bool {
	locs := make([]balancer.Location, len(existingReplicas))
	for i, replica := range existingReplicas {
		locs[i] = toBalancerLocation(replica.location)
	}
	target := balancer.Location{
		DataCenter: targetNode.dc,
		Rack:       targetNode.rack,
		NodeID:     targetNode.info.Id,
		Host:       pb.NewServerAddressFromDataNode(targetNode.info).ToHost(),
	}
	return balancer.IsGoodMove(placement, locs, sourceNode.info.Id, target)
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
