package shell

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func init() {
	Commands = append(Commands, &commandVolumeFixReplication{})
}

type commandVolumeFixReplication struct {
	collectionPattern *string
	// TODO: move parameter flags here so we don't shuffle them around via function calls.
}

func (c *commandVolumeFixReplication) Name() string {
	return "volume.fix.replication"
}

func (c *commandVolumeFixReplication) Help() string {
	return `add or remove replicas to volumes that are missing replicas or over-replicated

	This command finds all over-replicated volumes. If found, it will purge the oldest copies and stop.

	This command also finds all under-replicated volumes, and finds volume servers with free slots.
	If the free slots satisfy the replication requirement, the volume content is copied over and mounted.

	Misplaced volumes with a surplus replica have a misplaced replica deleted. Without a surplus, a
	well-placed replica is added first and the misplaced one is trimmed on a later pass, so the volume
	never drops below its intended replica count.

	volume.fix.replication                                # do not take action
	volume.fix.replication -apply                         # actually deleting or copying the volume files and mount the volume
	volume.fix.replication -collectionPattern=important*  # fix any collections with prefix "important"

	Note:
		* each time this will only add back one replica for each volume id that is under replicated.
		  If there are multiple replicas are missing, e.g. replica count is > 2, you may need to run this multiple times.
		* do not run this too quickly within seconds, since the new volume replica may take a few seconds
		  to register itself to the master.
		* under-replicated volumes are copied up to -maxParallelization at a time, with at most
		  -maxParallelizationPerServer concurrent copies onto any single destination server.

`
}

func (c *commandVolumeFixReplication) HasTag(tag CommandTag) bool {
	return false && tag == ResourceHeavy // resource intensive only when deleting and checking with replicas.
}

func (c *commandVolumeFixReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volFixReplicationCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.collectionPattern = volFixReplicationCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	applyChanges := volFixReplicationCommand.Bool("apply", false, "apply the fix")
	// TODO: remove this alias
	applyChangesAlias := volFixReplicationCommand.Bool("force", false, "apply the fix (alias for -apply)")
	verbose := volFixReplicationCommand.Bool("verbose", false, "show volumes being checked and their statuses")
	doDelete := volFixReplicationCommand.Bool("doDelete", true, "Also delete over-replicated volumes besides fixing under-replication")
	doCheck := volFixReplicationCommand.Bool("doCheck", true, "Also check synchronization before deleting")
	maxParallelization := volFixReplicationCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	maxParallelizationPerServer := volFixReplicationCommand.Int("maxParallelizationPerServer", 1, "run up to X volume copies onto the same destination server in parallel")
	retryCount := volFixReplicationCommand.Int("retry", 5, "how many times to retry")
	volumesPerStep := volFixReplicationCommand.Int("volumesPerStep", 0, "how many volumes to fix in one cycle")

	if err = volFixReplicationCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, volFixReplicationCommand, applyChangesAlias, applyChanges)
	infoAboutSimulationMode(writer, *applyChanges, "-apply")
	commandEnv.noLock = !*applyChanges

	if err = commandEnv.confirmIsLocked(args); *applyChanges && err != nil {
		return
	}

	ewg := NewErrorWaitGroup(*maxParallelization)
	underReplicatedVolumeIdsCount := 1
	for underReplicatedVolumeIdsCount > 0 {
		fixedVolumeReplicas := map[string]int{}

		// collect topology information
		if *verbose {
			fmt.Fprintf(writer, "wait 15 seconds and then collect topology information...\n")
		}
		topologyInfo, _, err := collectTopologyInfo(commandEnv, 15*time.Second)
		if err != nil {
			return err
		}

		// find all volumes that needs replication
		// collect all data nodes
		volumeReplicas, allLocations := collectVolumeReplicaLocations(topologyInfo)

		if *verbose {
			fmt.Fprintf(writer, "collected topology: %d locations, %d volumes to check\n", len(allLocations), len(volumeReplicas))
		}

		if len(allLocations) == 0 {
			return fmt.Errorf("no data nodes at all")
		}

		// find all under replicated volumes
		var underReplicatedVolumeIds, overReplicatedVolumeIds, misplacedVolumeIds []uint32
		for vid, replicas := range volumeReplicas {
			replica := replicas[0]

			// Filter here so the termination counter matches what gets fixed; else -apply loops forever.
			if !c.matchCollectionPattern(replica.info.Collection) {
				continue
			}

			replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))

			// build locations list for optional verbose output
			locations := make([]string, 0, len(replicas))
			for _, r := range replicas {
				locations = append(locations, r.location.String())
			}

			if *verbose {
				fmt.Fprintf(writer, "checking volume %d replication %s has %d replicas [%s]\n", replica.info.Id, replicaPlacement, len(replicas), strings.Join(locations, ", "))
			}

			switch classifyReplicaSet(replicaPlacement, replicas) {
			case replicaFixAddOne:
				underReplicatedVolumeIds = append(underReplicatedVolumeIds, vid)
				fmt.Fprintf(writer, "volume %d replication %s, but under replicated %+d\n", replica.info.Id, replicaPlacement, len(replicas))
			case replicaFixAddOneBeforeTrim:
				underReplicatedVolumeIds = append(underReplicatedVolumeIds, vid)
				fmt.Fprintf(writer, "volume %d replication %s is not well placed [%s], adding a well-placed replica before trimming the misplaced one\n", replica.info.Id, replicaPlacement, strings.Join(locations, ", "))
			case replicaFixTrimMisplaced:
				misplacedVolumeIds = append(misplacedVolumeIds, vid)
				fmt.Fprintf(writer, "volume %d replication %s is not well placed [%s]\n", replica.info.Id, replicaPlacement, strings.Join(locations, ", "))
			case replicaFixTrimOver:
				overReplicatedVolumeIds = append(overReplicatedVolumeIds, vid)
				fmt.Fprintf(writer, "volume %d replication %s, but over replicated %+d\n", replica.info.Id, replicaPlacement, len(replicas))
			}
		}
		underReplicatedVolumeIdsCount = len(underReplicatedVolumeIds)

		if !commandEnv.isLocked() {
			return fmt.Errorf("lock is lost")
		}

		var deletedVolumeReplicas atomic.Int64
		ewg.Reset()
		ewg.Add(func() error {
			// find the most underpopulated data nodes
			fixedVolumeReplicas, err = c.fixUnderReplicatedVolumes(commandEnv, writer, *applyChanges, underReplicatedVolumeIds, volumeReplicas, allLocations, *retryCount, *volumesPerStep, *maxParallelization, *maxParallelizationPerServer)
			return err
		})
		if *doDelete {
			ewg.Add(func() error {
				deleted, err := c.deleteOneVolume(commandEnv, writer, *applyChanges, *doCheck, overReplicatedVolumeIds, volumeReplicas, allLocations, pickOneReplicaToDelete)
				deletedVolumeReplicas.Add(int64(deleted))
				return err
			})
			ewg.Add(func() error {
				deleted, err := c.deleteOneVolume(commandEnv, writer, *applyChanges, *doCheck, misplacedVolumeIds, volumeReplicas, allLocations, pickOneMisplacedVolume)
				deletedVolumeReplicas.Add(int64(deleted))
				return err
			})
		}
		if err := ewg.Wait(); err != nil {
			return err
		}

		if !*applyChanges {
			break
		}

		// check that the topology has been updated
		if len(fixedVolumeReplicas) > 0 {
			fixedVolumes := make([]string, 0, len(fixedVolumeReplicas))
			for k, _ := range fixedVolumeReplicas {
				fixedVolumes = append(fixedVolumes, k)
			}
			volumeIdLocations, err := lookupVolumeIds(commandEnv, fixedVolumes)
			if err != nil {
				return err
			}
			for _, volumeIdLocation := range volumeIdLocations {
				volumeId := volumeIdLocation.VolumeOrFileId
				volumeIdLocationCount := len(volumeIdLocation.Locations)
				i := 0
				for fixedVolumeReplicas[volumeId] >= volumeIdLocationCount {
					fmt.Fprintf(writer, "the number of locations for volume %s has not increased yet, let's wait\n", volumeId)
					time.Sleep(time.Duration(i+1) * time.Second * 7)
					volumeLocIds, err := lookupVolumeIds(commandEnv, []string{volumeId})
					if err != nil {
						return err
					}
					volumeIdLocationCount = len(volumeLocIds[0].Locations)
					if *retryCount <= i {
						return fmt.Errorf("replicas volume %s mismatch in topology", volumeId)
					}
					i += 1
				}
			}
		}

		// Without progress the next pass would reclassify the same volumes and
		// loop forever, e.g. when no destination can accept a replica; stop
		// instead. Deletions count as progress since they free up slots.
		if underReplicatedVolumeIdsCount > 0 && len(fixedVolumeReplicas) == 0 && deletedVolumeReplicas.Load() == 0 {
			fmt.Fprintf(writer, "no progress made on %d under replicated volumes, stopping; free up capacity or adjust replica placement, then re-run\n", underReplicatedVolumeIdsCount)
			break
		}
	}
	return nil
}

// replicaFix is the single next action volume.fix.replication takes on one
// volume's replica set.
type replicaFix int

const (
	replicaFixNothing replicaFix = iota
	// replicaFixAddOne: the volume lacks a replica, in count or in
	// failure-domain spread; copy one to a well-placed destination.
	replicaFixAddOne
	// replicaFixAddOneBeforeTrim: the replica count is complete but a replica
	// is misplaced. Add a well-placed replica first; a later pass trims the
	// misplaced one as surplus. Deleting first would drop the volume below its
	// intended durability — for good, if no destination can take the
	// replacement copy.
	replicaFixAddOneBeforeTrim
	// replicaFixTrimMisplaced: more replicas than the policy asks for, at
	// least one misplaced; delete a misplaced one.
	replicaFixTrimMisplaced
	// replicaFixTrimOver: more replicas than the policy asks for; delete a
	// surplus one.
	replicaFixTrimOver
)

// classifyReplicaSet decides the next action for one volume. Add always wins
// over trim: a volume that lacks a replica and also has a misplaced or surplus
// one gets its missing replica first, and the trim happens on a later pass
// once the new copy registered in the topology.
func classifyReplicaSet(replicaPlacement *super_block.ReplicaPlacement, replicas []*VolumeReplica) replicaFix {
	switch {
	case replicaPlacement.GetCopyCount() > len(replicas) || !satisfyReplicaCurrentLocation(replicaPlacement, replicas):
		return replicaFixAddOne
	case isMisplaced(replicas, replicaPlacement):
		if len(replicas) <= replicaPlacement.GetCopyCount() {
			return replicaFixAddOneBeforeTrim
		}
		return replicaFixTrimMisplaced
	case replicaPlacement.GetCopyCount() < len(replicas):
		return replicaFixTrimOver
	}
	return replicaFixNothing
}

func collectVolumeReplicaLocations(topologyInfo *master_pb.TopologyInfo) (map[uint32][]*VolumeReplica, []location) {
	volumeReplicas := make(map[uint32][]*VolumeReplica)
	var allLocations []location
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		loc := newLocation(string(dc), string(rack), dn)
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				volumeReplicas[v.Id] = append(volumeReplicas[v.Id], &VolumeReplica{
					location: &loc,
					info:     v,
				})
			}
		}
		allLocations = append(allLocations, loc)
	})
	return volumeReplicas, allLocations
}

type SelectOneVolumeFunc func(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) *VolumeReplica

// checkOneVolume compares the index of replica a against b. With
// applyChanges=false it is a read-only divergence check; the over-replication
// trim must use that mode so it does not push the soon-to-be-deleted replica's
// needles into the survivor (which would resurrect data and is the opposite of
// a safe trim).
func checkOneVolume(a *VolumeReplica, b *VolumeReplica, writer io.Writer, commandEnv *CommandEnv, applyChanges bool) (err error) {
	aDB, bDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer func() {
		aDB.Close()
		bDB.Close()
	}()

	vcd := &volumeCheckDisk{
		writer:     writer,
		commandEnv: commandEnv,
		now:        time.Now(),

		verbose:            false,
		applyChanges:       applyChanges,
		syncDeletions:      false,
		nonRepairThreshold: float64(1),
	}

	// read index db
	if err = vcd.readIndexDatabase(aDB, a.info.Collection, a.info.Id, pb.NewServerAddressFromDataNode(a.location.dataNode)); err != nil {
		return fmt.Errorf("readIndexDatabase %s volume %d: %v", a.location.dataNode, a.info.Id, err)
	}
	if err := vcd.readIndexDatabase(bDB, b.info.Collection, b.info.Id, pb.NewServerAddressFromDataNode(b.location.dataNode)); err != nil {
		return fmt.Errorf("readIndexDatabase %s volume %d: %v", b.location.dataNode, b.info.Id, err)
	}
	if _, err = vcd.doVolumeCheckDisk(aDB, bDB, a, b, false, 0); err != nil {
		return fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", a.location.dataNode.Id, b.location.dataNode.Id, a.info.Id, err)
	}
	return
}

// matchCollectionPattern reports whether collection matches -collectionPattern:
// empty matches everything, CollectionDefault matches the unnamed collection.
func (c *commandVolumeFixReplication) matchCollectionPattern(collection string) bool {
	if *c.collectionPattern == "" {
		return true
	}
	if *c.collectionPattern == CollectionDefault {
		return collection == ""
	}
	return wildcard.MatchesWildcard(*c.collectionPattern, collection)
}

// deleteOneVolume trims one replica from each of the given volumes, and
// reports how many replicas it actually deleted.
func (c *commandVolumeFixReplication) deleteOneVolume(commandEnv *CommandEnv, writer io.Writer, applyChanges bool, doCheck bool, volumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location, selectOneVolumeFn SelectOneVolumeFunc) (deleted int, err error) {
	if len(volumeIds) == 0 {
		// nothing to do
		return 0, nil
	}

	for _, vid := range volumeIds {
		replicas := volumeReplicas[vid]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replicas[0].info.ReplicaPlacement))

		replica := selectOneVolumeFn(replicas, replicaPlacement)
		if replica == nil {
			fmt.Fprintf(writer, "skip trimming volume %d: no safe replica to delete (would leave only read-only survivors)\n", vid)
			continue
		}

		collectionIsMismatch := false
		for _, volumeReplica := range replicas {
			if volumeReplica.info.Collection != replica.info.Collection {
				fmt.Fprintf(writer, "skip delete volume %d as collection %s is mismatch: %s\n", replica.info.Id, replica.info.Collection, volumeReplica.info.Collection)
				collectionIsMismatch = true
			}
		}
		if collectionIsMismatch {
			continue
		}

		fmt.Fprintf(writer, "deleting volume %d from %s ...\n", replica.info.Id, replica.location.dataNode.Id)

		if !applyChanges {
			break
		}

		if doCheck {
			var checkErr error
			for _, replicaB := range replicas {
				if replicaB.location.dataNode == replica.location.dataNode {
					continue
				}
				// Read-only divergence check only: never write the doomed
				// replica's needles into a survivor while trimming.
				if checkErr = checkOneVolume(replica, replicaB, writer, commandEnv, false); checkErr != nil {
					fmt.Fprintf(writer, "sync volume %d on %s and %s: %v\n", replica.info.Id, replica.location.dataNode.Id, replicaB.location.dataNode.Id, checkErr)
					break
				}
			}
			if checkErr != nil {
				continue
			}
		}

		// Surplus replica being trimmed; keep the remote object since other
		// replicas of the same .vif still reference it.
		if err := deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(replica.info.Id),
			pb.NewServerAddressFromDataNode(replica.location.dataNode), false, true); err != nil {
			fmt.Fprintf(writer, "deleting volume %d from %s : %v", replica.info.Id, replica.location.dataNode.Id, err)
		} else {
			deleted++
		}

	}
	return deleted, nil
}

func (c *commandVolumeFixReplication) fixUnderReplicatedVolumes(commandEnv *CommandEnv, writer io.Writer, applyChanges bool, volumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location, retryCount int, volumesPerStep int, maxParallelization int, maxParallelizationPerServer int) (fixedVolumes map[string]int, err error) {
	fixedVolumes = map[string]int{}

	if len(volumeIds) == 0 {
		return fixedVolumes, nil
	}

	if len(volumeIds) > volumesPerStep && volumesPerStep > 0 {
		volumeIds = volumeIds[0:volumesPerStep]
	}

	// own a private copy of the locations list: the scheduler re-sorts it on
	// every reservation, and the caller's slice is shared with the concurrent
	// delete phases
	allLocations = slices.Clone(allLocations)

	scheduler := newVolumeCopyScheduler(maxParallelizationPerServer)
	var fixedVolumesMu sync.Mutex
	ewg := NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		ewg.Add(func() error {
			for i := 0; i < retryCount+1; i++ {
				if copied, err := c.fixOneUnderReplicatedVolume(commandEnv, writer, applyChanges, volumeReplicas, vid, allLocations, scheduler); err == nil {
					if applyChanges && copied {
						fixedVolumesMu.Lock()
						fixedVolumes[strconv.FormatUint(uint64(vid), 10)] = len(volumeReplicas[vid])
						fixedVolumesMu.Unlock()
					}
					break
				} else {
					fmt.Fprintf(writer, "fixing under replicated volume %d: %v\n", vid, err)
				}
			}
			return nil
		})
	}
	return fixedVolumes, ewg.Wait()
}

// volumeCopyScheduler serializes destination selection for concurrent volume
// copies: selection and free-slot accounting are atomic so parallel fixes see
// each other's reservations, and the per-server cap keeps many simultaneous
// copies from swamping one destination's disks.
type volumeCopyScheduler struct {
	mu           sync.Mutex
	cond         *sync.Cond
	inflight     map[string]int // destination dataNode.Id -> copies in flight
	maxPerServer int
}

func newVolumeCopyScheduler(maxPerServer int) *volumeCopyScheduler {
	if maxPerServer <= 0 {
		maxPerServer = 1
	}
	s := &volumeCopyScheduler{
		inflight:     make(map[string]int),
		maxPerServer: maxPerServer,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// reserveTarget picks the emptiest data node satisfying the replica placement
// and reserves a volume slot on it. When every eligible destination is at the
// per-server copy cap it waits for a copy to finish instead of failing.
// Returns nil only when no data node can accept the replica at all. With
// countInflight=false (simulation) the slot is reserved but no copy is
// counted in flight.
func (s *volumeCopyScheduler) reserveTarget(replicaPlacement *super_block.ReplicaPlacement, replicas []*VolumeReplica, allLocations []location, diskType string, countInflight bool) *location {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn := capacityByFreeVolumeCount(types.ToDiskType(diskType))
	for {
		keepDataNodesSorted(allLocations, types.ToDiskType(diskType))
		eligibleButBusy := false
		for _, dst := range allLocations {
			// check whether data nodes satisfy the constraints
			if fn(dst.dataNode) <= 0 || !satisfyReplicaPlacement(replicaPlacement, replicas, dst) {
				continue
			}
			if countInflight && s.inflight[dst.dataNode.Id] >= s.maxPerServer {
				eligibleButBusy = true
				continue
			}
			addVolumeCount(dst.dataNode.DiskInfos[diskType], 1)
			if countInflight {
				s.inflight[dst.dataNode.Id]++
			}
			return &dst
		}
		if !eligibleButBusy {
			return nil
		}
		s.cond.Wait()
	}
}

// releaseTarget ends a copy counted by reserveTarget. A failed copy also
// returns the reserved volume slot, so retries do not drain the topology's
// free-slot accounting.
func (s *volumeCopyScheduler) releaseTarget(dst *location, diskType string, copied bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight[dst.dataNode.Id]--
	if s.inflight[dst.dataNode.Id] <= 0 {
		delete(s.inflight, dst.dataNode.Id)
	}
	if !copied {
		addVolumeCount(dst.dataNode.DiskInfos[diskType], -1)
	}
	s.cond.Broadcast()
}

func (c *commandVolumeFixReplication) fixOneUnderReplicatedVolume(commandEnv *CommandEnv, writer io.Writer, applyChanges bool, volumeReplicas map[uint32][]*VolumeReplica, vid uint32, allLocations []location, scheduler *volumeCopyScheduler) (bool, error) {
	replicas := volumeReplicas[vid]
	replica := pickOneReplicaToCopyFrom(replicas)
	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))

	dst := scheduler.reserveTarget(replicaPlacement, replicas, allLocations, replica.info.DiskType, applyChanges)
	if dst == nil {
		fmt.Fprintf(writer, "failed to place volume %d replica as %s, existing:%+v\n", replica.info.Id, replicaPlacement, len(replicas))
		return false, nil
	}

	// ask the volume server to replicate the volume
	fmt.Fprintf(writer, "replicating volume %d %s from %s to dataNode %s ...\n", replica.info.Id, replicaPlacement, replica.location.dataNode.Id, dst.dataNode.Id)

	if !applyChanges {
		return true, nil
	}

	err := replicateVolumeToServer(commandEnv.option.GrpcDialOption, writer, needle.VolumeId(replica.info.Id),
		pb.NewServerAddressFromDataNode(replica.location.dataNode),
		pb.NewServerAddressFromDataNode(dst.dataNode),
		replica.info.DiskType)
	scheduler.releaseTarget(dst, replica.info.DiskType, err == nil)
	if err != nil {
		return false, err
	}

	return true, nil
}

func addVolumeCount(info *master_pb.DiskInfo, count int) {
	if info == nil {
		return
	}
	info.VolumeCount += int64(count)
	info.FreeVolumeCount -= int64(count)
}

func keepDataNodesSorted(dataNodes []location, diskType types.DiskType) {
	fn := capacityByFreeVolumeCount(diskType)
	slices.SortFunc(dataNodes, func(a, b location) int {
		return int(fn(b.dataNode) - fn(a.dataNode))
	})
}

func satisfyReplicaCurrentLocation(replicaPlacement *super_block.ReplicaPlacement, replicas []*VolumeReplica) bool {
	locs := make([]balancer.Location, len(replicas))
	for i, r := range replicas {
		locs[i] = toBalancerLocation(r.location)
	}
	return balancer.SatisfyReplicaCurrentLocation(replicaPlacement, locs)
}

/*
	if on an existing data node {
	  return false
	}

	if different from existing dcs {
	  if lack on different dcs {
	    return true
	  }else{
	    return false
	  }
	}

	if not on primary dc {
	  return false
	}

	if different from existing racks {
	  if lack on different racks {
	    return true
	  }else{
	    return false
	  }
	}

	if not on primary rack {
	  return false
	}

	if lacks on same rack {
	  return true
	} else {

	  return false
	}
*/
// satisfyReplicaPlacement reports whether placing a replica at possibleLocation
// is consistent with the replication policy given the existing replicas. Thin
// adapter over weed/topology/balancer so the shell and the maintenance worker
// share one placement implementation.
func satisfyReplicaPlacement(replicaPlacement *super_block.ReplicaPlacement, replicas []*VolumeReplica, possibleLocation location) bool {
	locs := make([]balancer.Location, len(replicas))
	for i, r := range replicas {
		locs[i] = toBalancerLocation(r.location)
	}
	return balancer.SatisfyReplicaPlacement(replicaPlacement, locs, toBalancerLocation(&possibleLocation))
}

type VolumeReplica struct {
	location *location
	info     *master_pb.VolumeInformationMessage
}

type location struct {
	dc       string
	rack     string
	dataNode *master_pb.DataNodeInfo
}

func newLocation(dc, rack string, dataNode *master_pb.DataNodeInfo) location {
	return location{
		dc:       dc,
		rack:     rack,
		dataNode: dataNode,
	}
}

func (l location) String() string {
	return fmt.Sprintf("%s %s %s", l.dc, l.rack, l.dataNode.Id)
}

func (l location) Rack() string {
	return fmt.Sprintf("%s %s", l.dc, l.rack)
}

func (l location) DataCenter() string {
	return l.dc
}

// toBalancerReplicas adapts shell replicas to the shared selection shape in
// weed/topology/balancer; selection results come back as indices into the
// same slice.
func toBalancerReplicas(replicas []*VolumeReplica) []balancer.Replica {
	out := make([]balancer.Replica, len(replicas))
	for i, r := range replicas {
		out[i] = balancer.Replica{Location: toBalancerLocation(r.location)}
		if r.info != nil {
			out[i].Size = r.info.Size
			out[i].ModifiedAtSecond = r.info.ModifiedAtSecond
			out[i].CompactRevision = r.info.CompactRevision
			out[i].ReadOnly = r.info.ReadOnly
		}
	}
	return out
}

func pickOneReplicaToCopyFrom(replicas []*VolumeReplica) *VolumeReplica {
	if i := balancer.PickOneReplicaToCopyFrom(toBalancerReplicas(replicas)); i >= 0 {
		return replicas[i]
	}
	return nil
}

// pickOneReplicaToDelete selects the replica to trim when over-replicated;
// see balancer.PickOneReplicaToDelete for the survivor-safety rules.
// VolumeStatus file_count>0 alone cannot prove the survivors' .dat is
// readable, so we do not over-claim survivor health.
func pickOneReplicaToDelete(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) *VolumeReplica {
	if i := balancer.PickOneReplicaToDelete(toBalancerReplicas(replicas), replicaPlacement); i >= 0 {
		return replicas[i]
	}
	return nil
}

// check and fix misplaced volumes

func isMisplaced(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) bool {
	return balancer.IsMisplaced(toBalancerReplicas(replicas), replicaPlacement)
}

func pickOneMisplacedVolume(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) *VolumeReplica {
	if i := balancer.PickOneMisplacedVolume(toBalancerReplicas(replicas), replicaPlacement); i >= 0 {
		return replicas[i]
	}
	return nil
}
