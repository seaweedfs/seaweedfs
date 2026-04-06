package shell

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeTierMove{})
}

type volumeTierMoveJob struct {
	src pb.ServerAddress
	vid needle.VolumeId
}

type commandVolumeTierMove struct {
	activeServers sync.Map
	queues        map[pb.ServerAddress]chan volumeTierMoveJob
	//activeServers     map[pb.ServerAddress]struct{}
	//activeServersLock sync.Mutex
	//activeServersCond *sync.Cond
}

func (c *commandVolumeTierMove) Name() string {
	return "volume.tier.move"
}

func (c *commandVolumeTierMove) Help() string {
	return `change a volume from one disk type to another

	volume.tier.move -fromDiskType=hdd -toDiskType=ssd [-collectionPattern=""] [-fullPercent=95] [-quietFor=1h] [-parallelLimit=4] [-toReplication=XYZ]

	The command ensures the target replication is fully achieved on the destination tier
	before deleting old replicas. This prevents data loss if a destination disk fails
	before replication repair completes.

	When -toReplication is specified, the moved volume is reconfigured with the new
	replication setting. Otherwise, the volume's existing replication is preserved.

	Note:
		Use -collectionPattern="_default" to match only the default collection (volumes with no collection name).
		Empty collectionPattern matches all collections.

`
}

func (c *commandVolumeTierMove) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeTierMove) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collectionPattern := tierCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	fullPercentage := tierCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := tierCommand.Duration("quietFor", 24*time.Hour, "select volumes without no writes for this period")
	source := tierCommand.String("fromDiskType", "", "the source disk type")
	target := tierCommand.String("toDiskType", "", "the target disk type")
	parallelLimit := tierCommand.Int("parallelLimit", 0, "limit the number of parallel copying jobs")
	applyChange := tierCommand.Bool("apply", false, "actually apply the changes")
	// TODO: remove this alias
	applyChangeAlias := tierCommand.Bool("force", false, "actually apply the changes (alias for -apply)")
	ioBytePerSecond := tierCommand.Int64("ioBytePerSecond", 0, "limit the speed of move")
	replicationString := tierCommand.String("toReplication", "", "the new target replication setting")

	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, tierCommand, applyChangeAlias, applyChange)
	infoAboutSimulationMode(writer, *applyChange, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}
	fromDiskType := types.ToDiskType(*source)
	toDiskType := types.ToDiskType(*target)

	if fromDiskType == toDiskType {
		return fmt.Errorf("source tier %s is the same as target tier %s", fromDiskType, toDiskType)
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	// collect all volumes that should change
	volumeIds, err := collectVolumeIdsForTierChange(topologyInfo, volumeSizeLimitMb, fromDiskType, *collectionPattern, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	fmt.Printf("tier move volumes: %v\n", volumeIds)

	// Collect volume ID to collection name mapping for the sync operation
	volumeIdToCollection := collectVolumeIdToCollection(topologyInfo, volumeIds)

	_, allLocations := collectVolumeReplicaLocations(topologyInfo)
	allLocations = filterLocationsByDiskType(allLocations, toDiskType)
	keepDataNodesSorted(allLocations, toDiskType)

	if len(allLocations) > 0 && *parallelLimit > 0 && *parallelLimit < len(allLocations) {
		allLocations = allLocations[:*parallelLimit]
	}

	wg := sync.WaitGroup{}
	bufferLen := len(allLocations)
	c.queues = make(map[pb.ServerAddress]chan volumeTierMoveJob)

	for _, dst := range allLocations {
		destServerAddress := pb.NewServerAddressFromDataNode(dst.dataNode)
		c.queues[destServerAddress] = make(chan volumeTierMoveJob, bufferLen)

		wg.Add(1)
		go func(dst location, jobs <-chan volumeTierMoveJob, applyChanges bool) {
			defer wg.Done()
			for job := range jobs {
				fmt.Fprintf(writer, "moving volume %d from %s to %s with disk type %s ...\n", job.vid, job.src, dst.dataNode.Id, toDiskType.ReadableString())

				locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(job.vid))
				if !found {
					fmt.Printf("volume %d not found", job.vid)
					continue
				}

				unlock := c.Lock(job.src)

				if applyChanges {
					if err := c.doMoveOneVolume(commandEnv, writer, job.vid, toDiskType, locations, job.src, dst, *ioBytePerSecond, replicationString); err != nil {
						fmt.Fprintf(writer, "move volume %d %s => %s: %v\n", job.vid, job.src, dst.dataNode.Id, err)
					}
				}
				unlock()
			}
		}(dst, c.queues[destServerAddress], *applyChange)
	}

	for _, vid := range volumeIds {
		collection := volumeIdToCollection[vid]
		if err = c.doVolumeTierMove(commandEnv, writer, vid, collection, toDiskType, allLocations); err != nil {
			fmt.Printf("tier move volume %d: %v\n", vid, err)
		}
		allLocations = rotateDataNodes(allLocations)
	}
	for key, _ := range c.queues {
		close(c.queues[key])
	}

	wg.Wait()

	return nil
}

func (c *commandVolumeTierMove) Lock(key pb.ServerAddress) func() {
	value, _ := c.activeServers.LoadOrStore(key, &sync.Mutex{})
	mtx := value.(*sync.Mutex)
	mtx.Lock()

	return func() { mtx.Unlock() }
}

func filterLocationsByDiskType(dataNodes []location, diskType types.DiskType) (ret []location) {
	for _, loc := range dataNodes {
		_, found := loc.dataNode.DiskInfos[string(diskType)]
		if found {
			ret = append(ret, loc)
		}
	}
	return
}

func rotateDataNodes(dataNodes []location) []location {
	if len(dataNodes) > 0 {
		return append(dataNodes[1:], dataNodes[0])
	} else {
		return dataNodes
	}
}

func isOneOf(server string, locations []wdclient.Location) bool {
	for _, loc := range locations {
		if server == loc.Url {
			return true
		}
	}
	return false
}

func (c *commandVolumeTierMove) doVolumeTierMove(commandEnv *CommandEnv, writer io.Writer, vid needle.VolumeId, collection string, toDiskType types.DiskType, allLocations []location) (err error) {
	// find volume location
	locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
	if !found {
		return fmt.Errorf("volume %d not found", vid)
	}

	// find one server with the most empty volume slots with target disk type
	hasFoundTarget := false
	fn := capacityByFreeVolumeCount(toDiskType)
	for _, dst := range allLocations {
		if fn(dst.dataNode) > 0 && !hasFoundTarget {
			// ask the volume server to replicate the volume
			if isOneOf(dst.dataNode.Id, locations) {
				continue
			}

			// Sync replicas and select the best one (with highest file count) for multi-replica volumes
			// This addresses data inconsistency risk in multi-replica volumes (issue #7797)
			// by syncing missing entries between replicas before moving
			sourceLoc, selectErr := syncAndSelectBestReplica(
				commandEnv.option.GrpcDialOption, vid, collection, locations, dst.dataNode.Id, writer)
			if selectErr != nil {
				fmt.Fprintf(writer, "failed to sync and select source replica for volume %d: %v\n", vid, selectErr)
				continue
			}
			sourceVolumeServer := sourceLoc.ServerAddress()

			if sourceVolumeServer == "" {
				continue
			}
			hasFoundTarget = true

			// adjust volume count
			addVolumeCount(dst.dataNode.DiskInfos[string(toDiskType)], 1)

			destServerAddress := pb.NewServerAddressFromDataNode(dst.dataNode)
			c.queues[destServerAddress] <- volumeTierMoveJob{sourceVolumeServer, vid}
		}
	}

	if !hasFoundTarget {
		fmt.Fprintf(writer, "can not find disk type %s for volume %d\n", toDiskType.ReadableString(), vid)
	}

	return nil
}

func (c *commandVolumeTierMove) doMoveOneVolume(commandEnv *CommandEnv, writer io.Writer, vid needle.VolumeId, toDiskType types.DiskType, locations []wdclient.Location, sourceVolumeServer pb.ServerAddress, dst location, ioBytePerSecond int64, replicationString *string) (err error) {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	// mark all replicas as read only
	if err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, false, false); err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, locations[0].Url, err)
	}
	newAddress := pb.NewServerAddressFromDataNode(dst.dataNode)

	if err = LiveMoveVolume(commandEnv.option.GrpcDialOption, writer, vid, sourceVolumeServer, newAddress, 5*time.Second, toDiskType.ReadableString(), ioBytePerSecond, true); err != nil {
		// mark all replicas as writable
		if err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, true, false); err != nil {
			glog.Errorf("mark volume %d as writable on %s: %v", vid, locations[0].Url, err)
		}

		return fmt.Errorf("move volume %d %s => %s : %v", vid, locations[0].Url, dst.dataNode.Id, err)
	}

	// If move is successful and replication is not empty, alter moved volume's replication setting
	if *replicationString != "" {
		if err = configureVolumeReplication(commandEnv.option.GrpcDialOption, vid, newAddress, *replicationString); err != nil {
			return fmt.Errorf("configure replication %s on volume %d at %s: %v", *replicationString, vid, newAddress, err)
		}
	}

	// Ensure the required number of replicas exist on the target tier BEFORE
	// deleting old replicas to avoid data-loss risk.
	// Use the explicit -toReplication if given, otherwise preserve the volume's
	// existing replication from the source tier.
	if replicateErr := c.ensureReplicationFulfilled(commandEnv, writer, vid, toDiskType, dst, *replicationString); replicateErr != nil {
		// Replication not fully achieved — do NOT delete old replicas.
		// Mark replicas writable again so the volume remains accessible.
		if markErr := markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, true, false); markErr != nil {
			glog.Errorf("mark volume %d as writable on old replicas: %v", vid, markErr)
		}
		return fmt.Errorf("volume %d moved to %s but failed to fulfill replication, old replicas preserved: %v", vid, dst.dataNode.Id, replicateErr)
	}

	// remove the remaining replicas
	for _, loc := range locations {
		if loc.Url != dst.dataNode.Id && loc.ServerAddress() != sourceVolumeServer {
			if err = deleteVolume(commandEnv.option.GrpcDialOption, vid, loc.ServerAddress(), false); err != nil {
				fmt.Fprintf(writer, "failed to delete volume %d on %s: %v\n", vid, loc.Url, err)
			}
		}
	}
	return nil
}

// ensureReplicationFulfilled creates additional replicas of the volume on the target tier
// to satisfy the requested replication placement. It re-collects topology after the initial
// move so it can see the newly placed volume and find suitable destinations for additional copies.
func (c *commandVolumeTierMove) ensureReplicationFulfilled(commandEnv *CommandEnv, writer io.Writer, vid needle.VolumeId, toDiskType types.DiskType, movedDst location, replicationString string) error {
	sourceAddress := pb.NewServerAddressFromDataNode(movedDst.dataNode)

	// Wait briefly for the master to receive heartbeats reflecting the move,
	// then re-collect topology to get the current state.
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 5*time.Second)
	if err != nil {
		return fmt.Errorf("collect topology: %v", err)
	}

	volumeReplicas, allLocations := collectVolumeReplicaLocations(topologyInfo)
	allLocations = filterLocationsByDiskType(allLocations, toDiskType)
	keepDataNodesSorted(allLocations, toDiskType)

	existingReplicas := volumeReplicas[uint32(vid)]
	if len(existingReplicas) == 0 {
		return fmt.Errorf("volume %d not found in topology after move", vid)
	}

	// Determine the target replication: use explicit -toReplication if given,
	// otherwise read the volume's existing replication setting.
	var replicaPlacement *super_block.ReplicaPlacement
	if replicationString != "" {
		replicaPlacement, err = super_block.NewReplicaPlacementFromString(replicationString)
		if err != nil {
			return fmt.Errorf("parse replication %s: %v", replicationString, err)
		}
	} else {
		replicaPlacement, err = super_block.NewReplicaPlacementFromByte(byte(existingReplicas[0].info.ReplicaPlacement))
		if err != nil {
			return fmt.Errorf("parse existing replication for volume %d: %v", vid, err)
		}
	}

	requiredCopies := replicaPlacement.GetCopyCount()
	if requiredCopies <= 1 {
		// No additional replicas needed (e.g., replication "000")
		return nil
	}

	// Filter to only replicas on the target disk type (the newly moved one).
	var targetTierReplicas []*VolumeReplica
	for _, r := range existingReplicas {
		if types.ToDiskType(r.info.DiskType) == toDiskType {
			targetTierReplicas = append(targetTierReplicas, r)
		}
	}
	if len(targetTierReplicas) == 0 {
		return fmt.Errorf("volume %d not found on target tier %s in topology after move", vid, toDiskType)
	}

	// Ensure all existing target-tier replicas have the correct replication metadata.
	// The primary moved replica is already configured in doMoveOneVolume, but there may
	// be pre-existing replicas on the target tier that need updating.
	if replicationString != "" {
		for _, r := range targetTierReplicas {
			addr := pb.NewServerAddressFromDataNode(r.location.dataNode)
			if configErr := configureVolumeReplication(commandEnv.option.GrpcDialOption, vid, addr, replicationString); configErr != nil {
				return fmt.Errorf("volume %d: failed to configure replication on existing replica %s: %v", vid, r.location.dataNode.Id, configErr)
			}
		}
	}

	additionalCopiesNeeded := requiredCopies - len(targetTierReplicas)
	if additionalCopiesNeeded <= 0 {
		return nil
	}

	fmt.Fprintf(writer, "volume %d: creating %d additional replica(s) for replication %s\n", vid, additionalCopiesNeeded, replicaPlacement)

	fn := capacityByFreeVolumeCount(toDiskType)
	copiesMade := 0
	for _, candidateDst := range allLocations {
		if copiesMade >= additionalCopiesNeeded {
			break
		}
		if fn(candidateDst.dataNode) <= 0 {
			continue
		}
		if !satisfyReplicaPlacement(replicaPlacement, targetTierReplicas, candidateDst) {
			continue
		}

		candidateAddress := pb.NewServerAddressFromDataNode(candidateDst.dataNode)
		fmt.Fprintf(writer, "volume %d: replicating from %s to %s\n", vid, sourceAddress, candidateDst.dataNode.Id)

		if copyErr := replicateVolumeToServer(commandEnv.option.GrpcDialOption, writer, vid, sourceAddress, candidateAddress, toDiskType.ReadableString()); copyErr != nil {
			return fmt.Errorf("replicate volume %d to %s: %v", vid, candidateDst.dataNode.Id, copyErr)
		}

		// Configure replication on the new replica if an explicit -toReplication was given.
		// Without it, VolumeCopy already preserves the source's replication from the super block.
		if replicationString != "" {
			if configErr := configureVolumeReplication(commandEnv.option.GrpcDialOption, vid, candidateAddress, replicationString); configErr != nil {
				return fmt.Errorf("volume %d: failed to configure replication on %s: %v", vid, candidateDst.dataNode.Id, configErr)
			}
		}

		// Track the new replica for placement decisions
		targetTierReplicas = append(targetTierReplicas, &VolumeReplica{
			location: &candidateDst,
			info:     targetTierReplicas[0].info,
		})
		addVolumeCount(candidateDst.dataNode.DiskInfos[string(toDiskType)], 1)
		copiesMade++
	}

	if copiesMade < additionalCopiesNeeded {
		return fmt.Errorf("could only create %d of %d additional replicas for volume %d (replication %s): not enough eligible destinations", copiesMade, additionalCopiesNeeded, vid, replicaPlacement)
	}

	fmt.Fprintf(writer, "volume %d: replication %s fulfilled with %d total copies\n", vid, replicaPlacement, requiredCopies)
	return nil
}

func collectVolumeIdsForTierChange(topologyInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, sourceTier types.DiskType, collectionPattern string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect %s volumes quiet for: %d seconds\n", sourceTier, quietSeconds)

	vidMap := make(map[uint32]bool)
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				// check collection name pattern
				if collectionPattern != "" {
					var matched bool
					if collectionPattern == CollectionDefault {
						matched = v.Collection == ""
					} else {
						var matchErr error
						matched, matchErr = filepath.Match(collectionPattern, v.Collection)
						if matchErr != nil {
							err = fmt.Errorf("collection pattern %q failed to match: %w", collectionPattern, matchErr)
							return
						}
					}
					if !matched {
						continue
					}
				}

				if v.ModifiedAtSecond+quietSeconds < nowUnixSeconds && types.ToDiskType(v.DiskType) == sourceTier {
					if float64(v.Size) > fullPercentage/100*float64(volumeSizeLimitMb)*1024*1024 {
						vidMap[v.Id] = true
					}
				}
			}
		}
	})

	// Check if an error occurred during iteration and return early
	if err != nil {
		return
	}

	for vid := range vidMap {
		vids = append(vids, needle.VolumeId(vid))
	}

	return
}
