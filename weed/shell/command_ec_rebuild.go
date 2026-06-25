package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandEcRebuild{})
}

type ecRebuilder struct {
	commandEnv   *CommandEnv
	ecNodes      []*EcNode
	writer       io.Writer
	applyChanges bool
	collections  []string
	volumeIds    []needle.VolumeId
	diskType     types.DiskType

	ewg       *ErrorWaitGroup
	ecNodesMu sync.Mutex
}

type commandEcRebuild struct {
}

func (c *commandEcRebuild) Name() string {
	return "ec.rebuild"
}

func (c *commandEcRebuild) Help() string {
	return `find and rebuild missing ec shards among volume servers

	ec.rebuild [-c EACH_COLLECTION|<collection_name>] [-apply] [-maxParallelization N] [-diskType=<disk_type>]

	Before rebuilding, asks volume servers to recover any shards left unmounted by
	a missing .ecx index (the index resides only on a peer server). Such shards are
	invisible to the master, so recovering them first avoids regenerating data that
	is actually present (issue #10104).

	Options:
	  -collection: specify a collection name, or "EACH_COLLECTION" to process all collections
	  -apply: actually perform the rebuild operations (default is dry-run mode)
	  -maxParallelization: number of volumes to rebuild concurrently (default: 10)
	                       Increase for faster rebuilds with more system resources.
	                       Decrease if experiencing resource contention or instability.
	  -diskType: disk type for EC shards (hdd, ssd, or empty for default hdd)

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			rebuildEcVolumes()
		}
	}

	func rebuildEcVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}

`
}

func (c *commandEcRebuild) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcRebuild) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fixCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := fixCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	volumeIdsStr := fixCommand.String("volumeIds", "", "optional comma-separated list of volume ID to process; defaults to all volumes in the collection")
	maxParallelization := fixCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	applyChanges := fixCommand.Bool("apply", false, "apply the changes")
	diskTypeStr := fixCommand.String("diskType", "", "disk type for EC shards (hdd, ssd, or empty for default hdd)")
	// TODO: remove this alias
	applyChangesAlias := fixCommand.Bool("force", false, "apply the changes (alias for -apply)")
	if err = fixCommand.Parse(args); err != nil {
		return nil
	}
	handleDeprecatedForceFlag(writer, fixCommand, applyChangesAlias, applyChanges)
	infoAboutSimulationMode(writer, *applyChanges, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	diskType := types.ToDiskType(*diskTypeStr)

	// collect all ec nodes
	allEcNodes, _, err := collectEcNodes(commandEnv, diskType)
	if err != nil {
		return err
	}

	var collections []string
	if *collection == "EACH_COLLECTION" {
		collections, err = ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
	} else {
		collections = []string{*collection}
	}

	var volumeIds []needle.VolumeId
	if *volumeIdsStr != "" {
		for _, vidStr := range strings.Split(*volumeIdsStr, ",") {
			vidStr = strings.TrimSpace(vidStr)
			if len(vidStr) == 0 {
				continue
			}
			if vid, err := strconv.ParseUint(vidStr, 10, 32); err == nil {
				volumeIds = append(volumeIds, needle.VolumeId(vid))
			} else {
				return fmt.Errorf("invalid volume ID %q", vidStr)
			}
		}
	}

	erb := &ecRebuilder{
		commandEnv:   commandEnv,
		ecNodes:      allEcNodes,
		writer:       writer,
		applyChanges: *applyChanges,
		collections:  collections,
		volumeIds:    volumeIds,
		diskType:     diskType,

		ewg: NewErrorWaitGroup(*maxParallelization),
	}

	// Recover shards left unmounted by a missing .ecx index before planning: such
	// shards never register with the master, so the rebuild below would treat the
	// volume as short or unrepairable even though its data is intact (issue #10104).
	erb.recoverMissingIndexes()

	fmt.Printf("rebuildEcVolumes for %d collection(s)\n", len(collections))
	for _, c := range collections {
		erb.rebuildEcVolumes(c)
	}

	return erb.ewg.Wait()
}

func (erb *ecRebuilder) write(format string, a ...any) {
	fmt.Fprintf(erb.writer, format, a...)
}

func (erb *ecRebuilder) isLocked() bool {
	return erb.commandEnv.isLocked()
}

// matchesVolumeId verifies whether the rebuilder is targeted at a given volume ID.
func (erb *ecRebuilder) matchesVolumeId(vid needle.VolumeId) bool {
	if len(erb.volumeIds) == 0 {
		return true
	}

	return slices.Contains(erb.volumeIds, vid)
}

// countLocalShards returns the number of shards already present locally on the node for the given volume.
// Unions across all of the node's disks, like prepareDataToRecover, so slot
// accounting matches what the rebuild will actually treat as local.
func (erb *ecRebuilder) countLocalShards(node *EcNode, collection string, volumeId needle.VolumeId) int {
	localShardsInfo := erasure_coding.NewShardsInfo()
	for _, diskInfo := range node.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if ecShardInfo.Collection == collection && needle.VolumeId(ecShardInfo.Id) == volumeId {
				localShardsInfo.Add(erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecShardInfo))
			}
		}
	}
	return localShardsInfo.Count()
}

// selectAndReserveRebuilder atomically selects a rebuilder node with sufficient free slots
// and reserves slots only for the non-local shards that need to be copied/generated.
func (erb *ecRebuilder) selectAndReserveRebuilder(collection string, volumeId needle.VolumeId) (*EcNode, int, error) {
	erb.ecNodesMu.Lock()
	defer erb.ecNodesMu.Unlock()

	if len(erb.ecNodes) == 0 {
		return nil, 0, fmt.Errorf("no ec nodes available")
	}

	// Find the node with the most free slots, considering local shards
	var bestNode *EcNode
	var bestSlotsNeeded int
	var maxAvailableSlots int
	var minSlotsNeeded int = erasure_coding.TotalShardsCount // Start with maximum possible
	for _, node := range erb.ecNodes {
		localShards := erb.countLocalShards(node, collection, volumeId)
		slotsNeeded := erasure_coding.TotalShardsCount - localShards
		if slotsNeeded < 0 {
			slotsNeeded = 0
		}

		if node.freeEcSlot > maxAvailableSlots {
			maxAvailableSlots = node.freeEcSlot
		}

		if slotsNeeded < minSlotsNeeded {
			minSlotsNeeded = slotsNeeded
		}

		if node.freeEcSlot >= slotsNeeded {
			if bestNode == nil || node.freeEcSlot > bestNode.freeEcSlot {
				bestNode = node
				bestSlotsNeeded = slotsNeeded
			}
		}
	}

	if bestNode == nil {
		return nil, 0, fmt.Errorf("no node has sufficient free slots for volume %d (need at least %d slots, max available: %d)",
			volumeId, minSlotsNeeded, maxAvailableSlots)
	}

	// Reserve slots only for non-local shards
	bestNode.freeEcSlot -= bestSlotsNeeded

	return bestNode, bestSlotsNeeded, nil
}

// releaseRebuilder releases the reserved slots back to the rebuilder node.
func (erb *ecRebuilder) releaseRebuilder(node *EcNode, slotsToRelease int) {
	erb.ecNodesMu.Lock()
	defer erb.ecNodesMu.Unlock()

	// Release slots by incrementing the free slot count
	node.freeEcSlot += slotsToRelease
}

func (erb *ecRebuilder) rebuildEcVolumes(collection string) {
	fmt.Printf("rebuildEcVolumes for %q\n", collection)

	// collect vid => each shard locations, similar to ecShardMap in topology.go
	ecShardMap := make(EcShardMap)
	erb.ecNodesMu.Lock()
	for _, ecNode := range erb.ecNodes {
		ecShardMap.registerEcNode(ecNode, collection)
	}
	erb.ecNodesMu.Unlock()

	for vid, locations := range ecShardMap {
		if !erb.matchesVolumeId(vid) {
			continue
		}
		shardCount := locations.shardCount()
		if shardCount == erasure_coding.TotalShardsCount {
			continue
		}
		if shardCount < erasure_coding.DataShardsCount {
			erb.write("ec volume %d is unrepairable with %d shards (need %d), skipping\n", vid, shardCount, erasure_coding.DataShardsCount)
			continue
		}

		// Capture variables for closure
		vid := vid
		locations := locations

		erb.ewg.Add(func() error {
			// Select rebuilder and reserve slots atomically per volume
			rebuilder, slotsToReserve, err := erb.selectAndReserveRebuilder(collection, vid)
			if err != nil {
				return fmt.Errorf("failed to select rebuilder for volume %d: %v", vid, err)
			}
			defer erb.releaseRebuilder(rebuilder, slotsToReserve)

			return erb.rebuildOneEcVolume(collection, vid, locations, rebuilder)
		})
	}
}

// recoverMissingIndexes asks every ec node to fetch a missing .ecx index from a
// peer and mount the on-disk shards it could not load on its own. Shards
// orphaned this way (index only on another server) are absent from the master
// topology, so without this pass ec.rebuild would regenerate or give up on
// shards whose data is actually present — and a volume whose every holder lacks
// the index would not appear in the topology at all. Each node therefore
// recovers all of its on-disk orphans (volume_id 0); an explicit -volumeIds
// list narrows that to the requested volumes. On apply it refreshes the topology
// so the rebuild planning sees the recovered shards (issue #10104).
func (erb *ecRebuilder) recoverMissingIndexes() {
	erb.ecNodesMu.Lock()
	nodes := append([]*EcNode(nil), erb.ecNodes...)
	erb.ecNodesMu.Unlock()
	if len(nodes) == 0 {
		return
	}

	// volume_id 0 means "recover every orphan on the node"; a -volumeIds list
	// narrows recovery to those ids (each scanned across collections server-side).
	vids := erb.volumeIds
	if len(vids) == 0 {
		vids = []needle.VolumeId{0}
	}

	if !erb.applyChanges {
		erb.write("would ask %d ec node(s) to recover EC shards left unmounted by a missing .ecx index\n", len(nodes))
		return
	}

	for _, node := range nodes {
		for _, vid := range vids {
			err := operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(node.info), erb.commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
				_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
					VolumeId:            uint32(vid),
					RecoverMissingIndex: true,
				})
				return mountErr
			})
			if err != nil {
				erb.write("%s recover missing index (volume %d): %v\n", node.info.Id, vid, err)
			}
		}
	}

	// Refresh topology so the rebuild planning sees shards the recovery registered.
	refreshed, _, err := collectEcNodes(erb.commandEnv, erb.diskType)
	if err != nil {
		erb.write("failed to refresh ec nodes after index recovery: %v\n", err)
		return
	}
	erb.ecNodesMu.Lock()
	erb.ecNodes = refreshed
	erb.ecNodesMu.Unlock()
}

func (erb *ecRebuilder) rebuildOneEcVolume(collection string, volumeId needle.VolumeId, locations EcShardLocations, rebuilder *EcNode) error {
	if !erb.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	fmt.Printf("rebuildOneEcVolume %s %d\n", collection, volumeId)

	// collect shard files to rebuilder local disk
	var generatedShardIds []erasure_coding.ShardId
	copiedShardIds, _, err := erb.prepareDataToRecover(rebuilder, collection, volumeId, locations)
	defer func() {
		// Clean up the working copies this run actually made, even when the
		// recoverability gate failed after some copies already succeeded:
		// they are temp files on the rebuilder nothing else reclaims. Dry-run
		// copies nothing (copiedShardIds is empty), so this issues no delete
		// RPC. Use a local error so a cleanup failure cannot mask the return.
		if !erb.applyChanges || len(copiedShardIds) == 0 {
			return
		}
		if derr := sourceServerDeleteEcShards(erb.commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), copiedShardIds); derr != nil {
			erb.write("%s delete copied ec shards %s %d.%v: %v\n", rebuilder.info.Id, collection, volumeId, copiedShardIds, derr)
		}
	}()
	if err != nil {
		return err
	}

	if !erb.applyChanges {
		return nil
	}

	// generate ec shards, and maybe ecx file
	generatedShardIds, err = erb.generateMissingShards(collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info))
	if err != nil {
		return err
	}

	// mount the generated shards
	err = mountEcShards(erb.commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), generatedShardIds)
	if err != nil {
		return err
	}

	// ensure ECNode updates are atomic
	erb.ecNodesMu.Lock()
	defer erb.ecNodesMu.Unlock()
	rebuilder.addEcVolumeShards(volumeId, collection, generatedShardIds, erb.diskType)

	return nil
}

func (erb *ecRebuilder) generateMissingShards(collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress) (rebuiltShardIds []erasure_coding.ShardId, err error) {

	err = operation.WithVolumeServerClient(false, sourceLocation, erb.commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, rebuildErr := volumeServerClient.VolumeEcShardsRebuild(context.Background(), &volume_server_pb.VolumeEcShardsRebuildRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		if rebuildErr == nil {
			rebuiltShardIds = erasure_coding.Uint32ToShardIds(resp.RebuiltShardIds)
		}
		return rebuildErr
	})
	return
}

func (erb *ecRebuilder) prepareDataToRecover(rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations) (copiedShardIds []erasure_coding.ShardId, localShardIds []erasure_coding.ShardId, err error) {

	needEcxFile := true
	localShardsInfo := erasure_coding.NewShardsInfo()
	for _, diskInfo := range rebuilder.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if ecShardInfo.Collection == collection && needle.VolumeId(ecShardInfo.Id) == volumeId {
				needEcxFile = false
				// Union across disks: the rebuilder may hold this volume's
				// shards on more than one disk. Overwriting per-disk would
				// make a shard on a non-last disk look remote and get copied
				// onto itself (O_TRUNC) and then node-wide deleted.
				localShardsInfo.Add(erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecShardInfo))
			}
		}
	}

	targetShardCount := erasure_coding.TotalShardsCount
	for i := erasure_coding.TotalShardsCount; i < len(locations); i++ {
		if len(locations[i]) > 0 {
			targetShardCount = i + 1
		}
	}

	// recoverableRemoteShards counts remote shards that can contribute to the
	// rebuild. Dry-run counts the plan; apply mode counts only successful copies.
	recoverableRemoteShards := 0
	for i := 0; i < targetShardCount; i++ {
		ecNodes := locations[i]
		shardId := erasure_coding.ShardId(i)
		if len(ecNodes) == 0 {
			erb.write("missing shard %d.%d\n", volumeId, shardId)
			continue
		}

		if localShardsInfo.Has(shardId) {
			localShardIds = append(localShardIds, shardId)
			erb.write("use existing shard %d.%d\n", volumeId, shardId)
			continue
		}

		// The rebuilder is itself the only listed holder: never copy a shard
		// onto itself (the in-place O_TRUNC would destroy it) nor schedule it
		// for the post-rebuild delete. Treat it as already local.
		if ecNodes[0].info.Id == rebuilder.info.Id {
			localShardIds = append(localShardIds, shardId)
			erb.write("use existing shard %d.%d (already on rebuilder)\n", volumeId, shardId)
			continue
		}

		if !erb.applyChanges {
			recoverableRemoteShards++
			erb.write("would copy %d.%d from %s\n", volumeId, shardId, ecNodes[0].info.Id)
			continue
		}

		copyErr := operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(rebuilder.info), erb.commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(volumeId),
				Collection:     collection,
				ShardIds:       []uint32{uint32(shardId)},
				CopyEcxFile:    needEcxFile,
				CopyEcjFile:    true,
				CopyVifFile:    needEcxFile,
				SourceDataNode: string(pb.NewServerAddressFromDataNode(ecNodes[0].info)),
			})
			return copyErr
		})
		if copyErr != nil {
			erb.write("%s failed to copy %d.%d from %s: %v\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id, copyErr)
			continue
		}
		recoverableRemoteShards++
		if needEcxFile {
			needEcxFile = false
		}
		erb.write("%s copied %d.%d from %s\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id)
		// Only shards this run actually copied are temp working files to be
		// deleted afterward; never a pre-existing local or remote shard.
		copiedShardIds = append(copiedShardIds, shardId)
	}

	if len(localShardIds)+recoverableRemoteShards >= erasure_coding.DataShardsCount {
		return copiedShardIds, localShardIds, nil
	}

	// Hand back what was copied so the caller deletes these orphaned working
	// shards: recovery failed, but the temp files are already on the rebuilder.
	return copiedShardIds, localShardIds, fmt.Errorf("%d shards are not enough to recover volume %d", len(localShardIds)+recoverableRemoteShards, volumeId)

}

type EcShardMap map[needle.VolumeId]EcShardLocations
type EcShardLocations [][]*EcNode

func (ecShardMap EcShardMap) registerEcNode(ecNode *EcNode, collection string) {
	for _, diskInfo := range ecNode.info.DiskInfos {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if shardInfo.Collection == collection {
				existing, found := ecShardMap[needle.VolumeId(shardInfo.Id)]
				if !found {
					// Use MaxShardCount (32) to support custom EC ratios
					existing = make([][]*EcNode, erasure_coding.MaxShardCount)
					ecShardMap[needle.VolumeId(shardInfo.Id)] = existing
				}
				for _, shardId := range erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo).Ids() {
					existing[shardId] = append(existing[shardId], ecNode)
				}
			}
		}
	}
}

func (ecShardLocations EcShardLocations) shardCount() (count int) {
	for _, locations := range ecShardLocations {
		if len(locations) > 0 {
			count++
		}
	}
	return
}
