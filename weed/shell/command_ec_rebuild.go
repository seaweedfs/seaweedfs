package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
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

	erb := &ecRebuilder{
		commandEnv:   commandEnv,
		ecNodes:      allEcNodes,
		writer:       writer,
		applyChanges: *applyChanges,
		collections:  collections,
		diskType:     diskType,

		ewg: NewErrorWaitGroup(*maxParallelization),
	}

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

// countLocalShards returns the number of shards already present locally on the node for the given volume.
func (erb *ecRebuilder) countLocalShards(node *EcNode, collection string, volumeId needle.VolumeId) int {
	for _, diskInfo := range node.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if ecShardInfo.Collection == collection && needle.VolumeId(ecShardInfo.Id) == volumeId {
				return erasure_coding.ShardsCountFromVolumeEcShardInformationMessage(ecShardInfo)
			}
		}
	}
	return 0
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
		shardCount := locations.shardCount()
		if shardCount == erasure_coding.TotalShardsCount {
			continue
		}
		if shardCount < erasure_coding.DataShardsCount {
			// Capture variables for closure
			vid := vid
			shardCount := shardCount
			erb.ewg.Add(func() error {
				return fmt.Errorf("ec volume %d is unrepairable with %d shards", vid, shardCount)
			})
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

func (erb *ecRebuilder) rebuildOneEcVolume(collection string, volumeId needle.VolumeId, locations EcShardLocations, rebuilder *EcNode) error {
	if !erb.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	fmt.Printf("rebuildOneEcVolume %s %d\n", collection, volumeId)

	// collect shard files to rebuilder local disk
	var generatedShardIds []erasure_coding.ShardId
	copiedShardIds, _, err := erb.prepareDataToRecover(rebuilder, collection, volumeId, locations)
	if err != nil {
		return err
	}
	defer func() {
		// clean up working files

		// ask the rebuilder to delete the copied shards
		err = sourceServerDeleteEcShards(erb.commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), copiedShardIds)
		if err != nil {
			erb.write("%s delete copied ec shards %s %d.%v\n", rebuilder.info.Id, collection, volumeId, copiedShardIds)
		}

	}()

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
				localShardsInfo = erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecShardInfo)
			}
		}
	}

	for i, ecNodes := range locations {
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

		var copyErr error
		if erb.applyChanges {
			copyErr = operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(rebuilder.info), erb.commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
					VolumeId:       uint32(volumeId),
					Collection:     collection,
					ShardIds:       []uint32{uint32(shardId)},
					CopyEcxFile:    needEcxFile,
					CopyEcjFile:    true,
					CopyVifFile:    needEcxFile,
					SourceDataNode: ecNodes[0].info.Id,
				})
				return copyErr
			})
			if copyErr == nil && needEcxFile {
				needEcxFile = false
			}
		}
		if copyErr != nil {
			erb.write("%s failed to copy %d.%d from %s: %v\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id, copyErr)
		} else {
			erb.write("%s copied %d.%d from %s\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id)
			copiedShardIds = append(copiedShardIds, shardId)
		}

	}

	if len(copiedShardIds)+len(localShardIds) >= erasure_coding.DataShardsCount {
		return copiedShardIds, localShardIds, nil
	}

	return nil, nil, fmt.Errorf("%d shards are not enough to recover volume %d", len(copiedShardIds)+len(localShardIds), volumeId)

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
