package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandEcRebuild{})
}

type commandEcRebuild struct {
}

func (c *commandEcRebuild) Name() string {
	return "ec.rebuild"
}

func (c *commandEcRebuild) Help() string {
	return `find and rebuild missing ec shards among volume servers

	ec.rebuild [-c EACH_COLLECTION|<collection_name>] [-force]

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
	applyChanges := fixCommand.Bool("force", false, "apply the changes")
	if err = fixCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyChanges, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect all ec nodes
	allEcNodes, _, err := collectEcNodes(commandEnv)
	if err != nil {
		return err
	}

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
		fmt.Printf("rebuildEcVolumes collections %+v\n", len(collections))
		for _, c := range collections {
			fmt.Printf("rebuildEcVolumes collection %+v\n", c)
			if err = rebuildEcVolumes(commandEnv, allEcNodes, c, writer, *applyChanges); err != nil {
				return err
			}
		}
	} else {
		if err = rebuildEcVolumes(commandEnv, allEcNodes, *collection, writer, *applyChanges); err != nil {
			return err
		}
	}

	return nil
}

func rebuildEcVolumes(commandEnv *CommandEnv, allEcNodes []*EcNode, collection string, writer io.Writer, applyChanges bool) error {

	fmt.Printf("rebuildEcVolumes %s\n", collection)

	// collect vid => each shard locations, similar to ecShardMap in topology.go
	ecShardMap := make(EcShardMap)
	for _, ecNode := range allEcNodes {
		ecShardMap.registerEcNode(ecNode, collection)
	}

	for vid, locations := range ecShardMap {
		shardCount := locations.shardCount()
		if shardCount == erasure_coding.TotalShardsCount {
			continue
		}
		if shardCount < erasure_coding.DataShardsCount {
			return fmt.Errorf("ec volume %d is unrepairable with %d shards\n", vid, shardCount)
		}

		sortEcNodesByFreeslotsDescending(allEcNodes)

		if allEcNodes[0].freeEcSlot < erasure_coding.TotalShardsCount {
			return fmt.Errorf("disk space is not enough")
		}

		if err := rebuildOneEcVolume(commandEnv, allEcNodes[0], collection, vid, locations, writer, applyChanges); err != nil {
			return err
		}
	}

	return nil
}

func rebuildOneEcVolume(commandEnv *CommandEnv, rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations, writer io.Writer, applyChanges bool) error {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	fmt.Printf("rebuildOneEcVolume %s %d\n", collection, volumeId)

	// collect shard files to rebuilder local disk
	var generatedShardIds []uint32
	copiedShardIds, _, err := prepareDataToRecover(commandEnv, rebuilder, collection, volumeId, locations, writer, applyChanges)
	if err != nil {
		return err
	}
	defer func() {
		// clean up working files

		// ask the rebuilder to delete the copied shards
		err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), copiedShardIds)
		if err != nil {
			fmt.Fprintf(writer, "%s delete copied ec shards %s %d.%v\n", rebuilder.info.Id, collection, volumeId, copiedShardIds)
		}

	}()

	if !applyChanges {
		return nil
	}

	// generate ec shards, and maybe ecx file
	generatedShardIds, err = generateMissingShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info))
	if err != nil {
		return err
	}

	// mount the generated shards
	err = mountEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), generatedShardIds)
	if err != nil {
		return err
	}

	rebuilder.addEcVolumeShards(volumeId, collection, generatedShardIds)

	return nil
}

func generateMissingShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress) (rebuiltShardIds []uint32, err error) {

	err = operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, rebuildErr := volumeServerClient.VolumeEcShardsRebuild(context.Background(), &volume_server_pb.VolumeEcShardsRebuildRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		if rebuildErr == nil {
			rebuiltShardIds = resp.RebuiltShardIds
		}
		return rebuildErr
	})
	return
}

func prepareDataToRecover(commandEnv *CommandEnv, rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations, writer io.Writer, applyBalancing bool) (copiedShardIds []uint32, localShardIds []uint32, err error) {

	needEcxFile := true
	var localShardBits erasure_coding.ShardBits
	for _, diskInfo := range rebuilder.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if ecShardInfo.Collection == collection && needle.VolumeId(ecShardInfo.Id) == volumeId {
				needEcxFile = false
				localShardBits = erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
			}
		}
	}

	for shardId, ecNodes := range locations {

		if len(ecNodes) == 0 {
			fmt.Fprintf(writer, "missing shard %d.%d\n", volumeId, shardId)
			continue
		}

		if localShardBits.HasShardId(erasure_coding.ShardId(shardId)) {
			localShardIds = append(localShardIds, uint32(shardId))
			fmt.Fprintf(writer, "use existing shard %d.%d\n", volumeId, shardId)
			continue
		}

		var copyErr error
		if applyBalancing {
			copyErr = operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(rebuilder.info), commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
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
			fmt.Fprintf(writer, "%s failed to copy %d.%d from %s: %v\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id, copyErr)
		} else {
			fmt.Fprintf(writer, "%s copied %d.%d from %s\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id)
			copiedShardIds = append(copiedShardIds, uint32(shardId))
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
					existing = make([][]*EcNode, erasure_coding.TotalShardsCount)
					ecShardMap[needle.VolumeId(shardInfo.Id)] = existing
				}
				for _, shardId := range erasure_coding.ShardBits(shardInfo.EcIndexBits).ShardIds() {
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
