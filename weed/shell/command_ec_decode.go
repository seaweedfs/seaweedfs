package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandEcDecode{})
}

type commandEcDecode struct {
}

func (c *commandEcDecode) Name() string {
	return "ec.decode"
}

func (c *commandEcDecode) Help() string {
	return `decode a erasure coded volume into a normal volume

	ec.decode [-collection=""] [-volumeId=<volume_id>] [-diskType=<disk_type>]

	The -collection parameter supports regular expressions for pattern matching:
	  - Use exact match: ec.decode -collection="^mybucket$"
	  - Match multiple buckets: ec.decode -collection="bucket.*"
	  - Match all collections: ec.decode -collection=".*"

	Options:
	  -diskType: source disk type where EC shards are stored (hdd, ssd, or empty for default hdd)

	Examples:
	  # Decode EC shards from HDD (default)
	  ec.decode -collection=mybucket

	  # Decode EC shards from SSD
	  ec.decode -collection=mybucket -diskType=ssd

`
}

func (c *commandEcDecode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcDecode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	decodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := decodeCommand.Int("volumeId", 0, "the volume id")
	collection := decodeCommand.String("collection", "", "the collection name")
	diskTypeStr := decodeCommand.String("diskType", "", "source disk type where EC shards are stored (hdd, ssd, or empty for default hdd)")
	if err = decodeCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)
	diskType := types.ToDiskType(*diskTypeStr)

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	// volumeId is provided
	if vid != 0 {
		return doEcDecode(commandEnv, topologyInfo, *collection, vid, diskType)
	}

	// apply to all volumes in the collection
	volumeIds, err := collectEcShardIds(topologyInfo, *collection, diskType)
	if err != nil {
		return err
	}
	fmt.Printf("ec decode volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doEcDecode(commandEnv, topologyInfo, *collection, vid, diskType); err != nil {
			return err
		}
	}

	return nil
}

func doEcDecode(commandEnv *CommandEnv, topoInfo *master_pb.TopologyInfo, collection string, vid needle.VolumeId, diskType types.DiskType) (err error) {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	// find volume location
	nodeToEcIndexBits := collectEcNodeShardBits(topoInfo, vid, diskType)

	fmt.Printf("ec volume %d shard locations: %+v\n", vid, nodeToEcIndexBits)

	// collect ec shards to the server with most space
	targetNodeLocation, err := collectEcShards(commandEnv, nodeToEcIndexBits, collection, vid)
	if err != nil {
		return fmt.Errorf("collectEcShards for volume %d: %v", vid, err)
	}

	// generate a normal volume
	err = generateNormalVolume(commandEnv.option.GrpcDialOption, vid, collection, targetNodeLocation)
	if err != nil {
		// Special case: if the EC index has no live entries, decoding is a no-op.
		// Just purge EC shards and return success without generating/mounting an empty volume.
		if isEcDecodeEmptyVolumeErr(err) {
			return unmountAndDeleteEcShards(commandEnv.option.GrpcDialOption, collection, nodeToEcIndexBits, vid)
		}
		return fmt.Errorf("generate normal volume %d on %s: %v", vid, targetNodeLocation, err)
	}

	// delete the previous ec shards
	err = mountVolumeAndDeleteEcShards(commandEnv.option.GrpcDialOption, collection, targetNodeLocation, nodeToEcIndexBits, vid)
	if err != nil {
		return fmt.Errorf("delete ec shards for volume %d: %v", vid, err)
	}

	return nil
}

func isEcDecodeEmptyVolumeErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() != codes.FailedPrecondition {
		return false
	}
	// Keep this robust against wording tweaks while still being specific.
	return strings.Contains(st.Message(), erasure_coding.EcNoLiveEntriesSubstring)
}

func unmountAndDeleteEcShards(grpcDialOption grpc.DialOption, collection string, nodeToEcIndexBits map[pb.ServerAddress]erasure_coding.ShardBits, vid needle.VolumeId) error {
	return unmountAndDeleteEcShardsWithPrefix("unmountAndDeleteEcShards", grpcDialOption, collection, nodeToEcIndexBits, vid)
}

func unmountAndDeleteEcShardsWithPrefix(prefix string, grpcDialOption grpc.DialOption, collection string, nodeToEcIndexBits map[pb.ServerAddress]erasure_coding.ShardBits, vid needle.VolumeId) error {
	ewg := NewErrorWaitGroup(len(nodeToEcIndexBits))

	// unmount and delete ec shards in parallel (one goroutine per location)
	for location, ecIndexBits := range nodeToEcIndexBits {
		location, ecIndexBits := location, ecIndexBits // capture loop variables for goroutine
		ewg.Add(func() error {
			fmt.Printf("unmount ec volume %d on %s has shards: %+v\n", vid, location, ecIndexBits.ShardIds())
			if err := unmountEcShards(grpcDialOption, vid, location, ecIndexBits.ToUint32Slice()); err != nil {
				return fmt.Errorf("%s unmount ec volume %d on %s: %w", prefix, vid, location, err)
			}

			fmt.Printf("delete ec volume %d on %s has shards: %+v\n", vid, location, ecIndexBits.ShardIds())
			if err := sourceServerDeleteEcShards(grpcDialOption, collection, vid, location, ecIndexBits.ToUint32Slice()); err != nil {
				return fmt.Errorf("%s delete ec volume %d on %s: %w", prefix, vid, location, err)
			}
			return nil
		})
	}
	return ewg.Wait()
}

func mountVolumeAndDeleteEcShards(grpcDialOption grpc.DialOption, collection string, targetNodeLocation pb.ServerAddress, nodeToEcIndexBits map[pb.ServerAddress]erasure_coding.ShardBits, vid needle.VolumeId) error {

	// mount volume
	if err := operation.WithVolumeServerClient(false, targetNodeLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{
			VolumeId: uint32(vid),
		})
		return mountErr
	}); err != nil {
		return fmt.Errorf("mountVolumeAndDeleteEcShards mount volume %d on %s: %v", vid, targetNodeLocation, err)
	}

	return unmountAndDeleteEcShardsWithPrefix("mountVolumeAndDeleteEcShards", grpcDialOption, collection, nodeToEcIndexBits, vid)
}

func generateNormalVolume(grpcDialOption grpc.DialOption, vid needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress) error {

	fmt.Printf("generateNormalVolume from ec volume %d on %s\n", vid, sourceVolumeServer)

	err := operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsToVolume(context.Background(), &volume_server_pb.VolumeEcShardsToVolumeRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
		})
		return genErr
	})

	return err

}

func collectEcShards(commandEnv *CommandEnv, nodeToEcIndexBits map[pb.ServerAddress]erasure_coding.ShardBits, collection string, vid needle.VolumeId) (targetNodeLocation pb.ServerAddress, err error) {

	maxShardCount := 0
	var existingEcIndexBits erasure_coding.ShardBits
	for loc, ecIndexBits := range nodeToEcIndexBits {
		toBeCopiedShardCount := ecIndexBits.MinusParityShards().ShardIdCount()
		if toBeCopiedShardCount > maxShardCount {
			maxShardCount = toBeCopiedShardCount
			targetNodeLocation = loc
			existingEcIndexBits = ecIndexBits
		}
	}

	fmt.Printf("collectEcShards: ec volume %d collect shards to %s from: %+v\n", vid, targetNodeLocation, nodeToEcIndexBits)

	var copiedEcIndexBits erasure_coding.ShardBits
	for loc, ecIndexBits := range nodeToEcIndexBits {
		if loc == targetNodeLocation {
			continue
		}

		needToCopyEcIndexBits := ecIndexBits.Minus(existingEcIndexBits).MinusParityShards()
		if needToCopyEcIndexBits.ShardIdCount() == 0 {
			continue
		}

		err = operation.WithVolumeServerClient(false, targetNodeLocation, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

			fmt.Printf("copy %d.%v %s => %s\n", vid, needToCopyEcIndexBits.ShardIds(), loc, targetNodeLocation)

			_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(vid),
				Collection:     collection,
				ShardIds:       needToCopyEcIndexBits.ToUint32Slice(),
				CopyEcxFile:    false,
				CopyEcjFile:    true,
				CopyVifFile:    true,
				SourceDataNode: string(loc),
			})
			if copyErr != nil {
				return fmt.Errorf("copy %d.%v %s => %s : %v\n", vid, needToCopyEcIndexBits.ShardIds(), loc, targetNodeLocation, copyErr)
			}

			fmt.Printf("mount %d.%v on %s\n", vid, needToCopyEcIndexBits.ShardIds(), targetNodeLocation)
			_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:   uint32(vid),
				Collection: collection,
				ShardIds:   needToCopyEcIndexBits.ToUint32Slice(),
			})
			if mountErr != nil {
				return fmt.Errorf("mount %d.%v on %s : %v\n", vid, needToCopyEcIndexBits.ShardIds(), targetNodeLocation, mountErr)
			}

			return nil
		})

		if err != nil {
			break
		}

		copiedEcIndexBits = copiedEcIndexBits.Plus(needToCopyEcIndexBits)

	}

	nodeToEcIndexBits[targetNodeLocation] = existingEcIndexBits.Plus(copiedEcIndexBits)

	return targetNodeLocation, err

}

func lookupVolumeIds(commandEnv *CommandEnv, volumeIds []string) (volumeIdLocations []*master_pb.LookupVolumeResponse_VolumeIdLocation, err error) {
	var resp *master_pb.LookupVolumeResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{VolumeOrFileIds: volumeIds})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.VolumeIdLocations, nil
}

func collectEcShardIds(topoInfo *master_pb.TopologyInfo, collectionPattern string, diskType types.DiskType) (vids []needle.VolumeId, err error) {
	// compile regex pattern for collection matching
	collectionRegex, err := compileCollectionPattern(collectionPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
	}

	vidMap := make(map[uint32]bool)
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if diskInfo, found := dn.DiskInfos[string(diskType)]; found {
			for _, v := range diskInfo.EcShardInfos {
				if collectionRegex.MatchString(v.Collection) {
					vidMap[v.Id] = true
				}
			}
		}
	})

	for vid := range vidMap {
		vids = append(vids, needle.VolumeId(vid))
	}

	return
}

func collectEcNodeShardBits(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, diskType types.DiskType) map[pb.ServerAddress]erasure_coding.ShardBits {

	nodeToEcIndexBits := make(map[pb.ServerAddress]erasure_coding.ShardBits)
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if diskInfo, found := dn.DiskInfos[string(diskType)]; found {
			for _, v := range diskInfo.EcShardInfos {
				if v.Id == uint32(vid) {
					nodeToEcIndexBits[pb.NewServerAddressFromDataNode(dn)] = erasure_coding.ShardBits(v.EcIndexBits)
				}
			}
		}
	})

	return nodeToEcIndexBits
}
