package shell

import (
	"context"
	"fmt"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func moveMountedShardToEcNode(ctx context.Context, commandEnv *CommandEnv, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destinationEcNode *EcNode, applyBalancing bool) error {

	fmt.Printf("moved ec shard %d.%d %s => %s\n", vid, shardId, existingLocation.info.Id, destinationEcNode.info.Id)

	if !applyBalancing {
		return nil
	}

	// ask destination node to copy shard and the ecx file from source node, and mount it
	copiedShardIds, err := oneServerCopyAndMountEcShardsFromSource(ctx, commandEnv.option.GrpcDialOption, destinationEcNode, uint32(shardId), 1, vid, collection, existingLocation.info.Id)
	if err != nil {
		return err
	}

	// unmount the to be deleted shards
	err = unmountEcShards(ctx, commandEnv.option.GrpcDialOption, vid, existingLocation.info.Id, copiedShardIds)
	if err != nil {
		return err
	}

	// ask source node to delete the shard, and maybe the ecx file
	err = sourceServerDeleteEcShards(ctx, commandEnv.option.GrpcDialOption, collection, vid, existingLocation.info.Id, copiedShardIds)
	if err != nil {
		return err
	}

	deleteEcVolumeShards(existingLocation, vid, copiedShardIds)

	return nil

}

func oneServerCopyAndMountEcShardsFromSource(ctx context.Context, grpcDialOption grpc.DialOption,
	targetServer *EcNode, startFromShardId uint32, shardCount int,
	volumeId needle.VolumeId, collection string, existingLocation string) (copiedShardIds []uint32, err error) {

	var shardIdsToCopy []uint32
	for shardId := startFromShardId; shardId < startFromShardId+uint32(shardCount); shardId++ {
		shardIdsToCopy = append(shardIdsToCopy, shardId)
	}
	fmt.Printf("allocate %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)

	err = operation.WithVolumeServerClient(targetServer.info.Id, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		if targetServer.info.Id != existingLocation {

			fmt.Printf("copy %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)
			_, copyErr := volumeServerClient.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(volumeId),
				Collection:     collection,
				ShardIds:       shardIdsToCopy,
				CopyEcxFile:    true,
				SourceDataNode: existingLocation,
			})
			if copyErr != nil {
				return fmt.Errorf("copy %d.%v %s => %s : %v\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id, copyErr)
			}
		}

		fmt.Printf("mount %d.%v on %s\n", volumeId, shardIdsToCopy, targetServer.info.Id)
		_, mountErr := volumeServerClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   shardIdsToCopy,
		})
		if mountErr != nil {
			return fmt.Errorf("mount %d.%v on %s : %v\n", volumeId, shardIdsToCopy, targetServer.info.Id, mountErr)
		}

		if targetServer.info.Id != existingLocation {
			copiedShardIds = shardIdsToCopy
			glog.V(0).Infof("%s ec volume %d deletes shards %+v", existingLocation, volumeId, copiedShardIds)
		}

		return nil
	})

	if err != nil {
		return
	}

	return
}

func eachDataNode(topo *master_pb.TopologyInfo, fn func(dc, rack string, dn *master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(dc.Id, rack.Id, dn)
			}
		}
	}
}

func sortEcNodes(ecNodes []*EcNode) {
	sort.Slice(ecNodes, func(i, j int) bool {
		return ecNodes[i].freeEcSlot > ecNodes[j].freeEcSlot
	})
}

func countShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, ecShardInfo := range ecShardInfos {
		shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		count += shardBits.ShardIdCount()
	}
	return
}

func countFreeShardSlots(dn *master_pb.DataNodeInfo) (count int) {
	return int(dn.FreeVolumeCount)*10 - countShards(dn.EcShardInfos)
}

type EcNode struct {
	info       *master_pb.DataNodeInfo
	dc         string
	rack       string
	freeEcSlot int
}

func collectEcNodes(ctx context.Context, commandEnv *CommandEnv) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {

	// list all possible locations
	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return nil, 0, err
	}

	// find out all volume servers with one slot left.
	eachDataNode(resp.TopologyInfo, func(dc, rack string, dn *master_pb.DataNodeInfo) {
		if freeEcSlots := countFreeShardSlots(dn); freeEcSlots > 0 {
			ecNodes = append(ecNodes, &EcNode{
				info:       dn,
				dc:         dc,
				rack:       rack,
				freeEcSlot: int(freeEcSlots),
			})
			totalFreeEcSlots += freeEcSlots
		}
	})

	sortEcNodes(ecNodes)

	return
}

func sourceServerDeleteEcShards(ctx context.Context, grpcDialOption grpc.DialOption,
	collection string, volumeId needle.VolumeId, sourceLocation string, toBeDeletedShardIds []uint32) error {

	fmt.Printf("delete %d.%v from %s\n", volumeId, toBeDeletedShardIds, sourceLocation)

	return operation.WithVolumeServerClient(sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   toBeDeletedShardIds,
		})
		return deleteErr
	})

}

func unmountEcShards(ctx context.Context, grpcDialOption grpc.DialOption,
	volumeId needle.VolumeId, sourceLocation string, toBeUnmountedhardIds []uint32) error {

	fmt.Printf("unmount %d.%v from %s\n", volumeId, toBeUnmountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: toBeUnmountedhardIds,
		})
		return deleteErr
	})
}

func mountEcShards(ctx context.Context, grpcDialOption grpc.DialOption,
	collection string, volumeId needle.VolumeId, sourceLocation string, toBeMountedhardIds []uint32) error {

	fmt.Printf("mount %d.%v on %s\n", volumeId, toBeMountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   toBeMountedhardIds,
		})
		return mountErr
	})
}
