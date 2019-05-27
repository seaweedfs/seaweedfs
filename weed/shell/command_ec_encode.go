package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

func init() {
	commands = append(commands, &commandEcEncode{})
}

type commandEcEncode struct {
}

func (c *commandEcEncode) Name() string {
	return "ec.encode"
}

func (c *commandEcEncode) Help() string {
	return `apply erasure coding to a volume

	This command will:
	1. freeze one volume
	2. apply erasure coding to the volume
	3. move the encoded shards to multiple volume servers

	The erasure coding is 10.4. So ideally you have more than 14 volume servers, and you can afford
	to lose 4 volume servers.

	If the number of volumes are not high, the worst case is that you only have 4 volume servers,
	and the shards are spread as 4,4,3,3, respectively. You can afford to lose one volume server.

	If you only have less than 4 volume servers, with erasure coding, at least you can afford to
	have 4 corrupted shard files.

`
}

func (c *commandEcEncode) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	collection := encodeCommand.String("collection", "", "the collection name")
	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}

	ctx := context.Background()

	// find volume location
	locations := commandEnv.masterClient.GetLocations(uint32(*volumeId))
	if len(locations) == 0 {
		return fmt.Errorf("volume %d not found", *volumeId)
	}

	// generate ec shards
	err = generateEcShards(ctx, commandEnv.option.GrpcDialOption, needle.VolumeId(*volumeId), *collection, locations[0].Url)
	if err != nil {
		return fmt.Errorf("generate ec shards for volume %d on %s: %v", *volumeId, locations[0].Url, err)
	}

	// balance the ec shards to current cluster
	err = balanceEcShards(ctx, commandEnv, needle.VolumeId(*volumeId), *collection, locations)
	if err != nil {
		return fmt.Errorf("balance ec shards for volume %d on %s: %v", *volumeId, locations[0].Url, err)
	}

	return err
}

func generateEcShards(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, collection string, sourceVolumeServer string) error {

	err := operation.WithVolumeServerClient(sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		return genErr
	})

	return err

}

func balanceEcShards(ctx context.Context, commandEnv *commandEnv, volumeId needle.VolumeId, collection string, existingLocations []wdclient.Location) (err error) {

	// list all possible locations
	var resp *master_pb.VolumeListResponse
	err = commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	// find out all volume servers with one volume slot left.
	var allDataNodes []*master_pb.DataNodeInfo
	var totalFreeEcSlots uint32
	eachDataNode(resp.TopologyInfo, func(dn *master_pb.DataNodeInfo) {
		if freeEcSlots := countFreeShardSlots(dn); freeEcSlots > 0 {
			allDataNodes = append(allDataNodes, dn)
			totalFreeEcSlots += freeEcSlots
		}
	})
	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		return fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	}
	sort.Slice(allDataNodes, func(i, j int) bool {
		return countFreeShardSlots(allDataNodes[j]) < countFreeShardSlots(allDataNodes[i])
	})
	if len(allDataNodes) > erasure_coding.TotalShardsCount {
		allDataNodes = allDataNodes[:erasure_coding.TotalShardsCount]
	}

	// calculate how many shards to allocate for these servers
	allocated := balancedEcDistribution(allDataNodes)

	// ask the data nodes to copy from the source volume server
	copiedShardIds, err := parallelCopyEcShardsFromSource(ctx, commandEnv.option.GrpcDialOption, allDataNodes, allocated, volumeId, collection, existingLocations[0])
	if err != nil {
		return nil
	}

	// ask the source volume server to clean up copied ec shards
	err = sourceServerDeleteEcShards(ctx, commandEnv.option.GrpcDialOption, volumeId, existingLocations[0], copiedShardIds)
	if err != nil {
		return fmt.Errorf("sourceServerDeleteEcShards %s %d.%v: %v", existingLocations[0], volumeId, copiedShardIds, err)
	}

	// ask the source volume server to delete the original volume
	for _, location := range existingLocations {
		err = deleteVolume(ctx, commandEnv.option.GrpcDialOption, volumeId, location.Url)
		if err != nil {
			return fmt.Errorf("deleteVolume %s volume %d: %v", location.Url, volumeId, err)
		}
	}

	return err

}

func parallelCopyEcShardsFromSource(ctx context.Context, grpcDialOption grpc.DialOption,
	targetServers []*master_pb.DataNodeInfo, allocated []uint32,
	volumeId needle.VolumeId, collection string, existingLocation wdclient.Location) (actuallyCopied []uint32, err error) {

	// parallelize
	shardIdChan := make(chan []uint32, len(targetServers))
	var wg sync.WaitGroup
	startFromShardId := uint32(0)
	for i, server := range targetServers {
		if allocated[i] <= 0 {
			continue
		}

		wg.Add(1)
		go func(server *master_pb.DataNodeInfo, startFromShardId uint32, shardCount uint32) {
			defer wg.Done()
			copiedShardIds, copyErr := oneServerCopyEcShardsFromSource(ctx, grpcDialOption, server,
				startFromShardId, shardCount, volumeId, collection, existingLocation)
			if copyErr != nil {
				err = copyErr
			} else {
				shardIdChan <- copiedShardIds
			}
		}(server, startFromShardId, allocated[i])
		startFromShardId += allocated[i]
	}
	wg.Wait()
	close(shardIdChan)

	if err != nil {
		return nil, err
	}

	for shardIds := range shardIdChan {
		actuallyCopied = append(actuallyCopied, shardIds...)
	}

	return
}

func oneServerCopyEcShardsFromSource(ctx context.Context, grpcDialOption grpc.DialOption,
	targetServer *master_pb.DataNodeInfo, startFromShardId uint32, shardCount uint32,
	volumeId needle.VolumeId, collection string, existingLocation wdclient.Location) (copiedShardIds []uint32, err error) {

	for shardId := startFromShardId; shardId < startFromShardId+shardCount; shardId++ {
		fmt.Printf("allocate %d.%d %s => %s\n", volumeId, shardId, existingLocation.Url, targetServer.Id)
		copiedShardIds = append(copiedShardIds, shardId)
	}

	err = operation.WithVolumeServerClient(targetServer.Id, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		if targetServer.Id != existingLocation.Url {

			_, copyErr := volumeServerClient.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(volumeId),
				Collection:     collection,
				ShardIds:      copiedShardIds,
				SourceDataNode: existingLocation.Url,
			})
			if copyErr != nil {
				return copyErr
			}
		}

		_, mountErr := volumeServerClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:  copiedShardIds,
		})
		if mountErr != nil {
			return mountErr
		}

		return nil
	})

	if err != nil {
		return
	}

	return
}

func sourceServerDeleteEcShards(ctx context.Context, grpcDialOption grpc.DialOption,
	volumeId needle.VolumeId, sourceLocation wdclient.Location, toBeDeletedShardIds []uint32) error {

	shouldDeleteEcx := len(toBeDeletedShardIds) == erasure_coding.TotalShardsCount

	return operation.WithVolumeServerClient(sourceLocation.Url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:        uint32(volumeId),
			ShardIds:       toBeDeletedShardIds,
			ShouldDeleteEcx: shouldDeleteEcx,
		})
		return deleteErr
	})

}

func balancedEcDistribution(servers []*master_pb.DataNodeInfo) (allocated []uint32) {
	freeSlots := make([]uint32, len(servers))
	allocated = make([]uint32, len(servers))
	for i, server := range servers {
		freeSlots[i] = countFreeShardSlots(server)
	}
	allocatedCount := 0
	for allocatedCount < erasure_coding.TotalShardsCount {
		for i, _ := range servers {
			if freeSlots[i]-allocated[i] > 0 {
				allocated[i] += 1
				allocatedCount += 1
			}
			if allocatedCount >= erasure_coding.TotalShardsCount {
				break
			}
		}
	}

	return allocated
}

func eachDataNode(topo *master_pb.TopologyInfo, fn func(*master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(dn)
			}
		}
	}
}

func countShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count uint32) {
	for _, ecShardInfo := range ecShardInfos {
		shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		count += uint32(shardBits.ShardIdCount())
	}
	return
}

func countFreeShardSlots(dn *master_pb.DataNodeInfo) (count uint32) {
	return uint32(dn.FreeVolumeCount)*10 - countShards(dn.EcShardInfos)
}
