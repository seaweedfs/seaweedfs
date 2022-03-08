package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"io"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func init() {
	Commands = append(Commands, &commandEcEncode{})
}

type commandEcEncode struct {
}

func (c *commandEcEncode) Name() string {
	return "ec.encode"
}

func (c *commandEcEncode) Help() string {
	return `apply erasure coding to a volume

	ec.encode [-collection=""] [-fullPercent=95 -quietFor=1h]
	ec.encode [-collection=""] [-volumeId=<volume_id>]

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

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	collection := encodeCommand.String("collection", "", "the collection name")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	parallelCopy := encodeCommand.Bool("parallelCopy", true, "copy shards in parallel")
	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	if vid != 0 {
		return doEcEncode(commandEnv, *collection, vid, *parallelCopy)
	}

	// apply to all volumes in the collection
	volumeIds, err := collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	fmt.Printf("ec encode volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doEcEncode(commandEnv, *collection, vid, *parallelCopy); err != nil {
			return err
		}
	}

	return nil
}

func doEcEncode(commandEnv *CommandEnv, collection string, vid needle.VolumeId, parallelCopy bool) (err error) {
	// find volume location
	locations, found := commandEnv.MasterClient.GetLocations(uint32(vid))
	if !found {
		return fmt.Errorf("volume %d not found", vid)
	}

	// fmt.Printf("found ec %d shards on %v\n", vid, locations)

	// mark the volume as readonly
	err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, false)
	if err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, locations[0].Url, err)
	}

	// generate ec shards
	err = generateEcShards(commandEnv.option.GrpcDialOption, vid, collection, locations[0].ServerAddress())
	if err != nil {
		return fmt.Errorf("generate ec shards for volume %d on %s: %v", vid, locations[0].Url, err)
	}

	// balance the ec shards to current cluster
	err = spreadEcShards(commandEnv, vid, collection, locations, parallelCopy)
	if err != nil {
		return fmt.Errorf("spread ec shards for volume %d from %s: %v", vid, locations[0].Url, err)
	}

	return nil
}

func generateEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress) error {

	fmt.Printf("generateEcShards %s %d on %s ...\n", collection, volumeId, sourceVolumeServer)

	err := operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		return genErr
	})

	return err

}

func spreadEcShards(commandEnv *CommandEnv, volumeId needle.VolumeId, collection string, existingLocations []wdclient.Location, parallelCopy bool) (err error) {

	allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv, "")
	if err != nil {
		return err
	}

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		return fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	}
	allocatedDataNodes := allEcNodes
	if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
		allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	}

	// calculate how many shards to allocate for these servers
	allocatedEcIds := balancedEcDistribution(allocatedDataNodes)

	// ask the data nodes to copy from the source volume server
	copiedShardIds, err := parallelCopyEcShardsFromSource(commandEnv.option.GrpcDialOption, allocatedDataNodes, allocatedEcIds, volumeId, collection, existingLocations[0], parallelCopy)
	if err != nil {
		return err
	}

	// unmount the to be deleted shards
	err = unmountEcShards(commandEnv.option.GrpcDialOption, volumeId, existingLocations[0].ServerAddress(), copiedShardIds)
	if err != nil {
		return err
	}

	// ask the source volume server to clean up copied ec shards
	err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, existingLocations[0].ServerAddress(), copiedShardIds)
	if err != nil {
		return fmt.Errorf("source delete copied ecShards %s %d.%v: %v", existingLocations[0].Url, volumeId, copiedShardIds, err)
	}

	// ask the source volume server to delete the original volume
	for _, location := range existingLocations {
		fmt.Printf("delete volume %d from %s\n", volumeId, location.Url)
		err = deleteVolume(commandEnv.option.GrpcDialOption, volumeId, location.ServerAddress())
		if err != nil {
			return fmt.Errorf("deleteVolume %s volume %d: %v", location.Url, volumeId, err)
		}
	}

	return err

}

func parallelCopyEcShardsFromSource(grpcDialOption grpc.DialOption, targetServers []*EcNode, allocatedEcIds [][]uint32, volumeId needle.VolumeId, collection string, existingLocation wdclient.Location, parallelCopy bool) (actuallyCopied []uint32, err error) {

	fmt.Printf("parallelCopyEcShardsFromSource %d %s\n", volumeId, existingLocation.Url)

	var wg sync.WaitGroup
	shardIdChan := make(chan []uint32, len(targetServers))
	copyFunc := func(server *EcNode, allocatedEcShardIds []uint32) {
		defer wg.Done()
		copiedShardIds, copyErr := oneServerCopyAndMountEcShardsFromSource(grpcDialOption, server,
			allocatedEcShardIds, volumeId, collection, existingLocation.ServerAddress())
		if copyErr != nil {
			err = copyErr
		} else {
			shardIdChan <- copiedShardIds
			server.addEcVolumeShards(volumeId, collection, copiedShardIds)
		}
	}
	cleanupFunc := func(server *EcNode, allocatedEcShardIds []uint32) {
		if err := unmountEcShards(grpcDialOption, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("unmount aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
		if err := sourceServerDeleteEcShards(grpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("remove aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
	}

	// maybe parallelize
	for i, server := range targetServers {
		if len(allocatedEcIds[i]) <= 0 {
			continue
		}

		wg.Add(1)
		if parallelCopy {
			go copyFunc(server, allocatedEcIds[i])
		} else {
			copyFunc(server, allocatedEcIds[i])
		}
	}
	wg.Wait()
	close(shardIdChan)

	if err != nil {
		for i, server := range targetServers {
			if len(allocatedEcIds[i]) <= 0 {
				continue
			}
			cleanupFunc(server, allocatedEcIds[i])
		}
		return nil, err
	}

	for shardIds := range shardIdChan {
		actuallyCopied = append(actuallyCopied, shardIds...)
	}

	return
}

func balancedEcDistribution(servers []*EcNode) (allocated [][]uint32) {
	allocated = make([][]uint32, len(servers))
	allocatedShardIdIndex := uint32(0)
	serverIndex := rand.Intn(len(servers))
	for allocatedShardIdIndex < erasure_coding.TotalShardsCount {
		if servers[serverIndex].freeEcSlot > 0 {
			allocated[serverIndex] = append(allocated[serverIndex], allocatedShardIdIndex)
			allocatedShardIdIndex++
		}
		serverIndex++
		if serverIndex >= len(servers) {
			serverIndex = 0
		}
	}

	return allocated
}

func collectVolumeIdsForEcEncode(commandEnv *CommandEnv, selectedCollection string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect volumes quiet for: %d seconds\n", quietSeconds)

	vidMap := make(map[uint32]bool)
	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Collection == selectedCollection && v.ModifiedAtSecond+quietSeconds < nowUnixSeconds {
					if float64(v.Size) > fullPercentage/100*float64(volumeSizeLimitMb)*1024*1024 {
						vidMap[v.Id] = true
					}
				}
			}
		}
	})

	for vid := range vidMap {
		vids = append(vids, needle.VolumeId(vid))
	}

	return
}
