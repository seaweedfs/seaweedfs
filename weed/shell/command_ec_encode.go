package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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

func (c *commandEcEncode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	collection := encodeCommand.String("collection", "", "the collection name")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	parallelCopy := encodeCommand.Bool("parallelCopy", true, "copy shards in parallel")
	forceChanges := encodeCommand.Bool("force", false, "force the encoding even if the cluster has less than recommended 4 nodes")
	concurrency := encodeCommand.Int("concurrency", 3, "limit total concurrent ec.encode volume number (default 3)")
	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	if !*forceChanges {
		var nodeCount int
		eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
			nodeCount++
		})
		if nodeCount < erasure_coding.ParityShardsCount {
			glog.V(0).Infof("skip erasure coding with %d nodes, less than recommended %d nodes", nodeCount, erasure_coding.ParityShardsCount)
			return nil
		}
	}

	vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	if vid != 0 {
		return doEcEncode(commandEnv, *collection, vid, []wdclient.Location{}, wdclient.Location{}, *parallelCopy)
	}

	// apply to all volumes in the collection
	volumeIds, err := collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	//A maximum of 3 volumes can be processed at one time
	maxProcessNum := *concurrency
	fmt.Printf("ec encode volumes: %v, concurrent number:%d \n", volumeIds, maxProcessNum)
	processVolumeIds := volumeIds
	if len(volumeIds) > maxProcessNum {
		processVolumeIds = volumeIds[0:maxProcessNum]
	}
	//load balancing for servers
	volumeLocationsMap, volumeChooseLocationMap := chooseLocationForVolumes(commandEnv, processVolumeIds)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors = make([]error, 0)
	wg.Add(len(processVolumeIds))

	for _, vid := range processVolumeIds {
		locations := volumeLocationsMap[vid]
		chooseLoc := volumeChooseLocationMap[vid]
		go func() {
			defer wg.Done()
			if err = doEcEncode(commandEnv, *collection, vid, locations, chooseLoc, *parallelCopy); err != nil {
				mu.Lock()
				errors = append(errors, err)
				fmt.Printf("doEcEncode error:%v \n", err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(errors) > 0 {
		return errors[0]
	}

	return nil
}

// Recursive function is used to obtain the permutations and combinations of an array
func permute(arr []int, n int, result *[][]int) {
	if n == 1 {
		temp := make([]int, len(arr))
		copy(temp, arr)
		*result = append(*result, temp)
		return
	} else {
		for i := 0; i < n; i++ {
			permute(arr, n-1, result)
			if n%2 == 1 {
				arr[0], arr[n-1] = arr[n-1], arr[0]
			} else {
				arr[i], arr[n-1] = arr[n-1], arr[i]
			}
		}
	}
	return
}

// server IP display times in volumes. if times is more, The lower the priority
func chooseLocationForVolumes(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId][]wdclient.Location, map[needle.VolumeId]wdclient.Location) {
	var serversDisplayTimesInVolumes = make(map[string]uint32)
	var volumeLocationsMap = make(map[needle.VolumeId][]wdclient.Location)
	var volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
	//1-192.168.3.74 = []
	for _, vid := range volumeIds {
		locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		if !found {
			continue
		}
		volumeLocationsMap[vid] = locations
		for _, loc := range locations {
			serverIp := splitIP(loc.Url)
			if len(serverIp) <= 0 {
				fmt.Printf("loc url is err:%s", loc.Url)
				continue
			}
			//init 1 times
			serversDisplayTimesInVolumes[serverIp] = uint32(1)
		}
	}
	var intVolumeIds = make([]int, 0)
	for _, vid := range volumeIds {
		intVolumeIds = append(intVolumeIds, int(vid))
	}

	sort.Ints(intVolumeIds)
	//startIndex := rand.IntN(len(intVolumeIds) - 1)
	var permuteResult [][]int
	permute(intVolumeIds, len(intVolumeIds), &permuteResult)

	for _, currentVolumeIds := range permuteResult {
		volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
		for key, _ := range serversDisplayTimesInVolumes {
			serversDisplayTimesInVolumes[key] = uint32(1)
		}
		for _, vid := range currentVolumeIds {
			locations := volumeLocationsMap[needle.VolumeId(vid)]
			if len(locations) == 0 {
				continue
			}
			var times = uint32(1000)
			var chooseLoc = wdclient.Location{}
			for _, loc := range locations {
				serverIp := splitIP(loc.Url)
				if len(serverIp) <= 0 {
					fmt.Printf("loc url is err:%s", loc.Url)
					continue
				}
				locTimes := serversDisplayTimesInVolumes[serverIp]
				if locTimes < times {
					times = locTimes
					chooseLoc = loc
				}
			}
			volumeChooseLocationMap[needle.VolumeId(vid)] = chooseLoc
			//////
			serverIp := splitIP(chooseLoc.Url)
			var newTimes = uint32(0)
			if v, b := serversDisplayTimesInVolumes[serverIp]; b {
				newTimes = v
			}
			newTimes++
			serversDisplayTimesInVolumes[serverIp] = newTimes
		}

		if checkFillFullServers(volumeChooseLocationMap, len(serversDisplayTimesInVolumes)) {
			break
		}
	}
	return volumeLocationsMap, volumeChooseLocationMap
}

func checkFillFullServers(mapLocation map[needle.VolumeId]wdclient.Location, serverLength int) bool {
	var servers = make(map[string]uint32)
	for _, loc := range mapLocation {
		serverIp := splitIP(loc.Url)
		servers[serverIp] = uint32(1)
	}

	return len(servers) == serverLength
}

func splitIP(url string) string {
	parts := strings.Split(url, ":")
	if len(parts) != 2 {
		return ""
	}
	return parts[0]
}

func doEcEncode(commandEnv *CommandEnv, collection string, vid needle.VolumeId, locations []wdclient.Location, chooseLoc wdclient.Location, parallelCopy bool) (err error) {
	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}
	// If volumeId is provided -> (ec.encode -volumeId=<volume_id>)
	if len(locations) == 0 {
		var found = false
		locations, found = commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		if !found {
			return fmt.Errorf("volume %d not found", vid)
		}
		chooseLoc = locations[0]
	}
	// mark the volume as readonly
	err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, false, false)
	if err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, chooseLoc.Url, err)
	}

	// generate and mount ec shards
	err = generateAndMountEcShards(commandEnv, vid, collection, chooseLoc.ServerAddress())
	if err != nil {
		return fmt.Errorf("generate ec shards for volume %d on %s: %v", vid, chooseLoc.Url, err)
	}

	// balance the ec shards to current cluster
	err = spreadEcShards(commandEnv, vid, collection, locations, chooseLoc, parallelCopy)
	if err != nil {
		return fmt.Errorf("spread ec shards for volume %d from %s: %v", vid, chooseLoc.Url, err)
	}

	return nil
}

func generateAndMountEcShards(commandEnv *CommandEnv, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress) error {

	fmt.Printf("generateEcShards %s %d on %s ...\n", collection, volumeId, sourceVolumeServer)

	allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv, "")
	if err != nil {
		return err
	}

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		return fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	}
	//Ensure EcNodes come from different rack, Prevent uneven distribution
	allocatedDataNodes := getEcNodesMustDifferentRacks(allEcNodes)
	if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
		allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	}

	// calculate how many shards to allocate for these servers
	allocatedEcIds, allocatedNodes := balancedEcDistribution2(allocatedDataNodes)

	err2 := operation.WithVolumeServerClient(false, sourceVolumeServer, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:       uint32(volumeId),
			Collection:     collection,
			AllocatedEcIds: allocatedEcIds,
		})
		return genErr
	})

	if err2 == nil {
		//mount
		for key, _ := range allocatedNodes {
			mountErr := mountEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.ServerAddress(key), allocatedEcIds[key].GetShardIds())
			if mountErr != nil {
				err2 = fmt.Errorf("mount %d.%v on %s : %v\n", volumeId, allocatedEcIds[key].GetShardIds(), key, mountErr)
			}
		}
		//add ec shards
		for key, server := range allocatedNodes {
			server.addEcVolumeShards(volumeId, collection, allocatedEcIds[key].GetShardIds())
		}
	}

	cleanupFunc := func(server *EcNode, allocatedEcShardIds []uint32) {
		if err := unmountEcShards(commandEnv.option.GrpcDialOption, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("unmount aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
		if err := sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("remove aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
	}

	if err2 != nil {
		for i, server := range allocatedNodes {
			if len(allocatedEcIds[i].GetShardIds()) <= 0 {
				continue
			}
			cleanupFunc(server, allocatedEcIds[i].GetShardIds())
		}
	}

	return err2

}

func getEcNodesMustDifferentRacks(allEcNodes []*EcNode) []*EcNode {
	racks := collectRacks(allEcNodes)
	// calculate average number of shards an ec rack should have for one volume
	averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))
	var rackEcNodes = make(map[string][]*EcNode)
	for _, node := range allEcNodes {
		if _, b := rackEcNodes[string(node.rack)]; !b {
			arr := make([]*EcNode, 0)
			rackEcNodes[string(node.rack)] = arr
		}
		if len(rackEcNodes[string(node.rack)]) >= averageShardsPerEcRack {
			continue
		}
		rackEcNodes[string(node.rack)] = append(rackEcNodes[string(node.rack)], node)
	}
	rackNodesSlice := make([]*EcNode, 0)
	for _, value := range rackEcNodes {
		for _, node := range value {
			rackNodesSlice = append(rackNodesSlice, node)
		}
	}

	sortEcNodesByFreeslotsDescending(rackNodesSlice)

	return rackNodesSlice
}

func spreadEcShards(commandEnv *CommandEnv, volumeId needle.VolumeId, collection string, existingLocations []wdclient.Location, chooseLoc wdclient.Location, parallelCopy bool) (err error) {

	//allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv, "")
	//if err != nil {
	//	return err
	//}
	//
	//if totalFreeEcSlots < erasure_coding.TotalShardsCount {
	//	return fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	//}
	//Ensure EcNodes come from different rack, Prevent uneven distribution
	//allocatedDataNodes := getEcNodesMustDifferentRacks(allEcNodes)
	//if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
	//	allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	//}

	// calculate how many shards to allocate for these servers
	//allocatedEcIds := balancedEcDistribution(allocatedDataNodes)

	// ask the data nodes to copy from the source volume server
	//copiedShardIds, err := parallelCopyEcShardsFromSource(commandEnv.option.GrpcDialOption, allocatedDataNodes, allocatedEcIds, volumeId, collection, chooseLoc, parallelCopy)
	//if err != nil {
	//	return err
	//}

	// unmount the to be deleted shards
	//err = unmountEcShards(commandEnv.option.GrpcDialOption, volumeId, chooseLoc.ServerAddress(), copiedShardIds)
	//if err != nil {
	//	return err
	//}

	// ask the source volume server to clean up copied ec shards
	//err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, chooseLoc.ServerAddress(), copiedShardIds)
	//if err != nil {
	//	return fmt.Errorf("source delete copied ecShards %s %d.%v: %v", chooseLoc.Url, volumeId, copiedShardIds, err)
	//}

	// ask the source volume server to delete the original volume
	for _, location := range existingLocations {
		fmt.Printf("delete volume %d from %s\n", volumeId, location.Url)
		//can't delete vif file after ec volume delete
		err = deleteVolumeAfterEc(commandEnv.option.GrpcDialOption, volumeId, location.ServerAddress(), true)
		if err != nil {
			return fmt.Errorf("deleteVolume %s volume %d: %v", location.Url, volumeId, err)
		}
	}

	return err

}

func parallelCopyEcShardsFromSource(grpcDialOption grpc.DialOption, targetServers []*EcNode, allocatedEcIds [][]uint32, volumeId needle.VolumeId, collection string, existingLocation wdclient.Location, parallelCopy bool) (actuallyCopied []uint32, err error) {

	fmt.Printf("parallelCopyEcShardsFromSource, %d %s, targetServers: %+v, allocatedEcIds: %v\n", volumeId, existingLocation.Url, targetServers, allocatedEcIds)

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

func balancedEcDistribution2(servers []*EcNode) (map[string]*volume_server_pb.EcIds, map[string]*EcNode) {
	allocated := make(map[string]*volume_server_pb.EcIds, len(servers))
	allocatedNodes := make(map[string]*EcNode, len(servers))
	allocatedShardIdIndex := uint32(0)
	serverIndex := rand.Intn(len(servers))
	for allocatedShardIdIndex < erasure_coding.TotalShardsCount {
		node := servers[serverIndex]
		serverAddress := pb.NewServerAddressWithGrpcPort(node.info.Id, int(node.info.GrpcPort))
		key := serverAddress.String()
		if node.freeEcSlot > 0 {
			if _, b := allocated[key]; !b {
				allocated[key] = &volume_server_pb.EcIds{ShardIds: make([]uint32, 0)}
			}
			allocated[key].ShardIds = append(allocated[key].ShardIds, allocatedShardIdIndex)
			allocatedNodes[key] = node
			allocatedShardIdIndex++
		}
		serverIndex++
		if serverIndex >= len(servers) {
			serverIndex = 0
		}
	}

	return allocated, allocatedNodes
}

func collectVolumeIdsForEcEncode(commandEnv *CommandEnv, selectedCollection string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect volumes quiet for: %d seconds and %.1f%% full\n", quietSeconds, fullPercentage)

	vidMap := make(map[uint32]bool)
	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				// ignore remote volumes
				if v.RemoteStorageName != "" && v.RemoteStorageKey != "" {
					continue
				}
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
