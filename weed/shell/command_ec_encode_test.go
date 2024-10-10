package shell

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/exp/maps"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestEcDistribution(t *testing.T) {

	topologyInfo := parseOutput(topoData)

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topologyInfo, "")

	sortEcNodesByFreeslotsDescending(ecNodes)

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		println("not enough free ec shard slots", totalFreeEcSlots)
	}
	allocatedDataNodes := ecNodes
	if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
		allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	}

	for _, dn := range allocatedDataNodes {
		// fmt.Printf("info %+v %+v\n", dn.info, dn)
		fmt.Printf("=> %+v %+v\n", dn.info.Id, dn.freeEcSlot)
	}

}

var vMap = map[needle.VolumeId][]wdclient.Location{
	needle.VolumeId(1): {wdclient.Location{Url: "192.168.3.74:10001"}, wdclient.Location{Url: "192.168.3.75:10001"}},
	needle.VolumeId(2): {wdclient.Location{Url: "192.168.3.76:10001"}, wdclient.Location{Url: "192.168.3.77:10001"}},
	needle.VolumeId(3): {wdclient.Location{Url: "192.168.3.78:10001"}, wdclient.Location{Url: "192.168.3.74:10002"}},
	needle.VolumeId(4): {wdclient.Location{Url: "192.168.3.75:10002"}, wdclient.Location{Url: "192.168.3.76:10002"}},
	needle.VolumeId(5): {wdclient.Location{Url: "192.168.3.77:10002"}, wdclient.Location{Url: "192.168.3.78:10002"}},
	needle.VolumeId(6): {wdclient.Location{Url: "192.168.3.74:10003"}, wdclient.Location{Url: "192.168.3.77:10003"}},
}

func GetLocationsClone(vid needle.VolumeId) (v []wdclient.Location, b bool) {
	v, b = vMap[vid]
	return
}

// serversDisplayTimesInVolumes: map[192.168.3.74:4 192.168.3.75:4 192.168.3.76:3 192.168.3.77:4 192.168.3.78:3]
// volumeLocationsMap: map[1:[{Url:192.168.3.74:10001} {Url:192.168.3.75:10001}] 2:[{Url:192.168.3.76:10001} {Url:192.168.3.77:10001}] 3:[{Url:192.168.3.78:10001} {Url:192.168.3.74:10002}] 4:[{Url:192.168.3.75:10002} {Url:192.168.3.76:10002}] 5:[{Url:192.168.3.77:10002} {Url:192.168.3.78:10002}] 6:[{Url:192.168.3.74:10003} {Url:192.168.3.77:10003}]]
// volumeChooseLocationMap: map[1:{Url:192.168.3.75:10001} 2:{Url:192.168.3.76:10001} 3:{Url:192.168.3.78:10001} 4:{Url:192.168.3.75:10002} 5:{Url:192.168.3.78:10002} 6:{Url:192.168.3.74:10003}]
func Test_getServerDisplayTimesInVolumes(t *testing.T) {
	volumeIds := maps.Keys(vMap)
	commandEnv := CommandEnv{}
	commandEnv.MasterClient = &wdclient.MasterClient{}

	volumeLocationsMap, volumeChooseLocationMap := getServerDisplayTimesInVolumesTest(&commandEnv, volumeIds)

	//fmt.Printf("serversDisplayTimesInVolumes: %v \n", serversDisplayTimesInVolumes)
	fmt.Printf("volumeLocationsMap: %v \n", volumeLocationsMap)
	fmt.Printf("volumeChooseLocationMap: %v \n", volumeChooseLocationMap)

	var wg sync.WaitGroup
	wg.Add(len(volumeIds))
	for _, vid := range volumeIds {
		locations := volumeLocationsMap[vid]
		chooseLoc := volumeChooseLocationMap[vid]
		go func() {
			defer wg.Done()
			time.Sleep(time.Second * 1)
			fmt.Printf("locations:%v \n", locations)
			fmt.Printf("location:%v \n", chooseLoc)
		}()
	}
	wg.Wait()

}

// server IP display times in volumes. if times is more, The lower the priority
func getServerDisplayTimesInVolumesTest(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId][]wdclient.Location, map[needle.VolumeId]wdclient.Location) {
	var serversDisplayTimesInVolumes = make(map[string]uint32)
	var volumeLocationsMap = make(map[needle.VolumeId][]wdclient.Location)
	var volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
	//1-192.168.3.74 = []
	for _, vid := range volumeIds {
		//locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		locations, found := GetLocationsClone(vid)
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
	var result [][]int
	permute(intVolumeIds, len(intVolumeIds), &result)

	for _, currentVolumeIds := range result {
		volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
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

func TestGetCombinations(t *testing.T) {
	var result [][]int
	arr := []int{222, 223, 224, 225, 226, 227}
	permute(arr, len(arr), &result)

	fmt.Printf("result:%v \n", result)
}
