package shell

import (
	_ "embed"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"

	//"google.golang.org/protobuf/proto"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestParsing(t *testing.T) {
	topo := parseOutput(topoData)

	assert.Equal(t, 5, len(topo.DataCenterInfos))

	topo = parseOutput(topoData2)

	dataNodes := topo.DataCenterInfos[0].RackInfos[0].DataNodeInfos
	assert.Equal(t, 14, len(dataNodes))
	diskInfo := dataNodes[0].DiskInfos[""]
	assert.Equal(t, 1559, len(diskInfo.VolumeInfos))
	assert.Equal(t, 6740, len(diskInfo.EcShardInfos))

}

func parseOutput(output string) *master_pb.TopologyInfo {
	lines := strings.Split(output, "\n")
	var topo *master_pb.TopologyInfo
	var dc *master_pb.DataCenterInfo
	var rack *master_pb.RackInfo
	var dn *master_pb.DataNodeInfo
	var disk *master_pb.DiskInfo
	for _, line := range lines {
		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")
		switch parts[0] {
		case "Topology":
			if topo == nil {
				topo = &master_pb.TopologyInfo{}
			}
		case "DataCenter":
			if dc == nil {
				dc = &master_pb.DataCenterInfo{
					Id: parts[1],
				}
				topo.DataCenterInfos = append(topo.DataCenterInfos, dc)
			} else {
				dc = nil
			}
		case "Rack":
			if rack == nil {
				rack = &master_pb.RackInfo{
					Id: parts[1],
				}
				dc.RackInfos = append(dc.RackInfos, rack)
			} else {
				rack = nil
			}
		case "DataNode":
			if dn == nil {
				dn = &master_pb.DataNodeInfo{
					Id:        parts[1],
					DiskInfos: make(map[string]*master_pb.DiskInfo),
				}
				rack.DataNodeInfos = append(rack.DataNodeInfos, dn)
			} else {
				dn = nil
			}
		case "Disk":
			if disk == nil {
				diskType := parts[1][:strings.Index(parts[1], "(")]
				volumeCountStr := parts[1][strings.Index(parts[1], ":")+1 : strings.Index(parts[1], "/")]
				maxVolumeCountStr := parts[1][strings.Index(parts[1], "/")+1:]
				maxVolumeCount, _ := strconv.Atoi(maxVolumeCountStr)
				volumeCount, _ := strconv.Atoi(volumeCountStr)
				disk = &master_pb.DiskInfo{
					Type:           diskType,
					MaxVolumeCount: int64(maxVolumeCount),
					VolumeCount:    int64(volumeCount),
				}
				dn.DiskInfos[types.ToDiskType(diskType).String()] = disk
			} else {
				disk = nil
			}
		case "volume":
			volumeLine := line[len("volume "):]
			volume := &master_pb.VolumeInformationMessage{}
			proto.UnmarshalText(volumeLine, volume)
			disk.VolumeInfos = append(disk.VolumeInfos, volume)
		case "ec":
			ecVolumeLine := line[len("ec volume "):]
			ecShard := &master_pb.VolumeEcShardInformationMessage{}
			for _, part := range strings.Split(ecVolumeLine, " ") {
				if strings.HasPrefix(part, "id:") {
					id, _ := strconv.ParseInt(part[len("id:"):], 10, 64)
					ecShard.Id = uint32(id)
				}
				if strings.HasPrefix(part, "collection:") {
					ecShard.Collection = part[len("collection:"):]
				}
				if strings.HasPrefix(part, "shards:") {
					shards := part[len("shards:["):]
					shards = strings.TrimRight(shards, "]")
					shardBits := erasure_coding.ShardBits(0)
					for _, shardId := range strings.Split(shards, ",") {
						sid, _ := strconv.Atoi(shardId)
						shardBits = shardBits.AddShardId(erasure_coding.ShardId(sid))
					}
					ecShard.EcIndexBits = uint32(shardBits)
				}
			}
			disk.EcShardInfos = append(disk.EcShardInfos, ecShard)
		}
	}

	return topo
}

//go:embed volume.list.txt
var topoData string

//go:embed volume.list2.txt
var topoData2 string

//go:embed volume.ecshards.txt
var topoDataEc string
