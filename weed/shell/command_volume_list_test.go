package shell

import (
	"bytes"
	"flag"
	"fmt"

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

// TODO: actually parsing all fields would be nice...
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
				topo = &master_pb.TopologyInfo{
					Id: parts[1],
				}
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
				// TODO: we need to parse EC shard sizes as well
				if strings.HasPrefix(part, "shards:") {
					shards := part[len("shards:["):]
					shards = strings.TrimRight(shards, "]")
					shardsInfo := erasure_coding.NewShardsInfo()
					for _, shardId := range strings.Split(shards, ",") {
						sid, _ := strconv.Atoi(shardId)
						shardsInfo.Set(erasure_coding.NewShardInfo(erasure_coding.ShardId(sid), 0))
					}
					ecShard.EcIndexBits = shardsInfo.Bitmap()
					ecShard.ShardSizes = shardsInfo.SizesInt64()
				}
			}
			disk.EcShardInfos = append(disk.EcShardInfos, ecShard)
		}
	}

	return topo
}

// TestWriteDataNodeInfo_SplitsCollapsedDisksByPhysicalDiskId verifies that
// the verbose Disk block in volume.list shows one entry per physical disk
// when the master collapsed several same-type disks under a single
// DiskInfos["hdd"] map entry. Before the split, six physical disks would
// appear as "Disk hdd ... id:0" with all volumes stacked on it.
func TestWriteDataNodeInfo_SplitsCollapsedDisksByPhysicalDiskId(t *testing.T) {
	dn := &master_pb.DataNodeInfo{
		Id: "node1:8081",
		DiskInfos: map[string]*master_pb.DiskInfo{
			"hdd": {
				Type:           "hdd",
				MaxVolumeCount: 60,
				VolumeInfos: []*master_pb.VolumeInformationMessage{
					{Id: 1, DiskId: 0, Collection: "c"},
					{Id: 2, DiskId: 1, Collection: "c"},
					{Id: 3, DiskId: 2, Collection: "c"},
				},
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{Id: 100, DiskId: 3, Collection: "c", EcIndexBits: 1},
					{Id: 101, DiskId: 4, Collection: "c", EcIndexBits: 1},
					{Id: 102, DiskId: 5, Collection: "c", EcIndexBits: 1},
				},
			},
		},
	}

	c := &commandVolumeList{}
	fs := flag.NewFlagSet("volume.list", flag.ContinueOnError)
	c.collectionPattern = fs.String("collection", "", "")
	c.dataCenter = fs.String("dataCenter", "", "")
	c.rack = fs.String("rack", "", "")
	c.dataNode = fs.String("dataNode", "", "")
	c.readonly = fs.Bool("readonly", false, "")
	c.writable = fs.Bool("writable", false, "")
	c.volumeId = fs.Uint64("volumeId", 0, "")

	var buf bytes.Buffer
	verbosity := 5
	rackInvocations := 0
	c.writeDataNodeInfo(&buf, dn, verbosity, func() { rackInvocations++ })

	out := buf.String()
	// Match the "id:N\n" line ending so substring checks don't accept
	// unrelated tokens like "ec volume id:101" as a match for id:1.
	for diskID := 0; diskID < 6; diskID++ {
		needle := fmt.Sprintf("id:%d\n", diskID)
		if !strings.Contains(out, needle) {
			t.Errorf("output missing %q; got:\n%s", needle, out)
		}
	}

	// The parent's outRackInfo callback rides the same code path as the
	// DataNode header — it fires from the inner writeDiskInfo callback,
	// once per disk before the fix. After the fix the header guard runs
	// outRackInfo at most once per DataNode. This proxy lets us pin the
	// "header printed once" invariant without depending on the exact
	// format of the rendered DataNode line.
	if rackInvocations != 1 {
		t.Errorf("DataNode header callback ran %d times; want 1 (regression: header printed once per split disk)", rackInvocations)
	}
}
