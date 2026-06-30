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

func TestWriteTopologyInfo_PrintsParentHeadersOnce(t *testing.T) {
	topo := topoFromNodes(
		volNode("node1:8081", &master_pb.VolumeInformationMessage{Id: 1, Collection: "c"}),
		volNode("node2:8081", &master_pb.VolumeInformationMessage{Id: 2, Collection: "c"}),
	)
	topo.DiskInfos = map[string]*master_pb.DiskInfo{"hdd": {Type: "hdd"}}
	topo.DataCenterInfos[0].DiskInfos = map[string]*master_pb.DiskInfo{"hdd": {Type: "hdd"}}
	topo.DataCenterInfos[0].RackInfos[0].DiskInfos = map[string]*master_pb.DiskInfo{"hdd": {Type: "hdd"}}

	c := &commandVolumeList{}
	fs := flag.NewFlagSet("volume.list", flag.ContinueOnError)
	c.collectionPattern = fs.String("collection", "", "")
	c.dataCenter = fs.String("dataCenter", "", "")
	c.rack = fs.String("rack", "", "")
	c.dataNode = fs.String("dataNode", "", "")
	c.readonly = fs.Bool("readonly", false, "")
	c.writable = fs.Bool("writable", false, "")
	c.volumeId = fs.Uint64("volumeId", 0, "")

	var dcBuf bytes.Buffer
	c.writeTopologyInfo(&dcBuf, topo, 30000, 1)
	assert.Equal(t, 1, strings.Count(dcBuf.String(), "  DataCenter dc1 hdd("))

	var rackBuf bytes.Buffer
	c.writeTopologyInfo(&rackBuf, topo, 30000, 2)
	assert.Equal(t, 1, strings.Count(rackBuf.String(), "    Rack rack1 hdd("))
}

// volNode builds a single-node topology from (volumeId, collection) pairs so the
// duplicate-detection tests can describe a cluster compactly.
func volNode(nodeId string, volumes ...*master_pb.VolumeInformationMessage) *master_pb.DataNodeInfo {
	return &master_pb.DataNodeInfo{
		Id: nodeId,
		DiskInfos: map[string]*master_pb.DiskInfo{
			"hdd": {Type: "hdd", VolumeInfos: volumes},
		},
	}
}

func topoFromNodes(nodes ...*master_pb.DataNodeInfo) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id:        "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: nodes}},
		}},
	}
}

// TestFindDuplicateVolumeIds covers the dangerous condition where the master
// reused a volume id across two collections (e.g. id 59788 in both infra-backup
// and ai-news-data-infrastructure) — the state that lets collection.delete
// destroy the wrong collection's data. The detector must flag exactly the shared
// ids, while same-id replicas of one volume must NOT be flagged.
func TestFindDuplicateVolumeIds(t *testing.T) {
	t.Run("flags id shared across collections", func(t *testing.T) {
		topo := topoFromNodes(
			volNode("srv0487:8082", &master_pb.VolumeInformationMessage{Id: 59788, Collection: "infra-backup"}),
			volNode("srv0502:8083", &master_pb.VolumeInformationMessage{Id: 59788, Collection: "ai-news-data-infrastructure"}),
			// A clean, single-collection volume that must not be flagged.
			volNode("srv0487:8085", &master_pb.VolumeInformationMessage{Id: 7069, Collection: "ai-news-data-infrastructure"}),
		)
		dup := findDuplicateVolumeIds(topo)
		assert.Equal(t, map[uint32][]string{
			59788: {"ai-news-data-infrastructure", "infra-backup"},
		}, dup)
	})

	t.Run("replicas of one volume are not duplicates", func(t *testing.T) {
		// Same id on three nodes, all the same collection: a normal replicated
		// volume, not a cross-collection clash.
		topo := topoFromNodes(
			volNode("n1", &master_pb.VolumeInformationMessage{Id: 42, Collection: "c"}),
			volNode("n2", &master_pb.VolumeInformationMessage{Id: 42, Collection: "c"}),
			volNode("n3", &master_pb.VolumeInformationMessage{Id: 42, Collection: "c"}),
		)
		assert.Empty(t, findDuplicateVolumeIds(topo))
	})

	t.Run("default empty collection counts as its own collection", func(t *testing.T) {
		topo := topoFromNodes(
			volNode("n1", &master_pb.VolumeInformationMessage{Id: 1, Collection: ""}),
			volNode("n2", &master_pb.VolumeInformationMessage{Id: 1, Collection: "x"}),
		)
		assert.Equal(t, map[uint32][]string{1: {"", "x"}}, findDuplicateVolumeIds(topo))
	})

	t.Run("normal volume and ec shard sharing an id across collections", func(t *testing.T) {
		topo := topoFromNodes(volNode("n1", &master_pb.VolumeInformationMessage{Id: 5, Collection: "a"}))
		topo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0].DiskInfos["hdd"].EcShardInfos =
			[]*master_pb.VolumeEcShardInformationMessage{{Id: 5, Collection: "b", EcIndexBits: 1}}
		assert.Equal(t, map[uint32][]string{5: {"a", "b"}}, findDuplicateVolumeIds(topo))
	})
}

func TestWriteDuplicateVolumeIdWarning(t *testing.T) {
	var buf bytes.Buffer
	writeDuplicateVolumeIdWarning(&buf, map[uint32][]string{
		59788: {"ai-news-data-infrastructure", "infra-backup"},
		1:     {"", "x"},
	})
	out := buf.String()
	assert.Contains(t, out, "WARNING: 2 volume id(s) exist in more than one collection")
	assert.Contains(t, out, "volume 59788 in collections: ai-news-data-infrastructure, infra-backup")
	// Empty collection name is rendered readably rather than as a blank.
	assert.Contains(t, out, `volume 1 in collections: "" (default), x`)
	// Lowest id first.
	assert.Less(t, strings.Index(out, "volume 1 "), strings.Index(out, "volume 59788 "))

	// Clean topology prints nothing.
	var empty bytes.Buffer
	writeDuplicateVolumeIdWarning(&empty, nil)
	assert.Empty(t, empty.String())
}
