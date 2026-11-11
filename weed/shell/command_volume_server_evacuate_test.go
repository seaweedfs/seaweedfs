package shell

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func TestEvacuatePrefersLowerVolumeRatio(t *testing.T) {
	volumes := mkByteVolumes(1, 1, 1)
	source := makeByteNode("source", 10, 0, 0, volumes)
	fuller := makeByteNode("fuller", 10, 0, 0, mkByteVolumes(10, 4, 1))
	emptier := makeByteNode("emptier", 10, 0, 0, mkByteVolumes(20, 2, 1))

	// Put the 40%-full server first: truncating 0.2 - 0.4 to int makes the ratios tie.
	moved, err := moveAwayOneNormalVolume(nil, nil, volumes[0], source, []*Node{fuller, emptier}, false)
	if err != nil || !moved {
		t.Fatalf("evacuate: moved=%v, err=%v", moved, err)
	}
	if _, found := emptier.selectedVolumes[volumes[0].Id]; !found {
		t.Fatal("expected evacuation to choose the server with lower occupancy")
	}
}

func TestVolumeServerEvacuate(t *testing.T) {
	targetServer := ""
	volumeRack := "rack1"
	c := commandVolumeServerEvacuate{
		targetServer: &targetServer,
		volumeRack:   &volumeRack,
	}
	c.topologyInfo = parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"
	if err := c.evacuateNormalVolumes(nil, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}

func newEvacuateCommandForTest(movesPerExec int) *commandVolumeServerEvacuate {
	targetServer := ""
	volumeRack := ""
	c := &commandVolumeServerEvacuate{
		targetServer:       &targetServer,
		volumeRack:         &volumeRack,
		maxParallelization: 1,
		movesPerExec:       movesPerExec,
	}
	c.topologyInfo = parseOutput(topoDataEc)
	return c
}

// ecShardCount counts the EC shards a volume server holds in the topology.
func ecShardCount(topo *master_pb.TopologyInfo, volumeServer string) (shards int) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				if dn.Id != volumeServer {
					continue
				}
				for _, disk := range dn.DiskInfos {
					for _, eci := range disk.EcShardInfos {
						shards += erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci).Count()
					}
				}
			}
		}
	}
	return shards
}

func TestVolumeServerEvacuateEcPlansEveryShard(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	volumeServer := "172.19.0.10:8702"
	shards := ecShardCount(c.topologyInfo, volumeServer)
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, volumeServer, false, false, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	planned := strings.Count(out.String(), "moving ec volume")
	if shards == 0 || planned != shards || c.movedCount != shards {
		t.Fatalf("%s holds %d ec shards, planned %d moves, counted %d", volumeServer, shards, planned, c.movedCount)
	}
	if left := ecShardCount(c.topologyInfo, volumeServer); left != 0 {
		t.Fatalf("the model still has %d ec shards on %s after the plan", left, volumeServer)
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if strings.HasPrefix(line, "moving ec volume") && strings.Fields(line)[6] == volumeServer {
			t.Fatalf("a shard is planned onto the evacuated server: %s", line)
		}
	}
}

func TestVolumeServerEvacuateEcMovesPerExec(t *testing.T) {
	all := newEvacuateCommandForTest(0)
	volumeServer := "172.19.0.10:8702"
	if err := all.evacuateEcVolumes(nil, volumeServer, false, false, io.Discard); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if all.movedCount < 3 {
		t.Fatalf("the test needs a server with at least 3 shards, it has %d", all.movedCount)
	}

	c := newEvacuateCommandForTest(2)
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, volumeServer, false, false, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if planned := strings.Count(out.String(), "moving ec volume"); planned != 2 || c.movedCount != 2 {
		t.Fatalf("expected 2 planned shard moves with -movesPerExec=2, got %d (counted %d)", planned, c.movedCount)
	}
	if !strings.Contains(out.String(), "reached -movesPerExec=2") {
		t.Fatalf("expected the limit to be reported, got:\n%s", out.String())
	}
}

// withEcShards gives an EC volume on a volume server the listed shards. The
// volume.list parser keeps only the first shard of a "shards:[5 11 12]" entry.
func withEcShards(topo *master_pb.TopologyInfo, volumeServer string, vid uint32, shardIds ...erasure_coding.ShardId) bool {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				if dn.Id != volumeServer {
					continue
				}
				for _, disk := range dn.DiskInfos {
					for _, eci := range disk.EcShardInfos {
						if eci.Id != vid {
							continue
						}
						si := erasure_coding.NewShardsInfo()
						for _, id := range shardIds {
							si.Set(erasure_coding.NewShardInfo(id, 0))
						}
						eci.EcIndexBits = si.Bitmap()
						eci.ShardSizes = si.SizesInt64()
						return true
					}
				}
			}
		}
	}
	return false
}

// Every limit, including the ones that end in the middle of a volume's shards,
// plans exactly that many moves.
func TestVolumeServerEvacuateEcMovesPerExecCutsInsideVolume(t *testing.T) {
	volumeServer := "172.19.0.20:8706"
	newCommand := func(movesPerExec int) *commandVolumeServerEvacuate {
		c := newEvacuateCommandForTest(movesPerExec)
		if !withEcShards(c.topologyInfo, volumeServer, 14322, 5, 11, 12) {
			t.Fatalf("ec volume 14322 is not on %s", volumeServer)
		}
		return c
	}
	shards := ecShardCount(newCommand(0).topologyInfo, volumeServer)
	if shards != 6 {
		t.Fatalf("expected 6 ec shards on %s, found %d", volumeServer, shards)
	}
	for limit := 1; limit <= shards; limit++ {
		c := newCommand(limit)
		var out bytes.Buffer
		if err := c.evacuateEcVolumes(nil, volumeServer, false, false, &out); err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if planned := strings.Count(out.String(), "moving ec volume"); planned != limit || c.movedCount != limit {
			t.Fatalf("limit %d: planned %d moves, counted %d", limit, planned, c.movedCount)
		}
	}
	c := newCommand(0)
	if err := c.evacuateEcVolumes(nil, volumeServer, false, false, io.Discard); err != nil {
		t.Fatalf("no limit: %v", err)
	}
	if c.movedCount != shards || ecShardCount(c.topologyInfo, volumeServer) != 0 {
		t.Fatalf("no limit: planned %d of %d shards", c.movedCount, shards)
	}
}
