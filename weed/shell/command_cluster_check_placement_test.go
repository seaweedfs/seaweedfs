package shell

import (
	"bytes"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func TestPrintDefaultPlacementWarnings(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{Id: "dc1", RackInfos: []*master_pb.RackInfo{
				{Id: "r1", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "10.0.0.1:8080"}}},
			}},
			{Id: topology.DefaultDataCenter, RackInfos: []*master_pb.RackInfo{
				{Id: topology.DefaultRack, DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "10.0.0.2:8080"}}},
			}},
		},
	}
	var buf bytes.Buffer
	printDefaultPlacementWarnings(&buf, topo)
	out := buf.String()
	if strings.Count(out, "warning:") != 1 || !strings.Contains(out, "10.0.0.2:8080") {
		t.Fatalf("unexpected output: %q", out)
	}
}
