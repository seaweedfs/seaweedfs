package shell

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestPrintClusterInfo(t *testing.T) {
	testCases := []struct {
		topology *master_pb.TopologyInfo
		humanize bool
		want     string
	}{
		{
			testTopology1, true,
			`cluster:
	id:       test_topo_1
	status:   unlocked
	nodes:    5
	topology: 5 DCs, 5 disks on 6 racks

`,
		},
		{
			testTopology1, false,
			`cluster:
	id:       test_topo_1
	status:   unlocked
	nodes:    5
	topology: 5 DC(s), 5 disk(s) on 6 rack(s)

`,
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		sp := &ClusterStatusPrinter{
			writer:   &buf,
			humanize: tc.humanize,
			topology: tc.topology,
		}
		sp.printClusterInfo()
		got := buf.String()

		if got != tc.want {
			t.Errorf("for %v: got %v, want %v", tc.topology.Id, got, tc.want)
		}
	}
}

func TestPrintVolumeInfo(t *testing.T) {
	testCases := []struct {
		topology *master_pb.TopologyInfo
		humanize bool
		want     string
	}{
		{
			testTopology2, true,
			`volumes:
	total:    12,056 volumes, 0 collections
	max size: 0 B
	regular:  5,302/25,063 volumes on 15,900 replicas, 15,900 writable (100%), 0 read-only (0%)
	EC:       6,754 EC volumes on 91,662 shards (13.57 shards/volume)

`,
		},
		{
			testTopology2, false,
			`volumes:
	total:    12056 volume(s), 0 collection(s)
	max size: 0 byte(s)
	regular:  5302/25063 volume(s) on 15900 replica(s), 15900 writable (100.00%), 0 read-only (0.00%)
	EC:       6754 EC volume(s) on 91662 shard(s) (13.57 shards/volume)

`,
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		sp := &ClusterStatusPrinter{
			writer:   &buf,
			humanize: tc.humanize,
			topology: tc.topology,
		}
		sp.printVolumeInfo()
		got := buf.String()

		if got != tc.want {
			t.Errorf("for %v: got %v, want %v", tc.topology.Id, got, tc.want)
		}
	}
}

func TestPrintStorageInfo(t *testing.T) {
	testCases := []struct {
		topology *master_pb.TopologyInfo
		humanize bool
		want     string
	}{
		{
			testTopology2, true,
			`storage:
	total:           5.9 TB
	regular volumes: 5.9 TB
	EC volumes:      0 B
	raw:             18 TB on volume replicas, 0 B on EC shards

`,
		},
		{
			testTopology2, false,
			`storage:
	total:           5892610895448 byte(s)
	regular volumes: 5892610895448 byte(s)
	EC volumes:      0 byte(s)
	raw:             17676186754616 byte(s) on volume replicas, 0 byte(s) on EC shards

`,
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		sp := &ClusterStatusPrinter{
			writer:   &buf,
			humanize: tc.humanize,
			topology: tc.topology,
		}
		sp.printStorageInfo()
		got := buf.String()

		if got != tc.want {
			t.Errorf("for %v: got %v, want %v", tc.topology.Id, got, tc.want)
		}
	}
}
