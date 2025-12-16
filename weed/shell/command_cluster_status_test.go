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

func TestPrintFilesInfo(t *testing.T) {
	testCases := []struct {
		regularVolumeStats RegularVolumeStats
		humanize           bool
		want               string
	}{
		{
			regularVolumeStats: RegularVolumeStats{
				1: []*VolumeReplicaStats{
					&VolumeReplicaStats{Id: "10.200.17.13:9001", VolumeId: 1, Files: 159, FilesDeleted: 8, TotalSize: 89762704},
					&VolumeReplicaStats{Id: "10.200.17.13:9002", VolumeId: 1, Files: 159, FilesDeleted: 8, TotalSize: 89762704},
					&VolumeReplicaStats{Id: "10.200.17.13:9008", VolumeId: 1, Files: 159, FilesDeleted: 8, TotalSize: 89762704},
				},
				2: []*VolumeReplicaStats{
					&VolumeReplicaStats{Id: "10.200.17.13:9003", VolumeId: 2, Files: 192, FilesDeleted: 21, TotalSize: 93788632},
					&VolumeReplicaStats{Id: "10.200.17.13:9004", VolumeId: 2, Files: 192, FilesDeleted: 21, TotalSize: 93788632},
					&VolumeReplicaStats{Id: "10.200.17.13:9005", VolumeId: 2, Files: 192, FilesDeleted: 21, TotalSize: 93788632},
				},
				3: []*VolumeReplicaStats{
					&VolumeReplicaStats{Id: "10.200.17.13:9001", VolumeId: 3, Files: 149, FilesDeleted: 0, TotalSize: 81643872},
					&VolumeReplicaStats{Id: "10.200.17.13:9006", VolumeId: 3, Files: 149, FilesDeleted: 0, TotalSize: 81643872},
					&VolumeReplicaStats{Id: "10.200.17.13:9009", VolumeId: 3, Files: 149, FilesDeleted: 0, TotalSize: 81643872},
				},
			},
			humanize: false,
			want: `files:
	regular:     500 file(s), 471 readable (94.20%), 29 deleted (5.80%), avg 530390 byte(s) per file
	regular raw: 1500 file(s), 1413 readable (94.20%), 87 deleted (5.80%), 795585624 byte(s) total
	EC:          [no data]
	EC raw:      [no data]

`,
		},
		{
			regularVolumeStats: RegularVolumeStats{
				1: []*VolumeReplicaStats{
					&VolumeReplicaStats{Id: "10.200.17.13:9001", VolumeId: 1, Files: 184, FilesDeleted: 33, TotalSize: 79187475},
					&VolumeReplicaStats{Id: "10.200.17.13:9008", VolumeId: 1, Files: 184, FilesDeleted: 33, TotalSize: 79187475},
				},
				2: []*VolumeReplicaStats{
					&VolumeReplicaStats{Id: "10.200.17.13:9004", VolumeId: 2, Files: 245, FilesDeleted: 4, TotalSize: 89501070},
					&VolumeReplicaStats{Id: "10.200.17.13:9005", VolumeId: 2, Files: 245, FilesDeleted: 4, TotalSize: 89501070},
				},
				3: []*VolumeReplicaStats{
					&VolumeReplicaStats{Id: "10.200.17.13:9006", VolumeId: 3, Files: 171, FilesDeleted: 12, TotalSize: 124049530},
					&VolumeReplicaStats{Id: "10.200.17.13:9009", VolumeId: 3, Files: 171, FilesDeleted: 12, TotalSize: 124049530},
				},
			},
			humanize: true,
			want: `files:
	regular:     600 files, 551 readable (91.83%), 49 deleted (8.16%), avg 488 kB per file
	regular raw: 1,200 files, 1,102 readable (91.83%), 98 deleted (8.16%), 586 MB total
	EC:          [no data]
	EC raw:      [no data]

`,
		},
	}

	for i, tc := range testCases {
		var buf bytes.Buffer
		sp := &ClusterStatusPrinter{
			writer:             &buf,
			humanize:           tc.humanize,
			regularVolumeStats: tc.regularVolumeStats,
		}
		sp.printFilesInfo()
		got := buf.String()

		if got != tc.want {
			t.Errorf("#%d: got %v, want %v", i, got, tc.want)
		}
	}
}
