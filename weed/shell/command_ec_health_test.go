package shell

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestEcShardMapRegister tests that EC shards are properly registered
func TestEcShardHealth(t *testing.T) {
	// Fake topology. EC volume 1 is fully replicated, EC volume 2 is under-replicated, and EC volume 3 is over-replicated.
	nodes := []*master_pb.DataNodeInfo{
		&master_pb.DataNodeInfo{
			Address: "localhost:1111",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d1": &master_pb.DiskInfo{
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						&master_pb.VolumeEcShardInformationMessage{
							Id:          1,
							EcIndexBits: 0b0000000011111,
						},
						&master_pb.VolumeEcShardInformationMessage{
							Id:          2,
							EcIndexBits: 0b0000000011111,
						},
						&master_pb.VolumeEcShardInformationMessage{
							Id:          3,
							EcIndexBits: 0b11111111111111,
						},
					},
				},
			},
		},
		&master_pb.DataNodeInfo{
			Address: "localhost:2222",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d2": &master_pb.DiskInfo{
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						&master_pb.VolumeEcShardInformationMessage{
							Id:          1,
							EcIndexBits: 0b11111111100000,
						},
						&master_pb.VolumeEcShardInformationMessage{
							Id:          2,
							EcIndexBits: 0b11111000000000,
						},
						&master_pb.VolumeEcShardInformationMessage{
							Id:          3,
							EcIndexBits: 0b00000000001111,
						},
					},
				},
			},
		},
	}

	testCases := []struct {
		name        string
		showDetails bool
		volumeIDMap map[uint32]bool
		want        string
		wantErr     error
	}{
		{
			name:        "default",
			showDetails: false,
			want: `Found 1/3 under-replicated EC volumes: [2]
Found 1/3 over-replicated EC volumes: [3]
`,
		},
		{
			name:        "single healthy volume",
			showDetails: false,
			volumeIDMap: map[uint32]bool{1: true},
			want: `EC volumes are healthy.
`,
		},
		{
			name:        "filtered by volume ID",
			showDetails: false,
			volumeIDMap: map[uint32]bool{1: true, 2: true, 5: true},
			want: `Found 1/2 under-replicated EC volumes: [2]
`,
		},
		{
			name:        "no valid volume IDs",
			showDetails: false,
			volumeIDMap: map[uint32]bool{5: true, 6: true},
			want:        "",
			wantErr:     fmt.Errorf("no EC volumes found"),
		},
		{
			name:        "with details",
			showDetails: true,
			want: `Found 1/3 under-replicated EC volumes: [2]
Found 1/3 over-replicated EC volumes: [3]

Shards map for under-replicated EC volume 2 (10/14 shards):
	00 => [localhost:1111]
	01 => [localhost:1111]
	02 => [localhost:1111]
	03 => [localhost:1111]
	04 => [localhost:1111]
	05 is missing
	06 is missing
	07 is missing
	08 is missing
	09 => [localhost:2222]
	10 (parity) => [localhost:2222]
	11 (parity) => [localhost:2222]
	12 (parity) => [localhost:2222]
	13 (parity) => [localhost:2222]

Shards map for over-replicated EC volume 3 (18/14 shards):
	00 => [localhost:1111 localhost:2222]
	01 => [localhost:1111 localhost:2222]
	02 => [localhost:1111 localhost:2222]
	03 => [localhost:1111 localhost:2222]
	04 => [localhost:1111]
	05 => [localhost:1111]
	06 => [localhost:1111]
	07 => [localhost:1111]
	08 => [localhost:1111]
	09 => [localhost:1111]
	10 (parity) => [localhost:1111]
	11 (parity) => [localhost:1111]
	12 (parity) => [localhost:1111]
	13 (parity) => [localhost:1111]
`,
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer

		cmd := &commandEcVolumeHealth{
			writer:      &buf,
			dataNodes:   nodes,
			volumeIDMap: tc.volumeIDMap,
		}
		gotErr := cmd.checkEcVolumes(tc.showDetails)
		got := buf.String()

		if got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
		if !reflect.DeepEqual(gotErr, tc.wantErr) {
			t.Errorf("%s: got error %v, want %v", tc.name, gotErr, tc.wantErr)
		}
	}
}
