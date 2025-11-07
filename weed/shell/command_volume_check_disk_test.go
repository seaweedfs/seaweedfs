package shell

import (
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type testCommandVolumeCheckDisk struct {
	commandVolumeCheckDisk
}

type shouldSkipVolume struct {
	a                 VolumeReplica
	b                 VolumeReplica
	pulseTimeAtSecond int64
	shouldSkipVolume  bool
}

func TestShouldSkipVolume(t *testing.T) {
	var tests = []shouldSkipVolume{
		{
			VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			1696583400,
			true,
		},
		{
			VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1001,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			1696583400,
			false,
		},
		{
			VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      101,
				ModifiedAtSecond: 1696583300},
			},
			1696583400,
			false,
		},
	}
	for num, tt := range tests {
		vcd := &volumeCheckDisk{
			writer:        os.Stdout,
			now:           time.Unix(tt.pulseTimeAtSecond, 0),
			verbose:       true,
			syncDeletions: true,
		}
		if isShould := vcd.shouldSkipVolume(&tt.a, &tt.b); isShould != tt.shouldSkipVolume {
			t.Fatalf("result of should skip volume is unexpected for %d test", num)
		}
	}
}
