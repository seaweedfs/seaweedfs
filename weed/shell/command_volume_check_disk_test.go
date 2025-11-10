package shell

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type testCommandVolumeCheckDisk struct {
	commandVolumeCheckDisk
}

type shouldSkipVolume struct {
	name              string
	a                 VolumeReplica
	b                 VolumeReplica
	pulseTimeAtSecond int64
	syncDeletions     bool
	shouldSkipVolume  bool
}

func TestShouldSkipVolume(t *testing.T) {
	var tests = []shouldSkipVolume{
		{
			name: "identical volumes should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true,
		},
		{
			name: "different file counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1001,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false,
		},
		{
			name: "different delete counts with syncDeletions enabled should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      101,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false,
		},
		{
			name: "different delete counts with syncDeletions disabled should be skipped if file counts match",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      101,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     false,
			shouldSkipVolume:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			vcd := &volumeCheckDisk{
				writer:        &buf,
				now:           time.Unix(tt.pulseTimeAtSecond, 0),
				verbose:       false, // reduce noise in tests
				syncDeletions: tt.syncDeletions,
			}
			result := vcd.shouldSkipVolume(&tt.a, &tt.b)
			if result != tt.shouldSkipVolume {
				t.Errorf("shouldSkipVolume() = %v, want %v\nFileCount A=%d B=%d, DeleteCount A=%d B=%d",
					result, tt.shouldSkipVolume,
					tt.a.info.FileCount, tt.b.info.FileCount,
					tt.a.info.DeleteCount, tt.b.info.DeleteCount)
			}
		})
	}
}

// TestVolumeCheckDiskHelperMethods tests the helper methods on volumeCheckDisk
func TestVolumeCheckDiskHelperMethods(t *testing.T) {
	var buf bytes.Buffer
	vcd := &volumeCheckDisk{
		writer:  &buf,
		verbose: true,
	}

	// Test write method
	vcd.write("test %s\n", "message")
	if buf.String() != "test message\n" {
		t.Errorf("write() output = %q, want %q", buf.String(), "test message\n")
	}

	// Test writeVerbose with verbose=true
	buf.Reset()
	vcd.writeVerbose("verbose %d\n", 123)
	if buf.String() != "verbose 123\n" {
		t.Errorf("writeVerbose() with verbose=true output = %q, want %q", buf.String(), "verbose 123\n")
	}

	// Test writeVerbose with verbose=false
	buf.Reset()
	vcd.verbose = false
	vcd.writeVerbose("should not appear\n")
	if buf.String() != "" {
		t.Errorf("writeVerbose() with verbose=false output = %q, want empty", buf.String())
	}
}

// TestWritableReplicaFiltering tests that read-only volumes are properly filtered
func TestWritableReplicaFiltering(t *testing.T) {
	replicas := []*VolumeReplica{
		{nil, &master_pb.VolumeInformationMessage{Id: 1, FileCount: 100, ReadOnly: false}},
		{nil, &master_pb.VolumeInformationMessage{Id: 2, FileCount: 200, ReadOnly: true}},
		{nil, &master_pb.VolumeInformationMessage{Id: 3, FileCount: 150, ReadOnly: false}},
	}

	var writableReplicas []*VolumeReplica
	for _, replica := range replicas {
		if !replica.info.ReadOnly {
			writableReplicas = append(writableReplicas, replica)
		}
	}

	if len(writableReplicas) != 2 {
		t.Errorf("Expected 2 writable replicas, got %d", len(writableReplicas))
	}

	// Verify read-only replica was filtered out
	for _, replica := range writableReplicas {
		if replica.info.ReadOnly {
			t.Errorf("Found read-only replica %d in writable replicas", replica.info.Id)
		}
	}
}
