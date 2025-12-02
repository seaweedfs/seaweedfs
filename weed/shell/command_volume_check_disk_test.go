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
		// Edge case: Zero file and delete counts
		{
			name: "volumes with zero file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        0,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        0,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true,
		},
		{
			name: "volumes with zero and non-zero file counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        0,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false,
		},
		// Edge case: Recently modified volumes (after pulse time)
		// Note: VolumePulsePeriod is 10 seconds, so pulse cutoff is now - 20 seconds
		// When both volumes are recently modified, skip check to avoid false positives
		{
			name: "recently modified volumes with same file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583395}, // Modified 5 seconds ago
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583390}, // Modified 10 seconds ago
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts = skip
		},
		{
			name: "one volume modified before pulse cutoff with different file counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583370}, // Modified 30 seconds ago (before cutoff at -20s)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        999,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583370}, // Same modification time
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false, // Different counts + old enough = needs sync
		},
		// Edge case: Different ModifiedAtSecond values, same file counts
		{
			name: "different modification times with same file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300}, // 100 seconds before pulse time
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583350}, // 50 seconds before pulse time
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts, both before cutoff
		},
		// Edge case: Very close to pulse time boundary
		{
			name: "volumes modified exactly at pulse cutoff boundary with different counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1001,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583379}, // Just before cutoff (pulseTime - 21s)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583379}, // Just before cutoff
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false, // At boundary with different counts - needs sync
		},
		{
			name: "volumes modified just after pulse cutoff boundary with same counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583381}, // Just after cutoff (pulseTime - 19s)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583381}, // Just after cutoff
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts + recent = skip to avoid false positive
		},
		// Edge case: Large file count differences
		{
			name: "large file count difference with old modification time should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        10000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        5000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false, // Large difference requires sync
		},
		// Edge case: Both volumes modified AFTER pulse cutoff time
		// When ModifiedAtSecond >= pulseTimeAtSecond for both volumes with same counts,
		// the condition (a.info.FileCount != b.info.FileCount) is false, so we skip
		// without calling eqVolumeFileCount
		{
			name: "both volumes modified after pulse cutoff with same file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583405}, // After pulse cutoff (1696583380)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583410}, // After pulse cutoff
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts = skip without calling eqVolumeFileCount
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
			result, err := vcd.shouldSkipVolume(&tt.a, &tt.b)
			if err != nil {
				// In unit tests, we expect no errors from shouldSkipVolume
				// since we're using test data without actual network calls
				t.Errorf("shouldSkipVolume() returned unexpected error: %v", err)
				return
			}
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
	vcd.write("test %s", "message")
	if buf.String() != "test message\n" {
		t.Errorf("write() output = %q, want %q", buf.String(), "test message\n")
	}

	// Test writeVerbose with verbose=true
	buf.Reset()
	vcd.writeVerbose("verbose %d", 123)
	if buf.String() != "verbose 123\n" {
		t.Errorf("writeVerbose() with verbose=true output = %q, want %q", buf.String(), "verbose 123\n")
	}

	// Test writeVerbose with verbose=false
	buf.Reset()
	vcd.verbose = false
	vcd.writeVerbose("should not appear")
	if buf.String() != "" {
		t.Errorf("writeVerbose() with verbose=false output = %q, want empty", buf.String())
	}
}
