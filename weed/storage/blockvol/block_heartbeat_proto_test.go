package blockvol

import (
	"testing"
)

func TestInfoMessageRoundTrip(t *testing.T) {
	orig := BlockVolumeInfoMessage{
		Path:          "/data/vol1.blk",
		VolumeSize:    1 << 30,
		BlockSize:     4096,
		Epoch:         42,
		Role:          RoleToWire(RolePrimary),
		WalHeadLsn:    1000,
		CheckpointLsn: 900,
		HasLease:      true,
		DiskType:      "ssd",
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if back != orig {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", back, orig)
	}
}

func TestShortInfoRoundTrip(t *testing.T) {
	orig := BlockVolumeShortInfoMessage{
		Path:       "/data/vol2.blk",
		VolumeSize: 2 << 30,
		BlockSize:  4096,
		DiskType:   "hdd",
	}
	pb := ShortInfoToProto(orig)
	back := ShortInfoFromProto(pb)
	if back != orig {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", back, orig)
	}
}

func TestAssignmentRoundTrip(t *testing.T) {
	orig := BlockVolumeAssignment{
		Path:       "/data/vol3.blk",
		Epoch:      7,
		Role:       RoleToWire(RoleReplica),
		LeaseTtlMs: 5000,
	}
	pb := AssignmentToProto(orig)
	back := AssignmentFromProto(pb)
	if back != orig {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", back, orig)
	}
}

func TestInfoMessagesSliceRoundTrip(t *testing.T) {
	origSlice := []BlockVolumeInfoMessage{
		{Path: "/a.blk", VolumeSize: 100, Epoch: 1},
		{Path: "/b.blk", VolumeSize: 200, Epoch: 2, HasLease: true},
	}
	pbs := InfoMessagesToProto(origSlice)
	back := InfoMessagesFromProto(pbs)
	if len(back) != len(origSlice) {
		t.Fatalf("length mismatch: got %d, want %d", len(back), len(origSlice))
	}
	for i := range origSlice {
		if back[i] != origSlice[i] {
			t.Fatalf("index %d mismatch:\n got  %+v\n want %+v", i, back[i], origSlice[i])
		}
	}
}

func TestNilProtoConversions(t *testing.T) {
	// Nil proto -> zero-value Go types.
	info := InfoMessageFromProto(nil)
	if info != (BlockVolumeInfoMessage{}) {
		t.Fatalf("nil info proto should yield zero value, got %+v", info)
	}
	short := ShortInfoFromProto(nil)
	if short != (BlockVolumeShortInfoMessage{}) {
		t.Fatalf("nil short proto should yield zero value, got %+v", short)
	}
	assign := AssignmentFromProto(nil)
	if assign != (BlockVolumeAssignment{}) {
		t.Fatalf("nil assignment proto should yield zero value, got %+v", assign)
	}
}
