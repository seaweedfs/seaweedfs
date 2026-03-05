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

func TestAssignmentRoundTripWithReplicaAddrs(t *testing.T) {
	orig := BlockVolumeAssignment{
		Path:            "/data/vol4.blk",
		Epoch:           10,
		Role:            RoleToWire(RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaDataAddr: "10.0.0.2:14260",
		ReplicaCtrlAddr: "10.0.0.2:14261",
		RebuildAddr:     "10.0.0.2:14262",
	}
	pb := AssignmentToProto(orig)
	back := AssignmentFromProto(pb)
	if back != orig {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", back, orig)
	}
}

func TestInfoMessageRoundTripWithReplicaAddrs(t *testing.T) {
	orig := BlockVolumeInfoMessage{
		Path:            "/data/vol5.blk",
		VolumeSize:      1 << 30,
		BlockSize:       4096,
		Epoch:           3,
		Role:            RoleToWire(RoleReplica),
		WalHeadLsn:      500,
		CheckpointLsn:   400,
		HasLease:        false,
		DiskType:        "ssd",
		ReplicaDataAddr: "10.0.0.3:14260",
		ReplicaCtrlAddr: "10.0.0.3:14261",
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if back != orig {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", back, orig)
	}
}

func TestAssignmentFromProtoNilFields(t *testing.T) {
	// Proto with no replica fields set -> empty strings in Go.
	pb := AssignmentToProto(BlockVolumeAssignment{
		Path:  "/data/vol6.blk",
		Epoch: 1,
		Role:  RoleToWire(RolePrimary),
	})
	back := AssignmentFromProto(pb)
	if back.ReplicaDataAddr != "" || back.ReplicaCtrlAddr != "" || back.RebuildAddr != "" {
		t.Fatalf("expected empty replica addrs, got data=%q ctrl=%q rebuild=%q",
			back.ReplicaDataAddr, back.ReplicaCtrlAddr, back.RebuildAddr)
	}
}

func TestInfoMessageFromProtoNilFields(t *testing.T) {
	pb := InfoMessageToProto(BlockVolumeInfoMessage{
		Path:  "/data/vol7.blk",
		Epoch: 1,
	})
	back := InfoMessageFromProto(pb)
	if back.ReplicaDataAddr != "" || back.ReplicaCtrlAddr != "" {
		t.Fatalf("expected empty replica addrs, got data=%q ctrl=%q",
			back.ReplicaDataAddr, back.ReplicaCtrlAddr)
	}
}

func TestLeaseTTLWithReplicaAddrs(t *testing.T) {
	orig := BlockVolumeAssignment{
		Path:            "/data/vol8.blk",
		Epoch:           5,
		Role:            RoleToWire(RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaDataAddr: "host:4260",
		ReplicaCtrlAddr: "host:4261",
	}
	pb := AssignmentToProto(orig)
	back := AssignmentFromProto(pb)
	if LeaseTTLFromWire(back.LeaseTtlMs).Milliseconds() != 30000 {
		t.Fatalf("lease TTL mismatch: got %v", LeaseTTLFromWire(back.LeaseTtlMs))
	}
	if back.ReplicaDataAddr != "host:4260" {
		t.Fatalf("ReplicaDataAddr mismatch: got %q", back.ReplicaDataAddr)
	}
}

func TestInfoMessage_ReplicaAddrsRoundTrip(t *testing.T) {
	// Verify slice round-trip preserves replica addrs.
	origSlice := []BlockVolumeInfoMessage{
		{Path: "/a.blk", ReplicaDataAddr: "h1:4260", ReplicaCtrlAddr: "h1:4261"},
		{Path: "/b.blk", ReplicaDataAddr: "", ReplicaCtrlAddr: ""},
	}
	pbs := InfoMessagesToProto(origSlice)
	back := InfoMessagesFromProto(pbs)
	if back[0].ReplicaDataAddr != "h1:4260" {
		t.Fatalf("slice[0] ReplicaDataAddr: got %q", back[0].ReplicaDataAddr)
	}
	if back[1].ReplicaDataAddr != "" {
		t.Fatalf("slice[1] ReplicaDataAddr should be empty, got %q", back[1].ReplicaDataAddr)
	}
}

func TestAssignmentsToProto(t *testing.T) {
	as := []BlockVolumeAssignment{
		{Path: "/a.blk", Epoch: 1, ReplicaDataAddr: "h:1"},
		{Path: "/b.blk", Epoch: 2, RebuildAddr: "h:2"},
	}
	pbs := AssignmentsToProto(as)
	if len(pbs) != 2 {
		t.Fatalf("len: got %d, want 2", len(pbs))
	}
	if pbs[0].ReplicaDataAddr != "h:1" {
		t.Fatalf("pbs[0].ReplicaDataAddr: got %q", pbs[0].ReplicaDataAddr)
	}
	if pbs[1].RebuildAddr != "h:2" {
		t.Fatalf("pbs[1].RebuildAddr: got %q", pbs[1].RebuildAddr)
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
