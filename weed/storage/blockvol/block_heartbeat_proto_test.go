package blockvol

import (
	"reflect"
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
	if !reflect.DeepEqual(back, orig) {
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
	if !reflect.DeepEqual(back, orig) {
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
	if !reflect.DeepEqual(back, orig) {
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
		if !reflect.DeepEqual(back[i], origSlice[i]) {
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
	if !reflect.DeepEqual(back, orig) {
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
	if !reflect.DeepEqual(back, orig) {
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
	if !reflect.DeepEqual(info, BlockVolumeInfoMessage{}) {
		t.Fatalf("nil info proto should yield zero value, got %+v", info)
	}
	short := ShortInfoFromProto(nil)
	if short != (BlockVolumeShortInfoMessage{}) {
		t.Fatalf("nil short proto should yield zero value, got %+v", short)
	}
	assign := AssignmentFromProto(nil)
	if !reflect.DeepEqual(assign, BlockVolumeAssignment{}) {
		t.Fatalf("nil assignment proto should yield zero value, got %+v", assign)
	}
}

// --- CP8-2 new tests ---

func TestInfoMessage_HealthScoreRoundTrip(t *testing.T) {
	orig := BlockVolumeInfoMessage{
		Path:          "/data/vol.blk",
		VolumeSize:    1 << 30,
		BlockSize:     4096,
		Epoch:         5,
		HealthScore:   0.85,
		ScrubErrors:   3,
		LastScrubTime: 1709000000,
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if back.HealthScore != 0.85 {
		t.Fatalf("HealthScore: got %f, want 0.85", back.HealthScore)
	}
	if back.ScrubErrors != 3 {
		t.Fatalf("ScrubErrors: got %d, want 3", back.ScrubErrors)
	}
	if back.LastScrubTime != 1709000000 {
		t.Fatalf("LastScrubTime: got %d, want 1709000000", back.LastScrubTime)
	}
}

func TestInfoMessage_ReplicaDegradedRoundTrip(t *testing.T) {
	orig := BlockVolumeInfoMessage{
		Path:            "/data/vol.blk",
		Epoch:           1,
		ReplicaDegraded: true,
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if !back.ReplicaDegraded {
		t.Fatal("ReplicaDegraded should be true after round-trip")
	}

	// Also verify false round-trips.
	orig.ReplicaDegraded = false
	pb = InfoMessageToProto(orig)
	back = InfoMessageFromProto(pb)
	if back.ReplicaDegraded {
		t.Fatal("ReplicaDegraded should be false after round-trip")
	}
}

func TestInfoMessage_ReplicaReadyRoundTrip(t *testing.T) {
	orig := BlockVolumeInfoMessage{
		Path:         "/data/vol.blk",
		Epoch:        1,
		ReplicaReady: true,
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if !back.ReplicaReady {
		t.Fatal("ReplicaReady should be true after round-trip")
	}

	orig.ReplicaReady = false
	pb = InfoMessageToProto(orig)
	back = InfoMessageFromProto(pb)
	if back.ReplicaReady {
		t.Fatal("ReplicaReady should be false after round-trip")
	}
}

func TestAssignment_MultiReplicaRoundTrip(t *testing.T) {
	orig := BlockVolumeAssignment{
		Path:       "/data/vol.blk",
		Epoch:      10,
		Role:       RoleToWire(RolePrimary),
		LeaseTtlMs: 30000,
		ReplicaAddrs: []ReplicaAddr{
			{DataAddr: "10.0.0.2:14260", CtrlAddr: "10.0.0.2:14261"},
			{DataAddr: "10.0.0.3:14260", CtrlAddr: "10.0.0.3:14261"},
		},
	}
	pb := AssignmentToProto(orig)
	back := AssignmentFromProto(pb)

	if len(back.ReplicaAddrs) != 2 {
		t.Fatalf("ReplicaAddrs len: got %d, want 2", len(back.ReplicaAddrs))
	}
	if back.ReplicaAddrs[0].DataAddr != "10.0.0.2:14260" {
		t.Fatalf("ReplicaAddrs[0].DataAddr: got %q", back.ReplicaAddrs[0].DataAddr)
	}
	if back.ReplicaAddrs[1].CtrlAddr != "10.0.0.3:14261" {
		t.Fatalf("ReplicaAddrs[1].CtrlAddr: got %q", back.ReplicaAddrs[1].CtrlAddr)
	}
	// Scalar fields should be empty when ReplicaAddrs is populated.
	if back.ReplicaDataAddr != "" || back.ReplicaCtrlAddr != "" {
		t.Fatalf("scalar addrs should be empty when ReplicaAddrs set, got data=%q ctrl=%q",
			back.ReplicaDataAddr, back.ReplicaCtrlAddr)
	}
}

func TestAssignment_PrecedenceRule(t *testing.T) {
	// When proto has BOTH ReplicaAddrs AND scalar fields,
	// ReplicaAddrs takes precedence and scalar fields are ignored.
	orig := BlockVolumeAssignment{
		Path:            "/data/vol.blk",
		Epoch:           5,
		Role:            RoleToWire(RolePrimary),
		ReplicaDataAddr: "scalar-data:1234",
		ReplicaCtrlAddr: "scalar-ctrl:1235",
		ReplicaAddrs: []ReplicaAddr{
			{DataAddr: "multi-data:4260", CtrlAddr: "multi-ctrl:4261"},
		},
	}
	pb := AssignmentToProto(orig)
	// Proto will have both scalar and repeated fields set.
	if pb.ReplicaDataAddr != "scalar-data:1234" {
		t.Fatalf("proto should preserve scalar, got %q", pb.ReplicaDataAddr)
	}
	if len(pb.ReplicaAddrs) != 1 {
		t.Fatalf("proto should have 1 ReplicaAddr, got %d", len(pb.ReplicaAddrs))
	}

	// FromProto: ReplicaAddrs non-empty → use it, ignore scalar.
	back := AssignmentFromProto(pb)
	if len(back.ReplicaAddrs) != 1 {
		t.Fatalf("back.ReplicaAddrs len: got %d, want 1", len(back.ReplicaAddrs))
	}
	if back.ReplicaDataAddr != "" {
		t.Fatalf("scalar ReplicaDataAddr should be empty due to precedence, got %q", back.ReplicaDataAddr)
	}
	if back.ReplicaCtrlAddr != "" {
		t.Fatalf("scalar ReplicaCtrlAddr should be empty due to precedence, got %q", back.ReplicaCtrlAddr)
	}
}

func TestAssignment_BackwardCompatScalar(t *testing.T) {
	// When proto has only scalar fields (no ReplicaAddrs), backward compat path.
	orig := BlockVolumeAssignment{
		Path:            "/data/vol.blk",
		Epoch:           3,
		Role:            RoleToWire(RolePrimary),
		LeaseTtlMs:      5000,
		ReplicaDataAddr: "host:4260",
		ReplicaCtrlAddr: "host:4261",
	}
	pb := AssignmentToProto(orig)
	back := AssignmentFromProto(pb)

	if back.ReplicaDataAddr != "host:4260" {
		t.Fatalf("scalar ReplicaDataAddr: got %q, want %q", back.ReplicaDataAddr, "host:4260")
	}
	if back.ReplicaCtrlAddr != "host:4261" {
		t.Fatalf("scalar ReplicaCtrlAddr: got %q, want %q", back.ReplicaCtrlAddr, "host:4261")
	}
	if len(back.ReplicaAddrs) != 0 {
		t.Fatalf("ReplicaAddrs should be empty for backward compat, got %d", len(back.ReplicaAddrs))
	}
}

func TestAssignmentsSlice_MultiReplicaRoundTrip(t *testing.T) {
	as := []BlockVolumeAssignment{
		{
			Path:  "/a.blk",
			Epoch: 1,
			ReplicaAddrs: []ReplicaAddr{
				{DataAddr: "h1:1", CtrlAddr: "h1:2"},
				{DataAddr: "h2:1", CtrlAddr: "h2:2"},
			},
		},
		{
			Path:            "/b.blk",
			Epoch:           2,
			ReplicaDataAddr: "scalar:1",
		},
	}
	pbs := AssignmentsToProto(as)
	back := AssignmentsFromProto(pbs)
	if len(back) != 2 {
		t.Fatalf("len: got %d, want 2", len(back))
	}
	// First: multi-replica path.
	if len(back[0].ReplicaAddrs) != 2 {
		t.Fatalf("back[0].ReplicaAddrs len: got %d, want 2", len(back[0].ReplicaAddrs))
	}
	// Second: scalar path.
	if back[1].ReplicaDataAddr != "scalar:1" {
		t.Fatalf("back[1].ReplicaDataAddr: got %q", back[1].ReplicaDataAddr)
	}
	if len(back[1].ReplicaAddrs) != 0 {
		t.Fatalf("back[1].ReplicaAddrs should be empty, got %d", len(back[1].ReplicaAddrs))
	}
}

func TestInfoMessage_DurabilityModeRoundTrip(t *testing.T) {
	for _, mode := range []string{"best_effort", "sync_all", "sync_quorum"} {
		t.Run(mode, func(t *testing.T) {
			orig := BlockVolumeInfoMessage{
				Path:           "/data/dur.blk",
				Epoch:          1,
				DurabilityMode: mode,
			}
			pb := InfoMessageToProto(orig)
			if pb.DurabilityMode != mode {
				t.Fatalf("ToProto: expected %q, got %q", mode, pb.DurabilityMode)
			}
			back := InfoMessageFromProto(pb)
			if back.DurabilityMode != mode {
				t.Fatalf("FromProto: expected %q, got %q", mode, back.DurabilityMode)
			}
		})
	}
}

func TestInfoMessage_DurabilityModeEmpty_BackwardCompat(t *testing.T) {
	// Empty string should round-trip as empty (V1 compat).
	orig := BlockVolumeInfoMessage{
		Path:  "/data/old.blk",
		Epoch: 1,
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if back.DurabilityMode != "" {
		t.Fatalf("empty mode should round-trip as empty, got %q", back.DurabilityMode)
	}
}

func TestInfoMessage_NvmeFieldsRoundTrip(t *testing.T) {
	orig := BlockVolumeInfoMessage{
		Path:     "/data/nvme.blk",
		Epoch:    1,
		NvmeAddr: "10.0.0.1:4420",
		NQN:      "nqn.2024-01.com.seaweedfs:vol.test-vol",
	}
	pb := InfoMessageToProto(orig)
	if pb.NvmeAddr != "10.0.0.1:4420" {
		t.Fatalf("ToProto NvmeAddr: got %q", pb.NvmeAddr)
	}
	if pb.Nqn != "nqn.2024-01.com.seaweedfs:vol.test-vol" {
		t.Fatalf("ToProto Nqn: got %q", pb.Nqn)
	}
	back := InfoMessageFromProto(pb)
	if back.NvmeAddr != orig.NvmeAddr {
		t.Fatalf("FromProto NvmeAddr: got %q, want %q", back.NvmeAddr, orig.NvmeAddr)
	}
	if back.NQN != orig.NQN {
		t.Fatalf("FromProto NQN: got %q, want %q", back.NQN, orig.NQN)
	}
}

func TestInfoMessage_NvmeFieldsEmpty_BackwardCompat(t *testing.T) {
	// Empty NVMe fields (NVMe disabled) should round-trip as empty.
	orig := BlockVolumeInfoMessage{
		Path:  "/data/iscsi-only.blk",
		Epoch: 1,
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if back.NvmeAddr != "" {
		t.Fatalf("NvmeAddr should be empty, got %q", back.NvmeAddr)
	}
	if back.NQN != "" {
		t.Fatalf("NQN should be empty, got %q", back.NQN)
	}
}

func TestInfoMessage_HealthFieldsZeroDefault(t *testing.T) {
	// Verify zero-valued health fields round-trip correctly (backward compat).
	orig := BlockVolumeInfoMessage{
		Path:  "/data/old.blk",
		Epoch: 1,
		// All CP8-2 fields at zero default.
	}
	pb := InfoMessageToProto(orig)
	back := InfoMessageFromProto(pb)
	if back.HealthScore != 0 {
		t.Fatalf("HealthScore zero default: got %f", back.HealthScore)
	}
	if back.ScrubErrors != 0 {
		t.Fatalf("ScrubErrors zero default: got %d", back.ScrubErrors)
	}
	if back.LastScrubTime != 0 {
		t.Fatalf("LastScrubTime zero default: got %d", back.LastScrubTime)
	}
	if back.ReplicaDegraded {
		t.Fatal("ReplicaDegraded zero default should be false")
	}
}

// --- Phase 10 P1: Stable server identity on the proto wire ---

func TestP10P1_ProtoRoundTrip_ScalarServerID(t *testing.T) {
	a := BlockVolumeAssignment{
		Path:            "/data/vol1.blk",
		Epoch:           5,
		Role:            RoleToWire(RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
		ReplicaServerID: "vs2-grpc:18080",
		RebuildAddr:     "10.0.0.1:5000",
	}

	pb := AssignmentToProto(a)
	if pb.ReplicaServerId != "vs2-grpc:18080" {
		t.Fatalf("proto encode: ReplicaServerId=%q, want vs2-grpc:18080", pb.ReplicaServerId)
	}

	decoded := AssignmentFromProto(pb)
	if decoded.ReplicaServerID != "vs2-grpc:18080" {
		t.Fatalf("proto decode: ReplicaServerID=%q, want vs2-grpc:18080", decoded.ReplicaServerID)
	}
}

func TestP10P1_ProtoRoundTrip_MultiReplicaServerID(t *testing.T) {
	a := BlockVolumeAssignment{
		Path:  "/data/vol1.blk",
		Epoch: 5,
		Role:  RoleToWire(RolePrimary),
		ReplicaAddrs: []ReplicaAddr{
			{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", ServerID: "vs2-grpc:18080"},
			{DataAddr: "10.0.0.3:9333", CtrlAddr: "10.0.0.3:9334", ServerID: "vs3-grpc:18080"},
		},
	}

	pb := AssignmentToProto(a)
	if pb.ReplicaAddrs[0].ServerId != "vs2-grpc:18080" {
		t.Fatalf("encode[0]: ServerId=%q", pb.ReplicaAddrs[0].ServerId)
	}
	if pb.ReplicaAddrs[1].ServerId != "vs3-grpc:18080" {
		t.Fatalf("encode[1]: ServerId=%q", pb.ReplicaAddrs[1].ServerId)
	}

	decoded := AssignmentFromProto(pb)
	if decoded.ReplicaAddrs[0].ServerID != "vs2-grpc:18080" {
		t.Fatalf("decode[0]: ServerID=%q", decoded.ReplicaAddrs[0].ServerID)
	}
	if decoded.ReplicaAddrs[1].ServerID != "vs3-grpc:18080" {
		t.Fatalf("decode[1]: ServerID=%q", decoded.ReplicaAddrs[1].ServerID)
	}
}

func TestP10P1_ProtoRoundTrip_MissingServerID_NotSynthesized(t *testing.T) {
	a := BlockVolumeAssignment{
		Path:            "/data/vol1.blk",
		Epoch:           5,
		Role:            RoleToWire(RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
		// ReplicaServerID intentionally empty.
	}

	pb := AssignmentToProto(a)
	if pb.ReplicaServerId != "" {
		t.Fatalf("encode: ReplicaServerId=%q, want empty", pb.ReplicaServerId)
	}

	decoded := AssignmentFromProto(pb)
	if decoded.ReplicaServerID != "" {
		t.Fatalf("decode: ReplicaServerID=%q, want empty (not synthesized)", decoded.ReplicaServerID)
	}
}
