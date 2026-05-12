package weed_server

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func TestHeartbeatOwnership_FirstClaimWins(t *testing.T) {
	o := newHeartbeatOwnership()

	if !o.claimVolume(1, "10.0.0.1", 8080, 1) {
		t.Fatalf("first claim must succeed")
	}
	if o.claimVolume(1, "10.6.6.6", 8080, 1) {
		t.Fatalf("attacker claim from a different peer must be rejected")
	}
	if !o.claimVolume(1, "10.0.0.1", 8080, 1) {
		t.Fatalf("re-claim from the original peer must succeed")
	}
}

func TestHeartbeatOwnership_RespectsReplicaCount(t *testing.T) {
	o := newHeartbeatOwnership()

	// 3 legitimate replicas
	if !o.claimVolume(2, "10.0.0.1", 8080, 3) {
		t.Fatal()
	}
	if !o.claimVolume(2, "10.0.0.2", 8080, 3) {
		t.Fatal()
	}
	if !o.claimVolume(2, "10.0.0.3", 8080, 3) {
		t.Fatal()
	}
	// 4th distinct peer is over count
	if o.claimVolume(2, "10.6.6.6", 8080, 3) {
		t.Fatalf("over-count claim must be rejected")
	}
}

func TestHeartbeatOwnership_Release(t *testing.T) {
	o := newHeartbeatOwnership()

	if !o.claimVolume(3, "10.0.0.1", 8080, 1) {
		t.Fatal()
	}
	o.release("10.0.0.1", 8080)
	if !o.claimVolume(3, "10.0.0.2", 8080, 1) {
		t.Fatalf("after release, another peer should be able to take the slot")
	}
}

// TestHeartbeatOwnership_EcShardsPerSlot verifies the core EC correctness
// property: each (vid, shard_id) pair has exactly one owner, but the same
// vid may legitimately be split across multiple peers when they claim
// disjoint shard bitmaps. The previous vid-only model rejected those.
func TestHeartbeatOwnership_EcShardsPerSlot(t *testing.T) {
	o := newHeartbeatOwnership()

	// Peer A claims shards 0,1,2 of vid 7.
	if got := o.claimEcShards(7, 0b0111, "10.0.0.1", 8080); got != 0b0111 {
		t.Fatalf("peer A should own shards 0-2, got 0x%x", got)
	}
	// Re-claim from the owning peer is fine.
	if got := o.claimEcShards(7, 0b0111, "10.0.0.1", 8080); got != 0b0111 {
		t.Fatalf("re-claim from owner should succeed, got 0x%x", got)
	}
	// Peer B claims shards 3,4 of the same vid: must succeed because the
	// slots are disjoint.
	if got := o.claimEcShards(7, 0b11000, "10.0.0.2", 8080); got != 0b11000 {
		t.Fatalf("peer B should own shards 3-4, got 0x%x", got)
	}
	// Peer C tries to hijack shard 0 (owned by A) and claim shard 5: only
	// the unowned bit comes through.
	if got := o.claimEcShards(7, 0b100001, "10.6.6.6", 8080); got != 0b100000 {
		t.Fatalf("hijack of shard 0 must be rejected; shard 5 must be granted; got 0x%x", got)
	}
}

// TestHeartbeatOwnership_EcShardReleaseIsolatesPeers asserts that
// disconnecting peer A frees only A's bindings, leaving B's intact.
func TestHeartbeatOwnership_EcShardReleaseIsolatesPeers(t *testing.T) {
	o := newHeartbeatOwnership()

	o.claimEcShards(9, 0b0111, "10.0.0.1", 8080)
	o.claimEcShards(9, 0b11000, "10.0.0.2", 8080)

	o.release("10.0.0.1", 8080)

	// B's shards still locked.
	if got := o.claimEcShards(9, 0b11000, "10.6.6.6", 8080); got != 0 {
		t.Fatalf("B's bindings must survive A's release, got 0x%x", got)
	}
	// A's freed shards may be reclaimed by anyone.
	if got := o.claimEcShards(9, 0b0111, "10.6.6.6", 8080); got != 0b0111 {
		t.Fatalf("A's freed shards should be reclaimable, got 0x%x", got)
	}
}

// TestHeartbeatOwnership_ReleaseScopedToOwner checks that volume bindings
// of peer B are not collateral damage when peer A disconnects.
func TestHeartbeatOwnership_ReleaseScopedToOwner(t *testing.T) {
	o := newHeartbeatOwnership()

	o.claimVolume(10, "10.0.0.1", 8080, 2)
	o.claimVolume(10, "10.0.0.2", 8080, 2)

	o.release("10.0.0.1", 8080)

	// Slot vacated by A is reclaimable by a third peer.
	if !o.claimVolume(10, "10.0.0.3", 8080, 2) {
		t.Fatalf("A's slot should be reclaimable after release")
	}
	// B still owns its slot; a fourth peer must not push past the cap.
	if o.claimVolume(10, "10.6.6.6", 8080, 2) {
		t.Fatalf("over-count claim must be rejected; B's slot was not freed")
	}
}

type fakeAddr struct {
	network string
	s       string
}

func (f fakeAddr) Network() string { return f.network }
func (f fakeAddr) String() string  { return f.s }

func tcpPeerContext(ip string, port int) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: port},
	})
}

func TestAuthorizeHeartbeatPeer_AcceptsMatchingIp(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{}}
	ctx := tcpPeerContext("10.0.0.1", 55555)

	if err := ms.authorizeHeartbeatPeer(ctx, &master_pb.Heartbeat{Ip: "10.0.0.1", Port: 8080}); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
}

func TestAuthorizeHeartbeatPeer_RejectsMismatch(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{}}
	ctx := tcpPeerContext("10.0.0.1", 55555)

	if err := ms.authorizeHeartbeatPeer(ctx, &master_pb.Heartbeat{Ip: "10.6.6.6", Port: 8080}); err == nil {
		t.Fatalf("attacker should be rejected when source ip does not match claim")
	}
}

func TestAuthorizeHeartbeatPeer_LoopbackAccepted(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{}}
	ctx := tcpPeerContext("127.0.0.1", 55555)

	// A loopback peer is allowed to advertise its external hostname.
	if err := ms.authorizeHeartbeatPeer(ctx, &master_pb.Heartbeat{Ip: "host.example", Port: 8080}); err != nil {
		t.Fatalf("loopback peer should be trusted: %v", err)
	}
}

// TestAuthorizeHeartbeatPeer_InProcessAccepted covers the `weed server` case
// where master + volume run in the same OS process and the volume server
// reaches the master through an in-process / bufconn gRPC connection. Those
// peers surface with a non-TCP net.Addr (e.g. "@") and cannot be spoofed by
// a remote attacker, so they must be trusted regardless of heartbeat.Ip.
func TestAuthorizeHeartbeatPeer_InProcessAccepted(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: fakeAddr{network: "bufconn", s: "@"},
	})

	if err := ms.authorizeHeartbeatPeer(ctx, &master_pb.Heartbeat{Ip: "host.example", Port: 8080}); err != nil {
		t.Fatalf("in-process peer should be trusted: %v", err)
	}
}

// TestFilterVolumeOwnership_RejectsSecondPeerOnSameVolumeId verifies the
// end-to-end behaviour for non-EC: with replica count 1 there is exactly one
// legal owner of a vid, and a second peer's claim is stripped before the
// topology sees it.
func TestFilterVolumeOwnership_RejectsSecondPeerOnSameVolumeId(t *testing.T) {
	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")

	ms := &MasterServer{
		option:             &MasterOption{},
		Topo:               topo,
		heartbeatOwnership: newHeartbeatOwnership(),
	}

	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnB := rack.GetOrCreateDataNode("10.6.6.6", 8080, 8081, "10.6.6.6", "B", map[string]uint32{"": 10})

	hbA := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		Volumes: []*master_pb.VolumeInformationMessage{
			{Id: 1, ReplicaPlacement: 0},
		},
	}
	ms.filterVolumeOwnership(dnA, hbA)
	if len(hbA.Volumes) != 1 {
		t.Fatalf("peer A should retain its claim, got %d volumes", len(hbA.Volumes))
	}

	hbB := &master_pb.Heartbeat{
		Ip:   "10.6.6.6",
		Port: 8080,
		Volumes: []*master_pb.VolumeInformationMessage{
			{Id: 1, ReplicaPlacement: 0},
		},
	}
	ms.filterVolumeOwnership(dnB, hbB)
	if len(hbB.Volumes) != 0 {
		t.Fatalf("peer B's hijack claim should be stripped, got %d volumes", len(hbB.Volumes))
	}
}

// TestFilterVolumeOwnership_ReleaseLetsNextPeerTakeOver covers the
// disconnect-and-replace flow: when peer A's stream ends, ownership is freed,
// and a different peer may then claim the same vid (e.g. after planned
// migration).
func TestFilterVolumeOwnership_ReleaseLetsNextPeerTakeOver(t *testing.T) {
	ms := &MasterServer{
		option:             &MasterOption{},
		heartbeatOwnership: newHeartbeatOwnership(),
	}

	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")
	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnB := rack.GetOrCreateDataNode("10.0.0.2", 8080, 8081, "10.0.0.2", "B", map[string]uint32{"": 10})

	hb := func(ip string) *master_pb.Heartbeat {
		return &master_pb.Heartbeat{Ip: ip, Port: 8080, Volumes: []*master_pb.VolumeInformationMessage{{Id: 5}}}
	}

	a := hb("10.0.0.1")
	ms.filterVolumeOwnership(dnA, a)
	if len(a.Volumes) != 1 {
		t.Fatalf("peer A should own the volume")
	}

	b := hb("10.0.0.2")
	ms.filterVolumeOwnership(dnB, b)
	if len(b.Volumes) != 0 {
		t.Fatalf("peer B must not hijack while peer A still owns the id")
	}

	ms.heartbeatOwnership.release(dnA.Ip, uint32(dnA.Port))

	c := hb("10.0.0.2")
	ms.filterVolumeOwnership(dnB, c)
	if len(c.Volumes) != 1 {
		t.Fatalf("after release, peer B should be able to take over")
	}
}

// TestFilterVolumeOwnership_EcShardsSplitAcrossPeers exercises the realistic
// EC topology where shards of the same vid live on different volume servers.
// Both peers must keep their disjoint shard bitmaps after filtering.
func TestFilterVolumeOwnership_EcShardsSplitAcrossPeers(t *testing.T) {
	ms := &MasterServer{
		option:             &MasterOption{},
		heartbeatOwnership: newHeartbeatOwnership(),
	}
	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")
	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnB := rack.GetOrCreateDataNode("10.0.0.2", 8080, 8081, "10.0.0.2", "B", map[string]uint32{"": 10})

	hbA := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		EcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 100, EcIndexBits: 0x7F, ShardSizes: []int64{1, 2, 3, 4, 5, 6, 7}}, // shards 0..6
		},
	}
	ms.filterVolumeOwnership(dnA, hbA)
	if len(hbA.EcShards) != 1 || hbA.EcShards[0].EcIndexBits != 0x7F {
		t.Fatalf("peer A should keep shards 0-6, got %+v", hbA.EcShards)
	}
	if len(hbA.EcShards[0].ShardSizes) != 7 {
		t.Fatalf("shard sizes for peer A should be preserved, got %v", hbA.EcShards[0].ShardSizes)
	}

	hbB := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		EcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 100, EcIndexBits: 0x3F80, ShardSizes: []int64{8, 9, 10, 11, 12, 13, 14}}, // shards 7..13
		},
	}
	ms.filterVolumeOwnership(dnB, hbB)
	if len(hbB.EcShards) != 1 || hbB.EcShards[0].EcIndexBits != 0x3F80 {
		t.Fatalf("peer B should keep shards 7-13, got %+v", hbB.EcShards)
	}
	if len(hbB.EcShards[0].ShardSizes) != 7 {
		t.Fatalf("shard sizes for peer B should be preserved, got %v", hbB.EcShards[0].ShardSizes)
	}
}

// TestFilterVolumeOwnership_EcShardHijackStripped checks the partial-overlap
// case: the granted bits are kept (with their sizes), the rejected bits are
// stripped, and ShardSizes is compacted in lock-step with EcIndexBits.
func TestFilterVolumeOwnership_EcShardHijackStripped(t *testing.T) {
	ms := &MasterServer{
		option:             &MasterOption{},
		heartbeatOwnership: newHeartbeatOwnership(),
	}
	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")
	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnAttacker := rack.GetOrCreateDataNode("10.6.6.6", 8080, 8081, "10.6.6.6", "X", map[string]uint32{"": 10})

	hbA := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		EcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 200, EcIndexBits: 0b0111, ShardSizes: []int64{10, 20, 30}}, // shards 0,1,2
		},
	}
	ms.filterVolumeOwnership(dnA, hbA)

	hbAttacker := &master_pb.Heartbeat{
		Ip:   "10.6.6.6",
		Port: 8080,
		EcShards: []*master_pb.VolumeEcShardInformationMessage{
			// claim shards 1 (taken), 3 (free), 5 (free), with sizes in
			// shard-id order.
			{Id: 200, EcIndexBits: 0b101010, ShardSizes: []int64{77, 88, 99}},
		},
	}
	ms.filterVolumeOwnership(dnAttacker, hbAttacker)
	if len(hbAttacker.EcShards) != 1 {
		t.Fatalf("attacker should keep the message with the granted bits only, got %d", len(hbAttacker.EcShards))
	}
	got := hbAttacker.EcShards[0]
	if got.EcIndexBits != 0b101000 {
		t.Fatalf("expected only shards 3 and 5 to survive, got 0x%x", got.EcIndexBits)
	}
	if len(got.ShardSizes) != 2 || got.ShardSizes[0] != 88 || got.ShardSizes[1] != 99 {
		t.Fatalf("ShardSizes must compact alongside EcIndexBits, got %v", got.ShardSizes)
	}
}

// TestHeartbeatOwnership_InvariantsAfterRelease covers the reverse-index
// bookkeeping: after release, the owner-keyed maps are empty for that peer.
func TestHeartbeatOwnership_InvariantsAfterRelease(t *testing.T) {
	o := newHeartbeatOwnership()
	owner := heartbeatOwner{ip: "10.0.0.1", port: 8080}

	o.claimVolume(1, owner.ip, owner.port, 1)
	o.claimVolume(2, owner.ip, owner.port, 1)
	o.claimEcShards(3, 0b101, owner.ip, owner.port)

	o.release(owner.ip, owner.port)

	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.volumes) != 0 || len(o.ecShards) != 0 {
		t.Fatalf("forward maps must be empty after sole owner releases: volumes=%v ecShards=%v", o.volumes, o.ecShards)
	}
	if _, ok := o.ownerToVolumes[owner]; ok {
		t.Fatalf("owner-volume reverse index leaked")
	}
	if _, ok := o.ownerToEcShards[owner]; ok {
		t.Fatalf("owner-ecshard reverse index leaked")
	}
}

func TestAuthorizeHeartbeatPeer_OptOut(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{AllowUntrustedHeartbeat: true}}
	ctx := tcpPeerContext("10.0.0.1", 55555)

	if err := ms.authorizeHeartbeatPeer(ctx, &master_pb.Heartbeat{Ip: "10.6.6.6", Port: 8080}); err != nil {
		t.Fatalf("opt-out must skip the check: %v", err)
	}
}

// TestFilterVolumeOwnership_DeletedEcShardsReleaseBinding covers the
// rebalance flow: peer A heartbeats shard 0 of vid 5, then a follow-up
// heartbeat from A reports the shard as deleted. The binding must release so
// peer B can claim shard 0 on its next heartbeat.
func TestFilterVolumeOwnership_DeletedEcShardsReleaseBinding(t *testing.T) {
	ms := &MasterServer{
		option:             &MasterOption{},
		heartbeatOwnership: newHeartbeatOwnership(),
	}
	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")
	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnB := rack.GetOrCreateDataNode("10.0.0.2", 8080, 8081, "10.0.0.2", "B", map[string]uint32{"": 10})

	// Peer A claims shard 0 of vid 5.
	hbAClaim := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1, ShardSizes: []int64{100}},
		},
	}
	ms.filterVolumeOwnership(dnA, hbAClaim)
	if len(hbAClaim.NewEcShards) != 1 || hbAClaim.NewEcShards[0].EcIndexBits != 0b1 {
		t.Fatalf("peer A initial claim must succeed, got %+v", hbAClaim.NewEcShards)
	}

	// Peer B tries to claim shard 0 first; the binding still belongs to A so
	// the claim is filtered out.
	hbBHijack := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1, ShardSizes: []int64{100}},
		},
	}
	ms.filterVolumeOwnership(dnB, hbBHijack)
	if len(hbBHijack.NewEcShards) != 0 {
		t.Fatalf("peer B must not hijack shard 0 while A owns it, got %+v", hbBHijack.NewEcShards)
	}

	// Peer A heartbeat reports shard 0 deleted. This must release the
	// binding so a different peer can take ownership next.
	hbADelete := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		DeletedEcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1},
		},
	}
	ms.filterVolumeOwnership(dnA, hbADelete)

	// Peer B re-tries the claim; this time the slot is free.
	hbBTakeover := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1, ShardSizes: []int64{100}},
		},
	}
	ms.filterVolumeOwnership(dnB, hbBTakeover)
	if len(hbBTakeover.NewEcShards) != 1 || hbBTakeover.NewEcShards[0].EcIndexBits != 0b1 {
		t.Fatalf("peer B should own shard 0 after A's release, got %+v", hbBTakeover.NewEcShards)
	}
}

// TestFilterVolumeOwnership_FullSnapshotDropsStaleEcBits covers the case
// where a peer drops a shard between heartbeats without sending an explicit
// DeletedEcShards entry (e.g. after a restart that reloaded fewer shards).
// The full EcShards snapshot must release the shard so another peer can
// claim it.
func TestFilterVolumeOwnership_FullSnapshotDropsStaleEcBits(t *testing.T) {
	ms := &MasterServer{
		option:             &MasterOption{},
		heartbeatOwnership: newHeartbeatOwnership(),
	}
	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")
	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnB := rack.GetOrCreateDataNode("10.0.0.2", 8080, 8081, "10.0.0.2", "B", map[string]uint32{"": 10})

	// Peer A snapshots shards {0, 1, 3} = 0b1011 for vid 5.
	hbAFull := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		EcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1011, ShardSizes: []int64{10, 20, 30}},
		},
	}
	ms.filterVolumeOwnership(dnA, hbAFull)
	if hbAFull.EcShards[0].EcIndexBits != 0b1011 {
		t.Fatalf("peer A initial snapshot should keep all three bits, got 0x%x", hbAFull.EcShards[0].EcIndexBits)
	}

	// Peer A re-snapshots with shard 1 dropped: 0b1001.
	hbAShrink := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		EcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1001, ShardSizes: []int64{10, 30}},
		},
	}
	ms.filterVolumeOwnership(dnA, hbAShrink)
	if hbAShrink.EcShards[0].EcIndexBits != 0b1001 {
		t.Fatalf("peer A shrink snapshot should keep the two surviving bits, got 0x%x", hbAShrink.EcShards[0].EcIndexBits)
	}

	// Peer B can now claim shard 1 because the diff released that bit.
	hbBTakeover := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b0010, ShardSizes: []int64{20}},
		},
	}
	ms.filterVolumeOwnership(dnB, hbBTakeover)
	if len(hbBTakeover.NewEcShards) != 1 || hbBTakeover.NewEcShards[0].EcIndexBits != 0b0010 {
		t.Fatalf("peer B should pick up shard 1 after A's snapshot diff, got %+v", hbBTakeover.NewEcShards)
	}

	// Bits 0 and 3 must still be locked to A; peer B cannot hijack them.
	hbBHijack := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 5, EcIndexBits: 0b1001, ShardSizes: []int64{10, 30}},
		},
	}
	ms.filterVolumeOwnership(dnB, hbBHijack)
	if len(hbBHijack.NewEcShards) != 0 {
		t.Fatalf("peer B must not steal bits A still owns, got %+v", hbBHijack.NewEcShards)
	}
}

// TestFilterVolumeOwnership_FullSnapshotDropsStaleVolumes covers the same
// pattern for non-EC volumes: when peer A no longer lists a vid in a full
// snapshot, the binding must release so another peer can take over.
func TestFilterVolumeOwnership_FullSnapshotDropsStaleVolumes(t *testing.T) {
	ms := &MasterServer{
		option:             &MasterOption{},
		heartbeatOwnership: newHeartbeatOwnership(),
	}
	topo := topology.NewTopology("t", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc")
	rack := dc.GetOrCreateRack("rack")
	dnA := rack.GetOrCreateDataNode("10.0.0.1", 8080, 8081, "10.0.0.1", "A", map[string]uint32{"": 10})
	dnB := rack.GetOrCreateDataNode("10.0.0.2", 8080, 8081, "10.0.0.2", "B", map[string]uint32{"": 10})

	hbAFull := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		Volumes: []*master_pb.VolumeInformationMessage{
			{Id: 7, ReplicaPlacement: 0},
			{Id: 8, ReplicaPlacement: 0},
		},
	}
	ms.filterVolumeOwnership(dnA, hbAFull)

	// Snapshot drops vid 7.
	hbAShrink := &master_pb.Heartbeat{
		Ip:   "10.0.0.1",
		Port: 8080,
		Volumes: []*master_pb.VolumeInformationMessage{
			{Id: 8, ReplicaPlacement: 0},
		},
	}
	ms.filterVolumeOwnership(dnA, hbAShrink)

	// Peer B claims vid 7; the binding has been released by the diff.
	hbB := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		Volumes: []*master_pb.VolumeInformationMessage{
			{Id: 7, ReplicaPlacement: 0},
		},
	}
	ms.filterVolumeOwnership(dnB, hbB)
	if len(hbB.Volumes) != 1 {
		t.Fatalf("peer B should pick up vid 7 after A's snapshot diff, got %+v", hbB.Volumes)
	}

	// Peer B cannot hijack vid 8 — A still owns it.
	hbBHijack := &master_pb.Heartbeat{
		Ip:   "10.0.0.2",
		Port: 8080,
		Volumes: []*master_pb.VolumeInformationMessage{
			{Id: 8, ReplicaPlacement: 0},
		},
	}
	ms.filterVolumeOwnership(dnB, hbBHijack)
	if len(hbBHijack.Volumes) != 0 {
		t.Fatalf("peer B must not hijack vid 8 while A owns it, got %+v", hbBHijack.Volumes)
	}
}

// TestHeartbeatOwnership_ReleaseEcShardsIgnoresForeign confirms that a
// malicious peer cannot use a forged DeletedEcShards entry to evict the
// legitimate owner of a shard slot.
func TestHeartbeatOwnership_ReleaseEcShardsIgnoresForeign(t *testing.T) {
	o := newHeartbeatOwnership()

	if got := o.claimEcShards(11, 0b1, "10.0.0.1", 8080); got != 0b1 {
		t.Fatalf("peer A must own shard 0")
	}

	// Attacker tries to release A's binding.
	o.releaseEcShards(11, 0b1, "10.6.6.6", 8080)

	// Peer C still cannot claim it.
	if got := o.claimEcShards(11, 0b1, "10.0.0.3", 8080); got != 0 {
		t.Fatalf("foreign release must be a no-op, got 0x%x", got)
	}

	// Owner can still release its own binding.
	o.releaseEcShards(11, 0b1, "10.0.0.1", 8080)
	if got := o.claimEcShards(11, 0b1, "10.0.0.3", 8080); got != 0b1 {
		t.Fatalf("after owner release, slot must be reclaimable, got 0x%x", got)
	}
}
