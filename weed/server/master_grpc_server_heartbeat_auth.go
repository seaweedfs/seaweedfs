package weed_server

import (
	"context"
	"fmt"
	"math/bits"
	"net"
	"sync"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// heartbeatOwner identifies the data node that first claimed a volume or ec
// shard id. Two heartbeats are considered to come from the same owner when
// both ip and port match.
type heartbeatOwner struct {
	ip   string
	port uint32
}

// ecShardKey identifies a single erasure-coded shard slot, scoped by the
// volume id it belongs to. EC distributes shards of the same volume id across
// multiple data nodes (one node per shard), so ownership must be tracked per
// (vid, shard_id) pair rather than per vid.
type ecShardKey struct {
	vid     uint32
	shardId uint32
}

// heartbeatOwnership tracks which (ip, port) currently owns each volume id
// and each ec shard slot. For replicated non-EC volumes, multiple owners are
// allowed up to the declared copy count; for EC, each (vid, shard_id) pair
// has exactly one owner, but the same vid may be split across many peers
// when each peer claims a disjoint subset of shard ids.
//
// All mutating operations also update reverse indexes keyed by owner so that
// stream disconnects can release a peer's bindings in O(M) where M is the
// number of slots that specific peer held, rather than walking the full map.
type heartbeatOwnership struct {
	mu              sync.Mutex
	volumes         map[uint32][]heartbeatOwner       // volume id -> owners
	ecShards        map[ecShardKey]heartbeatOwner     // (vid, shard) -> owner
	ownerToVolumes  map[heartbeatOwner]map[uint32]struct{}
	ownerToEcShards map[heartbeatOwner]map[ecShardKey]struct{}
}

func newHeartbeatOwnership() *heartbeatOwnership {
	return &heartbeatOwnership{
		volumes:         make(map[uint32][]heartbeatOwner),
		ecShards:        make(map[ecShardKey]heartbeatOwner),
		ownerToVolumes:  make(map[heartbeatOwner]map[uint32]struct{}),
		ownerToEcShards: make(map[heartbeatOwner]map[ecShardKey]struct{}),
	}
}

// claimVolume returns true if (ip, port) is allowed to claim ownership of vid.
// maxOwners caps how many distinct owners the same vid may have (one per
// replica slot). Pass 1 if the replica count is unknown.
func (o *heartbeatOwnership) claimVolume(vid uint32, ip string, port uint32, maxOwners int) bool {
	if maxOwners < 1 {
		maxOwners = 1
	}
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	owners := o.volumes[vid]
	for _, existing := range owners {
		if existing == owner {
			return true
		}
	}
	if len(owners) >= maxOwners {
		return false
	}
	o.volumes[vid] = append(owners, owner)
	o.indexVolumeLocked(owner, vid)
	return true
}

// claimEcShards intersects the requested EcIndexBits with the bits this peer
// is allowed to own, claiming each (vid, shard_id) pair independently. The
// returned bitmap contains only the shards bound to (ip, port); rejected bits
// are dropped so the caller can adjust ShardSizes alignment accordingly.
func (o *heartbeatOwnership) claimEcShards(vid uint32, requested uint32, ip string, port uint32) uint32 {
	if requested == 0 {
		return 0
	}
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	var granted uint32
	for bitmap := requested; bitmap != 0; {
		shardId := uint32(bits.TrailingZeros32(bitmap))
		bitmap &^= 1 << shardId
		key := ecShardKey{vid: vid, shardId: shardId}
		existing, taken := o.ecShards[key]
		if taken && existing != owner {
			continue
		}
		if !taken {
			o.ecShards[key] = owner
			o.indexEcShardLocked(owner, key)
		}
		granted |= 1 << shardId
	}
	return granted
}

// release drops every entry owned by (ip, port). Called when a heartbeat
// stream ends so that a legitimate restart of the same peer (or a planned
// migration) can re-register the same ids. Uses the reverse indexes so the
// cost is proportional to what this peer held, not to the size of the cluster.
func (o *heartbeatOwnership) release(ip string, port uint32) {
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	for vid := range o.ownerToVolumes[owner] {
		o.removeVolumeOwnerLocked(vid, owner)
	}
	delete(o.ownerToVolumes, owner)
	for key := range o.ownerToEcShards[owner] {
		if existing, ok := o.ecShards[key]; ok && existing == owner {
			delete(o.ecShards, key)
		}
	}
	delete(o.ownerToEcShards, owner)
}

// releaseVolume drops the binding for a single volume id held by (ip, port).
// Used when a heartbeat reports the volume as deleted from this peer or when
// a full snapshot no longer lists it, so a subsequent peer migration can
// claim the same id without waiting for the stream to disconnect.
func (o *heartbeatOwnership) releaseVolume(vid uint32, ip string, port uint32) {
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.releaseVolumeLocked(vid, owner)
}

// releaseEcShards drops the bindings for every shard set in the bitmap that
// is currently owned by (ip, port). Bits owned by other peers are ignored so
// a malicious or stale heartbeat cannot evict a legitimate owner.
func (o *heartbeatOwnership) releaseEcShards(vid uint32, bitmap uint32, ip string, port uint32) {
	if bitmap == 0 {
		return
	}
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	for b := bitmap; b != 0; {
		shardId := uint32(bits.TrailingZeros32(b))
		b &^= 1 << shardId
		o.releaseEcShardLocked(ecShardKey{vid: vid, shardId: shardId}, owner)
	}
}

// peerVolumes returns the set of volume ids currently owned by (ip, port).
// The caller may mutate the returned slice; the snapshot is taken under the
// owner-tracker lock so it is safe to walk concurrently with other heartbeats.
func (o *heartbeatOwnership) peerVolumes(ip string, port uint32) []uint32 {
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	held := o.ownerToVolumes[owner]
	if len(held) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(held))
	for vid := range held {
		out = append(out, vid)
	}
	return out
}

// peerEcShardBitmaps returns, for each EC volume id this peer currently owns,
// the bitmap of shard ids it holds. Used to diff a full snapshot against the
// existing bindings so dropped shards can be released.
func (o *heartbeatOwnership) peerEcShardBitmaps(ip string, port uint32) map[uint32]uint32 {
	owner := heartbeatOwner{ip: ip, port: port}
	o.mu.Lock()
	defer o.mu.Unlock()
	held := o.ownerToEcShards[owner]
	if len(held) == 0 {
		return nil
	}
	out := make(map[uint32]uint32, len(held))
	for key := range held {
		out[key.vid] |= 1 << key.shardId
	}
	return out
}

// removeVolumeOwnerLocked drops owner from o.volumes[vid] and trims the
// underlying slice. Caller must hold o.mu.
func (o *heartbeatOwnership) removeVolumeOwnerLocked(vid uint32, owner heartbeatOwner) {
	owners, ok := o.volumes[vid]
	if !ok {
		return
	}
	kept := owners[:0]
	for _, existing := range owners {
		if existing != owner {
			kept = append(kept, existing)
		}
	}
	if len(kept) == 0 {
		delete(o.volumes, vid)
	} else {
		o.volumes[vid] = kept
	}
}

// releaseVolumeLocked drops a single (vid, owner) binding and tidies the
// reverse index. Caller must hold o.mu.
func (o *heartbeatOwnership) releaseVolumeLocked(vid uint32, owner heartbeatOwner) {
	o.removeVolumeOwnerLocked(vid, owner)
	if set, ok := o.ownerToVolumes[owner]; ok {
		delete(set, vid)
		if len(set) == 0 {
			delete(o.ownerToVolumes, owner)
		}
	}
}

// releaseEcShardLocked drops a single (vid, shard, owner) binding and tidies
// the reverse index. Caller must hold o.mu.
func (o *heartbeatOwnership) releaseEcShardLocked(key ecShardKey, owner heartbeatOwner) {
	if existing, ok := o.ecShards[key]; !ok || existing != owner {
		return
	}
	delete(o.ecShards, key)
	if set, ok := o.ownerToEcShards[owner]; ok {
		delete(set, key)
		if len(set) == 0 {
			delete(o.ownerToEcShards, owner)
		}
	}
}

// indexVolumeLocked records the reverse-mapping owner -> vid. Caller must
// hold o.mu.
func (o *heartbeatOwnership) indexVolumeLocked(owner heartbeatOwner, vid uint32) {
	set, ok := o.ownerToVolumes[owner]
	if !ok {
		set = make(map[uint32]struct{})
		o.ownerToVolumes[owner] = set
	}
	set[vid] = struct{}{}
}

// indexEcShardLocked records the reverse-mapping owner -> (vid, shard).
// Caller must hold o.mu.
func (o *heartbeatOwnership) indexEcShardLocked(owner heartbeatOwner, key ecShardKey) {
	set, ok := o.ownerToEcShards[owner]
	if !ok {
		set = make(map[ecShardKey]struct{})
		o.ownerToEcShards[owner] = set
	}
	set[key] = struct{}{}
}

// authorizeHeartbeatPeer checks that heartbeat.Ip matches the connection's
// source address. Loopback peers are trusted because dev clusters routinely
// run master and volume server on the same host with mismatched advertised
// ips. In-process / bufconn / unix-socket peers (used by `weed server` where
// master and volume run in the same OS process) surface with a non-TCP
// net.Addr and are also trusted. Operators with a proxy in front of the
// master can disable this check via MasterOption.AllowUntrustedHeartbeat.
func (ms *MasterServer) authorizeHeartbeatPeer(ctx context.Context, heartbeat *master_pb.Heartbeat) error {
	if ms.option != nil && ms.option.AllowUntrustedHeartbeat {
		return nil
	}
	pr, ok := peer.FromContext(ctx)
	if !ok || pr.Addr == nil {
		return fmt.Errorf("heartbeat peer info missing")
	}
	if tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
		cn := tlsInfo.State.PeerCertificates[0].Subject.CommonName
		glog.V(2).Infof("heartbeat from CN=%q claiming ip=%s:%d", cn, heartbeat.Ip, heartbeat.Port)
	}
	tcpAddr, isTCP := pr.Addr.(*net.TCPAddr)
	if !isTCP {
		// In-process / bufconn / unix-socket peer; same OS process as the
		// master, so the source IP cannot be spoofed by a remote attacker.
		return nil
	}
	host := tcpAddr.IP.String()
	if host == heartbeat.Ip {
		return nil
	}
	if tcpAddr.IP.IsLoopback() {
		return nil
	}
	return fmt.Errorf("heartbeat ip %q does not match peer %q", heartbeat.Ip, host)
}

// volumeMaxOwners returns how many distinct peers may legitimately own the
// same volume id, derived from its replica placement byte. Falls back to one
// owner if the placement byte is malformed.
func volumeMaxOwners(replicaPlacement uint32) int {
	rp, err := super_block.NewReplicaPlacementFromByte(byte(replicaPlacement))
	if err != nil || rp == nil {
		return 1
	}
	return rp.GetCopyCount()
}

// filterVolumeOwnership drops volume / ec shard claims from the heartbeat
// whenever another peer already owns that id. The peer's own heartbeat keeps
// its existing claims; only foreign claims are stripped. EC shards are
// filtered per (vid, shard_id), so multiple peers can legitimately heartbeat
// the same EC vid as long as their shard bitmaps are disjoint.
//
// Bindings are also released when the heartbeat reports that this peer no
// longer owns an id, either explicitly via Deleted{Volumes,EcShards} or
// implicitly by omitting it from a full snapshot. Without this, a planned
// migration that moves a vid (or an EC shard) from peer A to peer B would
// leave B's heartbeats permanently filtered out because the first-claim
// binding to A never expires until A disconnects.
func (ms *MasterServer) filterVolumeOwnership(dn *topology.DataNode, heartbeat *master_pb.Heartbeat) {
	ip := dn.Ip
	port := uint32(dn.Port)

	// Explicit drops: release before we claim survivors so a same-heartbeat
	// rebalance (delete shard X, claim shard Y) does not race itself.
	for _, v := range heartbeat.DeletedVolumes {
		ms.heartbeatOwnership.releaseVolume(v.Id, ip, port)
	}
	for _, s := range heartbeat.DeletedEcShards {
		if s.EcIndexBits != 0 {
			ms.heartbeatOwnership.releaseEcShards(s.Id, s.EcIndexBits, ip, port)
		}
	}

	// Full snapshot of regular volumes: diff against this peer's existing
	// bindings and release every vid the peer no longer reports. Triggered by
	// a non-empty Volumes list or by HasNoVolumes=true (the latter means the
	// peer holds zero volumes after the snapshot).
	if len(heartbeat.Volumes) > 0 || heartbeat.HasNoVolumes {
		present := make(map[uint32]struct{}, len(heartbeat.Volumes))
		for _, v := range heartbeat.Volumes {
			present[v.Id] = struct{}{}
		}
		for _, vid := range ms.heartbeatOwnership.peerVolumes(ip, port) {
			if _, still := present[vid]; !still {
				ms.heartbeatOwnership.releaseVolume(vid, ip, port)
			}
		}
	}

	// Full snapshot of EC shards: same diff, but bit-wise per vid. Triggered
	// by EcShards being non-empty or HasNoEcShards=true.
	if len(heartbeat.EcShards) > 0 || heartbeat.HasNoEcShards {
		present := make(map[uint32]uint32, len(heartbeat.EcShards))
		for _, s := range heartbeat.EcShards {
			present[s.Id] |= s.EcIndexBits
		}
		for vid, held := range ms.heartbeatOwnership.peerEcShardBitmaps(ip, port) {
			dropped := held &^ present[vid]
			if dropped != 0 {
				ms.heartbeatOwnership.releaseEcShards(vid, dropped, ip, port)
			}
		}
	}

	if n := len(heartbeat.Volumes); n > 0 {
		kept := heartbeat.Volumes[:0]
		for _, v := range heartbeat.Volumes {
			if ms.heartbeatOwnership.claimVolume(v.Id, ip, port, volumeMaxOwners(v.ReplicaPlacement)) {
				kept = append(kept, v)
			} else {
				glog.V(0).Infof("rejecting volume %d claim from %s:%d (owned by another peer)", v.Id, ip, port)
			}
		}
		heartbeat.Volumes = kept
	}

	if n := len(heartbeat.NewVolumes); n > 0 {
		kept := heartbeat.NewVolumes[:0]
		for _, v := range heartbeat.NewVolumes {
			if ms.heartbeatOwnership.claimVolume(v.Id, ip, port, volumeMaxOwners(v.ReplicaPlacement)) {
				kept = append(kept, v)
			} else {
				glog.V(0).Infof("rejecting new volume %d claim from %s:%d (owned by another peer)", v.Id, ip, port)
			}
		}
		heartbeat.NewVolumes = kept
	}

	if n := len(heartbeat.EcShards); n > 0 {
		heartbeat.EcShards = ms.applyEcShardOwnership(heartbeat.EcShards, ip, port, "ec shard")
	}

	if n := len(heartbeat.NewEcShards); n > 0 {
		heartbeat.NewEcShards = ms.applyEcShardOwnership(heartbeat.NewEcShards, ip, port, "new ec shard")
	}
}

// applyEcShardOwnership claims (vid, shard_id) ownership for each shard set
// in the message bitmap. Bits owned by a different peer are cleared from
// EcIndexBits and the parallel ShardSizes array is compacted in lock-step;
// messages that end up with no remaining shards are dropped entirely.
func (ms *MasterServer) applyEcShardOwnership(messages []*master_pb.VolumeEcShardInformationMessage, ip string, port uint32, label string) []*master_pb.VolumeEcShardInformationMessage {
	kept := messages[:0]
	for _, s := range messages {
		granted := ms.heartbeatOwnership.claimEcShards(s.Id, s.EcIndexBits, ip, port)
		if granted == s.EcIndexBits {
			kept = append(kept, s)
			continue
		}
		rejected := s.EcIndexBits &^ granted
		glog.V(0).Infof("rejecting %s bits 0x%x for volume %d from %s:%d (owned by another peer)", label, rejected, s.Id, ip, port)
		if granted == 0 {
			continue
		}
		s.ShardSizes = compactShardSizes(s.EcIndexBits, granted, s.ShardSizes)
		s.EcIndexBits = granted
		kept = append(kept, s)
	}
	return kept
}

// compactShardSizes returns the subset of sizes that corresponds to the bits
// in granted. The input sizes slice is parallel to the set bits in original
// (in increasing shard-id order); the same ordering applies to the output.
func compactShardSizes(original, granted uint32, sizes []int64) []int64 {
	if len(sizes) == 0 || granted == original {
		return sizes
	}
	out := sizes[:0]
	var i int
	for bitmap := original; bitmap != 0; {
		shardId := uint32(bits.TrailingZeros32(bitmap))
		bitmap &^= 1 << shardId
		if i < len(sizes) && granted&(1<<shardId) != 0 {
			out = append(out, sizes[i])
		}
		i++
	}
	return out
}
