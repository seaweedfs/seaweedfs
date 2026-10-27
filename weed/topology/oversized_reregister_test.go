package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// The oversized verdict must outlive the location list: when every replica
// leaves, vid2location drops the entry and its per-replica marks with it, and
// a replica re-registering from a size-less report must not put the volume
// back on the writable list until a heartbeat re-measures it.
func TestOversizedVolumeStaysUnwritableAcrossLocationReset(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	dn1 := NewDataNode("server1")
	dn1.Ip, dn1.Port = "10.0.0.1", 8080
	dn2 := NewDataNode("server2")
	dn2.Ip, dn2.Port = "10.0.0.2", 8080

	v := storage.VolumeInfo{Id: 35, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Size: 300}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)

	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.UpdateOversizedState(&v, dn1)
	vl.UpdateOversizedState(&v, dn2)
	vl.EnsureCorrectWritables(&v)
	if isWritable(t, topo, v) {
		t.Fatal("an oversized volume should not be writable")
	}

	// Every replica leaves: the location list and its per-replica marks go.
	vl.SetVolumeUnavailable(dn1, v.Id)
	vl.SetVolumeUnavailable(dn2, v.Id)

	// Both replicas re-register from a report that carries no size.
	small := v
	small.Size = 0
	topo.RegisterVolumeLayout(small, dn1)
	topo.RegisterVolumeLayout(small, dn2)

	if isWritable(t, topo, v) {
		t.Fatal("an oversized volume was re-admitted to the writable list before its size was re-measured")
	}

	// A report under the limit clears the remembered verdict.
	shrunk := v
	shrunk.Size = 10
	vl.UpdateOversizedState(&shrunk, dn1)
	vl.UpdateOversizedState(&shrunk, dn2)
	vl.EnsureCorrectWritables(&shrunk)
	if !isWritable(t, topo, v) {
		t.Fatal("a shrunk volume should be writable again")
	}
}

// The writable admission gate itself must refuse a known-oversized volume:
// paths like quarantine release and mark-writable do not consult the marks.
func TestOversizedVolumeRejectsExplicitWritableMark(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	dn1 := NewDataNode("server1")
	dn1.Ip, dn1.Port = "10.0.0.1", 8080
	dn2 := NewDataNode("server2")
	dn2.Ip, dn2.Port = "10.0.0.2", 8080

	v := storage.VolumeInfo{Id: 35, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Size: 300}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)

	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.UpdateOversizedState(&v, dn1)
	vl.UpdateOversizedState(&v, dn2)
	vl.EnsureCorrectWritables(&v)

	if vl.SetVolumeWritable(dn1, v.Id) || isWritable(t, topo, v) {
		t.Fatal("mark-writable re-admitted an oversized volume")
	}

	// A vacuum commit asks the node whether the volume fits; its fresh
	// answer supersedes that replica's remembered verdict. Both replicas
	// commit at the new shrunken size while the cached reports on the nodes
	// still carry the pre-compaction size.
	shrunk := v
	shrunk.Size = 10
	if vl.SetVolumeAvailable(dn1, v.Id, uint64(shrunk.Size), false, false, false) {
		t.Fatal("the other replica's oversized verdict still stands")
	}
	if !vl.SetVolumeAvailable(dn2, v.Id, uint64(shrunk.Size), false, false, false) || !isWritable(t, topo, v) {
		t.Fatal("a committed volume under the limit should be writable")
	}
}

// One replica shrinking clears only its own remembered verdict: a second
// replica last measured oversized keeps the volume out of the writable list
// even after its location entry is gone, until it too reports under the
// limit.
func TestOversizedVerdictClearsOnlyForReportingReplica(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	dn1 := NewDataNode("server1")
	dn1.Ip, dn1.Port = "10.0.0.1", 8080
	dn2 := NewDataNode("server2")
	dn2.Ip, dn2.Port = "10.0.0.2", 8080

	v := storage.VolumeInfo{Id: 35, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Size: 300}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)

	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.UpdateOversizedState(&v, dn1)
	vl.UpdateOversizedState(&v, dn2)
	vl.EnsureCorrectWritables(&v)

	// dn2 leaves, taking its live mark with it; its verdict is remembered.
	vl.SetVolumeUnavailable(dn2, v.Id)

	// dn1 shrinks under the limit: its verdict clears, dn2's stands. The
	// compaction bump makes the size drop real rather than pending decay.
	shrunk := v
	shrunk.Size = 10
	shrunk.CompactRevision = v.CompactRevision + 1
	vl.UpdateOversizedState(&shrunk, dn1)
	vl.UpdateVolumeSize(v.Id, uint64(shrunk.Size), shrunk.CompactRevision, true)
	vl.EnsureCorrectWritables(&shrunk)
	if !vl.isMarkedOversized(v.Id) {
		t.Fatal("one replica's under-limit report cleared the absent replica's verdict")
	}

	// dn2 returns from a report that carries no size; the verdict must
	// still hold it out until it is re-measured.
	small := v
	small.Size = 0
	topo.RegisterVolumeLayout(small, dn2)
	if isWritable(t, topo, v) {
		t.Fatal("an oversized replica was re-admitted before its size was re-measured")
	}

	// dn2's own under-limit report clears the last verdict.
	vl.UpdateOversizedState(&shrunk, dn2)
	vl.UpdateVolumeSize(v.Id, uint64(shrunk.Size), shrunk.CompactRevision, true)
	vl.EnsureCorrectWritables(&shrunk)
	if !isWritable(t, topo, v) {
		t.Fatal("every replica under the limit should leave the volume writable")
	}
}

// The verdict TTL forgives a replica that never comes back; it must not
// forgive one that did. A remount announces the volume without a size, so an
// expired verdict still has to hold the volume out of the writable list
// until the remounted replica's first size-bearing report.
func TestExpiredOversizedVerdictHoldsForRemountedReplica(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	dn1 := NewDataNode("server1")
	dn1.Ip, dn1.Port = "10.0.0.1", 8080
	dn2 := NewDataNode("server2")
	dn2.Ip, dn2.Port = "10.0.0.2", 8080

	v := storage.VolumeInfo{Id: 35, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Size: 300}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)

	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.UpdateOversizedState(&v, dn1)
	vl.UpdateOversizedState(&v, dn2)

	// Every replica leaves, then the remembered verdicts age past the TTL.
	vl.SetVolumeUnavailable(dn1, v.Id)
	vl.SetVolumeUnavailable(dn2, v.Id)
	vl.accessLock.Lock()
	for nodeId := range vl.oversizedVolumes[v.Id] {
		vl.oversizedVolumes[v.Id][nodeId] = time.Now().Add(-2 * oversizedVerdictTTL)
	}
	vl.accessLock.Unlock()

	// Both replicas remount on a report that carries no size.
	sizeless := v
	sizeless.Size = 0
	topo.RegisterVolumeLayout(sizeless, dn1)
	topo.RegisterVolumeLayout(sizeless, dn2)

	if isWritable(t, topo, v) {
		t.Fatal("a remounted oversized volume was re-admitted before its size was re-measured")
	}

	// A replica that stayed gone loses its verdict to expiry: dn3 never
	// returns, so its entry must be swept once the TTL passes.
	dn3 := NewDataNode("server3")
	dn3.Ip, dn3.Port = "10.0.0.3", 8080
	vl.accessLock.Lock()
	vl.oversizedVolumes[v.Id][dn3.Id()] = time.Now().Add(-2 * oversizedVerdictTTL)
	vl.accessLock.Unlock()
	vl.isMarkedOversized(v.Id)
	vl.accessLock.RLock()
	_, kept := vl.oversizedVolumes[v.Id][dn3.Id()]
	vl.accessLock.RUnlock()
	if kept {
		t.Fatal("an absent replica's expired verdict should have been dropped")
	}

	// The next size-bearing report clears the stale verdict.
	shrunk := v
	shrunk.Size = 10
	vl.UpdateOversizedState(&shrunk, dn1)
	vl.UpdateOversizedState(&shrunk, dn2)
	vl.EnsureCorrectWritables(&shrunk)
	if !isWritable(t, topo, v) {
		t.Fatal("a shrunk volume should be writable again")
	}
}
