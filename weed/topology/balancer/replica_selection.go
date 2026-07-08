package balancer

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Replica pairs a replica's placement location with the volume attributes
// replica-selection decisions need. Like Location for placement checks, it is
// the shared shape the shell (from master_pb) and workers adapt to. Selection
// functions return an index into the given slice (-1 when nothing qualifies),
// so callers can map the choice back to their own richer replica type.
type Replica struct {
	Location         Location
	Size             uint64
	ModifiedAtSecond int64
	CompactRevision  uint32
	ReadOnly         bool
}

// SatisfyReplicaCurrentLocation reports whether the existing replicas already
// span the failure domains the policy requires (enough data centers, enough
// racks, and the same-rack count somewhere). A volume whose replica count is
// right but whose spread falls short still needs another replica.
func SatisfyReplicaCurrentLocation(rp *super_block.ReplicaPlacement, replicas []Location) bool {
	dcCounts, rackCounts, _ := countReplicas(replicas)

	if rp.DiffDataCenterCount+1 > len(dcCounts) {
		return false
	}
	if rp.DiffRackCount+1 > len(rackCounts) {
		return false
	}
	if rp.SameRackCount > 0 {
		for _, rackCount := range rackCounts {
			if rackCount >= rp.SameRackCount+1 {
				return true
			}
		}
		return false
	}
	return true
}

// PickOneReplicaToCopyFrom returns the most recently modified replica, the
// best source for adding a new replica.
func PickOneReplicaToCopyFrom(replicas []Replica) int {
	mostRecent := -1
	for i, r := range replicas {
		if mostRecent < 0 || r.ModifiedAtSecond > replicas[mostRecent].ModifiedAtSecond {
			mostRecent = i
		}
	}
	return mostRecent
}

// IsMisplaced reports whether any replica sits at a location inconsistent
// with the replication policy given the other replicas' locations.
func IsMisplaced(replicas []Replica, rp *super_block.ReplicaPlacement) bool {
	for i := range replicas {
		others := otherThan(replicas, i)
		locs := make([]Location, len(others))
		for j, r := range others {
			locs[j] = r.Location
		}
		if !SatisfyReplicaPlacement(rp, locs, replicas[i].Location) {
			return true
		}
	}
	return false
}

// PickSmallestReplica returns the smallest replica (ties broken by oldest
// then lowest compact revision).
func PickSmallestReplica(replicas []Replica) int {
	smallest := -1
	for i := range replicas {
		if smallest < 0 || lessReplica(replicas[i], replicas[smallest]) {
			smallest = i
		}
	}
	return smallest
}

// PickOneReplicaToDelete selects the replica to trim when over-replicated.
// It only ever removes the smallest of multiple healthy writable replicas: a
// ReadOnly/integrity-flagged replica is never chosen for deletion, and the
// trim is refused (returns -1) when removing a writable replica would leave
// only ReadOnly survivors. Among the writable replicas it prefers one whose
// removal still satisfies placement, so the trim does not strip the only
// replica in a required failure domain; it falls back to the smallest
// writable if none keeps placement (a later misplaced cycle then
// re-balances).
func PickOneReplicaToDelete(replicas []Replica, rp *super_block.ReplicaPlacement) int {
	var writable []int
	for i, r := range replicas {
		if !r.ReadOnly {
			writable = append(writable, i)
		}
	}
	if len(writable) < 2 {
		return -1
	}
	var placementSafe []int
	for _, i := range writable {
		if !IsMisplaced(otherThan(replicas, i), rp) {
			placementSafe = append(placementSafe, i)
		}
	}
	if len(placementSafe) > 0 {
		return pickSmallestAmong(replicas, placementSafe)
	}
	return pickSmallestAmong(replicas, writable)
}

// PickOneMisplacedVolume selects the replica to delete and recreate at a
// correct placement. This is relocation, not over-replication: unlike
// PickOneReplicaToDelete it must still act on read-only replicas (e.g. a full
// but misplaced volume), so it does not use the writable-survivor guard. It
// prefers a replica whose removal leaves the others well placed, and falls
// back to the smallest replica.
func PickOneMisplacedVolume(replicas []Replica, rp *super_block.ReplicaPlacement) int {
	var deletionCandidates []int
	for i := range replicas {
		if !IsMisplaced(otherThan(replicas, i), rp) {
			deletionCandidates = append(deletionCandidates, i)
		}
	}
	if smallest := pickSmallestAmong(replicas, deletionCandidates); smallest >= 0 {
		return smallest
	}
	return PickSmallestReplica(replicas)
}

func pickSmallestAmong(replicas []Replica, candidates []int) int {
	smallest := -1
	for _, i := range candidates {
		if smallest < 0 || lessReplica(replicas[i], replicas[smallest]) {
			smallest = i
		}
	}
	return smallest
}

func lessReplica(a, b Replica) bool {
	if a.Size != b.Size {
		return a.Size < b.Size
	}
	if a.ModifiedAtSecond != b.ModifiedAtSecond {
		return a.ModifiedAtSecond < b.ModifiedAtSecond
	}
	return a.CompactRevision < b.CompactRevision
}

func otherThan(replicas []Replica, index int) (others []Replica) {
	for i := range replicas {
		if index != i {
			others = append(others, replicas[i])
		}
	}
	return
}
