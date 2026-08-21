package storage

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// volumeReportKey identifies one reported copy. Keyed by disk as well as id
// because a volume id can be mounted on two disks, and reporting one of them
// would leave the other's changes untold.
type volumeReportKey struct {
	diskId   uint32
	volumeId uint32
}

// reportedVolume is what the master was told about one volume copy: the hash
// that detects change, the heartbeat pass that last found the copy held, and
// enough identity to name the volume if it departs.
type reportedVolume struct {
	hash  uint64
	pass  uint64
	short *master_pb.VolumeShortInformationMessage
}

// volumeReportState remembers what the master was last told about each volume,
// so a heartbeat can carry only what moved since.
//
// It is per-connection: a server that reconnects, or reaches a different
// master, knows nothing about what that master holds and starts again from the
// full list. The zero value has told no master anything, so it sends the whole
// list until one accepts changes.
type volumeReportState struct {
	mu sync.Mutex
	// deltasAccepted is set once the master says it compares digests. Until
	// then the whole list goes every time, which is what an older master needs.
	deltasAccepted bool
	fullListNeeded bool
	// fullListGeneration counts requests for the whole list, so one arriving
	// while a heartbeat is being built is not marked satisfied by it.
	fullListGeneration uint64
	// pass numbers heartbeats, so one can mark the copies it finds held without
	// building a second map of them.
	pass         uint64
	lastReported map[volumeReportKey]reportedVolume
}

// reset drops everything known about the master's view.
func (s *volumeReportState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltasAccepted = false
	s.fullListNeeded = true
	s.fullListGeneration++
	s.lastReported = nil
}

func (s *volumeReportState) acceptDeltas() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltasAccepted = true
}

func (s *volumeReportState) requestFullList() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fullListNeeded = true
	s.fullListGeneration++
}

// begin opens a heartbeat: whether it must carry the whole list, the request it
// answers, and the pass number that marks the copies it finds still held.
func (s *volumeReportState) begin() (full bool, generation uint64, pass uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pass++
	return s.fullListNeeded || !s.deltasAccepted, s.fullListGeneration, s.pass
}

// record marks one volume copy as held by the heartbeat being built, and reports
// whether the master needs telling about it. It updates the entry already held
// rather than build a second map beside it, so a server whose volumes are quiet
// allocates nothing per volume per heartbeat.
func (s *volumeReportState) record(m *master_pb.VolumeInformationMessage, hash uint64, pass uint64) bool {
	key := volumeReportKey{diskId: m.DiskId, volumeId: m.Id}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, known := s.lastReported[key]; known {
		changed := previous.hash != hash
		if changed {
			// Only a departure hands a short message out, and that entry leaves
			// the map in the same step, so the one held here is read by no one.
			fillShortInformation(previous.short, m)
		}
		previous.hash, previous.pass = hash, pass
		s.lastReported[key] = previous
		return changed
	}
	if s.lastReported == nil {
		s.lastReported = make(map[volumeReportKey]reportedVolume)
	}
	short := &master_pb.VolumeShortInformationMessage{}
	fillShortInformation(short, m)
	s.lastReported[key] = reportedVolume{hash: hash, pass: pass, short: short}
	return true
}

func fillShortInformation(short *master_pb.VolumeShortInformationMessage, m *master_pb.VolumeInformationMessage) {
	short.Id = m.Id
	short.Collection = m.Collection
	short.ReplicaPlacement = m.ReplicaPlacement
	short.Version = m.Version
	short.Ttl = m.Ttl
	short.DiskType = m.DiskType
	short.DiskId = m.DiskId
}

// commit closes the heartbeat. Copies this pass did not find are forgotten, so
// one that comes back is reported again, and those whose volume left the server
// altogether are returned. A delta heartbeat says nothing through silence, so
// they must be named or the master keeps counting them until a digest mismatch
// buys it a full list — long enough for a busy cluster to run its free-slot
// accounting dry. A volume that moved disks is still held, so it is not a
// departure; a full list is already the whole truth, so it names none.
func (s *volumeReportState) commit(pass uint64, generation uint64, full bool) []*master_pb.VolumeShortInformationMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A request that arrived while this heartbeat was being built asked about a
	// later state than it carries, so it stands.
	if s.fullListGeneration == generation {
		s.fullListNeeded = false
	}
	var goneKeys []volumeReportKey
	for key, prior := range s.lastReported {
		if prior.pass != pass {
			goneKeys = append(goneKeys, key)
		}
	}
	if len(goneKeys) == 0 {
		return nil
	}
	goneIds := make(map[uint32]bool, len(goneKeys))
	for _, key := range goneKeys {
		goneIds[key.volumeId] = true
	}
	for key, prior := range s.lastReported {
		if prior.pass == pass {
			delete(goneIds, key.volumeId)
		}
	}
	var gone []*master_pb.VolumeShortInformationMessage
	for _, key := range goneKeys {
		if !full && goneIds[key.volumeId] {
			gone = append(gone, s.lastReported[key].short)
		}
		delete(s.lastReported, key)
	}
	return gone
}
