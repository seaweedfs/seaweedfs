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
// that detects change, and enough identity to name the volume if it departs.
type reportedVolume struct {
	hash  uint64
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
	lastReported       map[volumeReportKey]reportedVolume
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

// begin reports whether this heartbeat must carry the whole list, and the
// request it answers.
func (s *volumeReportState) begin() (full bool, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fullListNeeded || !s.deltasAccepted, s.fullListGeneration
}

// changed reports whether the master needs telling about this volume, given
// what it was last told.
func (s *volumeReportState) changed(m *master_pb.VolumeInformationMessage, hash uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, known := s.lastReported[volumeReportKey{diskId: m.DiskId, volumeId: m.Id}]
	return !known || previous.hash != hash
}

// departed returns the volumes the master was told about that the current
// report no longer holds on any disk. A delta heartbeat says nothing through
// silence, so these must be named or the master keeps counting them until a
// digest mismatch buys it a full list — long enough for a busy cluster to run
// its free-slot accounting dry. A volume that moved disks is still held, so it
// is not a departure.
func (s *volumeReportState) departed(current map[volumeReportKey]reportedVolume) []*master_pb.VolumeShortInformationMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.lastReported) == 0 {
		return nil
	}
	liveIds := make(map[uint32]bool, len(current))
	for key := range current {
		liveIds[key.volumeId] = true
	}
	var gone []*master_pb.VolumeShortInformationMessage
	for key, prior := range s.lastReported {
		if _, still := current[key]; still {
			continue
		}
		if liveIds[key.volumeId] {
			continue
		}
		gone = append(gone, prior.short)
	}
	return gone
}

// commit records what this heartbeat told the master. Volumes absent from
// reported are forgotten, so one that comes back is reported again.
func (s *volumeReportState) commit(reported map[volumeReportKey]reportedVolume, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReported = reported
	// A request that arrived while this heartbeat was being built asked about a
	// later state than it carries, so it stands.
	if s.fullListGeneration == generation {
		s.fullListNeeded = false
	}
}
