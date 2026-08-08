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

// volumeReportState remembers what the master was last told about each volume,
// so a heartbeat can carry only what moved since.
//
// It is per-connection: a server that reconnects, or reaches a different
// master, knows nothing about what that master holds and starts again from the
// full list.
type volumeReportState struct {
	mu sync.Mutex
	// deltasAccepted is set once the master says it compares digests. Until
	// then the whole list goes every time, which is what an older master needs.
	deltasAccepted bool
	fullListNeeded bool
	lastReported   map[volumeReportKey]uint64
}

func newVolumeReportState() *volumeReportState {
	return &volumeReportState{fullListNeeded: true}
}

// reset drops everything known about the master's view.
func (s *volumeReportState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltasAccepted = false
	s.fullListNeeded = true
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
}

// begin reports whether this heartbeat must carry the whole list.
func (s *volumeReportState) begin() (full bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fullListNeeded || !s.deltasAccepted
}

// changed reports whether the master needs telling about this volume, given
// what it was last told.
func (s *volumeReportState) changed(m *master_pb.VolumeInformationMessage, hash uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, known := s.lastReported[volumeReportKey{diskId: m.DiskId, volumeId: m.Id}]
	return !known || previous != hash
}

// commit records what this heartbeat told the master. Volumes absent from
// reported are forgotten, so one that comes back is reported again.
func (s *volumeReportState) commit(reported map[volumeReportKey]uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReported = reported
	s.fullListNeeded = false
}
