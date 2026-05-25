package posixlock

import (
	"sync"
	"time"
)

// Manager is the owner filer's in-memory authority for POSIX advisory locks
// across inodes. Lock state lives here, not in replicated metadata: it is
// transient coordination, so keeping it out of the meta-log avoids churn and
// does not pollute what subscribers see (the distributed lock manager holds its
// locks the same way). A `Set` per inode key, plus a session index so a dead
// mount's locks are reaped in O(locks held) rather than by scanning every inode.
//
// key is an opaque inode identity supplied by the caller — the file's path, or
// "hl:"+hex(HardLinkId) for a hardlinked inode — so all names of one inode share
// a Set. The Manager is safe for concurrent use.
type Manager struct {
	mu       sync.Mutex
	byKey    map[string]*Set            // inode key -> held locks
	bySid    map[uint64]map[string]bool // session -> keys it currently holds locks on
	lastSeen map[uint64]time.Time       // session -> last keepalive; only renewing sessions are leased
}

func NewManager() *Manager {
	return &Manager{
		byKey:    make(map[string]*Set),
		bySid:    make(map[uint64]map[string]bool),
		lastSeen: make(map[uint64]time.Time),
	}
}

// Renew records a keepalive from a session, placing it under lease management.
// Only sessions that have renewed are subject to ReapExpired, so a session that
// never sends keepalives (e.g. before the mount keepalive exists) is never reaped.
func (m *Manager) Renew(sid uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSeen[sid] = time.Now()
}

// ReapExpired releases the locks of every leased session whose last keepalive is
// older than ttl — a dead or partitioned mount. Sessions that never renewed are
// left untouched. Returns the reaped session ids.
func (m *Manager) ReapExpired(ttl time.Duration) []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := time.Now().Add(-ttl)
	var reaped []uint64
	for sid, seen := range m.lastSeen {
		if seen.After(cutoff) {
			continue
		}
		for key := range m.bySid[sid] {
			s := m.byKey[key]
			if s == nil {
				continue
			}
			s.ReleaseSession(sid)
			if s.Empty() {
				delete(m.byKey, key)
			}
		}
		delete(m.bySid, sid)
		delete(m.lastSeen, sid)
		reaped = append(reaped, sid)
	}
	return reaped
}

// TryLock grants lk on key, or returns the conflicting lock and false. The set
// is created on first use and dropped again when it empties.
func (m *Manager) TryLock(key string, lk Range) (Range, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.byKey[key]
	if !ok {
		s = &Set{}
	}
	if c, granted := s.Acquire(lk); !granted {
		return c, false
	}
	if !ok {
		m.byKey[key] = s
	}
	m.index(lk.Sid, key)
	return Range{}, true
}

// Unlock releases lk's owner's locks within its namespace over lk's range.
func (m *Manager) Unlock(key string, lk Range) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.byKey[key]
	if s == nil {
		return
	}
	s.Release(lk)
	m.afterRelease(key, s, lk.Sid)
}

// GetLk reports the lock that would block proposed on key, if any.
func (m *Manager) GetLk(key string, proposed Range) (Range, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.byKey[key]
	if s == nil {
		return Range{}, false
	}
	return s.Conflict(proposed)
}

// ReleasePosixOwner drops (sid, owner)'s fcntl locks on key — the flush-time path.
func (m *Manager) ReleasePosixOwner(key string, sid, owner uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.byKey[key]
	if s == nil {
		return
	}
	s.ReleasePosixOwner(sid, owner)
	m.afterRelease(key, s, sid)
}

// ReleaseFlockOwner drops (sid, owner)'s flock locks on key — the release-time path.
func (m *Manager) ReleaseFlockOwner(key string, sid, owner uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.byKey[key]
	if s == nil {
		return
	}
	s.ReleaseFlockOwner(sid, owner)
	m.afterRelease(key, s, sid)
}

// ReleaseSession drops every lock held by a session across all inodes it touched,
// reaping a mount whose lease expired. O(locks held by the session).
func (m *Manager) ReleaseSession(sid uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.bySid[sid] {
		s := m.byKey[key]
		if s == nil {
			continue
		}
		s.ReleaseSession(sid)
		if s.Empty() {
			delete(m.byKey, key)
		}
	}
	delete(m.bySid, sid)
}

// afterRelease prunes the session index when sid no longer holds any lock on key,
// and drops the set when it empties. Only sid's presence can have changed, since
// a release only removes sid's locks.
func (m *Manager) afterRelease(key string, s *Set, sid uint64) {
	if s.Empty() {
		delete(m.byKey, key)
		m.deindex(sid, key)
		return
	}
	if !setHasSession(s, sid) {
		m.deindex(sid, key)
	}
}

func (m *Manager) index(sid uint64, key string) {
	keys := m.bySid[sid]
	if keys == nil {
		keys = make(map[string]bool)
		m.bySid[sid] = keys
	}
	keys[key] = true
}

func (m *Manager) deindex(sid uint64, key string) {
	keys := m.bySid[sid]
	if keys == nil {
		return
	}
	delete(keys, key)
	if len(keys) == 0 {
		delete(m.bySid, sid)
	}
}

func setHasSession(s *Set, sid uint64) bool {
	for _, l := range s.Locks() {
		if l.Sid == sid {
			return true
		}
	}
	return false
}
