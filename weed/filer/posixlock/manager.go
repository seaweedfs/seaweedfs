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
//
// Unscoped sessions (empty Principal) are leased as a whole once they renew.
// A scoped session is leased per key from its first acquisition: the lease ends
// at the earlier of Lease.TTL after its last acquisition or renewal on that key
// and Lease.NotAfter. Expired scoped locks are dropped before any operation on
// their key, so they never block a new holder until the next sweep.
type Manager struct {
	mu        sync.Mutex
	now       func() time.Time
	byKey     map[string]*Set                  // inode key -> held locks
	bySession map[Session]map[string]bool      // session -> keys it currently holds locks on
	lastSeen  map[Session]time.Time            // unscoped session -> last keepalive; only renewing sessions are leased
	leases    map[Session]map[string]time.Time // scoped session -> key -> lease deadline
}

// Session is a mount session under the authority that holds it. Sid is chosen
// by the client; Principal, set by the server from the verified credential, is
// empty for unscoped callers.
type Session struct {
	Principal string
	Sid       uint64
}

// Lease bounds a scoped session's hold on a key. It is ignored for unscoped
// sessions.
type Lease struct {
	TTL      time.Duration
	NotAfter time.Time
}

func NewManager() *Manager {
	return NewManagerWithClock(time.Now)
}

// NewManagerWithClock returns a Manager whose leases follow now.
func NewManagerWithClock(now func() time.Time) *Manager {
	return &Manager{
		now:       now,
		byKey:     make(map[string]*Set),
		bySession: make(map[Session]map[string]bool),
		lastSeen:  make(map[Session]time.Time),
		leases:    make(map[Session]map[string]time.Time),
	}
}

// Renew records a keepalive from a session. An unscoped session is placed under
// lease management as a whole, regardless of key: only sessions that have
// renewed are subject to ReapExpired, so a session that never sends keepalives
// (e.g. before the mount keepalive exists) is never reaped. A scoped session
// renews only its existing lease on key.
func (m *Manager) Renew(key string, ss Session, lease Lease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ss.Principal == "" {
		m.lastSeen[ss] = m.now()
		return
	}
	m.live(key)
	if _, held := m.leases[ss][key]; held {
		m.lease(ss, key, lease)
	}
}

// ReapExpired releases the locks of every leased unscoped session whose last
// keepalive is older than ttl — a dead or partitioned mount — and of every
// scoped lease past its deadline. Unscoped sessions that never renewed are left
// untouched. Returns the reaped session ids.
func (m *Manager) ReapExpired(ttl time.Duration) []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	cutoff := now.Add(-ttl)
	var reaped []uint64
	for ss, seen := range m.lastSeen {
		if seen.After(cutoff) {
			continue
		}
		for key := range m.bySession[ss] {
			s := m.byKey[key]
			if s == nil {
				continue
			}
			s.ReleaseSession(ss)
			if s.Empty() {
				delete(m.byKey, key)
			}
		}
		delete(m.bySession, ss)
		delete(m.lastSeen, ss)
		reaped = append(reaped, ss.Sid)
	}
	for ss, keys := range m.leases {
		expired := false
		for key, deadline := range keys {
			if now.Before(deadline) {
				continue
			}
			if s := m.byKey[key]; s != nil {
				s.ReleaseSession(ss)
				if s.Empty() {
					delete(m.byKey, key)
				}
			}
			m.deindex(ss, key)
			expired = true
		}
		if expired {
			reaped = append(reaped, ss.Sid)
		}
	}
	return reaped
}

// TryLock grants lk on key, or returns the conflicting lock and false. The set
// is created on first use and dropped again when it empties. A grant to a
// scoped session starts or extends its lease on key.
func (m *Manager) TryLock(key string, lk Range, lease Lease) (Range, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.live(key)
	created := s == nil
	if created {
		s = &Set{}
	}
	if c, granted := s.Acquire(lk); !granted {
		return c, false
	}
	if created {
		m.byKey[key] = s
	}
	m.index(lk.Session(), key)
	if lk.Principal != "" {
		m.lease(lk.Session(), key, lease)
	}
	return Range{}, true
}

// Track records a lock the server already granted, without arbitration. A mount
// uses it to mirror its own held locks so it can re-assert them to the inode's
// current owner filer after an ownership change or owner restart.
func (m *Manager) Track(key string, lk Range) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.byKey[key]
	if !ok {
		s = &Set{}
		m.byKey[key] = s
	}
	s.Grant(lk)
	m.index(lk.Session(), key)
}

// Snapshot returns a copy of the held locks per key. A mount calls it to drive
// re-assertion keepalives; the filer never does.
func (m *Manager) Snapshot() map[string][]Range {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]Range, len(m.byKey))
	for key, s := range m.byKey {
		out[key] = append([]Range(nil), s.locks...)
	}
	return out
}

// Reassert rebuilds session ss's locks on key from the client's authoritative
// list, renewing the lease. It replaces ss's existing locks on the key, then
// re-acquires each asserted lock as ss — arbitrating against other sessions so
// it never double-grants. Locks that lost to another session in a migration
// window are returned as conflicts. The owner filer calls this on a
// re-assertion keepalive.
func (m *Manager) Reassert(key string, ss Session, locks []Range, lease Lease) (conflicts []Range) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ss.Principal == "" {
		m.lastSeen[ss] = m.now()
	}

	s := m.live(key)
	if s == nil {
		if len(locks) == 0 {
			return nil
		}
		s = &Set{}
		m.byKey[key] = s
	}
	s.ReleaseSession(ss)
	for _, lk := range locks {
		lk.Sid, lk.Principal = ss.Sid, ss.Principal
		if c, granted := s.Acquire(lk); !granted {
			conflicts = append(conflicts, c)
		}
	}
	if setHasSession(s, ss) {
		m.index(ss, key)
		if ss.Principal != "" {
			m.lease(ss, key, lease)
		}
	} else {
		m.deindex(ss, key)
	}
	if s.Empty() {
		delete(m.byKey, key)
	}
	return conflicts
}

// Unlock releases lk's owner's locks within its namespace over lk's range.
func (m *Manager) Unlock(key string, lk Range) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.live(key)
	if s == nil {
		return
	}
	s.Release(lk)
	m.afterRelease(key, s, lk.Session())
}

// GetLk reports the lock that would block proposed on key, if any.
func (m *Manager) GetLk(key string, proposed Range) (Range, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.live(key)
	if s == nil {
		return Range{}, false
	}
	return s.Conflict(proposed)
}

// ReleasePosixOwner drops (ss, owner)'s fcntl locks on key — the flush-time path.
func (m *Manager) ReleasePosixOwner(key string, ss Session, owner uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.live(key)
	if s == nil {
		return
	}
	s.ReleasePosixOwner(ss, owner)
	m.afterRelease(key, s, ss)
}

// ReleaseFlockOwner drops (ss, owner)'s flock locks on key — the release-time path.
func (m *Manager) ReleaseFlockOwner(key string, ss Session, owner uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.live(key)
	if s == nil {
		return
	}
	s.ReleaseFlockOwner(ss, owner)
	m.afterRelease(key, s, ss)
}

// ReleaseSession drops every lock held by a session across all inodes it touched,
// reaping a mount whose lease expired. O(locks held by the session).
func (m *Manager) ReleaseSession(ss Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.bySession[ss] {
		s := m.byKey[key]
		if s == nil {
			continue
		}
		s.ReleaseSession(ss)
		if s.Empty() {
			delete(m.byKey, key)
		}
	}
	delete(m.bySession, ss)
	delete(m.leases, ss)
}

// live returns key's set after dropping the locks of scoped sessions whose
// lease on key has ended, or nil when nothing is held.
func (m *Manager) live(key string) *Set {
	s := m.byKey[key]
	if s == nil {
		return nil
	}
	now := m.now()
	var expired []Session
	for _, l := range s.locks {
		ss := l.Session()
		if ss.Principal == "" {
			continue
		}
		if deadline, ok := m.leases[ss][key]; ok && now.Before(deadline) {
			continue
		}
		expired = append(expired, ss)
	}
	for _, ss := range expired {
		s.ReleaseSession(ss)
		m.deindex(ss, key)
	}
	if s.Empty() {
		delete(m.byKey, key)
		return nil
	}
	return s
}

// lease sets ss's deadline on key to the earlier of lease.TTL from now and
// lease.NotAfter.
func (m *Manager) lease(ss Session, key string, lease Lease) {
	deadline := m.now().Add(lease.TTL)
	if lease.NotAfter.Before(deadline) {
		deadline = lease.NotAfter
	}
	keys := m.leases[ss]
	if keys == nil {
		keys = make(map[string]time.Time)
		m.leases[ss] = keys
	}
	keys[key] = deadline
}

// afterRelease prunes the session index when ss no longer holds any lock on key,
// and drops the set when it empties. Only ss's presence can have changed, since
// a release only removes ss's locks.
func (m *Manager) afterRelease(key string, s *Set, ss Session) {
	if s.Empty() {
		delete(m.byKey, key)
		m.deindex(ss, key)
		return
	}
	if !setHasSession(s, ss) {
		m.deindex(ss, key)
	}
}

func (m *Manager) index(ss Session, key string) {
	keys := m.bySession[ss]
	if keys == nil {
		keys = make(map[string]bool)
		m.bySession[ss] = keys
	}
	keys[key] = true
}

// deindex forgets that ss holds locks on key, along with its lease there.
func (m *Manager) deindex(ss Session, key string) {
	if keys := m.bySession[ss]; keys != nil {
		delete(keys, key)
		if len(keys) == 0 {
			delete(m.bySession, ss)
		}
	}
	if keys := m.leases[ss]; keys != nil {
		delete(keys, key)
		if len(keys) == 0 {
			delete(m.leases, ss)
		}
	}
}

func setHasSession(s *Set, ss Session) bool {
	for _, l := range s.Locks() {
		if l.Session() == ss {
			return true
		}
	}
	return false
}
