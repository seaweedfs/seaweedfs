package lock_manager

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var LockErrorNonEmptyTokenOnNewLock = fmt.Errorf("lock: non-empty token on a new lock")
var LockErrorNonEmptyTokenOnExpiredLock = fmt.Errorf("lock: non-empty token on an expired lock")
var LockErrorTokenMismatch = fmt.Errorf("lock: token mismatch")
var UnlockErrorTokenMismatch = fmt.Errorf("unlock: token mismatch")
var LockNotFound = fmt.Errorf("lock not found")

// LockManager local lock manager, used by distributed lock manager
type LockManager struct {
	locks map[string]*Lock
	// Released lease identities are retained only until their maximum expiry.
	removed        map[releasedLease]int64
	accessLock     sync.RWMutex
	nextGeneration atomic.Int64
}
type releasedLease struct{ path, id string }

// Tokens are opaque to clients. Keep a stable acquisition ID while rotating the
// secret on renewal; local generation counters can overlap across primaries.
func leaseID(token string) string {
	id, _, _ := strings.Cut(token, "/")
	return id
}

func newLockToken(previous string) string {
	id := leaseID(previous)
	if id == "" {
		id = uuid.New().String()
	}
	return id + "/" + uuid.New().String()
}

type Lock struct {
	Token       string
	PrevToken   string // token superseded by the latest renewal; a holder whose renewal response was lost still presents it
	ExpiredAtNs int64
	Key         string // only used for moving locks
	Owner       string
	IsBackup    bool  // true if this node holds the lock as a backup
	Generation  int64 // monotonic fencing token, increments on fresh acquisition
	Seq         int64 // per-lock sequence number, increments on every mutation (acquire/renew/unlock)
	replicated  bool  // a copy may exist elsewhere, requiring a release record
}

func NewLockManager() *LockManager {
	t := &LockManager{
		locks: make(map[string]*Lock),
	}
	go t.CleanUp()
	return t
}

func (lm *LockManager) NextGeneration() int64 {
	return lm.nextGeneration.Add(1)
}

func compareMutationVersion(generation, seq int64, existingGeneration, existingSeq int64) int {
	switch {
	case generation < existingGeneration:
		return -1
	case generation > existingGeneration:
		return 1
	case seq < existingSeq:
		return -1
	case seq > existingSeq:
		return 1
	default:
		return 0
	}
}

func (lm *LockManager) advanceGenerationFloor(generation int64) {
	for {
		current := lm.nextGeneration.Load()
		if generation < current {
			return
		}
		if lm.nextGeneration.CompareAndSwap(current, generation) {
			return
		}
	}
}

// rememberRemoval runs under accessLock. Delayed messages for this lease are
// rejected until its maximum expiry, after which inserts reject them as expired.
func (lm *LockManager) rememberRemoval(path, token string, expiredAtNs int64) {
	if token == "" || expiredAtNs <= time.Now().UnixNano() {
		return
	}
	if lm.removed == nil {
		lm.removed = make(map[releasedLease]int64)
	}
	key := releasedLease{path, leaseID(token)}
	lm.removed[key] = max(lm.removed[key], expiredAtNs)
}

func (lm *LockManager) wasRemoved(path, token string) bool {
	return lm.removed[releasedLease{path, leaseID(token)}] > time.Now().UnixNano()
}

func (lm *LockManager) Lock(path string, expiredAtNs int64, token string, owner string) (lockOwner, renewToken string, generation int64, seq int64, err error) {
	lockOwner, renewToken, generation, seq, _, err = lm.lockWithExpiry(path, expiredAtNs, token, owner)
	return
}

// lockWithExpiry also returns the effective expiry for replication. Renewals
// cannot shorten it, so a release carries a validity bound for every older RPC.
func (lm *LockManager) lockWithExpiry(path string, expiredAtNs int64, token string, owner string) (lockOwner, renewToken string, generation int64, seq int64, leaseExpires int64, err error) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	leaseExpires = expiredAtNs

	glog.V(4).Infof("lock %s %v %v %v", path, time.Unix(0, expiredAtNs), token, owner)

	if oldValue, found := lm.locks[path]; found {
		if oldValue.ExpiredAtNs > 0 && oldValue.ExpiredAtNs < time.Now().UnixNano() {
			// lock is expired, set to a new lock
			if token != "" {
				glog.V(4).Infof("lock expired key %s non-empty token %v owner %v ts %s", path, token, owner, time.Unix(0, oldValue.ExpiredAtNs))
				err = LockErrorNonEmptyTokenOnExpiredLock
				return
			} else {
				// new lock
				renewToken = newLockToken("")
				generation = lm.NextGeneration()
				seq = 1
				glog.V(4).Infof("key %s new token %v owner %v generation %d", path, renewToken, owner, generation)
				lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq}
				return
			}
		}
		// not expired
		leaseExpires = max(expiredAtNs, oldValue.ExpiredAtNs)
		lockOwner = oldValue.Owner
		if oldValue.Token == token {
			// token matches, renew the lock
			renewToken = newLockToken(oldValue.Token)
			generation = oldValue.Generation // keep same generation on renewal
			seq = oldValue.Seq + 1
			glog.V(4).Infof("key %s old token %v owner %v => %v owner %v", path, oldValue.Token, oldValue.Owner, renewToken, owner)
			lm.locks[path] = &Lock{Token: renewToken, PrevToken: oldValue.Token, ExpiredAtNs: leaseExpires, Owner: owner, Generation: generation, Seq: seq, replicated: oldValue.replicated}
			return
		}
		if token != "" && token == oldValue.PrevToken && owner == oldValue.Owner {
			// The latest renewal already rotated the token but its response
			// was lost: replay the current token so the holder recovers it
			// instead of conflicting with its own lease until it expires.
			renewToken = oldValue.Token
			generation = oldValue.Generation
			seq = oldValue.Seq + 1
			oldValue.ExpiredAtNs = leaseExpires
			oldValue.Seq = seq
			return
		}
		if token == "" {
			// new lock
			glog.V(4).Infof("key %s locked by %v", path, oldValue.Owner)
			err = fmt.Errorf("lock already owned by %v", oldValue.Owner)
			return
		}
		glog.V(4).Infof("key %s expected token %v owner %v received %v from %v", path, oldValue.Token, oldValue.Owner, token, owner)
		err = fmt.Errorf("lock: token mismatch")
		return
	} else {
		glog.V(4).Infof("key %s no lock owner %v", path, owner)
		if token == "" {
			// new lock
			renewToken = newLockToken("")
			generation = lm.NextGeneration()
			seq = 1
			glog.V(4).Infof("key %s new token %v owner %v generation %d", path, renewToken, owner, generation)
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq}
			return
		} else {
			glog.V(4).Infof("key %s non-empty token %v owner %v", path, token, owner)
			err = LockErrorNonEmptyTokenOnNewLock
			return
		}
	}
}

func (lm *LockManager) Unlock(path string, token string) (isUnlocked bool, generation int64, seq int64, err error) {
	released, err := lm.unlock(path, token)
	if released != nil {
		return true, released.Generation, released.Seq, err
	}
	return false, 0, 0, err
}

// unlock returns the released lease and its maximum expiry atomically, including
// when the caller uses a superseded token after losing a renewal response.
func (lm *LockManager) unlock(path, token string) (*Lock, error) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	old, found := lm.locks[path]
	if !found {
		return nil, LockNotFound
	}
	expired := old.ExpiredAtNs > 0 && old.ExpiredAtNs < time.Now().UnixNano()
	if !expired && old.Token != token && (token == "" || token != old.PrevToken) {
		return nil, UnlockErrorTokenMismatch
	}
	if expired {
		glog.V(4).Infof("key %s expired at %v", path, time.Unix(0, old.ExpiredAtNs))
	} else {
		glog.V(4).Infof("key %s unlocked with %v", path, token)
	}
	released := *old
	released.Seq++
	if released.replicated {
		lm.rememberRemoval(path, released.Token, released.ExpiredAtNs)
	}
	delete(lm.locks, path)
	return &released, nil
}

func (lm *LockManager) CleanUp() {
	for {
		time.Sleep(time.Minute)
		lm.cleanUp(time.Now().UnixNano())
	}
}

func (lm *LockManager) cleanUp(now int64) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	for key, lock := range lm.locks {
		if lock != nil && now >= lock.ExpiredAtNs {
			glog.V(4).Infof("key %s expired at %v", key, time.Unix(0, lock.ExpiredAtNs))
			delete(lm.locks, key)
		}
	}
	for key, expires := range lm.removed {
		if now >= expires {
			delete(lm.removed, key)
		}
	}
	if len(lm.removed) == 0 {
		lm.removed = nil
	}
}

// SelectLocks takes out locks by key
// if selectFn returns true, the lock will be removed and returned
func (lm *LockManager) SelectLocks(selectFn func(key string) bool) (locks []*Lock) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	now := time.Now().UnixNano()

	for key, lock := range lm.locks {
		if now > lock.ExpiredAtNs {
			glog.V(4).Infof("key %s expired at %v", key, time.Unix(0, lock.ExpiredAtNs))
			delete(lm.locks, key)
			continue
		}
		if selectFn(key) {
			glog.V(4).Infof("key %s selected and deleted", key)
			delete(lm.locks, key)
			lock.Key = key
			locks = append(locks, lock)
		}
	}
	return
}

// InsertLock inserts a lock from a transfer if it is newer than the current state.
func (lm *LockManager) InsertLock(path string, expiredAtNs int64, token string, prevToken string, owner string, generation int64, seq int64) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if expiredAtNs <= time.Now().UnixNano() || lm.wasRemoved(path, token) {
		return false
	}

	if existing, found := lm.locks[path]; found && existing.ExpiredAtNs > time.Now().UnixNano() {
		if compareMutationVersion(generation, seq, existing.Generation, existing.Seq) <= 0 {
			glog.V(4).Infof("lock %s: rejecting stale transfer gen=%d seq=%d (current gen=%d seq=%d)", path, generation, seq, existing.Generation, existing.Seq)
			return false
		}
		if leaseID(existing.Token) == leaseID(token) {
			expiredAtNs = max(expiredAtNs, existing.ExpiredAtNs)
		}
	}
	lm.locks[path] = &Lock{Token: token, PrevToken: prevToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq, replicated: true}
	lm.advanceGenerationFloor(generation)
	return true
}

// InsertBackupLock inserts or updates a lock as a backup copy.
// It rejects stale mutations by comparing (generation, seq). If a current primary
// already exists on this node, newer replicated state refreshes that primary copy
// without demoting it back to a backup.
func (lm *LockManager) InsertBackupLock(path string, expiredAtNs int64, token string, prevToken string, owner string, generation int64, seq int64) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if expiredAtNs <= time.Now().UnixNano() || lm.wasRemoved(path, token) {
		return
	}

	if existing, found := lm.locks[path]; found && existing.ExpiredAtNs > time.Now().UnixNano() {
		if compareMutationVersion(generation, seq, existing.Generation, existing.Seq) <= 0 {
			glog.V(4).Infof("backup lock %s: rejecting stale gen=%d seq=%d (current gen=%d seq=%d)", path, generation, seq, existing.Generation, existing.Seq)
			return
		}
		if leaseID(existing.Token) == leaseID(token) {
			expiredAtNs = max(expiredAtNs, existing.ExpiredAtNs)
		}
		lm.locks[path] = &Lock{
			Token:       token,
			PrevToken:   prevToken,
			ExpiredAtNs: expiredAtNs,
			Owner:       owner,
			IsBackup:    existing.IsBackup,
			Generation:  generation,
			Seq:         seq,
			replicated:  true,
		}
		lm.advanceGenerationFloor(generation)
		return
	}
	lm.locks[path] = &Lock{Token: token, PrevToken: prevToken, ExpiredAtNs: expiredAtNs, Owner: owner, IsBackup: true, Generation: generation, Seq: seq, replicated: true}
	lm.advanceGenerationFloor(generation)
}

// markReplicated must precede sending a copy. If release won the race, skip
// the send; otherwise unlock will retain this lease's identity until expiry.
func (lm *LockManager) markReplicated(path, token string) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	lock, found := lm.locks[path]
	if !found || leaseID(lock.Token) != leaseID(token) {
		return false
	}
	lock.replicated = true
	return true
}

// RemoveLock removes a lock by key
func (lm *LockManager) RemoveLock(path string) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	// Ownership eviction is not a release: the same live mutation may return
	// in a later transfer. Only completed releases create tombstones.
	delete(lm.locks, path)
}

// RemoveBackupLockIfSeq records a release even if its acquire has not arrived.
// A release can only delete its own lease, never a different primary's lease
// whose local generation may be smaller or equal.
func (lm *LockManager) RemoveBackupLockIfSeq(path string, generation, seq int64, token string, expiredAtNs int64) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	lm.advanceGenerationFloor(generation)
	if existing, found := lm.locks[path]; found {
		// Older primaries omitted release metadata. Preserve ordered releases
		// for their legacy tokens without letting them delete a new lease.
		if token == "" && !strings.Contains(existing.Token, "/") {
			token = existing.Token
		}
		if existing.Generation != generation || leaseID(existing.Token) != leaseID(token) {
			lm.rememberRemoval(path, token, expiredAtNs)
			return false
		}
		if seq < existing.Seq {
			return false
		}
		expiredAtNs = max(expiredAtNs, existing.ExpiredAtNs)
		delete(lm.locks, path)
	}
	lm.rememberRemoval(path, token, expiredAtNs)
	return true
}

// GetLock returns a copy of the lock for a key, if it exists and is not expired
func (lm *LockManager) GetLock(key string) (*Lock, bool) {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	lock, found := lm.locks[key]
	if !found {
		return nil, false
	}
	if time.Now().UnixNano() > lock.ExpiredAtNs {
		return nil, false
	}
	// Return a copy
	cp := *lock
	return &cp, true
}

func (lm *LockManager) GetLockOwner(key string) (owner string, err error) {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	if lock, found := lm.locks[key]; found {
		if time.Now().UnixNano() > lock.ExpiredAtNs {
			err = LockNotFound
			return
		}
		return lock.Owner, nil
	}
	err = LockNotFound
	return
}

// AllLocks returns a copy of all non-expired locks
func (lm *LockManager) AllLocks() []*Lock {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	now := time.Now().UnixNano()
	var result []*Lock
	for key, lock := range lm.locks {
		if now > lock.ExpiredAtNs {
			continue
		}
		cp := *lock
		cp.Key = key
		result = append(result, &cp)
	}
	return result
}

// PromoteLock changes a backup lock to a primary lock
func (lm *LockManager) PromoteLock(key string) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if lock, found := lm.locks[key]; found && lock.IsBackup {
		lock.IsBackup = false
		return true
	}
	return false
}

// DemoteLock changes a primary lock to a backup lock
func (lm *LockManager) DemoteLock(key string) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if lock, found := lm.locks[key]; found && !lock.IsBackup {
		lock.IsBackup = true
		lock.replicated = true
		return true
	}
	return false
}
