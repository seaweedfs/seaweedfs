package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

type LockClient struct {
	grpcDialOption  grpc.DialOption
	maxLockDuration time.Duration
	sleepDuration   time.Duration
	seedFiler       pb.ServerAddress

	// ring is an optional client-side view of the filer lock hash ring. When
	// populated, a new lock starts at the key's primary filer instead of the
	// seed filer, avoiding the seed->primary forward hop. A stale view stays
	// correct: the filer forwards to the real primary as a fallback.
	ringMu      sync.RWMutex
	ring        *lock_manager.HashRing
	ringVersion int64

	// priorRing is the ring before the most recent change, kept for priorWindow so a
	// route-by-key caller can consult a just-moved key's previous owner during a
	// rebalance. Mirrors the master's lock_manager.LockRing.PriorOwner cooling-off.
	priorRing     *lock_manager.HashRing
	ringChangedAt time.Time
	priorWindow   time.Duration
}

func NewLockClient(grpcDialOption grpc.DialOption, seedFiler pb.ServerAddress) *LockClient {
	return &LockClient{
		grpcDialOption:  grpcDialOption,
		maxLockDuration: 5 * time.Second,
		sleepDuration:   2473 * time.Millisecond,
		seedFiler:       seedFiler,
		priorWindow:     5 * time.Second,
	}
}

// SetRing mirrors the master's LockRingUpdate so the client computes the same
// primary the filers do. A non-zero version at or below the current one is
// ignored once a ring exists, dropping reordered and redundant broadcasts;
// version 0 always applies (bootstrap).
func (lc *LockClient) SetRing(servers []pb.ServerAddress, version int64) {
	lc.ringMu.Lock()
	defer lc.ringMu.Unlock()
	if version != 0 && version <= lc.ringVersion && lc.ring != nil {
		return
	}
	lc.ringVersion = version
	// Build a fresh ring (not an in-place mutation) so the outgoing ring survives as
	// priorRing with its own servers for the cooling-off window.
	newRing := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	newRing.SetServers(servers)
	if lc.ring != nil {
		lc.priorRing = lc.ring
		lc.ringChangedAt = time.Now()
	}
	lc.ring = newRing
}

// hostForKey returns the filer that should own key per the current ring view,
// falling back to the seed filer when no view has been received yet.
func (lc *LockClient) hostForKey(key string) pb.ServerAddress {
	lc.ringMu.RLock()
	defer lc.ringMu.RUnlock()
	if lc.ring == nil {
		return lc.seedFiler
	}
	if primary := lc.ring.GetPrimary(key); primary != "" {
		return primary
	}
	return lc.seedFiler
}

// PrimaryForKey returns the ring owner for key, or "" before any ring arrives.
// Unlike hostForKey it does not fall back to the seed, so a route-by-key caller
// stays on the distributed lock until the ring is known.
func (lc *LockClient) PrimaryForKey(key string) pb.ServerAddress {
	lc.ringMu.RLock()
	defer lc.ringMu.RUnlock()
	if lc.ring == nil {
		return ""
	}
	return lc.ring.GetPrimary(key)
}

// PriorOwnerForKey returns key's owner from the prior ring while a rebalance is
// within the cooling-off window and ownership actually moved, else "". Lets a
// route-by-key reader consult a just-moved key's previous owner before the new
// owner's NotFound is final (the new owner may not have replicated the key yet).
func (lc *LockClient) PriorOwnerForKey(key string) pb.ServerAddress {
	lc.ringMu.RLock()
	defer lc.ringMu.RUnlock()
	if lc.ring == nil || lc.priorRing == nil {
		return ""
	}
	if time.Since(lc.ringChangedAt) > lc.priorWindow {
		return ""
	}
	current := lc.ring.GetPrimary(key)
	prior := lc.priorRing.GetPrimary(key)
	if prior != "" && prior != current {
		return prior
	}
	return ""
}

type LiveLock struct {
	key                 string
	renewToken          string
	expireAtNs          int64
	hostFiler           pb.ServerAddress
	cancelCh            chan struct{}
	renewalDone         chan struct{} // closed when the renewal goroutine exits; nil if there is none
	grpcDialOption      grpc.DialOption
	isLocked            int32 // 0 = unlocked, 1 = locked; use atomic operations
	self                string
	lc                  *LockClient
	owner               string
	lockTTL             time.Duration
	consecutiveFailures int   // Track connection failures to trigger fallback
	generation          int64 // fencing token from the lock server
}

// NewShortLivedLock creates a lock with a 5-second duration
func (lc *LockClient) NewShortLivedLock(key string, owner string) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		hostFiler:      lc.hostForKey(key),
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(5 * time.Second).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		self:           owner,
		lc:             lc,
	}
	lock.retryUntilLocked(5 * time.Second)
	return
}

// NewBlockingLongLivedLock blocks until the lock is acquired, then starts a
// background renewal goroutine that keeps the lock alive. This combines the
// synchronous acquisition of NewShortLivedLock with the auto-renewal of
// StartLongLivedLock. Release with Stop().
func (lc *LockClient) NewBlockingLongLivedLock(key, owner string, lockTTL time.Duration) *LiveLock {
	if lockTTL == 0 {
		lockTTL = lock_manager.LiveLockTTL
	}
	lock := &LiveLock{
		key:            key,
		hostFiler:      lc.hostForKey(key),
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lockTTL).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		self:           owner,
		lc:             lc,
		lockTTL:        lockTTL,
	}
	// Block until acquired
	lock.retryUntilLocked(lockTTL)
	// Start renewal goroutine using a ticker for interruptible sleep
	lock.renewalDone = make(chan struct{})
	go func() {
		defer close(lock.renewalDone)
		renewInterval := lockTTL / 2
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-lock.cancelCh:
				return
			case <-ticker.C:
				if err := lock.AttemptToLock(lockTTL); err != nil {
					glog.V(0).Infof("lock renewal failed for %s: %v", key, err)
					atomic.StoreInt32(&lock.isLocked, 0)
				}
			}
		}
	}()
	return lock
}

// StartLongLivedLock starts a goroutine to lock the key and returns immediately.
// lockTTL specifies how long the lock should be held. The renewal interval is
// automatically derived as lockTTL / 2 to ensure timely renewals.
func (lc *LockClient) StartLongLivedLock(key string, owner string, onLockOwnerChange func(newLockOwner string), lockTTL time.Duration) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		hostFiler:      lc.hostForKey(key),
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lockTTL).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		self:           owner,
		lc:             lc,
		lockTTL:        lockTTL,
	}
	if lock.lockTTL == 0 {
		lock.lockTTL = lock_manager.LiveLockTTL
	}
	lock.renewalDone = make(chan struct{})
	go func() {
		defer close(lock.renewalDone)
		renewInterval := lock.lockTTL / 2
		isLocked := false
		lockOwner := ""
		for {
			// Check for cancellation BEFORE attempting to lock to avoid race condition
			// where Stop() is called after sleep but before lock attempt
			select {
			case <-lock.cancelCh:
				return
			default:
			}

			if isLocked {
				if err := lock.AttemptToLock(lock.lockTTL); err != nil {
					glog.V(0).Infof("Lost lock %s: %v", key, err)
					isLocked = false
					atomic.StoreInt32(&lock.isLocked, 0)
				}
			} else {
				if err := lock.AttemptToLock(lock.lockTTL); err == nil {
					isLocked = true
					// Note: AttemptToLock already sets lock.isLocked atomically on success
				}
			}
			if lockOwner != lock.LockOwner() && lock.LockOwner() != "" {
				glog.V(0).Infof("Lock owner changed from %s to %s", lockOwner, lock.LockOwner())
				onLockOwnerChange(lock.LockOwner())
				lockOwner = lock.LockOwner()
			}
			// Sleep until the next attempt, but wake immediately on Stop() so
			// the goroutine exits and closes renewalDone before Stop()'s bounded
			// wait elapses. An uninterruptible sleep here (up to 5*renewInterval
			// when unlocked) can outlast that wait and break the shutdown
			// synchronization.
			sleepFor := renewInterval
			if !isLocked {
				sleepFor = 5 * renewInterval
			}
			timer := time.NewTimer(sleepFor)
			select {
			case <-lock.cancelCh:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return
}

// retryUntilLocked blocks until the lock is acquired, polling at the steady
// short cadence that AttemptToLock already enforces on contention (~1s). It
// deliberately avoids util.RetryUntil's exponential backoff (which grows to
// several seconds): when a holder on another mount releases the lock, the
// waiter must pick it up promptly, otherwise cross-mount write handoff stalls
// long enough to time out clients.
func (lock *LiveLock) retryUntilLocked(lockDuration time.Duration) {
	for lock.renewToken == "" {
		if err := lock.AttemptToLock(lockDuration); err != nil {
			glog.V(1).Infof("create lock %s: %v", lock.key, err)
		}
	}
}

func (lock *LiveLock) AttemptToLock(lockDuration time.Duration) error {
	glog.V(4).Infof("LOCK: AttemptToLock key=%s owner=%s", lock.key, lock.self)
	errorMessage, err := lock.doLock(lockDuration)
	if err != nil {
		glog.V(1).Infof("LOCK: doLock failed for key=%s: %v", lock.key, err)
		time.Sleep(time.Second)
		return err
	}
	if errorMessage != "" {
		if strings.Contains(errorMessage, "lock already owned") {
			glog.V(3).Infof("LOCK: doLock returned error message for key=%s: %s", lock.key, errorMessage)
		} else {
			glog.V(2).Infof("LOCK: doLock returned error message for key=%s: %s", lock.key, errorMessage)
		}
		time.Sleep(time.Second)
		return fmt.Errorf("%v", errorMessage)
	}
	if atomic.LoadInt32(&lock.isLocked) == 0 {
		// Only log when transitioning from unlocked to locked
		glog.V(1).Infof("LOCK: Successfully acquired key=%s owner=%s", lock.key, lock.self)
	}
	atomic.StoreInt32(&lock.isLocked, 1)
	return nil
}

func (lock *LiveLock) StopShortLivedLock() error {
	if atomic.LoadInt32(&lock.isLocked) == 0 {
		return nil
	}
	defer func() {
		atomic.StoreInt32(&lock.isLocked, 0)
	}()
	return pb.WithFilerClient(false, 0, lock.hostFiler, lock.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DistributedUnlock(context.Background(), &filer_pb.UnlockRequest{
			Name:       lock.key,
			RenewToken: lock.renewToken,
		})
		return err
	})
}

// Stop stops a long-lived lock by closing the cancel channel and releasing the lock
func (lock *LiveLock) Stop() error {
	// Close the cancel channel to stop the long-lived lock goroutine
	select {
	case <-lock.cancelCh:
		// Already closed
	default:
		close(lock.cancelCh)
	}

	// Wait for the renewal goroutine to fully exit before unlocking. A renewal
	// in flight when we close cancelCh rotates renewToken on the server; if we
	// then unlock with the token we read here, the unlock fails with a token
	// mismatch and the lock lingers until its TTL expires — blocking other
	// mounts waiting on the same file. Waiting for the goroutine to return also
	// makes the renewToken read below race-free (channel close = happens-before).
	if lock.renewalDone != nil {
		select {
		case <-lock.renewalDone:
		case <-time.After(lock.lockTTL + 2*time.Second):
			// The renewal goroutine is wedged, almost certainly in a stuck
			// renewal RPC. Do not unlock here: the renewToken may be rotated
			// when that RPC finally returns, so an unlock sent now could race
			// it, be rejected on a stale token, and leave the lock lingering
			// anyway. cancelCh is closed, so the goroutine stops renewing once
			// its in-flight call returns and the lock then expires within its
			// TTL on its own.
			glog.Warningf("lock %s: renewal goroutine still running at shutdown; letting lock expire via TTL", lock.key)
			return nil
		}
	}

	// Also release the lock if held
	// Note: We intentionally don't clear renewToken here because
	// StopShortLivedLock needs it to properly unlock
	return lock.StopShortLivedLock()
}

func (lock *LiveLock) doLock(lockDuration time.Duration) (errorMessage string, err error) {
	glog.V(4).Infof("LOCK: doLock calling DistributedLock - key=%s filer=%s owner=%s",
		lock.key, lock.hostFiler, lock.self)

	previousHostFiler := lock.hostFiler
	previousOwner := lock.owner

	err = pb.WithFilerClient(false, 0, lock.hostFiler, lock.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DistributedLock(context.Background(), &filer_pb.LockRequest{
			Name:          lock.key,
			SecondsToLock: int64(lockDuration.Seconds()),
			RenewToken:    lock.renewToken,
			IsMoved:       false,
			Owner:         lock.self,
		})
		glog.V(4).Infof("LOCK: DistributedLock response - key=%s err=%v", lock.key, err)
		if err == nil && resp != nil {
			lock.renewToken = resp.RenewToken
			if resp.Generation > 0 {
				atomic.StoreInt64(&lock.generation, resp.Generation)
			}
			lock.consecutiveFailures = 0 // Reset failure counter on success
			glog.V(4).Infof("LOCK: Got renewToken for key=%s", lock.key)
		} else {
			//this can be retried. Need to remember the last valid renewToken
			lock.renewToken = ""
			glog.V(1).Infof("LOCK: Cleared renewToken for key=%s (err=%v)", lock.key, err)
		}
		if resp != nil {
			errorMessage = resp.Error
			if resp.LockHostMovedTo != "" && !pb.ServerAddress(resp.LockHostMovedTo).Equals(previousHostFiler) {
				// Only log if the host actually changed
				glog.V(2).Infof("LOCK: Host changed from %s to %s for key=%s", previousHostFiler, resp.LockHostMovedTo, lock.key)
				lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
				// Don't update seedFiler - keep original for fallback
			} else if resp.LockHostMovedTo != "" {
				lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
			}
			if resp.LockOwner != "" && resp.LockOwner != previousOwner {
				// Only log if the owner actually changed
				glog.V(2).Infof("LOCK: Owner changed from %s to %s for key=%s", previousOwner, resp.LockOwner, lock.key)
				lock.owner = resp.LockOwner
			} else if resp.LockOwner != "" {
				lock.owner = resp.LockOwner
			} else if previousOwner != "" {
				glog.V(2).Infof("LOCK: Owner cleared for key=%s", lock.key)
				lock.owner = ""
			}
		}
		return err
	})

	if err != nil && !lock.hostFiler.Equals(lock.lc.seedFiler) {
		lock.consecutiveFailures++
		// Fall back to seed filer after 3 consecutive connection failures
		if lock.consecutiveFailures >= 3 {
			glog.V(0).Infof("LOCK: Connection failed %d times for key=%s filer=%s, falling back to seed filer=%s",
				lock.consecutiveFailures, lock.key, lock.hostFiler, lock.lc.seedFiler)
			lock.hostFiler = lock.lc.seedFiler
			lock.consecutiveFailures = 0
			lock.renewToken = ""
		}
	}

	return
}

func (lock *LiveLock) LockOwner() string {
	return lock.owner
}

// Generation returns the fencing token for this lock.
// It increments on each fresh acquisition and stays the same on renewal.
func (lock *LiveLock) Generation() int64 {
	return atomic.LoadInt64(&lock.generation)
}

// IsLocked returns true if this instance currently holds the lock
func (lock *LiveLock) IsLocked() bool {
	return atomic.LoadInt32(&lock.isLocked) == 1
}
