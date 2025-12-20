package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type LockClient struct {
	grpcDialOption  grpc.DialOption
	maxLockDuration time.Duration
	sleepDuration   time.Duration
	seedFiler       pb.ServerAddress
}

func NewLockClient(grpcDialOption grpc.DialOption, seedFiler pb.ServerAddress) *LockClient {
	return &LockClient{
		grpcDialOption:  grpcDialOption,
		maxLockDuration: 5 * time.Second,
		sleepDuration:   2473 * time.Millisecond,
		seedFiler:       seedFiler,
	}
}

type LiveLock struct {
	key            string
	renewToken     string
	expireAtNs     int64
	hostFiler      pb.ServerAddress
	cancelCh       chan struct{}
	grpcDialOption grpc.DialOption
	isLocked       int32 // 0 = unlocked, 1 = locked; use atomic operations
	self           string
	lc             *LockClient
	owner          string
	lockTTL        time.Duration
}

// NewShortLivedLock creates a lock with a 5-second duration
func (lc *LockClient) NewShortLivedLock(key string, owner string) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		hostFiler:      lc.seedFiler,
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(5 * time.Second).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		self:           owner,
		lc:             lc,
	}
	lock.retryUntilLocked(5 * time.Second)
	return
}

// StartLongLivedLock starts a goroutine to lock the key and returns immediately.
// lockTTL specifies how long the lock should be held. The renewal interval is
// automatically derived as lockTTL / 2 to ensure timely renewals.
func (lc *LockClient) StartLongLivedLock(key string, owner string, onLockOwnerChange func(newLockOwner string), lockTTL time.Duration) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		hostFiler:      lc.seedFiler,
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
	go func() {
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
			select {
			case <-lock.cancelCh:
				return
			default:
				if isLocked {
					time.Sleep(renewInterval / 2)
				} else {
					time.Sleep(renewInterval)
				}
			}
		}
	}()
	return
}

func (lock *LiveLock) retryUntilLocked(lockDuration time.Duration) {
	util.RetryUntil("create lock:"+lock.key, func() error {
		return lock.AttemptToLock(lockDuration)
	}, func(err error) (shouldContinue bool) {
		if err != nil {
			glog.Warningf("create lock %s: %s", lock.key, err)
		}
		return lock.renewToken == ""
	})
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

	// Wait a brief moment for the goroutine to see the closed channel
	// This reduces the race condition window where the goroutine might
	// attempt one more lock operation after we've released the lock
	time.Sleep(10 * time.Millisecond)

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
			glog.V(4).Infof("LOCK: Got renewToken for key=%s", lock.key)
		} else {
			//this can be retried. Need to remember the last valid renewToken
			lock.renewToken = ""
			glog.V(1).Infof("LOCK: Cleared renewToken for key=%s (err=%v)", lock.key, err)
		}
		if resp != nil {
			errorMessage = resp.Error
			if resp.LockHostMovedTo != "" && resp.LockHostMovedTo != string(previousHostFiler) {
				// Only log if the host actually changed
				glog.V(2).Infof("LOCK: Host changed from %s to %s for key=%s", previousHostFiler, resp.LockHostMovedTo, lock.key)
				lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
				lock.lc.seedFiler = lock.hostFiler
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
	return
}

func (lock *LiveLock) LockOwner() string {
	return lock.owner
}

// IsLocked returns true if this instance currently holds the lock
func (lock *LiveLock) IsLocked() bool {
	return atomic.LoadInt32(&lock.isLocked) == 1
}
