package cluster

import (
	"context"
	"fmt"
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
	isLocked       bool
	self           string
	lc             *LockClient
	owner          string
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
func (lc *LockClient) StartLongLivedLock(key string, owner string, onLockOwnerChange func(newLockOwner string)) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		hostFiler:      lc.seedFiler,
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lock_manager.LiveLockTTL).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		self:           owner,
		lc:             lc,
	}
	go func() {
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
				if err := lock.AttemptToLock(lock_manager.LiveLockTTL); err != nil {
					glog.V(0).Infof("Lost lock %s: %v", key, err)
					isLocked = false
				}
			} else {
				if err := lock.AttemptToLock(lock_manager.LiveLockTTL); err == nil {
					isLocked = true
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
				time.Sleep(lock_manager.RenewInterval)
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
		glog.V(1).Infof("LOCK: doLock returned error message for key=%s: %s", lock.key, errorMessage)
		time.Sleep(time.Second)
		return fmt.Errorf("%v", errorMessage)
	}
	if !lock.isLocked {
		// Only log when transitioning from unlocked to locked
		glog.V(1).Infof("LOCK: Successfully acquired key=%s owner=%s", lock.key, lock.self)
	}
	lock.isLocked = true
	return nil
}

func (lock *LiveLock) StopShortLivedLock() error {
	if !lock.isLocked {
		return nil
	}
	defer func() {
		lock.isLocked = false
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
				glog.V(1).Infof("LOCK: Host changed from %s to %s for key=%s", previousHostFiler, resp.LockHostMovedTo, lock.key)
				lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
				lock.lc.seedFiler = lock.hostFiler
			} else if resp.LockHostMovedTo != "" {
				lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
			}
			if resp.LockOwner != "" && resp.LockOwner != previousOwner {
				// Only log if the owner actually changed
				glog.V(1).Infof("LOCK: Owner changed from %s to %s for key=%s", previousOwner, resp.LockOwner, lock.key)
				lock.owner = resp.LockOwner
			} else if resp.LockOwner != "" {
				lock.owner = resp.LockOwner
			} else if previousOwner != "" {
				glog.V(1).Infof("LOCK: Owner cleared for key=%s", lock.key)
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
