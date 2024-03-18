package cluster

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"time"
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
	errorMessage, err := lock.doLock(lockDuration)
	if err != nil {
		time.Sleep(time.Second)
		return err
	}
	if errorMessage != "" {
		time.Sleep(time.Second)
		return fmt.Errorf("%v", errorMessage)
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

func (lock *LiveLock) doLock(lockDuration time.Duration) (errorMessage string, err error) {
	err = pb.WithFilerClient(false, 0, lock.hostFiler, lock.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DistributedLock(context.Background(), &filer_pb.LockRequest{
			Name:          lock.key,
			SecondsToLock: int64(lockDuration.Seconds()),
			RenewToken:    lock.renewToken,
			IsMoved:       false,
			Owner:         lock.self,
		})
		if err == nil && resp != nil {
			lock.renewToken = resp.RenewToken
		} else {
			//this can be retried. Need to remember the last valid renewToken
			lock.renewToken = ""
		}
		if resp != nil {
			errorMessage = resp.Error
			if resp.LockHostMovedTo != "" {
				lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
				lock.lc.seedFiler = lock.hostFiler
			}
			if resp.LockOwner != "" {
				lock.owner = resp.LockOwner
				// fmt.Printf("lock %s owner: %s\n", lock.key, lock.owner)
			} else {
				// fmt.Printf("lock %s has no owner\n", lock.key)
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
