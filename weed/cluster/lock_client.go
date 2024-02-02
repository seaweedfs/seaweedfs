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
	filer          pb.ServerAddress
	cancelCh       chan struct{}
	grpcDialOption grpc.DialOption
	isLocked       bool
	owner          string
	lc             *LockClient
}

// NewShortLivedLock creates a lock with a 5-second duration
func (lc *LockClient) NewShortLivedLock(key string, owner string) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		filer:          lc.seedFiler,
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(5*time.Second).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		owner:          owner,
		lc:             lc,
	}
	lock.retryUntilLocked(5*time.Second)
	return
}

// StartLock starts a goroutine to lock the key and returns immediately.
func (lc *LockClient) StartLock(key string, owner string) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		filer:          lc.seedFiler,
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lock_manager.MaxDuration).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		owner:          owner,
		lc:             lc,
	}
	go func() {
		lock.retryUntilLocked(lock_manager.MaxDuration)
		lc.keepLock(lock)
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

func (lock *LiveLock) IsLocked() bool {
	return lock!=nil && lock.isLocked
}

func (lock *LiveLock) StopShortLivedLock() error {
	if !lock.isLocked {
		return nil
	}
	defer func() {
		lock.isLocked = false
	}()
	return pb.WithFilerClient(false, 0, lock.filer, lock.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DistributedUnlock(context.Background(), &filer_pb.UnlockRequest{
			Name:       lock.key,
			RenewToken: lock.renewToken,
		})
		return err
	})
}

func (lc *LockClient) keepLock(lock *LiveLock) {
	ticker := time.Tick(lc.sleepDuration)
	for {
		select {
		case <-ticker:
			// renew the lock if lock.expireAtNs is still greater than now
			util.RetryUntil("keep lock:"+lock.key, func() error {
				lockDuration := time.Duration(lock.expireAtNs-time.Now().UnixNano()) * time.Nanosecond
				if lockDuration > lc.maxLockDuration {
					lockDuration = lc.maxLockDuration
				}
				if lockDuration <= 0 {
					return nil
				}

				errorMessage, err := lock.doLock(lockDuration)
				if err != nil {
					lock.isLocked = false
					time.Sleep(time.Second)
					glog.V(0).Infof("keep lock %s: %v", lock.key, err)
					return err
				}
				if errorMessage != "" {
					lock.isLocked = false
					time.Sleep(time.Second)
					glog.V(4).Infof("keep lock message %s: %v", lock.key, errorMessage)
					return fmt.Errorf("keep lock error: %v", errorMessage)
				}
				return nil
			}, func(err error) (shouldContinue bool) {
				if err == nil {
					return false
				}
				glog.Warningf("keep lock %s: %v", lock.key, err)
				return true
			})
			if !lock.isLocked {
				return
			}
		case <-lock.cancelCh:
			return
		}
	}
}

func (lock *LiveLock) doLock(lockDuration time.Duration) (errorMessage string, err error) {
	err = pb.WithFilerClient(false, 0, lock.filer, lock.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.DistributedLock(context.Background(), &filer_pb.LockRequest{
			Name:          lock.key,
			SecondsToLock: int64(lockDuration.Seconds()),
			RenewToken:    lock.renewToken,
			IsMoved:       false,
			Owner:         lock.owner,
		})
		if err == nil {
			lock.renewToken = resp.RenewToken
		}
		if resp != nil {
			errorMessage = resp.Error
			if resp.MovedTo != "" {
				lock.filer = pb.ServerAddress(resp.MovedTo)
				lock.lc.seedFiler = lock.filer
			}
		}
		return err
	})
	return
}
