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

// NewLock creates a lock with a very long duration
func (lc *LockClient) NewLock(key string, owner string) (lock *LiveLock) {
	return lc.doNewLock(key, lock_manager.MaxDuration, owner)
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
		util.RetryUntil("create lock:"+key, func() error {
			errorMessage, err := lock.doLock(lock_manager.MaxDuration)
			if err != nil {
				glog.Infof("create lock %s: %s", key, err)
				time.Sleep(time.Second)
				return err
			}
			if errorMessage != "" {
				glog.Infof("create lock %s: %s", key, errorMessage)
				time.Sleep(time.Second)
				return fmt.Errorf("%v", errorMessage)
			}
			lock.isLocked = true
			return nil
		}, func(err error) (shouldContinue bool) {
			if err != nil {
				glog.Warningf("create lock %s: %s", key, err)
				time.Sleep(time.Second)
			}
			return lock.renewToken == ""
		})
		lc.keepLock(lock)
	}()
	return
}

func (lc *LockClient) doNewLock(key string, lockDuration time.Duration, owner string) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		filer:          lc.seedFiler,
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lockDuration).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		owner:          owner,
		lc:             lc,
	}
	var needRenewal bool
	if lockDuration > lc.maxLockDuration {
		lockDuration = lc.maxLockDuration
		needRenewal = true
	}
	util.RetryUntil("create lock:"+key, func() error {
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
	}, func(err error) (shouldContinue bool) {
		if err != nil {
			glog.Warningf("create lock %s: %s", key, err)
		}
		return lock.renewToken == ""
	})

	if needRenewal {
		go lc.keepLock(lock)
	}

	return
}

func (lock *LiveLock) IsLocked() bool {
	return lock.isLocked
}

func (lock *LiveLock) StopLock() error {
	close(lock.cancelCh)
	if !lock.isLocked {
		return nil
	}
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
					return err
				}
				if errorMessage != "" {
					lock.isLocked = false
					time.Sleep(time.Second)
					return fmt.Errorf("%v", errorMessage)
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
