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
}

func NewLockClient(grpcDialOption grpc.DialOption) *LockClient {
	return &LockClient{
		grpcDialOption:  grpcDialOption,
		maxLockDuration: 5 * time.Second,
		sleepDuration:   4 * time.Millisecond,
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
}

// NewLockWithTimeout locks the key with the given duration
func (lc *LockClient) NewLockWithTimeout(filer pb.ServerAddress, key string, lockDuration time.Duration) (lock *LiveLock) {
	return lc.doNewLock(filer, key, lockDuration)
}

// NewLock creates a lock with a very long duration
func (lc *LockClient) NewLock(filer pb.ServerAddress, key string) (lock *LiveLock) {
	return lc.doNewLock(filer, key, lock_manager.MaxDuration)
}

func (lc *LockClient) doNewLock(filer pb.ServerAddress, key string, lockDuration time.Duration) (lock *LiveLock) {
	lock = &LiveLock{
		key:            key,
		filer:          filer,
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lockDuration).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
	}
	var needRenewal bool
	if lockDuration > lc.maxLockDuration {
		lockDuration = lc.maxLockDuration
		needRenewal = true
	}
	util.RetryForever("create lock:"+key, func() error {
		errorMessage, err := lock.doLock(lockDuration)
		if err != nil {
			return err
		}
		if errorMessage != "" {
			return fmt.Errorf("%v", errorMessage)
		}
		return nil
	}, func(err error) (shouldContinue bool) {
		if err != nil {
			glog.Warningf("create lock %s: %s", key, err)
		}
		return lock.renewToken == ""
	})

	lock.isLocked = true

	if needRenewal {
		go lc.keepLock(lock)
	}

	return
}

func (lock *LiveLock) IsLocked() bool {
	return lock.isLocked
}

func (lock *LiveLock) Unlock() error {
	close(lock.cancelCh)
	return pb.WithFilerClient(false, 0, lock.filer, lock.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.Unlock(context.Background(), &filer_pb.UnlockRequest{
			Name:       lock.key,
			RenewToken: lock.renewToken,
		})
		return err
	})
}

func (lc *LockClient) keepLock(lock *LiveLock) {
	for {
		select {
		case <-time.After(lc.sleepDuration):
			// renew the lock if lock.expireAtNs is still greater than now
			util.RetryForever("keep lock:"+lock.key, func() error {
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
					return err
				}
				if errorMessage != "" {
					lock.isLocked = false
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
		resp, err := client.Lock(context.Background(), &filer_pb.LockRequest{
			Name:          lock.key,
			SecondsToLock: int64(lockDuration.Seconds()),
			RenewToken:    lock.renewToken,
			IsMoved:       false,
		})
		if err == nil {
			lock.renewToken = resp.RenewToken
		}
		if resp != nil {
			errorMessage = resp.Error
			if resp.MovedTo != "" {
				lock.filer = pb.ServerAddress(resp.MovedTo)
			}
		}
		return err
	})
	return
}
