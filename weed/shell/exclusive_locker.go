package shell

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

const (
	RenewInteval     = 4 * time.Second
	SafeRenewInteval = 3 * time.Second
)

type ExclusiveLocker struct {
	masterClient *wdclient.MasterClient
	token        int64
	lockTsNs     int64
	isLocking    bool
}

func NewExclusiveLocker(masterClient *wdclient.MasterClient) *ExclusiveLocker {
	return &ExclusiveLocker{
		masterClient: masterClient,
	}
}

func (l *ExclusiveLocker) GetToken() (token int64, lockTsNs int64) {
	for time.Unix(0, atomic.LoadInt64(&l.lockTsNs)).Add(SafeRenewInteval).Before(time.Now()) {
		// wait until now is within the safe lock period, no immediate renewal to change the token
		time.Sleep(100 * time.Millisecond)
	}
	return atomic.LoadInt64(&l.token), atomic.LoadInt64(&l.lockTsNs)
}

func (l *ExclusiveLocker) Lock() {
	// retry to get the lease
	for {
		if err := l.masterClient.WithClient(func(client master_pb.SeaweedClient) error {
			resp, err := client.LeaseAdminToken(context.Background(), &master_pb.LeaseAdminTokenRequest{
				PreviousToken:    atomic.LoadInt64(&l.token),
				PreviousLockTime: atomic.LoadInt64(&l.lockTsNs),
			})
			if err == nil {
				atomic.StoreInt64(&l.token, resp.Token)
				atomic.StoreInt64(&l.lockTsNs, resp.LockTsNs)
			}
			return err
		}); err != nil {
			time.Sleep(RenewInteval)
		} else {
			break
		}
	}

	l.isLocking = true

	// start a goroutine to renew the lease
	go func() {
		for l.isLocking {
			if err := l.masterClient.WithClient(func(client master_pb.SeaweedClient) error {
				resp, err := client.LeaseAdminToken(context.Background(), &master_pb.LeaseAdminTokenRequest{
					PreviousToken:    atomic.LoadInt64(&l.token),
					PreviousLockTime: atomic.LoadInt64(&l.lockTsNs),
				})
				if err == nil {
					atomic.StoreInt64(&l.token, resp.Token)
					atomic.StoreInt64(&l.lockTsNs, resp.LockTsNs)
				}
				return err
			}); err != nil {
				glog.Error("failed to renew lock: %v", err)
				return
			} else {
				time.Sleep(RenewInteval)
			}

		}
	}()

}

func (l *ExclusiveLocker) Unlock() {
	l.isLocking = false
	l.masterClient.WithClient(func(client master_pb.SeaweedClient) error {
		client.ReleaseAdminToken(context.Background(), &master_pb.ReleaseAdminTokenRequest{
			PreviousToken:    atomic.LoadInt64(&l.token),
			PreviousLockTime: atomic.LoadInt64(&l.lockTsNs),
		})
		return nil
	})
}
