package exclusive_locks

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	RenewInterval     = 4 * time.Second
	SafeRenewInterval = 3 * time.Second
	InitLockInterval  = 1 * time.Second
)

type ExclusiveLocker struct {
	token        int64
	lockTsNs     int64
	isLocked     atomic.Bool
	masterClient *wdclient.MasterClient
	lockName     string
	message      string
	clientName   string
	// serializes renew and release RPCs: a renewal in flight during a release
	// would re-create the lock on the master and leave it held until expiry
	mu sync.Mutex
	// Each lock has and only has one goroutine
	renewGoroutineRunning atomic.Bool
}

func NewExclusiveLocker(masterClient *wdclient.MasterClient, lockName string) *ExclusiveLocker {
	return &ExclusiveLocker{
		masterClient: masterClient,
		lockName:     lockName,
	}
}

func (l *ExclusiveLocker) IsLocked() bool {
	return l.isLocked.Load()
}

func (l *ExclusiveLocker) GetToken() (token int64, lockTsNs int64) {
	for time.Unix(0, atomic.LoadInt64(&l.lockTsNs)).Add(SafeRenewInterval).Before(time.Now()) {
		// wait until now is within the safe lock period, no immediate renewal to change the token
		time.Sleep(100 * time.Millisecond)
	}
	return atomic.LoadInt64(&l.token), atomic.LoadInt64(&l.lockTsNs)
}

func (l *ExclusiveLocker) RequestLock(clientName string) {
	if l.isLocked.Load() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// retry to get the lease
	for {
		if err := l.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
			resp, err := client.LeaseAdminToken(ctx, &master_pb.LeaseAdminTokenRequest{
				PreviousToken:    atomic.LoadInt64(&l.token),
				PreviousLockTime: atomic.LoadInt64(&l.lockTsNs),
				LockName:         l.lockName,
				ClientName:       clientName,
			})
			if err == nil {
				atomic.StoreInt64(&l.token, resp.Token)
				atomic.StoreInt64(&l.lockTsNs, resp.LockTsNs)
			}
			return err
		}); err != nil {
			glog.V(2).Infof("Failed to acquire lock %s: %v", l.lockName, err)
			time.Sleep(InitLockInterval)
		} else {
			break
		}
	}

	l.mu.Lock()
	l.clientName = clientName
	l.isLocked.Store(true)
	l.mu.Unlock()
	glog.V(1).Infof("Acquired lock %s", l.lockName)

	// Each lock has and only has one goroutine
	if l.renewGoroutineRunning.CompareAndSwap(false, true) {
		// start a goroutine to renew the lease
		go func() {
			defer l.renewGoroutineRunning.Store(false)
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()

			for {
				if err := l.renewLease(ctx2); err != nil {
					glog.Warningf("Failed to renew lock %s: %v", l.lockName, err)
					l.isLocked.Store(false)
					return
				}
				time.Sleep(RenewInterval)
			}
		}()
	}

}

func (l *ExclusiveLocker) renewLease(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isLocked.Load() {
		return nil
	}
	return l.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.LeaseAdminToken(ctx, &master_pb.LeaseAdminTokenRequest{
			PreviousToken:    atomic.LoadInt64(&l.token),
			PreviousLockTime: atomic.LoadInt64(&l.lockTsNs),
			LockName:         l.lockName,
			ClientName:       l.clientName,
			Message:          l.message,
		})
		if err == nil {
			atomic.StoreInt64(&l.token, resp.Token)
			atomic.StoreInt64(&l.lockTsNs, resp.LockTsNs)
			glog.V(2).Infof("Renewed lock %s: ts %d token %d", l.lockName, l.lockTsNs, l.token)
		}
		return err
	})
}

func (l *ExclusiveLocker) ReleaseLock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.isLocked.Store(false)
	l.clientName = ""

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		client.ReleaseAdminToken(ctx, &master_pb.ReleaseAdminTokenRequest{
			PreviousToken:    atomic.LoadInt64(&l.token),
			PreviousLockTime: atomic.LoadInt64(&l.lockTsNs),
			LockName:         l.lockName,
		})
		return nil
	})
	atomic.StoreInt64(&l.token, 0)
	atomic.StoreInt64(&l.lockTsNs, 0)
}

func (l *ExclusiveLocker) SetMessage(message string) {
	l.message = message
}
