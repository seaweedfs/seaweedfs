package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

/*
How exclusive lock works?
-----------

Shell
------
When shell lock,
  * lease an admin token (lockTime, token)
  * start a goroutine to renew the admin token periodically

When shell unlock
  * stop the renewal goroutine
  * sends a release lock request

Master
------
Master maintains:
  * randomNumber
  * lastLockTime
When master receives the lease/renew request from shell
  If lastLockTime still fresh {
    if is a renew and token is valid {
      // for renew
      generate the randomNumber => token
      return
    }
    refuse
    return
  } else {
    // for fresh lease request
    generate the randomNumber => token
    return
  }

When master receives the release lock request from shell
  set the lastLockTime to zero


The volume server does not need to verify.
This makes the lock/unlock optional, similar to what golang code usually does.

*/

const (
	LockDuration = 10 * time.Second
)

type AdminLock struct {
	accessSecret   int64
	accessLockTime time.Time
	lastClient     string
	lastMessage    string
}

type AdminLocks struct {
	locks map[string]*AdminLock
	sync.RWMutex
}

func NewAdminLocks() *AdminLocks {
	return &AdminLocks{
		locks: make(map[string]*AdminLock),
	}
}

func (locks *AdminLocks) isLocked(lockName string) (clientName string, message string, isLocked bool) {
	locks.RLock()
	defer locks.RUnlock()
	adminLock, found := locks.locks[lockName]
	if !found {
		return "", "", false
	}
	glog.V(4).Infof("isLocked %v: %v", adminLock.lastClient, adminLock.lastMessage)
	return adminLock.lastClient, adminLock.lastMessage, adminLock.accessLockTime.Add(LockDuration).After(time.Now())
}

func (locks *AdminLocks) isValidToken(lockName string, ts time.Time, token int64) bool {
	locks.RLock()
	defer locks.RUnlock()
	adminLock, found := locks.locks[lockName]
	if !found {
		return false
	}
	return adminLock.accessLockTime.Equal(ts) && adminLock.accessSecret == token
}

func (locks *AdminLocks) generateToken(lockName string, clientName string) (ts time.Time, token int64) {
	locks.Lock()
	defer locks.Unlock()
	lock := &AdminLock{
		accessSecret:   rand.Int64(),
		accessLockTime: time.Now(),
		lastClient:     clientName,
	}
	locks.locks[lockName] = lock
	stats.MasterAdminLock.WithLabelValues(clientName).Set(1)
	return lock.accessLockTime, lock.accessSecret
}

func (locks *AdminLocks) deleteLock(lockName string) {
	locks.Lock()
	stats.MasterAdminLock.WithLabelValues(locks.locks[lockName].lastClient).Set(0)
	defer locks.Unlock()
	delete(locks.locks, lockName)
}

func (ms *MasterServer) LeaseAdminToken(ctx context.Context, req *master_pb.LeaseAdminTokenRequest) (*master_pb.LeaseAdminTokenResponse, error) {
	resp := &master_pb.LeaseAdminTokenResponse{}

	if !ms.Topo.IsLeader() {
		return resp, raft.NotLeaderError
	}

	if lastClient, lastMessage, isLocked := ms.adminLocks.isLocked(req.LockName); isLocked {
		glog.V(4).Infof("LeaseAdminToken %v", lastClient)
		if req.PreviousToken != 0 && ms.adminLocks.isValidToken(req.LockName, time.Unix(0, req.PreviousLockTime), req.PreviousToken) {
			// for renew
			ts, token := ms.adminLocks.generateToken(req.LockName, req.ClientName)
			resp.Token, resp.LockTsNs = token, ts.UnixNano()
			return resp, nil
		}
		// refuse since still locked
		return resp, fmt.Errorf("already locked by %v: %v", lastClient, lastMessage)
	}
	// for fresh lease request
	ts, token := ms.adminLocks.generateToken(req.LockName, req.ClientName)
	resp.Token, resp.LockTsNs = token, ts.UnixNano()
	return resp, nil
}

func (ms *MasterServer) ReleaseAdminToken(ctx context.Context, req *master_pb.ReleaseAdminTokenRequest) (*master_pb.ReleaseAdminTokenResponse, error) {
	resp := &master_pb.ReleaseAdminTokenResponse{}
	if ms.adminLocks.isValidToken(req.LockName, time.Unix(0, req.PreviousLockTime), req.PreviousToken) {
		ms.adminLocks.deleteLock(req.LockName)
	}
	return resp, nil
}

func (ms *MasterServer) Ping(ctx context.Context, req *master_pb.PingRequest) (resp *master_pb.PingResponse, pingErr error) {
	resp = &master_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(),
	}
	if req.TargetType == cluster.FilerType {
		pingErr = pb.WithFilerClient(false, 0, pb.ServerAddress(req.Target), ms.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			pingResp, err := client.Ping(ctx, &filer_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if req.TargetType == cluster.VolumeServerType {
		pingErr = pb.WithVolumeServerClient(false, pb.ServerAddress(req.Target), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if req.TargetType == cluster.MasterType {
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), ms.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			pingResp, err := client.Ping(ctx, &master_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if pingErr != nil {
		pingErr = fmt.Errorf("ping %s %s: %v", req.TargetType, req.Target, pingErr)
	}
	resp.StopTimeNs = time.Now().UnixNano()
	return
}
