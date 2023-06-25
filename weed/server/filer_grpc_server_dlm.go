package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"time"
)

// Lock is a grpc handler to handle FilerServer's LockRequest
func (fs *FilerServer) Lock(ctx context.Context, req *filer_pb.LockRequest) (resp *filer_pb.LockResponse, err error) {

	resp = &filer_pb.LockResponse{}
	snapshot := fs.filer.LockRing.GetSnapshot()
	if snapshot == nil {
		resp.Error = "no lock server found"
		return
	} else {
		server := lock_manager.HashKeyToServer(req.Name, snapshot)
		if server != fs.option.Host {
			resp.Error = fmt.Sprintf("not the lock server for %s", req.Name)
			resp.MovedTo = string(server)
			return
		}
	}

	renewToken, err := fs.dlm.Lock(req.Name, time.Duration(req.SecondsToLock)*time.Second, req.PreviousLockToken)

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	} else {
		resp.RenewToken = renewToken
	}

	return resp, nil
}

// Unlock is a grpc handler to handle FilerServer's UnlockRequest
func (fs *FilerServer) Unlock(ctx context.Context, req *filer_pb.UnlockRequest) (resp *filer_pb.UnlockResponse, err error) {

	resp = &filer_pb.UnlockResponse{}
	snapshot := fs.filer.LockRing.GetSnapshot()
	if snapshot == nil {
		resp.Error = "no lock server found"
		return
	} else {
		server := lock_manager.HashKeyToServer(req.Name, snapshot)
		if server != fs.option.Host {
			resp.Error = fmt.Sprintf("not the lock server for %s", req.Name)
			resp.MovedTo = string(server)
			return
		}
	}

	_, err = fs.dlm.Unlock(req.Name, req.LockToken)
	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	}

	return resp, nil

}

// TransferLocks is a grpc handler to handle FilerServer's TransferLocksRequest
func (fs *FilerServer) TransferLocks(ctx context.Context, req *filer_pb.TransferLocksRequest) (*filer_pb.TransferLocksResponse, error) {

	now := time.Now()
	for _, lock := range req.Locks {
		duration := time.Duration(lock.ExpirationNs - now.UnixNano())
		if _, err := fs.dlm.Lock(lock.Name, duration, lock.RenewToken); err != nil {
			glog.Errorf("receive transferred lock %v to %v: %v", lock.Name, fs.option.Host, err)
			return nil, err
		}
	}

	return &filer_pb.TransferLocksResponse{}, nil

}

func (fs *FilerServer) OnDlmChangeSnapshot(snapshot []pb.ServerAddress) {
	locks := fs.dlm.TakeOutLocksByKey(func(key string) bool {
		server := lock_manager.HashKeyToServer(key, snapshot)
		return server != fs.option.Host
	})
	if len(locks) == 0 {
		return
	}

	for _, lock := range locks {
		server := lock_manager.HashKeyToServer(lock.Key, snapshot)
		if err := pb.WithFilerClient(false, 0, server, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.TransferLocks(context.Background(), &filer_pb.TransferLocksRequest{
				Locks: []*filer_pb.Lock{
					{
						Name:         lock.Key,
						RenewToken:   lock.Token,
						ExpirationNs: lock.ExpirationNs,
					},
				},
			})
			return err
		}); err != nil {
			// it may not be worth retrying, since the lock may have expired
			glog.Errorf("transfer lock %v to %v: %v", lock.Key, server, err)
		}
	}

}
