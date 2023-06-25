package weed_server

import (
	"context"
	"fmt"
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
	}

	var movedTo pb.ServerAddress
	expiredAtNs := time.Now().Add(time.Duration(req.SecondsToLock) * time.Second).UnixNano()
	resp.RenewToken, movedTo, err = fs.dlm.Lock(fs.option.Host, req.Name, expiredAtNs, req.PreviousLockToken, snapshot)

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	}
	if movedTo != "" {
		resp.MovedTo = string(movedTo)
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
	}

	_, err = fs.dlm.Unlock(fs.option.Host, req.Name, req.LockToken, snapshot)
	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	}

	return resp, nil

}

// TransferLocks is a grpc handler to handle FilerServer's TransferLocksRequest
func (fs *FilerServer) TransferLocks(ctx context.Context, req *filer_pb.TransferLocksRequest) (*filer_pb.TransferLocksResponse, error) {

	for _, lock := range req.Locks {
		fs.dlm.InsertLock(lock.Name, lock.ExpiredAtNs, lock.RenewToken)
	}

	return &filer_pb.TransferLocksResponse{}, nil

}

func (fs *FilerServer) OnDlmChangeSnapshot(snapshot []pb.ServerAddress) {
	locks := fs.dlm.SelectNotOwnedLocks(fs.option.Host, snapshot)
	if len(locks) == 0 {
		return
	}

	for _, lock := range locks {
		server := fs.dlm.CalculateTargetServer(lock.Key, snapshot)
		if err := pb.WithFilerClient(false, 0, server, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.TransferLocks(context.Background(), &filer_pb.TransferLocksRequest{
				Locks: []*filer_pb.Lock{
					{
						Name:        lock.Key,
						RenewToken:  lock.Token,
						ExpiredAtNs: lock.ExpiredAtNs,
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
