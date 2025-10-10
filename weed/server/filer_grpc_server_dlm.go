package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DistributedLock is a grpc handler to handle FilerServer's LockRequest
func (fs *FilerServer) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (resp *filer_pb.LockResponse, err error) {

	glog.V(4).Infof("FILER LOCK: Received DistributedLock request - name=%s owner=%s renewToken=%s secondsToLock=%d isMoved=%v",
		req.Name, req.Owner, req.RenewToken, req.SecondsToLock, req.IsMoved)

	resp = &filer_pb.LockResponse{}

	var movedTo pb.ServerAddress
	expiredAtNs := time.Now().Add(time.Duration(req.SecondsToLock) * time.Second).UnixNano()
	resp.LockOwner, resp.RenewToken, movedTo, err = fs.filer.Dlm.LockWithTimeout(req.Name, expiredAtNs, req.RenewToken, req.Owner)
	glog.V(4).Infof("FILER LOCK: LockWithTimeout result - name=%s lockOwner=%s renewToken=%s movedTo=%s err=%v",
		req.Name, resp.LockOwner, resp.RenewToken, movedTo, err)
	glog.V(4).Infof("lock %s %v %v %v, isMoved=%v %v", req.Name, req.SecondsToLock, req.RenewToken, req.Owner, req.IsMoved, movedTo)
	if movedTo != "" && movedTo != fs.option.Host && !req.IsMoved {
		glog.V(0).Infof("FILER LOCK: Forwarding to correct filer - from=%s to=%s", fs.option.Host, movedTo)
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			secondResp, err := client.DistributedLock(ctx, &filer_pb.LockRequest{
				Name:          req.Name,
				SecondsToLock: req.SecondsToLock,
				RenewToken:    req.RenewToken,
				IsMoved:       true,
				Owner:         req.Owner,
			})
			if err == nil {
				resp.RenewToken = secondResp.RenewToken
				resp.LockOwner = secondResp.LockOwner
				resp.Error = secondResp.Error
				glog.V(0).Infof("FILER LOCK: Forwarded lock acquired - name=%s renewToken=%s", req.Name, resp.RenewToken)
			} else {
				glog.V(0).Infof("FILER LOCK: Forward failed - name=%s err=%v", req.Name, err)
			}
			return err
		})
	}

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
		glog.V(0).Infof("FILER LOCK: Error - name=%s error=%s", req.Name, resp.Error)
	}
	if movedTo != "" {
		resp.LockHostMovedTo = string(movedTo)
	}

	glog.V(4).Infof("FILER LOCK: Returning response - name=%s renewToken=%s lockOwner=%s error=%s movedTo=%s",
		req.Name, resp.RenewToken, resp.LockOwner, resp.Error, resp.LockHostMovedTo)

	return resp, nil
}

// Unlock is a grpc handler to handle FilerServer's UnlockRequest
func (fs *FilerServer) DistributedUnlock(ctx context.Context, req *filer_pb.UnlockRequest) (resp *filer_pb.UnlockResponse, err error) {

	resp = &filer_pb.UnlockResponse{}

	var movedTo pb.ServerAddress
	movedTo, err = fs.filer.Dlm.Unlock(req.Name, req.RenewToken)

	if !req.IsMoved && movedTo != "" {
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			secondResp, err := client.DistributedUnlock(ctx, &filer_pb.UnlockRequest{
				Name:       req.Name,
				RenewToken: req.RenewToken,
				IsMoved:    true,
			})
			resp.Error = secondResp.Error
			return err
		})
	}

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	}
	if movedTo != "" {
		resp.MovedTo = string(movedTo)
	}

	return resp, nil

}

func (fs *FilerServer) FindLockOwner(ctx context.Context, req *filer_pb.FindLockOwnerRequest) (*filer_pb.FindLockOwnerResponse, error) {
	owner, movedTo, err := fs.filer.Dlm.FindLockOwner(req.Name)
	if !req.IsMoved && movedTo != "" || err == lock_manager.LockNotFound {
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			secondResp, err := client.FindLockOwner(ctx, &filer_pb.FindLockOwnerRequest{
				Name:    req.Name,
				IsMoved: true,
			})
			if err != nil {
				return err
			}
			owner = secondResp.Owner
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if owner == "" {
		glog.V(0).Infof("find lock %s moved to %v: %v", req.Name, movedTo, err)
		return nil, status.Error(codes.NotFound, fmt.Sprintf("lock %s not found", req.Name))
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &filer_pb.FindLockOwnerResponse{
		Owner: owner,
	}, nil
}

// TransferLocks is a grpc handler to handle FilerServer's TransferLocksRequest
func (fs *FilerServer) TransferLocks(ctx context.Context, req *filer_pb.TransferLocksRequest) (*filer_pb.TransferLocksResponse, error) {

	for _, lock := range req.Locks {
		fs.filer.Dlm.InsertLock(lock.Name, lock.ExpiredAtNs, lock.RenewToken, lock.Owner)
	}

	return &filer_pb.TransferLocksResponse{}, nil

}

func (fs *FilerServer) OnDlmChangeSnapshot(snapshot []pb.ServerAddress) {
	locks := fs.filer.Dlm.SelectNotOwnedLocks(snapshot)
	if len(locks) == 0 {
		return
	}

	for _, lock := range locks {
		server := fs.filer.Dlm.CalculateTargetServer(lock.Key, snapshot)
		// Use a context with timeout for lock transfer to avoid hanging indefinitely
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pb.WithFilerClient(false, 0, server, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.TransferLocks(ctx, &filer_pb.TransferLocksRequest{
				Locks: []*filer_pb.Lock{
					{
						Name:        lock.Key,
						RenewToken:  lock.Token,
						ExpiredAtNs: lock.ExpiredAtNs,
						Owner:       lock.Owner,
					},
				},
			})
			return err
		})
		cancel()
		if err != nil {
			// it may not be worth retrying, since the lock may have expired
			glog.Errorf("transfer lock %v to %v: %v", lock.Key, server, err)
		}
	}

}
