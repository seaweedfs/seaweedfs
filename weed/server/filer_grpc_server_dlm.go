package weed_server

import (
	"context"
	"fmt"
	"strings"
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
	var generation int64
	resp.LockOwner, resp.RenewToken, generation, movedTo, err = fs.filer.Dlm.LockWithTimeout(req.Name, expiredAtNs, req.RenewToken, req.Owner)
	resp.Generation = generation
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
				resp.Generation = secondResp.Generation
				glog.V(0).Infof("FILER LOCK: Forwarded lock acquired - name=%s renewToken=%s", req.Name, resp.RenewToken)
			} else {
				glog.V(0).Infof("FILER LOCK: Forward failed - name=%s err=%v", req.Name, err)
			}
			return err
		})
	}

	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
		if !strings.Contains(resp.Error, "lock already owned") {
			glog.V(0).Infof("FILER LOCK: Error - name=%s error=%s", req.Name, resp.Error)
		}
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

// TransferLocks is a grpc handler to handle FilerServer's TransferLocksRequest.
// Preserves generation/seq and re-replicates to this node's backup.
func (fs *FilerServer) TransferLocks(ctx context.Context, req *filer_pb.TransferLocksRequest) (*filer_pb.TransferLocksResponse, error) {

	for _, lock := range req.Locks {
		fs.filer.Dlm.InsertLock(lock.Name, lock.ExpiredAtNs, lock.RenewToken, lock.Owner, lock.Generation, lock.Seq)
	}

	return &filer_pb.TransferLocksResponse{}, nil

}

// ReplicateLock handles lock replication from a primary to this backup node.
// Uses seq for causal ordering — rejects stale mutations.
func (fs *FilerServer) ReplicateLock(ctx context.Context, req *filer_pb.ReplicateLockRequest) (*filer_pb.ReplicateLockResponse, error) {
	if req.IsUnlock {
		fs.filer.Dlm.RemoveBackupLockIfSeq(req.Name, req.Seq)
		glog.V(4).Infof("FILER REPLICATE: removed backup lock %s seq=%d", req.Name, req.Seq)
	} else {
		fs.filer.Dlm.InsertBackupLock(req.Name, req.ExpiredAtNs, req.RenewToken, req.Owner, req.Generation, req.Seq)
		glog.V(4).Infof("FILER REPLICATE: inserted backup lock %s owner=%s generation=%d seq=%d", req.Name, req.Owner, req.Generation, req.Seq)
	}
	return &filer_pb.ReplicateLockResponse{}, nil
}

// OnDlmChangeSnapshot is called when the lock ring topology changes.
// It handles:
// 1. Promoting backup locks to primary when this node becomes the new primary
// 2. Re-replicating to the new backup
// 3. Transferring locks that no longer belong here
func (fs *FilerServer) OnDlmChangeSnapshot(snapshot []pb.ServerAddress) {
	locks := fs.filer.Dlm.AllLocks()
	if len(locks) == 0 {
		return
	}

	for _, lock := range locks {
		primary, backup := fs.filer.Dlm.LockRing.GetPrimaryAndBackup(lock.Key)

		if primary == fs.option.Host {
			if lock.IsBackup {
				// We held this as backup, now we're primary → promote
				fs.filer.Dlm.PromoteLock(lock.Key)
				glog.V(0).Infof("DLM: promoted backup lock %s to primary", lock.Key)
			}
			// Replicate to (possibly new) backup
			if backup != "" && fs.filer.Dlm.ReplicateFn != nil {
				go fs.filer.Dlm.ReplicateFn(backup, lock.Key, lock.ExpiredAtNs, lock.Token, lock.Owner, lock.Generation, lock.Seq, false)
			}
		} else if backup == fs.option.Host {
			if !lock.IsBackup {
				// We were primary, now we're backup → transfer to new primary first, then demote
				glog.V(0).Infof("DLM: transferring primary lock %s to new primary=%s", lock.Key, primary)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := pb.WithFilerClient(false, 0, primary, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
					_, err := client.TransferLocks(ctx, &filer_pb.TransferLocksRequest{
						Locks: []*filer_pb.Lock{
							{
								Name:        lock.Key,
								RenewToken:  lock.Token,
								ExpiredAtNs: lock.ExpiredAtNs,
								Owner:       lock.Owner,
								Generation:  lock.Generation,
								Seq:         lock.Seq,
							},
						},
					})
					return err
				})
				cancel()
				if err != nil {
					glog.Errorf("DLM: failed to transfer lock %s to new primary %s: %v, keeping as primary", lock.Key, primary, err)
				} else {
					// Only demote after successful handoff
					fs.filer.Dlm.DemoteLock(lock.Key)
					glog.V(0).Infof("DLM: demoted lock %s to backup after successful transfer", lock.Key)
				}
			}
			// else: already backup, keep as is
		} else {
			// We're neither primary nor backup → transfer to primary and remove locally
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := pb.WithFilerClient(false, 0, primary, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				_, err := client.TransferLocks(ctx, &filer_pb.TransferLocksRequest{
					Locks: []*filer_pb.Lock{
						{
							Name:        lock.Key,
							RenewToken:  lock.Token,
							ExpiredAtNs: lock.ExpiredAtNs,
							Owner:       lock.Owner,
							Generation:  lock.Generation,
							Seq:         lock.Seq,
						},
					},
				})
				return err
			})
			cancel()
			if err != nil {
				glog.Errorf("DLM: failed to transfer lock %s to %s: %v", lock.Key, primary, err)
			} else {
				fs.filer.Dlm.RemoveBackupLock(lock.Key)
			}
		}
	}
}

// SetupDlmReplication sets up the replication callback for the DLM.
// Called during filer server initialization.
func (fs *FilerServer) SetupDlmReplication() {
	fs.filer.Dlm.ReplicateFn = func(server pb.ServerAddress, key string, expiredAtNs int64, token string, owner string, generation int64, seq int64, isUnlock bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := pb.WithFilerClient(false, 0, server, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.ReplicateLock(ctx, &filer_pb.ReplicateLockRequest{
				Name:        key,
				RenewToken:  token,
				ExpiredAtNs: expiredAtNs,
				Owner:       owner,
				Generation:  generation,
				Seq:         seq,
				IsUnlock:    isUnlock,
			})
			return err
		})
		if err != nil {
			glog.V(1).Infof("DLM: failed to replicate lock %s to %s: %v", key, server, err)
		}
	}
}
