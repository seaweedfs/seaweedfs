package mount

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// lockRingWait bounds how long startup waits for the first lock ring; until
// one arrives, locks start at the seed filer and lookups use any filer.
const lockRingWait = 10 * time.Second

// followLockRing feeds the masters' lock ring into lockClient, so locks and
// owner reads go straight to the filer that owns a file.
func (wfs *WFS) followLockRing() {
	var masters []string
	var filerGroup string
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		masters, filerGroup = resp.Masters, resp.FilerGroup
		return nil
	})
	if err != nil || len(masters) == 0 {
		glog.Warningf("dlm: cannot follow the lock ring, masters %v: %v", masters, err)
		return
	}

	masterClient := wdclient.NewMasterClient(wfs.option.GrpcDialOption, filerGroup, cluster.MountType, "", wfs.option.DataCenter, "",
		*pb.ServerAddresses(strings.Join(masters, ",")).ToServiceDiscovery())
	masterClient.SetOnLockRingUpdateFn(func(update *master_pb.LockRingUpdate) {
		servers := make([]pb.ServerAddress, 0, len(update.Servers))
		for _, s := range update.Servers {
			servers = append(servers, pb.ServerAddress(s))
		}
		wfs.lockClient.SetRing(servers, update.Version)
	})
	masterClient.SetOnMasterChangeFn(func(previous, current pb.ServerAddress) {
		wfs.lockClient.ResetRing()
	})
	go masterClient.KeepConnectedToMaster(context.Background())

	root := wfs.option.FilerMountRootPath
	for deadline := time.Now().Add(lockRingWait); wfs.lockClient.PrimaryForKey(root) == ""; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			glog.Warningf("dlm: no lock ring from masters %v after %v", masters, lockRingWait)
			return
		}
	}
}

// entryWriteOwner is the filer that serializes path's writes per this mount's
// ring view, matching the filer's routing: the prior owner while a rebalance
// cools off, else the primary. "" before a ring arrives.
func (wfs *WFS) entryWriteOwner(path util.FullPath) pb.ServerAddress {
	if wfs.lockClient == nil {
		return ""
	}
	key := s3_constants.ObjectWriteRouteKeyPrefix + string(path)
	if prior := wfs.lockClient.PriorOwnerForKey(key); prior != "" {
		return prior
	}
	return wfs.lockClient.PrimaryForKey(key)
}
