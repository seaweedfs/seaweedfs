package weed_server

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

const (
	remoteCacheEvictInterval = 30 * time.Second
	remoteCacheEvictMinAge   = time.Minute
	remoteCacheVacuumWait    = 5 * time.Minute
)

// uncacheRemoteEntry drops the local chunks of one remote-mounted entry, the
// same state transition remote.uncache applies through UpdateEntry. Cleared
// chunks go to the deletion queue and are reclaimed by the next compaction.
func (fs *FilerServer) uncacheRemoteEntry(ctx context.Context, fullPath util.FullPath, minCacheAge time.Duration) (freedBytes int64, err error) {
	pathLock := fs.entryLockTable.AcquireLock("uncacheRemoteEntry", fullPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(fullPath, pathLock)

	current, err := fs.filer.FindEntry(ctx, fullPath)
	if err != nil {
		return 0, err
	}
	if !filer.IsEvictableRemoteEntry(current) {
		return 0, nil
	}
	if time.Since(time.Unix(0, current.Remote.LastLocalSyncTsNs)) < minCacheAge {
		return 0, nil
	}

	newEntry := current.ShallowClone()
	newEntry.Chunks = nil
	newEntry.Remote = proto.Clone(current.Remote).(*filer_pb.RemoteEntry)
	newEntry.Remote.LastLocalSyncTsNs = 0

	freedBytes = int64(current.Size())
	if err := fs.filer.CreateEntry(ctx, newEntry, current, false, false, nil, true, fs.filer.MaxFilenameLength); err != nil {
		return 0, err
	}
	stats.RemoteCacheEvictedCounter.Inc()
	glog.V(1).InfofCtx(ctx, "uncacheRemoteEntry %s freed %d bytes", fullPath, freedBytes)
	return freedBytes, nil
}

// evictRemoteCachedEntries drops local chunks of remote-mounted entries
// oldest-cached first until bytesNeeded is met or candidates run out. The
// first pass honors a minimum cache age so a just-fetched hot object is not
// dropped under a reader; when aged candidates cannot cover the request a
// second pass accepts any synchronized cached entry.
func (fs *FilerServer) evictRemoteCachedEntries(ctx context.Context, bytesNeeded int64) (freed int64) {
	fs.remoteCacheEvictMu.Lock()
	defer fs.remoteCacheEvictMu.Unlock()
	if fs.filer.RemoteStorage == nil {
		return 0
	}
	mounts := fs.filer.RemoteStorage.MountedDirectories()
	for _, minCacheAge := range []time.Duration{remoteCacheEvictMinAge, 0} {
		for _, entry := range fs.filer.ListEvictableRemoteEntries(ctx, mounts, minCacheAge) {
			if bytesNeeded > 0 && freed >= bytesNeeded {
				return freed
			}
			n, err := fs.uncacheRemoteEntry(ctx, entry.FullPath, minCacheAge)
			if err != nil {
				glog.WarningfCtx(ctx, "evict remote cache %s: %v", entry.FullPath, err)
				continue
			}
			freed += n
		}
	}
	return freed
}

// remoteCacheDiskPressure reports the worst per-disk usage ratio across the
// cluster and how many bytes it sits above the eviction threshold.
func (fs *FilerServer) remoteCacheDiskPressure(ctx context.Context) (bytesToFree int64, over bool) {
	threshold := fs.option.RemoteCacheEvictThreshold
	if threshold <= 0 {
		return 0, false
	}
	err := fs.filer.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}
		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, dn := range rack.DataNodeInfos {
					for _, disk := range dn.DiskInfos {
						if disk.DiskTotalBytes == 0 {
							continue
						}
						used := disk.DiskTotalBytes - disk.DiskFreeBytes
						if float64(used) >= float64(disk.DiskTotalBytes)*threshold {
							over = true
							if need := int64(used - uint64(float64(disk.DiskTotalBytes)*threshold*0.95)); need > bytesToFree {
								bytesToFree = need
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		glog.WarningfCtx(ctx, "remote cache disk pressure check: %v", err)
		return 0, false
	}
	return bytesToFree, over
}

// maybeVacuumRemoteCacheVolumes compacts volumes holding freshly-deleted
// remote cache chunks (or orphaned partial fills) so evicted space actually
// returns to the filesystem. Cooldown keeps it from running back-to-back.
func (fs *FilerServer) maybeVacuumRemoteCacheVolumes(ctx context.Context) {
	if last := fs.remoteCacheLastVacuum.Load(); last != nil && time.Since(*last) < remoteCacheVacuumWait {
		return
	}
	now := time.Now()
	fs.remoteCacheLastVacuum.Store(&now)
	if err := fs.filer.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		_, err := client.VacuumVolume(ctx, &master_pb.VacuumVolumeRequest{GarbageThreshold: 0.1})
		return err
	}); err != nil {
		glog.WarningfCtx(ctx, "remote cache vacuum: %v", err)
	}
}

// reclaimRemoteCacheSpace evicts remote-cached content and compacts volumes to
// release disk space under capacity pressure.
func (fs *FilerServer) reclaimRemoteCacheSpace(ctx context.Context, bytesNeeded int64) {
	freed := fs.evictRemoteCachedEntries(ctx, bytesNeeded)
	if freed > 0 {
		glog.V(0).InfofCtx(ctx, "remote cache eviction freed %d bytes", freed)
	}
	fs.maybeVacuumRemoteCacheVolumes(ctx)
}

func isRemoteCacheCapacityError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "writable volumes") ||
		strings.Contains(msg, "free volumes") ||
		strings.Contains(msg, "no space left") ||
		strings.Contains(msg, "out of space")
}

// runRemoteCacheEviction periodically evicts remote-cached entries once any
// disk crosses the configured usage threshold, with a vacuum pass to reclaim
// the deleted chunks.
func (fs *FilerServer) runRemoteCacheEviction() {
	if fs.option.RemoteCacheEvictThreshold <= 0 {
		return
	}
	ticker := time.NewTicker(remoteCacheEvictInterval)
	defer ticker.Stop()
	for range ticker.C {
		ctx := context.Background()
		if fs.filer.RemoteStorage == nil || len(fs.filer.RemoteStorage.MountedDirectories()) == 0 {
			continue
		}
		bytesToFree, over := fs.remoteCacheDiskPressure(ctx)
		if !over {
			continue
		}
		glog.V(0).Infof("remote cache: disk usage over %.0f%%, evicting %d bytes", fs.option.RemoteCacheEvictThreshold*100, bytesToFree)
		fs.reclaimRemoteCacheSpace(ctx, bytesToFree)
	}
}
