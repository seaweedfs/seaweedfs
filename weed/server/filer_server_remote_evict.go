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
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

const (
	remoteCacheEvictInterval  = 30 * time.Second
	remoteCacheEvictMinAge    = time.Minute
	remoteCacheVacuumCooldown = time.Minute
)

// uncacheRemoteEntry drops the local chunks of one remote-mounted entry, the
// same state transition remote.uncache applies through UpdateEntry. Cleared
// chunks go to the deletion queue and are reclaimed by the next compaction.
// When vids is set, only entries holding chunks on those volumes count toward
// the freed bytes, and entries contributing nothing are left untouched.
func (fs *FilerServer) uncacheRemoteEntry(ctx context.Context, fullPath util.FullPath, minCacheAge time.Duration, vids map[uint32]struct{}) (freedBytes int64, err error) {
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

	freedBytes = remoteEntryBytesOnVids(current, vids)
	if freedBytes == 0 {
		return 0, nil
	}

	newEntry := current.ShallowClone()
	newEntry.Chunks = nil
	newEntry.Remote = proto.Clone(current.Remote).(*filer_pb.RemoteEntry)
	newEntry.Remote.LastLocalSyncTsNs = 0

	if err := fs.filer.CreateEntry(ctx, newEntry, current, false, false, nil, true, fs.filer.MaxFilenameLength); err != nil {
		return 0, err
	}
	stats.RemoteCacheEvictedCounter.Inc()
	glog.V(1).InfofCtx(ctx, "uncacheRemoteEntry %s freed %d bytes", fullPath, freedBytes)
	return freedBytes, nil
}

// remoteEntryBytesOnVids sums the entry's chunk bytes on the given volumes; a
// nil set counts the whole object.
func remoteEntryBytesOnVids(entry *filer.Entry, vids map[uint32]struct{}) int64 {
	if vids == nil {
		return int64(entry.Size())
	}
	var bytes int64
	for _, chunk := range entry.Chunks {
		fid, err := needle.ParseFileIdFromString(chunk.GetFileIdString())
		if err != nil {
			continue
		}
		if _, ok := vids[uint32(fid.VolumeId)]; ok {
			bytes += int64(chunk.Size)
		}
	}
	return bytes
}

// evictRemoteCachedEntries drops local chunks of remote-mounted entries
// oldest-cached first until bytesNeeded is met or candidates run out. The
// first pass honors a minimum cache age so a just-fetched hot object is not
// dropped under a reader; when aged candidates cannot cover the request a
// second pass accepts any synchronized cached entry. When pressuredVids is
// set, only bytes on those volumes count and entries elsewhere are skipped.
func (fs *FilerServer) evictRemoteCachedEntries(ctx context.Context, bytesNeeded int64, pressuredVids map[uint32]struct{}) (freed int64) {
	if fs.filer.RemoteStorage == nil {
		return 0
	}
	mounts := fs.filer.RemoteStorage.MountedDirectories()
	for _, minCacheAge := range []time.Duration{remoteCacheEvictMinAge, 0} {
		for _, entry := range fs.filer.ListEvictableRemoteEntries(ctx, mounts, minCacheAge) {
			if bytesNeeded > 0 && freed >= bytesNeeded {
				return freed
			}
			n, err := fs.uncacheRemoteEntry(ctx, entry.FullPath, minCacheAge, pressuredVids)
			if err != nil {
				glog.WarningfCtx(ctx, "evict remote cache %s: %v", entry.FullPath, err)
				continue
			}
			freed += n
		}
	}
	return freed
}

// remoteCacheDiskPressure reports per-disk usage across the cluster: the total
// bytes to reclaim and the volumes hosted on disks over the eviction threshold.
func (fs *FilerServer) remoteCacheDiskPressure(ctx context.Context) (bytesToFree int64, pressuredVids map[uint32]struct{}, over bool) {
	threshold := fs.option.RemoteCacheEvictThreshold
	if threshold <= 0 {
		return 0, nil, false
	}
	pressuredVids = make(map[uint32]struct{})
	err := fs.filer.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}
		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, dn := range rack.DataNodeInfos {
					for _, disk := range dn.DiskInfos {
						for _, pd := range disk.SplitByPhysicalDisk() {
							if pd.DiskTotalBytes == 0 {
								continue
							}
							used := pd.DiskTotalBytes - pd.DiskFreeBytes
							if float64(used) < float64(pd.DiskTotalBytes)*threshold {
								continue
							}
							over = true
							bytesToFree += int64(used) - int64(float64(pd.DiskTotalBytes)*threshold*0.95)
							for _, vi := range pd.VolumeInfos {
								pressuredVids[vi.Id] = struct{}{}
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
		return 0, nil, false
	}
	return bytesToFree, pressuredVids, over
}

// maybeVacuumRemoteCacheVolumes flushes the deletion queue so fresh tombstones
// land on the volume servers, then compacts only the volumes that received them
// (evicted chunks or orphaned partial fills). Vids that miss the cooldown window
// stay pending until the janitor retries them.
func (fs *FilerServer) maybeVacuumRemoteCacheVolumes(ctx context.Context) {
	fileIds := fs.filer.FlushFileIdDeletionQueue(filer.LookupByMasterClientFn(fs.filer.MasterClient))
	pending := fs.notePendingRemoteCacheVids(fileIds)
	if len(pending) == 0 {
		return
	}
	if last := fs.remoteCacheLastVacuum.Load(); last != nil && time.Since(*last) < remoteCacheVacuumCooldown {
		return
	}
	now := time.Now()
	fs.remoteCacheLastVacuum.Store(&now)
	if err := fs.filer.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		for vid := range pending {
			if _, err := client.VacuumVolume(ctx, &master_pb.VacuumVolumeRequest{VolumeId: vid, GarbageThreshold: 0.1}); err != nil {
				glog.WarningfCtx(ctx, "remote cache vacuum volume %d: %v", vid, err)
				continue
			}
			fs.clearPendingRemoteCacheVid(vid)
		}
		return nil
	}); err != nil {
		glog.WarningfCtx(ctx, "remote cache vacuum: %v", err)
	}
}

func (fs *FilerServer) notePendingRemoteCacheVids(fileIds []string) map[uint32]struct{} {
	fs.remoteCachePendingVidsMu.Lock()
	defer fs.remoteCachePendingVidsMu.Unlock()
	if fs.remoteCachePendingVids == nil {
		fs.remoteCachePendingVids = make(map[uint32]struct{})
	}
	for _, fid := range fileIds {
		if parsed, err := needle.ParseFileIdFromString(fid); err == nil {
			fs.remoteCachePendingVids[uint32(parsed.VolumeId)] = struct{}{}
		}
	}
	out := make(map[uint32]struct{}, len(fs.remoteCachePendingVids))
	for vid := range fs.remoteCachePendingVids {
		out[vid] = struct{}{}
	}
	return out
}

func (fs *FilerServer) clearPendingRemoteCacheVid(vid uint32) {
	fs.remoteCachePendingVidsMu.Lock()
	defer fs.remoteCachePendingVidsMu.Unlock()
	delete(fs.remoteCachePendingVids, vid)
}

// reclaimRemoteCacheSpace evicts remote-cached content and compacts volumes to
// release disk space under capacity pressure. A pass already in flight is
// enough; callers that would queue behind it just fall back to remote reads.
func (fs *FilerServer) reclaimRemoteCacheSpace(ctx context.Context, bytesNeeded int64, pressuredVids map[uint32]struct{}) {
	if !fs.remoteCacheEvictMu.TryLock() {
		return
	}
	defer fs.remoteCacheEvictMu.Unlock()
	freed := fs.evictRemoteCachedEntries(ctx, bytesNeeded, pressuredVids)
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
func (fs *FilerServer) evictCtx() context.Context {
	if fs.remoteCacheEvictCtx == nil {
		return context.Background()
	}
	return fs.remoteCacheEvictCtx
}

func (fs *FilerServer) runRemoteCacheEviction() {
	if fs.option.RemoteCacheEvictThreshold <= 0 || fs.remoteCacheEvictCtx == nil {
		return
	}
	ctx := fs.remoteCacheEvictCtx
	ticker := time.NewTicker(remoteCacheEvictInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		fs.maybeVacuumRemoteCacheVolumes(ctx)
		if fs.filer.RemoteStorage == nil || len(fs.filer.RemoteStorage.MountedDirectories()) == 0 {
			continue
		}
		bytesToFree, pressuredVids, over := fs.remoteCacheDiskPressure(ctx)
		if !over {
			continue
		}
		glog.V(0).Infof("remote cache: disk usage over %.0f%%, evicting %d bytes", fs.option.RemoteCacheEvictThreshold*100, bytesToFree)
		fs.reclaimRemoteCacheSpace(ctx, bytesToFree, pressuredVids)
	}
}
