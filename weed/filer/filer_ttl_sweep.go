package filer

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	expiredEntrySweepMinInterval = time.Hour
	expiredEntrySweepMaxInterval = 24 * time.Hour
	expiredEntrySweepPageSize    = 1024
)

// loopSweepingExpiredEntries walks the paths that have a TTL rule. Expiry is
// otherwise applied only when an entry is looked up or its directory listed,
// so on a store without native TTL the rows of entries nobody visits again
// outlive their volumes.
func (f *Filer) loopSweepingExpiredEntries(self pb.ServerAddress) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-f.deletionQuit
		cancel()
	}()

	lastSwept := make(map[string]time.Time)
	ticker := time.NewTicker(expiredEntrySweepMinInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		f.sweepExpiredEntries(ctx, self, lastSwept, time.Now())
	}
}

func (f *Filer) sweepExpiredEntries(ctx context.Context, self pb.ServerAddress, lastSwept map[string]time.Time, now time.Time) {
	fc := f.FilerConf
	if fc == nil {
		return
	}
	for locationPrefix, interval := range expiredEntrySweepPrefixes(fc) {
		if now.Sub(lastSwept[locationPrefix]) < interval || !f.sweepsExpiredEntries(locationPrefix, self) {
			continue
		}
		expired, err := f.sweepExpiredEntriesUnder(ctx, locationPrefix)
		if err != nil {
			glog.V(0).Infof("sweep expired entries under %s: %v", locationPrefix, err)
			continue
		}
		lastSwept[locationPrefix] = now
		if expired > 0 {
			glog.V(0).Infof("removed %d expired entries under %s", expired, locationPrefix)
		}
	}
}

// expiredEntrySweepPrefixes maps each TTL rule that no other TTL rule
// encloses to how often it is swept: its TTL, kept between an hour and a day.
func expiredEntrySweepPrefixes(fc *FilerConf) map[string]time.Duration {
	intervals := make(map[string]time.Duration)
	for _, rule := range fc.ToProto().Locations {
		ttl, err := needle.ReadTTL(rule.GetTtl())
		if err != nil || ttl.ToSeconds() == 0 {
			continue
		}
		interval := time.Duration(ttl.ToSeconds()) * time.Second
		if interval < expiredEntrySweepMinInterval {
			interval = expiredEntrySweepMinInterval
		} else if interval > expiredEntrySweepMaxInterval {
			interval = expiredEntrySweepMaxInterval
		}
		intervals[rule.LocationPrefix] = interval
	}
	prefixes := make([]string, 0, len(intervals))
	for locationPrefix := range intervals {
		prefixes = append(prefixes, locationPrefix)
	}
	sort.Strings(prefixes)
	swept := make(map[string]time.Duration)
	enclosing := ""
	for _, locationPrefix := range prefixes {
		if enclosing != "" && strings.HasPrefix(locationPrefix, enclosing) {
			if intervals[locationPrefix] < swept[enclosing] {
				swept[enclosing] = intervals[locationPrefix]
			}
			continue
		}
		enclosing = locationPrefix
		swept[locationPrefix] = intervals[locationPrefix]
	}
	return swept
}

// sweepsExpiredEntries leaves a prefix to its lock-ring owner when that filer
// runs on this filer's store; a filer with a store of its own sweeps it.
func (f *Filer) sweepsExpiredEntries(locationPrefix string, self pb.ServerAddress) bool {
	if f.Dlm == nil {
		return true
	}
	primary := f.Dlm.LockRing.GetPrimary(locationPrefix)
	if primary == "" || primary == self {
		return true
	}
	return f.MetaAggregator == nil || !f.MetaAggregator.SharesStore(primary)
}

func (f *Filer) sweepExpiredEntriesUnder(ctx context.Context, locationPrefix string) (int64, error) {
	ctx = context.WithValue(ctx, lazyListContextKey{}, true)
	if strings.HasSuffix(locationPrefix, "/") {
		dir := strings.TrimSuffix(locationPrefix, "/")
		if dir == "" {
			dir = "/"
		}
		return f.sweepExpiredEntriesIn(ctx, util.FullPath(dir), "")
	}
	dir, name := util.FullPath(locationPrefix).DirAndName()
	return f.sweepExpiredEntriesIn(ctx, util.FullPath(dir), name)
}

// sweepExpiredEntriesIn lists dir and everything below it, so the listing
// removes the expired entries it passes, then drops dir if that emptied it.
func (f *Filer) sweepExpiredEntriesIn(ctx context.Context, dir util.FullPath, namePrefix string) (expired int64, err error) {
	startFileName := ""
	for {
		var subDirs []util.FullPath
		var listed int64
		expiredCount, lastFileName, listErr := f.doListDirectoryEntries(ctx, dir, startFileName, false, expiredEntrySweepPageSize, namePrefix, func(entry *Entry) (bool, error) {
			listed++
			if entry.IsDirectory() {
				subDirs = append(subDirs, entry.FullPath)
			}
			return true, nil
		})
		if listErr != nil {
			return expired, listErr
		}
		expired += expiredCount
		for _, subDir := range subDirs {
			subExpired, subErr := f.sweepExpiredEntriesIn(ctx, subDir, "")
			expired += subExpired
			if subErr != nil {
				return expired, subErr
			}
		}
		if listed+expiredCount < expiredEntrySweepPageSize || lastFileName == "" {
			break
		}
		startFileName = lastFileName
	}
	if expired > 0 && dir != "/" {
		f.DeleteEmptyParentDirectories(ctx, dir, util.FullPath(f.DirBucketsPath))
	}
	return expired, nil
}

// deleteExpiredEntry removes the row at p only while it still holds an
// expired entry, under the lock writers take, so a write that replaced the
// listed entry keeps its row. A contended path is left to a later listing.
func (f *Filer) deleteExpiredEntry(ctx context.Context, p util.FullPath) error {
	if lt := f.EntryLockTable; lt != nil {
		lock, acquired := lt.TryAcquireLock("deleteExpiredEntry", p, util.ExclusiveLock)
		if !acquired {
			return nil
		}
		defer lt.ReleaseLock(p, lock)
	}
	current, err := f.Store.FindEntry(ctx, p)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		return err
	}
	if current.IsDirectory() || current.TtlSec <= 0 || !current.Crtime.Add(time.Duration(current.TtlSec)*time.Second).Before(time.Now()) {
		return nil
	}
	return f.Store.DeleteOneEntry(ctx, current)
}
