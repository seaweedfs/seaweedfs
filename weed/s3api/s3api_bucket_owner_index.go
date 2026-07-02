package s3api

import (
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The owner index maps bucket owner -> owned buckets so ListBuckets can serve
// an identity's buckets without scanning the global /buckets directory:
//
//	<BucketsPath>/.system/owners/<escaped owner>/<bucket>
//
// Index entries are zero-length files whose Crtime mirrors the bucket's
// creation time. The bucket handlers write the index synchronously, the
// /buckets metadata subscription reconciles changes made elsewhere (weed
// shell, another gateway, direct filer operations), and a startup backfill
// indexes pre-existing buckets. The .complete marker is written once the
// backfill has finished; until it exists readers keep scanning /buckets.
const (
	s3SystemFolder              = ".system"
	bucketOwnerIndexFolder      = "owners"
	bucketOwnerIndexReadyMarker = ".complete"
)

func (s3a *S3ApiServer) bucketOwnerIndexPath() string {
	return s3a.option.BucketsPath + "/" + s3SystemFolder + "/" + bucketOwnerIndexFolder
}

func (s3a *S3ApiServer) bucketOwnerDir(owner string) string {
	return s3a.bucketOwnerIndexPath() + "/" + escapeOwnerName(owner)
}

// escapeOwnerName makes an owner id safe to use as a single directory name.
// url.PathEscape escapes "/", so no owner name can traverse out of the index;
// the two names PathEscape leaves dangerous as-is are encoded by hand.
func escapeOwnerName(owner string) string {
	switch escaped := url.PathEscape(owner); escaped {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return escaped
	}
}

func bucketEntryOwner(entry *filer_pb.Entry) string {
	if entry == nil || entry.Extended == nil {
		return ""
	}
	return string(entry.Extended[s3_constants.AmzIdentityId])
}

func (s3a *S3ApiServer) addBucketToOwnerIndex(owner, bucket string, crtime int64) error {
	return s3a.mkFile(s3a.bucketOwnerDir(owner), bucket, nil, func(entry *filer_pb.Entry) {
		entry.Attributes.Crtime = crtime
	})
}

func (s3a *S3ApiServer) removeBucketFromOwnerIndex(owner, bucket string) error {
	return s3a.rm(s3a.bucketOwnerDir(owner), bucket, false, false)
}

// maintainBucketOwnerIndex applies a /buckets metadata event to the owner
// index, covering bucket changes made outside this gateway's handlers. Every
// operation is idempotent, so overlap with the handlers' synchronous writes
// and with other gateways is harmless.
func (s3a *S3ApiServer) maintainBucketOwnerIndex(oldEntry, newEntry *filer_pb.Entry) {
	var oldOwner, newOwner string
	if oldEntry != nil && (!oldEntry.IsDirectory || strings.HasPrefix(oldEntry.Name, ".")) {
		oldEntry = nil
	}
	if newEntry != nil && (!newEntry.IsDirectory || strings.HasPrefix(newEntry.Name, ".")) {
		newEntry = nil
	}
	if oldEntry != nil {
		oldOwner = bucketEntryOwner(oldEntry)
	}
	if newEntry != nil {
		newOwner = bucketEntryOwner(newEntry)
	}

	unchanged := oldEntry != nil && newEntry != nil && oldEntry.Name == newEntry.Name && oldOwner == newOwner
	if unchanged {
		return
	}
	if oldOwner != "" {
		if err := s3a.removeBucketFromOwnerIndex(oldOwner, oldEntry.Name); err != nil {
			glog.V(1).Infof("owner index: remove %s/%s: %v", oldOwner, oldEntry.Name, err)
		}
	}
	if newOwner != "" {
		if err := s3a.addBucketToOwnerIndex(newOwner, newEntry.Name, newEntry.Attributes.Crtime); err != nil {
			glog.V(1).Infof("owner index: add %s/%s: %v", newOwner, newEntry.Name, err)
		}
	}
}

// bucketOwnerIndexReady reports whether the backfill marker exists, caching a
// positive answer for the life of the process.
func (s3a *S3ApiServer) bucketOwnerIndexReady() bool {
	if s3a.ownerIndexReady.Load() {
		return true
	}
	if exists, err := s3a.exists(s3a.bucketOwnerIndexPath(), bucketOwnerIndexReadyMarker, false); err == nil && exists {
		s3a.ownerIndexReady.Store(true)
		return true
	}
	return false
}

// startBucketOwnerIndexBackfill brings the owner index up to date with the
// buckets that existed before it was introduced, then writes the ready
// marker. Multiple gateways may race the backfill; every step is idempotent.
func (s3a *S3ApiServer) startBucketOwnerIndexBackfill() {
	util.RetryUntil("bucketOwnerIndexBackfill", func() error {
		if s3a.bucketOwnerIndexReady() {
			return nil
		}
		return s3a.backfillBucketOwnerIndex()
	}, func(err error) bool {
		glog.V(1).Infof("bucket owner index backfill: %v", err)
		return true
	})
}

func (s3a *S3ApiServer) backfillBucketOwnerIndex() error {
	startFrom := ""
	for {
		entries, isLast, err := s3a.list(s3a.option.BucketsPath, "", startFrom, false, bucketScanPageSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			startFrom = entry.Name
			if !entry.IsDirectory || strings.HasPrefix(entry.Name, ".") {
				continue
			}
			if owner := bucketEntryOwner(entry); owner != "" {
				if err := s3a.addBucketToOwnerIndex(owner, entry.Name, entry.Attributes.Crtime); err != nil {
					return err
				}
			}
		}
		if isLast {
			break
		}
	}
	if err := s3a.mkFile(s3a.bucketOwnerIndexPath(), bucketOwnerIndexReadyMarker, nil, nil); err != nil {
		return err
	}
	s3a.ownerIndexReady.Store(true)
	glog.V(0).Infof("bucket owner index ready")
	return nil
}
