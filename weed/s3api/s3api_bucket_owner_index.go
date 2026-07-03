package s3api

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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
// url.PathEscape escapes "/", so no owner name can traverse out of the index.
// It leaves dots alone, so a leading dot is encoded by hand: "." and ".."
// must not resolve as path segments, and other dot-names would collide with
// index-internal entries like the ready marker. PathEscape never emits a
// literal "%2E" itself, so the encoding stays collision-free.
func escapeOwnerName(owner string) string {
	escaped := url.PathEscape(owner)
	if strings.HasPrefix(escaped, ".") {
		escaped = "%2E" + escaped[1:]
	}
	return escaped
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

// listBucketsFromOwnerIndex serves ListBuckets from the owner index: the
// identity's owned buckets merged with the buckets its legacy actions name
// explicitly. Cost is proportional to the identity's own buckets, not to the
// global bucket count.
func (s3a *S3ApiServer) listBucketsFromOwnerIndex(r *http.Request, identity *Identity, grantedNames []string, prefix, startAfter string, maxBuckets int) ([]ListAllMyBucketsEntry, string, error) {
	granted := s3a.resolveGrantedBuckets(r, identity, grantedNames, prefix, startAfter)
	var listOwned bucketPageLister
	if identity.Name != "" {
		ownerDir := s3a.bucketOwnerDir(identity.Name)
		listOwned = func(startFrom string) ([]*filer_pb.Entry, bool, error) {
			return s3a.list(ownerDir, prefix, startFrom, false, bucketScanPageSize)
		}
	}
	return listMergedBuckets(listOwned, granted, startAfter, maxBuckets)
}

// resolveGrantedBuckets maps action-granted bucket names to listing entries,
// dropping duplicates, names outside the requested page, buckets that no
// longer exist, and names the identity cannot actually list.
func (s3a *S3ApiServer) resolveGrantedBuckets(r *http.Request, identity *Identity, names []string, prefix, startAfter string) (granted []ListAllMyBucketsEntry) {
	slices.Sort(names)
	names = slices.Compact(names)
	for _, name := range names {
		if name <= startAfter || !strings.HasPrefix(name, prefix) || strings.HasPrefix(name, ".") {
			continue
		}
		config, errCode := s3a.getBucketConfig(name)
		if errCode != s3err.ErrNone {
			if errCode != s3err.ErrNoSuchBucket {
				glog.V(1).Infof("owner index: resolve granted bucket %s: %v", name, errCode)
			}
			continue
		}
		if !s3a.bucketVisibleToIdentity(r, name, config.IdentityId, identity) {
			continue
		}
		granted = append(granted, ListAllMyBucketsEntry{
			Name:         name,
			CreationDate: time.Unix(config.Crtime, 0).UTC(),
		})
	}
	return granted
}

type bucketPageLister func(startFrom string) (entries []*filer_pb.Entry, isLast bool, err error)

// listMergedBuckets merges the sorted owned-bucket index stream with the
// pre-filtered, sorted granted entries, returning up to maxBuckets and a
// continuation token when more remain.
func listMergedBuckets(listOwned bucketPageLister, granted []ListAllMyBucketsEntry, startAfter string, maxBuckets int) (buckets []ListAllMyBucketsEntry, nextToken string, err error) {
	gi := 0
	startFrom := startAfter
	if listOwned != nil {
	owned:
		for len(buckets) <= maxBuckets {
			entries, isLast, listErr := listOwned(startFrom)
			if listErr != nil {
				return nil, "", listErr
			}
			if len(entries) == 0 {
				break
			}
			for _, entry := range entries {
				startFrom = entry.Name
				if entry.IsDirectory || strings.HasPrefix(entry.Name, ".") {
					continue
				}
				for gi < len(granted) && granted[gi].Name < entry.Name {
					buckets = append(buckets, granted[gi])
					gi++
					if len(buckets) > maxBuckets {
						break owned
					}
				}
				if gi < len(granted) && granted[gi].Name == entry.Name {
					gi++ // also owned; keep the index entry
				}
				buckets = append(buckets, ListAllMyBucketsEntry{
					Name:         entry.Name,
					CreationDate: time.Unix(entry.Attributes.Crtime, 0).UTC(),
				})
				if len(buckets) > maxBuckets {
					break owned
				}
			}
			if isLast {
				break
			}
		}
	}
	for gi < len(granted) && len(buckets) <= maxBuckets {
		buckets = append(buckets, granted[gi])
		gi++
	}
	if len(buckets) > maxBuckets {
		buckets = buckets[:maxBuckets]
		nextToken = encodeContinuationToken(buckets[maxBuckets-1].Name)
	}
	return buckets, nextToken, nil
}

func (s3a *S3ApiServer) backfillBucketOwnerIndex() error {
	startFrom := ""
	for {
		pageStart := startFrom
		entries, isLast, err := s3a.list(s3a.option.BucketsPath, "", startFrom, false, bucketScanPageSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		var indexed []*filer_pb.Entry
		for _, entry := range entries {
			startFrom = entry.Name
			if !entry.IsDirectory || strings.HasPrefix(entry.Name, ".") {
				continue
			}
			if owner := bucketEntryOwner(entry); owner != "" {
				if err := s3a.addBucketToOwnerIndex(owner, entry.Name, entry.Attributes.Crtime); err != nil {
					return err
				}
				indexed = append(indexed, entry)
			}
		}
		// A bucket deleted while this page was in flight would leave a permanent
		// phantom record: the index write lands after the delete already tried to
		// clean up. Re-list the page and drop records for buckets that vanished;
		// a delete after this point sees the record and removes it itself.
		if err := s3a.dropVanishedFromOwnerIndex(pageStart, indexed); err != nil {
			return err
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

func (s3a *S3ApiServer) dropVanishedFromOwnerIndex(startFrom string, indexed []*filer_pb.Entry) error {
	if len(indexed) == 0 {
		return nil
	}
	last := indexed[len(indexed)-1].Name
	present := make(map[string]bool, len(indexed))
	from := startFrom
scan:
	for {
		entries, isLast, err := s3a.list(s3a.option.BucketsPath, "", from, false, bucketScanPageSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			if entry.Name > last {
				break scan
			}
			from = entry.Name
			present[entry.Name] = true
		}
		if isLast {
			break
		}
	}
	for _, entry := range indexed {
		if present[entry.Name] {
			continue
		}
		if err := s3a.removeBucketFromOwnerIndex(bucketEntryOwner(entry), entry.Name); err != nil {
			return err
		}
	}
	return nil
}
