package filer2

import (
	"context"
	"math"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type BucketName string
type BucketOption struct {
	Name        BucketName
	Replication string
}
type FilerBuckets struct {
	dirBucketsPath string
	buckets        map[BucketName]*BucketOption
	sync.RWMutex
}

func (f *Filer) LoadBuckets(dirBucketsPath string) {

	f.buckets = &FilerBuckets{
		buckets: make(map[BucketName]*BucketOption),
	}
	f.DirBucketsPath = dirBucketsPath

	limit := math.MaxInt32

	entries, err := f.ListDirectoryEntries(context.Background(), FullPath(dirBucketsPath), "", false, limit)

	if err != nil {
		glog.V(1).Infof("no buckets found: %v", err)
		return
	}

	glog.V(1).Infof("buckets found: %d", len(entries))

	f.buckets.Lock()
	for _, entry := range entries {
		f.buckets.buckets[BucketName(entry.Name())] = &BucketOption{
			Name:        BucketName(entry.Name()),
			Replication: entry.Replication,
		}
	}
	f.buckets.Unlock()

}

func (f *Filer) ReadBucketOption(buketName string) (replication string) {

	f.buckets.RLock()
	defer f.buckets.RUnlock()

	option, found := f.buckets.buckets[BucketName(buketName)]

	if !found {
		return ""
	}
	return option.Replication

}

func (f *Filer) isBucket(entry *Entry) bool {
	if !entry.IsDirectory() {
		return false
	}
	parent, dirName := entry.FullPath.DirAndName()
	if parent != f.DirBucketsPath {
		return false
	}

	f.buckets.RLock()
	defer f.buckets.RUnlock()

	_, found := f.buckets.buckets[BucketName(dirName)]

	return found

}

func (f *Filer) maybeAddBucket(entry *Entry) {
	if !entry.IsDirectory() {
		return
	}
	parent, dirName := entry.FullPath.DirAndName()
	if parent != f.DirBucketsPath {
		return
	}
	f.addBucket(dirName, &BucketOption{
		Name:        BucketName(dirName),
		Replication: entry.Replication,
	})
}

func (f *Filer) addBucket(buketName string, bucketOption *BucketOption) {

	f.buckets.Lock()
	defer f.buckets.Unlock()

	f.buckets.buckets[BucketName(buketName)] = bucketOption

}

func (f *Filer) deleteBucket(buketName string) {

	f.buckets.Lock()
	defer f.buckets.Unlock()

	delete(f.buckets.buckets, BucketName(buketName))

}
