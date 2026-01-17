package filer

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (f *Filer) CanRename(ctx context.Context, source, target util.FullPath, oldName string) error {
	sourcePath := source.Child(oldName)
	if strings.HasPrefix(string(target), string(sourcePath)) {
		return fmt.Errorf("mv: can not move directory to a subdirectory of itself")
	}

	// Check if attempting to rename a bucket itself
	// Need to load the entry to check if it's a bucket
	entry, err := f.FindEntry(ctx, sourcePath)
	if err == nil && f.IsBucket(entry) {
		return fmt.Errorf("bucket renaming is not allowed")
	}

	sourceBucket := f.DetectBucket(source)
	targetBucket := f.DetectBucket(target)
	if sourceBucket != targetBucket {
		return fmt.Errorf("can not move across collection %s => %s", sourceBucket, targetBucket)
	}

	return nil
}

func (f *Filer) DetectBucket(source util.FullPath) (bucket string) {
	if strings.HasPrefix(string(source), f.DirBucketsPath+"/") {
		bucketAndObjectKey := string(source)[len(f.DirBucketsPath)+1:]
		t := strings.Index(bucketAndObjectKey, "/")
		if t < 0 {
			bucket = bucketAndObjectKey
		}
		if t > 0 {
			bucket = bucketAndObjectKey[:t]
		}
	}
	return bucket
}
