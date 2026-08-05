package s3api

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// An explicit directory marker - the key "dir/" created by PutObject on a
// trailing-slash key - is stored as the filer directory itself rather than as an
// object beside it. That makes it a poor fit for versioning: a delete marker would
// have to replace an entry that other keys live under, and the history would have to
// sit inside the directory it describes, where a listing keeps meeting it.
//
// So the key is not versioned. Deleting it does what deleting it in an unversioned
// bucket already does: the directory is removed when nothing is left under it, and
// demoted to a plain directory when children remain. Listings need no version lookup
// to tell what a directory stands for, and a bucket made of directory markers costs
// the same to list versioned as unversioned.

// deleteDirectoryMarker removes the key "<dir>/", along with any version history an
// older build recorded for it - nothing writes one now, and leaving it behind would
// let a stale delete marker shadow a key that exists.
func (s3a *S3ApiServer) deleteDirectoryMarker(bucket, object string) s3err.ErrorCode {
	markerDir := s3a.bucketDir(bucket) + "/" + strings.TrimSuffix(strings.TrimPrefix(object, "/"), "/")

	// A directory that holds uploaded data is a file a child write promoted, not a
	// marker: the data belongs to the key without the trailing slash, which this
	// request does not name. Deleting it would destroy another key's object.
	dir, name := util.FullPath(markerDir).DirAndName()
	if entry, err := s3a.getEntry(dir, name); err == nil && (len(entry.GetChunks()) > 0 || entry.IsInRemoteOnly()) {
		glog.V(2).Infof("deleteDirectoryMarker: %s/%s is a promoted file, leaving it alone", bucket, object)
		return s3err.ErrNone
	}

	if _, err := s3a.getEntry(markerDir, s3_constants.VersionsFolder); err == nil {
		if rmErr := s3a.rm(markerDir, s3_constants.VersionsFolder, true, true); rmErr != nil {
			glog.Warningf("deleteDirectoryMarker: %s/%s: stale history: %v", bucket, object, rmErr)
		}
	}

	if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s3a.deleteUnversionedObjectWithClient(client, bucket, object, false)
	}); err != nil {
		glog.Errorf("deleteDirectoryMarker: failed to delete %s/%s: %v", bucket, object, err)
		return s3err.ErrInternalError
	}
	return s3err.ErrNone
}
