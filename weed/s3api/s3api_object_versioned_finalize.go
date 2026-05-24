package s3api

import (
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// objectWriteOwner resolves the filer that owns all of an object's writes,
// regardless of versioning state, or "" when no ring view is available. Normal,
// suspended, and versioned writes to the same object hash to one owner and
// serialize on its per-path lock.
func (s3a *S3ApiServer) objectWriteOwner(bucket, object string) pb.ServerAddress {
	if s3a.objectWriteLockClient == nil {
		return ""
	}
	return s3a.objectWriteLockClient.PrimaryForKey("s3.object.write:" + s3a.toFilerPath(bucket, object))
}

// routedVersionedFinalize flips the .versions pointer to the newest version and
// demotes the prior latest, atomically under the object's per-path lock on the
// owner filer, via a single RECOMPUTE_LATEST. The version file is already
// written; the owner re-derives the pointer by scanning the directory.
func (s3a *S3ApiServer) routedVersionedFinalize(owner pb.ServerAddress, bucket, object string, useInvertedFormat bool) s3err.ErrorCode {
	versionsPath := s3a.toFilerPath(bucket, object+s3_constants.VersionsFolder)
	vdir, vname := util.FullPath(versionsPath).DirAndName()
	req := &filer_pb.ObjectTransactionRequest{
		LockKey: s3a.toFilerPath(bucket, object),
		Mutations: []*filer_pb.ObjectMutation{{
			Type:      filer_pb.ObjectMutation_RECOMPUTE_LATEST,
			Directory: vdir,
			Name:      vname,
			Recompute: &filer_pb.Recompute{
				ScanDir: versionsPath,
				// Inverted ids sort newest-first, so the newest is the first
				// ascending entry; legacy ids sort oldest-first (scan to the last).
				Descending: !useInvertedFormat,
				NameToKey:  s3_constants.ExtLatestVersionFileNameKey,
				SizeToKey:  s3_constants.ExtLatestVersionSizeKey,
				MtimeToKey: s3_constants.ExtLatestVersionMtimeKey,
				CopyExtended: map[string]string{
					s3_constants.ExtLatestVersionIdKey:          s3_constants.ExtVersionIdKey,
					s3_constants.ExtLatestVersionETagKey:        s3_constants.ExtETagKey,
					s3_constants.ExtLatestVersionOwnerKey:       s3_constants.ExtAmzOwnerKey,
					s3_constants.ExtLatestVersionIsDeleteMarker: s3_constants.ExtDeleteMarkerKey,
				},
				DemoteKey:   s3_constants.ExtNoncurrentSinceNsKey,
				DemoteValue: []byte(strconv.FormatInt(time.Now().UnixNano(), 10)),
			},
		}},
	}
	resp, err := s3a.objectTxnOnFiler(owner, req)
	switch {
	case err != nil:
		glog.Errorf("routedVersionedFinalize: %s/%s on %s: %v", bucket, object, owner, err)
		return s3err.ErrInternalError
	case resp.Error != "":
		glog.Errorf("routedVersionedFinalize: %s/%s: %s", bucket, object, resp.Error)
		return s3err.ErrInternalError
	default:
		return s3err.ErrNone
	}
}

// versionedAfterCreate returns the putToFiler hook that finalizes a versioned
// write: the routed RECOMPUTE_LATEST when the owner is known, else the existing
// lock-free updateLatestVersionInDirectory.
func (s3a *S3ApiServer) versionedAfterCreate(bucket, object, versionId, versionFileName string, useInvertedFormat bool) func(*filer_pb.Entry) s3err.ErrorCode {
	owner := s3a.objectWriteOwner(bucket, object)
	return func(versionEntry *filer_pb.Entry) s3err.ErrorCode {
		if owner != "" {
			return s3a.routedVersionedFinalize(owner, bucket, object, useInvertedFormat)
		}
		if err := s3a.updateLatestVersionInDirectory(bucket, object, versionId, versionFileName, versionEntry); err != nil {
			glog.Errorf("putVersionedObject: failed to update latest version in directory: %v", err)
			return s3err.ErrInternalError
		}
		return s3err.ErrNone
	}
}
