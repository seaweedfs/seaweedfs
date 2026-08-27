package s3api

import (
	"errors"
	"net/http"
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

// deleteDirectoryMarker removes the key "<dir>/". Callers hold the object write lock,
// so the entry this decides about cannot change between the read and the delete.
func (s3a *S3ApiServer) deleteDirectoryMarker(r *http.Request, bucket, object string) s3err.ErrorCode {
	// The key is deleted the unversioned way, but Object Lock still covers it: the
	// gateway lists it as an object and serves retention set on it, and the history
	// dropped below can be a real one this key was given before it grew past the
	// size that makes a PUT a marker.
	governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
	if err := s3a.enforceObjectLockProtections(r, bucket, object, "", governanceBypassAllowed); err != nil {
		glog.V(2).Infof("deleteDirectoryMarker: object lock check failed for %s/%s: %v", bucket, object, err)
		return s3err.ErrAccessDenied
	}

	markerDir := s3a.bucketDir(bucket) + "/" + strings.TrimSuffix(strings.TrimPrefix(object, "/"), "/")
	dir, name := util.FullPath(markerDir).DirAndName()

	entry, err := s3a.getEntry(dir, name)
	switch {
	case errors.Is(err, filer_pb.ErrNotFound):
		return s3err.ErrNone // deleting a key that is not there is a success
	case err != nil:
		// The entry may be a file a child write promoted to a directory, whose data
		// belongs to the key without the trailing slash. Deleting without knowing
		// would destroy it, so fail and leave the retry to the client.
		glog.Errorf("deleteDirectoryMarker: cannot read %s/%s: %v", bucket, object, err)
		return s3err.ErrInternalError
	case len(entry.GetChunks()) > 0 || entry.IsInRemoteOnly():
		// A promoted file, not a marker: "dir/" does not name its data.
		glog.V(2).Infof("deleteDirectoryMarker: %s/%s holds uploaded data, leaving it alone", bucket, object)
		return s3err.ErrNone
	}

	// Drop a history an older build recorded for this key. Nothing writes one now, and
	// leaving it behind keeps reporting the key in ListObjectVersions, so a history we
	// cannot read or remove fails the delete rather than half finishing it.
	switch _, historyErr := s3a.getEntry(markerDir, s3_constants.VersionsFolder); {
	case historyErr == nil:
		// The check above resolves the latest version, while the removal below takes
		// every entry under the key, so each has to be clear of a lock of its own.
		versionsDir := markerDir + "/" + s3_constants.VersionsFolder
		for startFrom := ""; ; {
			entries, isLast, listErr := s3a.list(versionsDir, "", startFrom, false, 1000)
			if listErr != nil {
				glog.Errorf("deleteDirectoryMarker: cannot list history of %s/%s: %v", bucket, object, listErr)
				return s3err.ErrInternalError
			}
			for _, entry := range entries {
				startFrom = entry.Name
				versionId, named := entry.Extended[s3_constants.ExtVersionIdKey]
				if !named {
					// An entry an older build left without a version id is what this
					// removal is here to clear, but one still under a lock cannot be
					// named to check it, so leave it alone rather than take it blind.
					retention, retentionActive, _ := s3a.getRetentionFromEntry(entry)
					_, legalHoldActive, _ := s3a.getLegalHoldFromEntry(entry)
					held := retentionActive && retention != nil &&
						(retention.Mode == s3_constants.RetentionModeCompliance || !governanceBypassAllowed)
					if held || legalHoldActive {
						glog.V(2).Infof("deleteDirectoryMarker: unnamed history entry %s of %s/%s is locked", entry.Name, bucket, object)
						return s3err.ErrAccessDenied
					}
					continue
				}
				if err := s3a.enforceObjectLockProtections(r, bucket, object, string(versionId), governanceBypassAllowed); err != nil {
					glog.V(2).Infof("deleteDirectoryMarker: version %s of %s/%s is locked: %v", versionId, bucket, object, err)
					return s3err.ErrAccessDenied
				}
			}
			if isLast || len(entries) == 0 {
				break
			}
		}
		if rmErr := s3a.rm(markerDir, s3_constants.VersionsFolder, true, true); rmErr != nil {
			glog.Errorf("deleteDirectoryMarker: failed to remove stale history of %s/%s: %v", bucket, object, rmErr)
			return s3err.ErrInternalError
		}
	case !errors.Is(historyErr, filer_pb.ErrNotFound):
		glog.Errorf("deleteDirectoryMarker: cannot read stale history of %s/%s: %v", bucket, object, historyErr)
		return s3err.ErrInternalError
	}

	if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s3a.deleteUnversionedObjectWithClient(client, bucket, object, false)
	}); err != nil {
		glog.Errorf("deleteDirectoryMarker: failed to delete %s/%s: %v", bucket, object, err)
		return s3err.ErrInternalError
	}
	return s3err.ErrNone
}
