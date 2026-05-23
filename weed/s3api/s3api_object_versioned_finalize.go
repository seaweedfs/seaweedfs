package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// errVersionedPreconditionFailed is returned by versioned finalize helpers whose
// contract is (…, error) when the routed FinalizeVersionedWrite reports a failed
// precondition, so callers can surface it as 412 rather than a generic 500.
var errVersionedPreconditionFailed = errors.New("versioned write precondition failed")

// finalizeCodeToError adapts a routedVersionedFinalize result to an error: nil on
// success, the precondition sentinel on 412, else a generic error.
func finalizeCodeToError(code s3err.ErrorCode) error {
	switch code {
	case s3err.ErrNone:
		return nil
	case s3err.ErrPreconditionFailed:
		return errVersionedPreconditionFailed
	default:
		return fmt.Errorf("versioned finalize failed: code %v", code)
	}
}

// objectWriteOwner returns the filer that owns all of an object's writes, or ""
// when no ring view is available. It hashes the object key with the same prefix
// the non-versioned path uses (routedObjectOwner), so normal, suspended, and
// versioned writes to the same object all resolve to one owner and serialize on
// the same lock — regardless of versioning state. The whole object subtree
// (object, .versions dir, version files) belongs to this owner.
func (s3a *S3ApiServer) objectWriteOwner(bucket, object string) pb.ServerAddress {
	if s3a.objectWriteLockClient == nil {
		return ""
	}
	// Object-lock (WORM) buckets keep ALL of their writes on the distributed
	// lock. Retention enforcement is a gateway-side check that the per-path-locked
	// filer ops don't perform, so routing only some of such a bucket's writes
	// would split serialization between the entry lock and the distributed lock
	// (they don't serialize against each other). Excluding the bucket entirely
	// keeps it internally consistent.
	if locked, err := s3a.isObjectLockEnabled(bucket); err != nil || locked {
		return ""
	}
	return s3a.objectWriteLockClient.PrimaryForKey("s3.object.write:" + s3a.toFilerPath(bucket, object))
}

// versionedPointerExtended computes the Extended keys to set and to clear on the
// .versions directory entry for a new latest version, mirroring
// setCachedListMetadata (clear cached fields, then set id/filename + cached
// size/mtime/etag/owner/delete-marker). The filer applies deletes before sets.
func versionedPointerExtended(versionId, versionFileName string, versionEntry *filer_pb.Entry) (set map[string][]byte, del []string) {
	set = map[string][]byte{
		s3_constants.ExtLatestVersionIdKey:       []byte(versionId),
		s3_constants.ExtLatestVersionFileNameKey: []byte(versionFileName),
	}
	if versionEntry.Attributes != nil {
		set[s3_constants.ExtLatestVersionSizeKey] = []byte(strconv.FormatUint(versionEntry.Attributes.FileSize, 10))
		set[s3_constants.ExtLatestVersionMtimeKey] = []byte(strconv.FormatInt(versionEntry.Attributes.Mtime, 10))
	}
	isDeleteMarker := []byte("false")
	if versionEntry.Extended != nil {
		if v, ok := versionEntry.Extended[s3_constants.ExtETagKey]; ok {
			set[s3_constants.ExtLatestVersionETagKey] = v
		}
		if v, ok := versionEntry.Extended[s3_constants.ExtAmzOwnerKey]; ok {
			set[s3_constants.ExtLatestVersionOwnerKey] = v
		}
		if v, ok := versionEntry.Extended[s3_constants.ExtDeleteMarkerKey]; ok {
			isDeleteMarker = v
		}
	}
	set[s3_constants.ExtLatestVersionIsDeleteMarker] = isDeleteMarker
	del = []string{
		s3_constants.ExtLatestVersionSizeKey,
		s3_constants.ExtLatestVersionMtimeKey,
		s3_constants.ExtLatestVersionETagKey,
		s3_constants.ExtLatestVersionOwnerKey,
		s3_constants.ExtLatestVersionIsDeleteMarker,
	}
	return set, del
}

// routedVersionedFinalize finalizes a versioned write by routing the precondition
// check, the demote of the previous latest, and the pointer flip to the owner of
// the object's .versions directory, where they run atomically under one local
// lock. The caller (putToFiler with uniqueVersionPath) has already created the
// version file and skips the object write lock and gateway precondition. A
// transport/in-band error maps to InternalError (the client retries); there is
// no silent non-atomic fallback.
func (s3a *S3ApiServer) routedVersionedFinalize(owner pb.ServerAddress, bucket, object, versionId, versionFileName string, versionEntry *filer_pb.Entry, cond *filer_pb.WriteCondition) s3err.ErrorCode {
	set, del := versionedPointerExtended(versionId, versionFileName, versionEntry)
	req := &filer_pb.FinalizeVersionedWriteRequest{
		LockKey:               s3a.toFilerPath(bucket, object),
		VersionsDir:           s3a.toFilerPath(bucket, object+s3_constants.VersionsFolder),
		SetExtended:           set,
		DeleteExtended:        del,
		PriorLatestKey:        s3_constants.ExtLatestVersionFileNameKey,
		NoncurrentSinceKey:    s3_constants.ExtNoncurrentSinceNsKey,
		NoncurrentSinceNs:     time.Now().UnixNano(),
		Condition:             cond,
		LatestEtagKey:         s3_constants.ExtLatestVersionETagKey,
		LatestDeleteMarkerKey: s3_constants.ExtLatestVersionIsDeleteMarker,
	}
	var resp *filer_pb.FinalizeVersionedWriteResponse
	err := pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.FinalizeVersionedWrite(context.Background(), req)
		return e
	})
	switch {
	case err != nil:
		glog.Errorf("routedVersionedFinalize: %s/%s on %s: %v", bucket, object, owner, err)
		return s3err.ErrInternalError
	case resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED:
		return s3err.ErrPreconditionFailed
	case resp.ErrorCode != filer_pb.FilerError_OK:
		if code, ok := filerErrorCodeToS3Error(resp.ErrorCode); ok {
			return code
		}
		glog.Errorf("routedVersionedFinalize: %s/%s unexpected code %v", bucket, object, resp.ErrorCode)
		return s3err.ErrInternalError
	case resp.Error != "":
		glog.Errorf("routedVersionedFinalize: %s/%s: %s", bucket, object, resp.Error)
		return s3err.ErrInternalError
	default:
		return s3err.ErrNone
	}
}

// versionedAfterCreate returns the putToFiler afterCreate hook for a versioned
// write: the routed atomic finalize when the .versions owner is known and the
// precondition reduces to a single primitive, otherwise the existing lock-based
// updateLatestVersionInDirectory path. The bool reports whether the routed path
// was selected, which the caller passes as putToFiler's uniqueVersionPath.
func (s3a *S3ApiServer) versionedAfterCreate(r *http.Request, bucket, object, versionId, versionFileName string) (func(*filer_pb.Entry) s3err.ErrorCode, bool) {
	owner := s3a.objectWriteOwner(bucket, object)
	cond, condOk := buildWriteCondition(r)
	if owner != "" && condOk {
		return func(versionEntry *filer_pb.Entry) s3err.ErrorCode {
			return s3a.routedVersionedFinalize(owner, bucket, object, versionId, versionFileName, versionEntry, cond)
		}, true
	}
	return func(versionEntry *filer_pb.Entry) s3err.ErrorCode {
		if err := s3a.updateLatestVersionInDirectory(bucket, object, versionId, versionFileName, versionEntry); err != nil {
			glog.Errorf("putVersionedObject: failed to update latest version in directory: %v", err)
			return s3err.ErrInternalError
		}
		return s3err.ErrNone
	}, false
}
