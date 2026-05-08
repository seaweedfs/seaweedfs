package s3api

import (
	"bytes"
	"context"
	"errors"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// LifecycleDelete executes one (rule, action) verdict: re-fetch, identity
// CAS, object-lock check, dispatch by kind. Errors surface as outcomes;
// reader cursors and pending state are the worker's concern.
func (s3a *S3ApiServer) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	if req == nil || req.Bucket == "" || req.ObjectPath == "" {
		return blocked("FATAL_EVENT_ERROR: empty bucket or object_path"), nil
	}

	// MPU init lives at .uploads/<id>/; not handled by getObjectEntry.
	if req.ActionKind == s3_lifecycle_pb.ActionKind_ABORT_MPU {
		return s3a.lifecycleAbortMPU(ctx, req)
	}

	entry, err := s3a.getObjectEntry(req.Bucket, req.ObjectPath, req.VersionId)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrLatestVersionNotFound) {
			return noopResolved("NOT_FOUND"), nil
		}
		glog.V(1).Infof("lifecycle: live fetch %s/%s@%s: %v", req.Bucket, req.ObjectPath, req.VersionId, err)
		return retryLater("TRANSPORT_ERROR: " + err.Error()), nil
	}

	if !identityMatches(computeEntryIdentity(entry), req.ExpectedIdentity) {
		return noopResolved("STALE_IDENTITY"), nil
	}

	// Lifecycle never bypasses governance/compliance; the http.Request is
	// only read when bypass is allowed, so nil is safe here.
	if err := s3a.enforceObjectLockProtections(nil, req.Bucket, req.ObjectPath, req.VersionId, false); err != nil {
		glog.V(2).Infof("lifecycle: SKIPPED_OBJECT_LOCK %s/%s@%s: %v", req.Bucket, req.ObjectPath, req.VersionId, err)
		return &s3_lifecycle_pb.LifecycleDeleteResponse{
			Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK,
			Reason:  err.Error(),
		}, nil
	}

	return s3a.lifecycleDispatch(ctx, req, entry)
}

func (s3a *S3ApiServer) lifecycleDispatch(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest, entry *filer_pb.Entry) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	switch req.ActionKind {
	case s3_lifecycle_pb.ActionKind_EXPIRATION_DAYS, s3_lifecycle_pb.ActionKind_EXPIRATION_DATE:
		// Current-version expiration: Enabled -> delete marker; Suspended
		// -> delete null + new marker; Off -> remove. Filer errors classify
		// as RETRY_LATER; the worker's budget promotes to BLOCKED.
		state, vErr := s3a.getVersioningState(req.Bucket)
		if vErr != nil {
			if errors.Is(vErr, filer_pb.ErrNotFound) {
				return noopResolved("BUCKET_NOT_FOUND"), nil
			}
			return retryLater("TRANSPORT_ERROR: versioning lookup: " + vErr.Error()), nil
		}
		switch state {
		case s3_constants.VersioningEnabled:
			if _, err := s3a.createDeleteMarker(req.Bucket, req.ObjectPath); err != nil {
				return retryLater("TRANSPORT_ERROR: createDeleteMarker: " + err.Error()), nil
			}
			return done(), nil
		case s3_constants.VersioningSuspended:
			// Best-effort null delete; NotFound is benign.
			if err := s3a.deleteSpecificObjectVersion(req.Bucket, req.ObjectPath, "null"); err != nil {
				if !errors.Is(err, filer_pb.ErrNotFound) && !errors.Is(err, ErrVersionNotFound) {
					return retryLater("TRANSPORT_ERROR: deleteNullVersion: " + err.Error()), nil
				}
			}
			if _, err := s3a.createDeleteMarker(req.Bucket, req.ObjectPath); err != nil {
				return retryLater("TRANSPORT_ERROR: createDeleteMarker: " + err.Error()), nil
			}
			return done(), nil
		default:
			err := s3a.WithFilerClient(false, func(c filer_pb.SeaweedFilerClient) error {
				return s3a.deleteUnversionedObjectWithClient(c, req.Bucket, req.ObjectPath)
			})
			if err != nil {
				if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrObjectNotFound) {
					return noopResolved("NOT_FOUND_AT_DELETE"), nil
				}
				return retryLater("TRANSPORT_ERROR: deleteUnversioned: " + err.Error()), nil
			}
			return done(), nil
		}

	case s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS,
		s3_lifecycle_pb.ActionKind_NEWER_NONCURRENT,
		s3_lifecycle_pb.ActionKind_EXPIRED_DELETE_MARKER:
		// EXPIRED_DELETE_MARKER targets the marker version itself.
		if req.VersionId == "" {
			return blocked("FATAL_EVENT_ERROR: version_id required for noncurrent / delete-marker delete"), nil
		}
		// Latest-pointer guard for noncurrent kinds: refuse to delete
		// the version that the .versions/ directory currently points
		// to. The router can't always tell current from noncurrent
		// without sibling state, so the server checks here.
		if req.ActionKind == s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS ||
			req.ActionKind == s3_lifecycle_pb.ActionKind_NEWER_NONCURRENT {
			isLatest, lookupErr := s3a.isCurrentLatestVersion(req.Bucket, req.ObjectPath, req.VersionId)
			if lookupErr != nil {
				if errors.Is(lookupErr, filer_pb.ErrNotFound) || errors.Is(lookupErr, ErrObjectNotFound) {
					return noopResolved("NOT_FOUND"), nil
				}
				return retryLater("TRANSPORT_ERROR: latest-pointer lookup: " + lookupErr.Error()), nil
			}
			if isLatest {
				return noopResolved("VERSION_IS_LATEST"), nil
			}
		}
		if err := s3a.deleteSpecificObjectVersion(req.Bucket, req.ObjectPath, req.VersionId); err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrObjectNotFound) {
				return noopResolved("NOT_FOUND_AT_DELETE"), nil
			}
			return retryLater("TRANSPORT_ERROR: deleteSpecificVersion: " + err.Error()), nil
		}
		return done(), nil

	case s3_lifecycle_pb.ActionKind_ABORT_MPU:
		return blocked("FATAL_EVENT_ERROR: ABORT_MPU dispatched after fetch"), nil

	default:
		return blocked("FATAL_EVENT_ERROR: unknown action_kind " + req.ActionKind.String()), nil
	}
}

func (s3a *S3ApiServer) lifecycleAbortMPU(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	// req.ObjectPath is `.uploads/<upload_id>` (set by the router from the
	// init directory's bucket-relative path); reject anything that isn't
	// exactly that shape so a malformed event can't escalate to a wider rm.
	const uploadsPrefix = s3_constants.MultipartUploadsFolder + "/"
	if !strings.HasPrefix(req.ObjectPath, uploadsPrefix) {
		return blocked("FATAL_EVENT_ERROR: ABORT_MPU object_path missing .uploads/ prefix"), nil
	}
	uploadID := req.ObjectPath[len(uploadsPrefix):]
	// Reject "." and ".." explicitly: util.JoinPath in the filer cleans
	// path components, so .uploads/.. would resolve to the bucket root.
	if uploadID == "" || uploadID == "." || uploadID == ".." || strings.ContainsRune(uploadID, '/') {
		return blocked("FATAL_EVENT_ERROR: ABORT_MPU object_path malformed: " + req.ObjectPath), nil
	}

	uploadsFolder := s3a.genUploadsFolder(req.Bucket)
	// Pre-check existence: filer.DeleteEntry suppresses ErrNotFound and
	// returns success, so without this check an already-aborted upload
	// would report DONE instead of the correct NOOP_RESOLVED.
	exists, err := s3a.exists(uploadsFolder, uploadID, true)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return noopResolved("NOT_FOUND"), nil
		}
		return retryLater("TRANSPORT_ERROR: exists: " + err.Error()), nil
	}
	if !exists {
		return noopResolved("NOT_FOUND"), nil
	}
	if err := s3a.rm(uploadsFolder, uploadID, true, true); err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return noopResolved("NOT_FOUND_AT_DELETE"), nil
		}
		glog.V(1).Infof("lifecycle abort_mpu %s/%s: %v", req.Bucket, req.ObjectPath, err)
		return retryLater("TRANSPORT_ERROR: rm: " + err.Error()), nil
	}
	return done(), nil
}

// isCurrentLatestVersion reports whether versionId is the version the
// .versions/ directory currently points to. SeaweedFS records the latest
// version on the parent directory's Extended map; without consulting it,
// a noncurrent-kind dispatch can't safely distinguish current from
// noncurrent and would risk deleting the live version. Returns
// (false, nil) when the directory has no latest pointer (e.g., the
// bucket isn't versioned in this object's history).
func (s3a *S3ApiServer) isCurrentLatestVersion(bucket, object, versionId string) (bool, error) {
	versionsDir := s3a.bucketDir(bucket) + "/" + object + s3_constants.VersionsFolder
	parent, name := path.Split(versionsDir)
	parent = strings.TrimRight(parent, "/")
	if parent == "" {
		parent = "/"
	}
	entry, err := s3a.getEntry(parent, name)
	if err != nil {
		return false, err
	}
	if entry == nil || len(entry.Extended) == 0 {
		return false, nil
	}
	latest, ok := entry.Extended[s3_constants.ExtLatestVersionIdKey]
	if !ok {
		return false, nil
	}
	return string(latest) == versionId, nil
}

// computeEntryIdentity captures (mtime, size, head fid, sorted-Extended hash):
// an overwrite changes mtime/size/fid; a metadata edit changes Extended; a
// snapshot-restore that preserves mtime+size still differs in head_fid.
func computeEntryIdentity(entry *filer_pb.Entry) *s3_lifecycle_pb.EntryIdentity {
	if entry == nil {
		return nil
	}
	id := &s3_lifecycle_pb.EntryIdentity{}
	if entry.Attributes != nil {
		// FuseAttributes splits the timestamp across Mtime (seconds) and
		// MtimeNs (nanosecond component); EntryIdentity.MtimeNs is the
		// combined nanoseconds-since-epoch value.
		id.MtimeNs = entry.Attributes.Mtime*int64(1e9) + int64(entry.Attributes.MtimeNs)
		id.Size = int64(entry.Attributes.FileSize)
	}
	if len(entry.GetChunks()) > 0 {
		id.HeadFid = entry.GetChunks()[0].GetFileIdString()
	}
	id.ExtendedHash = s3lifecycle.HashExtended(entry.Extended)
	return id
}

func identityMatches(live, want *s3_lifecycle_pb.EntryIdentity) bool {
	if want == nil {
		// No CAS witness (early bootstrap); skip.
		return true
	}
	if live == nil {
		return false
	}
	if live.MtimeNs != want.MtimeNs || live.Size != want.Size {
		return false
	}
	if live.HeadFid != want.HeadFid {
		return false
	}
	return bytes.Equal(live.ExtendedHash, want.ExtendedHash)
}

func done() *s3_lifecycle_pb.LifecycleDeleteResponse {
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
}
func noopResolved(reason string) *s3_lifecycle_pb.LifecycleDeleteResponse {
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED, Reason: reason}
}
func blocked(reason string) *s3_lifecycle_pb.LifecycleDeleteResponse {
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED, Reason: reason}
}
func retryLater(reason string) *s3_lifecycle_pb.LifecycleDeleteResponse {
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER, Reason: reason}
}
