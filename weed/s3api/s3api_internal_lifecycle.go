package s3api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
)

// LifecycleDelete executes one (rule, action) verdict against one entry.
// Steps: re-fetch live, identity CAS, object-lock check, dispatch by kind.
// Errors surface as outcomes (NOOP / RETRY_LATER / BLOCKED); the handler
// itself doesn't touch reader cursors or pending state.
func (s3a *S3ApiServer) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	if req == nil || req.Bucket == "" || req.ObjectPath == "" {
		return blocked("FATAL_EVENT_ERROR: empty bucket or object_path"), nil
	}

	// MPU init lives at <bucket>/.uploads/<id>/; doesn't go through getObjectEntry.
	if req.ActionKind == s3_lifecycle_pb.ActionKind_ABORT_MPU {
		return s3a.lifecycleAbortMPU(ctx, req)
	}

	entry, err := s3a.getObjectEntry(req.Bucket, req.ObjectPath, req.VersionId)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrLatestVersionNotFound) {
			return noopResolved("NOT_FOUND"), nil
		}
		// Transient: worker retries; sustained failures eventually promote
		// to BLOCKED via the retry budget.
		glog.V(1).Infof("lifecycle: live fetch %s/%s@%s: %v", req.Bucket, req.ObjectPath, req.VersionId, err)
		return &s3_lifecycle_pb.LifecycleDeleteResponse{
			Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
			Reason:  "TRANSPORT_ERROR: " + err.Error(),
		}, nil
	}

	live := computeEntryIdentity(entry)
	if !identityMatches(live, req.ExpectedIdentity) {
		return noopResolved("STALE_IDENTITY"), nil
	}

	// Lifecycle never bypasses governance/compliance; *http.Request is only
	// dereferenced when bypass is allowed, so passing nil is safe here.
	if err := s3a.enforceObjectLockProtections(nil, req.Bucket, req.ObjectPath, req.VersionId, false); err != nil {
		// Lock-held / lookup error: skip and let the safety scan revisit
		// once the hold lifts. Lifecycle doesn't track retain-until itself.
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
		// Versioned bucket -> create delete marker; non-versioned -> remove.
		versioned, vErr := s3a.isVersioningEnabled(req.Bucket)
		if vErr != nil {
			return blocked("FATAL_EVENT_ERROR: versioning lookup: " + vErr.Error()), nil
		}
		if versioned {
			if _, err := s3a.createDeleteMarker(req.Bucket, req.ObjectPath); err != nil {
				return blocked("FATAL_EVENT_ERROR: createDeleteMarker: " + err.Error()), nil
			}
			return done(), nil
		}
		err := s3a.WithFilerClient(false, func(c filer_pb.SeaweedFilerClient) error {
			return s3a.deleteUnversionedObjectWithClient(c, req.Bucket, req.ObjectPath)
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return noopResolved("NOT_FOUND_AT_DELETE"), nil
			}
			return blocked("FATAL_EVENT_ERROR: deleteUnversioned: " + err.Error()), nil
		}
		return done(), nil

	case s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS,
		s3_lifecycle_pb.ActionKind_NEWER_NONCURRENT,
		s3_lifecycle_pb.ActionKind_EXPIRED_DELETE_MARKER:
		// EXPIRED_DELETE_MARKER targets the marker version itself; same code
		// path as any other version-specific delete.
		if req.VersionId == "" {
			return blocked("FATAL_EVENT_ERROR: version_id required for noncurrent / delete-marker delete"), nil
		}
		if err := s3a.deleteSpecificObjectVersion(req.Bucket, req.ObjectPath, req.VersionId); err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrVersionNotFound) {
				return noopResolved("NOT_FOUND_AT_DELETE"), nil
			}
			return blocked("FATAL_EVENT_ERROR: deleteSpecificVersion: " + err.Error()), nil
		}
		return done(), nil

	case s3_lifecycle_pb.ActionKind_ABORT_MPU:
		return blocked("FATAL_EVENT_ERROR: ABORT_MPU dispatched after fetch"), nil

	default:
		return blocked("FATAL_EVENT_ERROR: unknown action_kind " + req.ActionKind.String()), nil
	}
}

func (s3a *S3ApiServer) lifecycleAbortMPU(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	// TODO(phase-5): plumb to abortMultipartUpload (currently expects an
	// *s3.AbortMultipartUploadInput; lifecycle has no HTTP request).
	return &s3_lifecycle_pb.LifecycleDeleteResponse{
		Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
		Reason:  "ABORT_MPU not yet wired",
	}, nil
}

// computeEntryIdentity captures mtime, size, head fid, and a sorted-Extended
// hash. An overwrite changes mtime/size/fid; a metadata edit changes
// Extended; a snapshot-restore that preserves mtime+size still differs in
// head_fid.
func computeEntryIdentity(entry *filer_pb.Entry) *s3_lifecycle_pb.EntryIdentity {
	if entry == nil {
		return nil
	}
	id := &s3_lifecycle_pb.EntryIdentity{}
	if entry.Attributes != nil {
		id.MtimeNs = entry.Attributes.Mtime
		id.Size = int64(entry.Attributes.FileSize)
	}
	if len(entry.GetChunks()) > 0 {
		id.HeadFid = entry.GetChunks()[0].FileId
	}
	id.ExtendedHash = hashExtended(entry.Extended)
	return id
}

// hashExtended is length-prefixed so a forged tag value can't collide with
// a legitimate two-tag map.
func hashExtended(ext map[string][]byte) []byte {
	if len(ext) == 0 {
		return nil
	}
	keys := make([]string, 0, len(ext))
	for k := range ext {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(strconv.Itoa(len(k))))
		h.Write([]byte{':'})
		h.Write([]byte(k))
		v := ext[k]
		h.Write([]byte(strconv.Itoa(len(v))))
		h.Write([]byte{':'})
		h.Write(v)
	}
	return h.Sum(nil)
}

func identityMatches(live, want *s3_lifecycle_pb.EntryIdentity) bool {
	if want == nil {
		// No CAS witness supplied (early bootstrap path); skip the check.
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
