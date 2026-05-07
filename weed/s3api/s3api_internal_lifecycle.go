package s3api

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
)

// LifecycleDelete is the worker-to-S3 RPC the lifecycle worker calls to
// execute one (rule, action) verdict against one entry. The server is the
// only component that mutates filer state, so it gets the final decision:
// re-fetch, CAS-on-identity, object-lock check, dispatch.
//
// Steps (mirror the design's "Internal service-principal delete API"):
//   1. Re-fetch the live entry. NOT_FOUND -> NOOP_RESOLVED (someone deleted it).
//   2. Compute live EntryIdentity and compare against request.expected_identity.
//      Mismatch -> NOOP_RESOLVED (STALE_IDENTITY); the worker's verdict was
//      based on a now-stale view, drop it.
//   3. Run object-lock protections with governanceBypass=false. Lifecycle is
//      never allowed to bypass legal-hold or compliance retention; if the
//      check refuses, return SKIPPED_OBJECT_LOCK and let the worker advance
//      with a logged + counted skip per design (object lock is an operator
//      concern, not a lifecycle responsibility).
//   4. Dispatch by action_kind to the appropriate internal helper.
//
// Errors that surface to the worker as outcomes (not gRPC errors):
//   - filer-fetch transport failures during step 1 -> RETRY_LATER.
//   - dispatch-side failures the server can't classify as transient ->
//     BLOCKED with reason="FATAL_EVENT_ERROR: <detail>"; the worker writes
//     a durable BlockerRecord and pauses the failing stream.
//
// The handler does NOT touch reader cursors, pending files, or the durable
// blocker file — those live on the worker side. The server's response is
// pure verdict.
func (s3a *S3ApiServer) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	if req == nil || req.Bucket == "" || req.ObjectPath == "" {
		return &s3_lifecycle_pb.LifecycleDeleteResponse{
			Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
			Reason:  "FATAL_EVENT_ERROR: empty bucket or object_path",
		}, nil
	}

	// Step 1: re-fetch the live entry. ABORT_MPU is special-cased — the
	// upload-init "entry" lives under <bucket>/.uploads/<id>/ and goes
	// through a different code path; for all other kinds we go through
	// getObjectEntry.
	if req.ActionKind == s3_lifecycle_pb.ActionKind_ABORT_MPU {
		return s3a.lifecycleAbortMPU(ctx, req)
	}

	entry, err := s3a.getObjectEntry(req.Bucket, req.ObjectPath, req.VersionId)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrLatestVersionNotFound) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
				Reason:  "NOT_FOUND",
			}, nil
		}
		// Unknown fetch error: treat as transient. The worker will retry;
		// sustained failures eventually promote to BLOCKED via retry budget.
		glog.V(1).Infof("lifecycle: live fetch %s/%s@%s: %v", req.Bucket, req.ObjectPath, req.VersionId, err)
		return &s3_lifecycle_pb.LifecycleDeleteResponse{
			Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
			Reason:  "TRANSPORT_ERROR: " + err.Error(),
		}, nil
	}

	// Step 2: identity CAS.
	live := computeEntryIdentity(entry)
	if !identityMatches(live, req.ExpectedIdentity) {
		return &s3_lifecycle_pb.LifecycleDeleteResponse{
			Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
			Reason:  "STALE_IDENTITY",
		}, nil
	}

	// Step 3: object lock. Lifecycle never bypasses governance/compliance.
	// enforceObjectLockProtections only inspects the http.Request when
	// governanceBypassAllowed=true, so passing nil is safe here.
	if err := s3a.enforceObjectLockProtections(nil, req.Bucket, req.ObjectPath, req.VersionId, false); err != nil {
		// Any error from the lock check (legal hold, compliance, governance
		// without bypass, or a transient lookup failure) is treated as a
		// skip — lifecycle does not retain its own knowledge of when the
		// hold lifts; the safety scan picks up such objects on its next
		// pass.
		glog.V(2).Infof("lifecycle: SKIPPED_OBJECT_LOCK %s/%s@%s: %v", req.Bucket, req.ObjectPath, req.VersionId, err)
		return &s3_lifecycle_pb.LifecycleDeleteResponse{
			Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK,
			Reason:  err.Error(),
		}, nil
	}

	// Step 4: dispatch by kind.
	return s3a.lifecycleDispatch(ctx, req, entry)
}

// lifecycleDispatch routes to the appropriate internal helper based on the
// action kind. Errors from the helper are classified as fatal (deterministic
// per-event failures) -> BLOCKED.
func (s3a *S3ApiServer) lifecycleDispatch(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest, entry *filer_pb.Entry) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	switch req.ActionKind {
	case s3_lifecycle_pb.ActionKind_EXPIRATION_DAYS, s3_lifecycle_pb.ActionKind_EXPIRATION_DATE:
		// Current-version expiration. On a versioned bucket, this creates
		// a delete marker; on non-versioned, the entry is removed outright.
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
		// Specific-version expiration. EXPIRED_DELETE_MARKER targets a
		// delete marker that's the sole survivor — same code path as
		// other version-specific deletes.
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
		// Already handled before fetch; reaching here means dispatch was
		// called with the wrong shape. Treat as fatal — the worker
		// shouldn't have produced this request.
		return blocked("FATAL_EVENT_ERROR: ABORT_MPU dispatched after fetch"), nil

	default:
		return blocked("FATAL_EVENT_ERROR: unknown action_kind " + req.ActionKind.String()), nil
	}
}

// lifecycleAbortMPU aborts an in-flight multipart upload identified by the
// upload-init path. The "object_path" carries the upload directory under
// <bucket>/.uploads/<uploadId>/; we extract uploadId for the abort call.
func (s3a *S3ApiServer) lifecycleAbortMPU(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	// TODO(phase-2c-followup): plumb to abortMultipartUpload helper. The
	// helper takes an *s3.AbortMultipartUploadInput, which carries
	// bucket+key+uploadId; the lifecycle path needs a non-HTTP entry into
	// it. Out of scope for this commit; returning RETRY_LATER tells the
	// worker to back off until a follow-up wires it in.
	return &s3_lifecycle_pb.LifecycleDeleteResponse{
		Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
		Reason:  "ABORT_MPU not yet wired",
	}, nil
}

// computeEntryIdentity extracts the CAS witness from a live entry. mtime,
// size, head fid, and a stable hash over the sorted Extended map are
// sufficient: an in-place metadata edit changes Extended; an overwrite
// changes mtime/size/fid; a snapshot-restore that preserves mtime+size
// still gets a different head_fid.
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

// hashExtended returns a stable sha256 over the sorted Extended map. Empty
// map / nil input return a zero-length slice (which compares equal to
// another zero-length slice via bytes.Equal in identityMatches).
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
		// length-prefixed to avoid delimiter ambiguity
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
		// Caller didn't provide an identity to CAS against; treat as match.
		// (Used in early bootstrap-walker calls before identity is known.)
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
	return bytesEqual(live.ExtendedHash, want.ExtendedHash)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

