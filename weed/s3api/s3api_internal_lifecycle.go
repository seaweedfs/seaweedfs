package s3api

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// LifecycleDelete executes one (rule, action) verdict: re-fetch, identity
// CAS, object-lock check, dispatch by kind. Errors surface as outcomes;
// reader cursors and pending state are the worker's concern.
func (s3a *S3ApiServer) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
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

// LifecycleReady authorizes the caller as LifecycleDelete does and touches
// no bucket or object metadata.
func (s3a *S3ApiServer) LifecycleReady(ctx context.Context, _ *s3_lifecycle_pb.LifecycleReadyRequest) (*s3_lifecycle_pb.LifecycleReadyResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	return &s3_lifecycle_pb.LifecycleReadyResponse{}, nil
}

func (s3a *S3ApiServer) lifecycleDispatch(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest, entry *filer_pb.Entry) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	metadataOnly := entryUsesMetadataOnlyDelete(entry)
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
		// An unaddressed read already resolves the latest when enabled.
		if state == s3_constants.VersioningSuspended || (state == s3_constants.VersioningEnabled && req.VersionId != "") {
			if resp := s3a.lifecycleRequireLatest(req, entry); resp != nil {
				return resp, nil
			}
		}
		// Past the attempt deadline the worker may already be resending
		// this request, so a late marker write would stack a second one.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch state {
		case s3_constants.VersioningEnabled:
			written, resp := s3a.lifecycleDeleteMarker(req.Bucket, req.ObjectPath, lifecycleAddressedVersion(req, entry))
			if resp != nil {
				return resp, nil
			}
			if !written {
				return noopResolved("LATEST_IS_DELETE_MARKER"), nil
			}
			return done(), nil
		case s3_constants.VersioningSuspended:
			// Marker first: the pointer outranks the null object, so the key
			// reads as deleted once it lands, and a null delete that fails
			// after it leaves a noncurrent version instead of exposing an
			// older one to a retry that then finds no null to expire.
			nullEntry, resp := s3a.lifecycleNullVersion(req, entry)
			if resp != nil {
				return resp, nil
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if _, resp := s3a.lifecycleDeleteMarker(req.Bucket, req.ObjectPath, lifecycleAddressedVersion(req, entry)); resp != nil {
				return resp, nil
			}
			if nullEntry == nil {
				return done(), nil
			}
			return s3a.lifecycleDeleteNullVersion(req, nullEntry, metadataOnly), nil
		default:
			err := s3a.WithFilerClient(false, func(c filer_pb.SeaweedFilerClient) error {
				return s3a.deleteUnversionedObjectWithClient(ctx, c, req.Bucket, req.ObjectPath, metadataOnly)
			})
			if err != nil {
				if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrObjectNotFound) {
					return noopResolved("NOT_FOUND_AT_DELETE"), nil
				}
				return retryLater("TRANSPORT_ERROR: deleteUnversioned: " + err.Error()), nil
			}
			recordMetadataOnlyIf(metadataOnly, req)
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
		// Re-check sole-survivor: a fresh PUT can land between schedule
		// and dispatch. Identity-CAS upstream covers the marker bytes;
		// this covers the directory shape.
		if req.ActionKind == s3_lifecycle_pb.ActionKind_EXPIRED_DELETE_MARKER {
			outcome, err := s3a.checkSoleSurvivorMarker(ctx, req.Bucket, req.ObjectPath, req.VersionId)
			if outcome != nil || err != nil {
				return outcome, err
			}
		}
		// Route the version delete when an owner resolves: its recompute
		// repoints .versions while the file is removed, all on the filer the
		// routed reads consult. A routed failure retries rather than falling
		// back, where an absent entry would delete as success while the owner
		// still holds the version. Lifecycle never bypasses retention.
		versionDeleted := false
		if owner := s3a.routableWriteOwner(req.Bucket, req.ObjectPath); owner != "" {
			worm, lockErr := s3a.isObjectLockEnabled(req.Bucket)
			if lockErr == nil {
				// The "null" version is the object entry, not a .versions file.
				if req.VersionId == "null" {
					code, settled := s3a.routedDeleteNullVersion(owner, req.Bucket, req.ObjectPath, worm, false, metadataOnly, nil)
					if settled {
						if code == s3err.ErrAccessDenied {
							return retryLater("ACCESS_DENIED: deleteSpecificVersion: object lock"), nil
						}
						if code != s3err.ErrNone {
							return retryLater("TRANSPORT_ERROR: routedDeleteNullVersion: " + s3err.GetAPIError(code).Code), nil
						}
						versionDeleted = true
					}
				} else {
					switch code := s3a.routedDeleteSpecificVersion(owner, req.Bucket, req.ObjectPath, req.VersionId, worm && !isDeleteMarkerEntry(entry), false, metadataOnly, nil); code {
					case s3err.ErrNone:
						versionDeleted = true
					case s3err.ErrAccessDenied:
						return retryLater("ACCESS_DENIED: deleteSpecificVersion: object lock"), nil
					default:
						return retryLater("TRANSPORT_ERROR: routedDeleteSpecificVersion: " + s3err.GetAPIError(code).Code), nil
					}
				}
			}
		}
		if !versionDeleted {
			if err := s3a.deleteSpecificObjectVersion(ctx, req.Bucket, req.ObjectPath, req.VersionId, metadataOnly); err != nil {
				if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrObjectNotFound) {
					return noopResolved("NOT_FOUND_AT_DELETE"), nil
				}
				return retryLater("TRANSPORT_ERROR: deleteSpecificVersion: " + err.Error()), nil
			}
		}
		recordMetadataOnlyIf(metadataOnly, req)
		return done(), nil

	case s3_lifecycle_pb.ActionKind_ABORT_MPU:
		return blocked("FATAL_EVENT_ERROR: ABORT_MPU dispatched after fetch"), nil

	default:
		return blocked("FATAL_EVENT_ERROR: unknown action_kind " + req.ActionKind.String()), nil
	}
}

// lifecycleDeleteMarker writes an expiration's delete marker as a
// compare-and-set on the .versions pointer, skipping it when the latest is
// already a delete marker. An attempt the worker gave up on may still be
// running while its retry writes, so whichever commits second sees the marker.
// A non-empty expected is the version that passed the expiration checks; a
// different latest resolves the request as stale instead of hiding it.
func (s3a *S3ApiServer) lifecycleDeleteMarker(bucket, object, expected string) (written bool, resp *s3_lifecycle_pb.LifecycleDeleteResponse) {
	normalized := s3_constants.NormalizeObjectKey(object)
	if s3a.objectWriteOwner(bucket, normalized) != "" {
		return s3a.lifecycleDeleteMarkerIf(bucket, object, normalized, expected)
	}
	// Off the ring the marker commits without the guard, so the pointer read
	// and the write serialize with other writers on the object write lock.
	code := s3a.withObjectWriteLock(bucket, normalized, nil, func() s3err.ErrorCode {
		written, resp = s3a.lifecycleDeleteMarkerIf(bucket, object, normalized, expected)
		return s3err.ErrNone
	})
	if code != s3err.ErrNone {
		return false, retryLater("TRANSPORT_ERROR: object write lock: " + s3err.GetAPIError(code).Code)
	}
	return written, resp
}

func (s3a *S3ApiServer) lifecycleDeleteMarkerIf(bucket, object, normalized, expected string) (bool, *s3_lifecycle_pb.LifecycleDeleteResponse) {
	for range 3 {
		pointer, err := s3a.getObjectScopedEntryRoutedByKey(bucket, normalized, s3a.bucketDir(bucket), normalized+s3_constants.VersionsFolder)
		if err != nil {
			if !isObjectEntryMiss(err) {
				return false, retryLater("TRANSPORT_ERROR: latest-pointer lookup: " + err.Error())
			}
			pointer = nil
		}
		if pointer != nil && string(pointer.Extended[s3_constants.ExtLatestVersionIsDeleteMarker]) == "true" && !nullVersionIsLatest(pointer) {
			return false, nil
		}
		if expected != "" && pointer != nil {
			if latest := pointerLatestVersion(pointer); latest != "" && latest != expected {
				return false, noopResolved("VERSION_NOT_LATEST")
			}
		}
		guard := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
			Kind:          filer_pb.WriteCondition_IF_ENTRY_EQUAL,
			ExpectedEntry: pointer,
		}}}
		if _, err := s3a.createDeleteMarkerIf(bucket, object, guard); err != nil {
			if errors.Is(err, errLatestVersionChanged) {
				continue
			}
			return false, retryLater("TRANSPORT_ERROR: createDeleteMarker: " + err.Error())
		}
		return true, nil
	}
	return false, retryLater("LATEST_CHANGED: createDeleteMarker")
}

// lifecycleNullVersion reads the null object the suspended expiration will
// delete. When it is the version validated for expiration, a different or
// missing entry resolves the request as stale.
func (s3a *S3ApiServer) lifecycleNullVersion(req *s3_lifecycle_pb.LifecycleDeleteRequest, validated *filer_pb.Entry) (*filer_pb.Entry, *s3_lifecycle_pb.LifecycleDeleteResponse) {
	nullEntry, err := s3a.getObjectEntryRoutedByKey(req.Bucket, req.ObjectPath)
	if err != nil && !isObjectEntryMiss(err) {
		return nil, retryLater("TRANSPORT_ERROR: null version lookup: " + err.Error())
	}
	if err != nil || !isNullVersionObject(nullEntry, req.ObjectPath) {
		nullEntry = nil
	}
	if lifecycleAddressedVersion(req, validated) == "null" && (nullEntry == nil || !identityMatches(computeEntryIdentity(nullEntry), computeEntryIdentity(validated))) {
		return nil, noopResolved("STALE_IDENTITY")
	}
	return nullEntry, nil
}

// lifecycleDeleteNullVersion deletes the null version once the marker hides
// it, even past the attempt deadline. Only the entry read before the marker is
// deleted: a null object written since is newer and stays. Lifecycle never
// bypasses retention.
func (s3a *S3ApiServer) lifecycleDeleteNullVersion(req *s3_lifecycle_pb.LifecycleDeleteRequest, nullEntry *filer_pb.Entry, metadataOnly bool) *s3_lifecycle_pb.LifecycleDeleteResponse {
	if owner := s3a.routableWriteOwner(req.Bucket, req.ObjectPath); owner != "" {
		worm, lockErr := s3a.isObjectLockEnabled(req.Bucket)
		if lockErr != nil {
			return retryLater("TRANSPORT_ERROR: object lock lookup: " + lockErr.Error())
		}
		guard := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
			Kind:          filer_pb.WriteCondition_IF_ENTRY_EQUAL,
			ExpectedEntry: nullEntry,
		}}}
		switch code, _ := s3a.routedDeleteNullVersion(owner, req.Bucket, req.ObjectPath, worm, false, metadataOnly, guard); code {
		case s3err.ErrNone, s3err.ErrPreconditionFailed:
			return done()
		case s3err.ErrAccessDenied:
			return retryLater("ACCESS_DENIED: deleteNullVersion: object lock")
		default:
			return retryLater("TRANSPORT_ERROR: routedDeleteNullVersion: " + s3err.GetAPIError(code).Code)
		}
	}
	// Off the ring, writers serialize on the object write lock instead.
	dir, name := util.NewFullPath(s3a.bucketDir(req.Bucket), req.ObjectPath).DirAndName()
	var opErr error
	code := s3a.withObjectWriteLock(req.Bucket, req.ObjectPath, func() s3err.ErrorCode {
		current, err := s3a.getEntry(dir, name)
		switch {
		case errors.Is(err, filer_pb.ErrNotFound):
			return s3err.ErrPreconditionFailed
		case err != nil:
			opErr = err
			return s3err.ErrInternalError
		case !identityMatches(computeEntryIdentity(current), computeEntryIdentity(nullEntry)):
			return s3err.ErrPreconditionFailed
		}
		return s3err.ErrNone
	}, func() s3err.ErrorCode {
		if opErr = s3a.deleteSpecificObjectVersion(context.Background(), req.Bucket, req.ObjectPath, "null", metadataOnly); opErr != nil {
			return s3err.ErrInternalError
		}
		return s3err.ErrNone
	})
	switch {
	case code == s3err.ErrNone || code == s3err.ErrPreconditionFailed:
		return done()
	case opErr != nil:
		return retryLater("TRANSPORT_ERROR: deleteNullVersion: " + opErr.Error())
	default:
		return retryLater("TRANSPORT_ERROR: deleteNullVersion: " + s3err.GetAPIError(code).Code)
	}
}

// pointerLatestVersion reads the latest version off a .versions pointer the
// way getLatestObjectVersion does: a named pointer with its file outranks the
// null object, the null-is-latest signal names the null version, and anything
// else leaves the latest to the caller's earlier read.
func pointerLatestVersion(pointer *filer_pb.Entry) string {
	id := string(pointer.Extended[s3_constants.ExtLatestVersionIdKey])
	if id != "" && len(pointer.Extended[s3_constants.ExtLatestVersionFileNameKey]) > 0 {
		return id
	}
	if nullVersionIsLatest(pointer) {
		return "null"
	}
	return ""
}

// lifecycleRequireLatest refuses a current-version expiration whose version
// is no longer the key's latest. A version file never changes, so the identity
// check cannot see a newer write.
func (s3a *S3ApiServer) lifecycleRequireLatest(req *s3_lifecycle_pb.LifecycleDeleteRequest, entry *filer_pb.Entry) *s3_lifecycle_pb.LifecycleDeleteResponse {
	latest, err := s3a.getLatestObjectVersion(req.Bucket, req.ObjectPath)
	if err != nil {
		if isObjectEntryMiss(err) {
			return noopResolved("NOT_FOUND")
		}
		return retryLater("TRANSPORT_ERROR: latest-version lookup: " + err.Error())
	}
	if versionIdOrNull(latest) != lifecycleAddressedVersion(req, entry) {
		return noopResolved("VERSION_NOT_LATEST")
	}
	return nil
}

func versionIdOrNull(e *filer_pb.Entry) string {
	if id := string(e.Extended[s3_constants.ExtVersionIdKey]); id != "" {
		return id
	}
	return "null"
}

func lifecycleAddressedVersion(req *s3_lifecycle_pb.LifecycleDeleteRequest, entry *filer_pb.Entry) string {
	if req.VersionId != "" {
		return req.VersionId
	}
	return versionIdOrNull(entry)
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
	uploadEntry, err := s3a.getEntry(uploadsFolder, uploadID)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return noopResolved("NOT_FOUND"), nil
		}
		return retryLater("TRANSPORT_ERROR: getEntry: " + err.Error()), nil
	}
	if uploadEntry == nil {
		return noopResolved("NOT_FOUND"), nil
	}
	object := string(uploadEntry.Extended[s3_constants.ExtMultipartObjectKey])
	code := s3a.withObjectWriteLock(req.Bucket, object, nil, func() s3err.ErrorCode {
		return s3a.removeUploadDir(req.Bucket, uploadID, object)
	})
	if code == s3err.ErrNone {
		return done(), nil
	}
	return retryLater("TRANSPORT_ERROR: removeUploadDir: " + s3err.GetAPIError(code).Code), nil
}

// checkSoleSurvivorMarker returns nil to proceed with the delete, or a
// terminal response when state has drifted: count != 1, the surviving
// entry is a different version, the .versions/ directory's latest
// pointer doesn't name versionId, or a bare null-version exists outside
// .versions/. Pointer missing while a marker is present is treated as
// retry-later — the create races with the directory metadata update.
func (s3a *S3ApiServer) checkSoleSurvivorMarker(ctx context.Context, bucket, object, versionId string) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	bucketDir := s3a.bucketDir(bucket)
	versionsDir := bucketDir + "/" + object + s3_constants.VersionsFolder
	count := 0
	var firstName string
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.SeaweedList(ctx, client, versionsDir, "", func(entry *filer_pb.Entry, _ bool) error {
			count++
			if count == 1 && entry != nil {
				firstName = entry.Name
			}
			return nil
		}, "", false, 2)
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return noopResolved("NOT_FOUND"), nil
		}
		return retryLater("TRANSPORT_ERROR: sole-survivor list: " + err.Error()), nil
	}
	if count == 0 {
		return noopResolved("NOT_FOUND"), nil
	}
	if count > 1 {
		return noopResolved("NOT_SOLE_SURVIVOR"), nil
	}
	// SeaweedList delivered a single callback but with a nil entry; we
	// can't compare names so retry rather than silently bypass the
	// marker-replaced check.
	if firstName == "" {
		return retryLater("PENDING_SURVIVOR_ENTRY"), nil
	}
	if versionId != "" && firstName != s3a.getVersionFileName(versionId) {
		return noopResolved("MARKER_REPLACED"), nil
	}
	// Latest-pointer check: createDeleteMarker writes the marker file
	// and then updates the parent directory's Extended map. Reading
	// before the second step lands would see count==1 but no pointer;
	// retry-later rather than mistakenly delete.
	parent, name := path.Split(versionsDir)
	parent = strings.TrimRight(parent, "/")
	if parent == "" {
		parent = "/"
	}
	versionsEntry, err := s3a.getEntry(parent, name)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return noopResolved("NOT_FOUND"), nil
		}
		return retryLater("TRANSPORT_ERROR: latest-pointer lookup: " + err.Error()), nil
	}
	if versionsEntry == nil {
		return noopResolved("NOT_FOUND"), nil
	}
	latest, hasPointer := versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey]
	if !hasPointer || len(latest) == 0 {
		return retryLater("PENDING_LATEST_POINTER"), nil
	}
	if string(latest) != versionId {
		return noopResolved("MARKER_NOT_LATEST"), nil
	}
	// Null-version check: pre-versioning objects survive as the bare
	// <bucket>/<key>. Both regular files and explicit S3 directory-key
	// markers (object names ending in /) qualify; the listing path
	// (s3api_object_versioning.go processExplicitDirectory) treats both
	// as the null version. getEntry uses NewFullPath so a trailing slash
	// in object splits the same as a regular key.
	bareEntry, err := s3a.getEntry(bucketDir, object)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil, nil
		}
		return retryLater("TRANSPORT_ERROR: null-version lookup: " + err.Error()), nil
	}
	if bareEntry != nil && (!bareEntry.IsDirectory || bareEntry.IsDirectoryKeyObject()) {
		return noopResolved("NULL_VERSION_PRESENT"), nil
	}
	return nil, nil
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

// entryUsesMetadataOnlyDelete reports whether the lifecycle delete path
// can skip per-chunk DeleteFile RPCs and rely on the volume's TTL to
// reclaim chunks. Per-write TTL stamping (PR 9377) sets Attributes.TtlSec
// on every entry whose lifecycle rule fits within volume TTL — observing
// a non-zero TtlSec on the live entry is the authoritative signal.
// Defensive nil-checks because the caller may be racing a concurrent
// rewrite that nil-ed Attributes briefly during meta-log replay.
func entryUsesMetadataOnlyDelete(entry *filer_pb.Entry) bool {
	return entry != nil && entry.Attributes != nil && entry.Attributes.TtlSec > 0
}

// recordMetadataOnlyIf bumps the metadata-only counter when on=true.
// Skipped when off so callers don't need a guard at every call site.
// rule_hash is hex-encoded so operators can group by rule when
// debugging; nil rule_hash collapses to the empty string.
func recordMetadataOnlyIf(on bool, req *s3_lifecycle_pb.LifecycleDeleteRequest) {
	if !on || req == nil {
		return
	}
	stats_collect.S3LifecycleMetadataOnlyCounter.WithLabelValues(req.Bucket, hex.EncodeToString(req.RuleHash)).Inc()
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
