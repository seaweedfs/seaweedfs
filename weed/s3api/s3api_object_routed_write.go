package s3api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// routedObjectOwner returns the filer that owns this object's metadata for
// route-by-key on a single-entry object write, or ok=false when the write must
// keep the distributed lock. Only versioning-*enabled* buckets are excluded:
// their writes go to <obj>/.versions and flip the latest pointer (the versioned
// finalize path handles those). Suspended and unversioned writes both go to the
// main object path, so they route here. Object-lock buckets stay on the lock
// path. On any lookup error it falls back to be safe.
func (s3a *S3ApiServer) routedObjectOwner(bucket, object string) (pb.ServerAddress, bool) {
	if object == "" {
		return "", false
	}
	if enabled, err := s3a.isVersioningEnabled(bucket); err != nil || enabled {
		return "", false
	}
	if locked, err := s3a.isObjectLockEnabled(bucket); err != nil || locked {
		return "", false
	}
	owner := s3a.objectWriteOwner(bucket, object)
	if owner == "" {
		return "", false
	}
	return owner, true
}

// routedObjectWrite decides whether an object PUT can take the route-by-key
// fast path: the metadata write goes straight to the key's owner filer, which
// serializes it with its local per-path lock and evaluates the precondition,
// instead of acquiring a distributed lock. It returns the owner filer and the
// reduced precondition, or ok=false to fall back to withObjectWriteLock.
//
// The fast path additionally requires no post-create hook and a precondition
// that reduces to one primitive.
func (s3a *S3ApiServer) routedObjectWrite(r *http.Request, bucket, object string, hasAfterCreate bool) (owner pb.ServerAddress, cond *filer_pb.WriteCondition, ok bool) {
	if hasAfterCreate {
		return "", nil, false
	}
	cond, condOk := buildWriteCondition(r)
	if !condOk {
		return "", nil, false
	}
	owner, ok = s3a.routedObjectOwner(bucket, object)
	if !ok {
		return "", nil, false
	}
	return owner, cond, true
}

// buildWriteCondition reduces the request's conditional headers to a single
// WriteCondition primitive the filer can evaluate. It only handles the
// unambiguous single-condition cases; anything else (header combinations,
// time-based conditions, ETag lists, weak ETags) returns ok=false so the caller
// keeps the existing gateway-side evaluation under the distributed lock.
func buildWriteCondition(r *http.Request) (*filer_pb.WriteCondition, bool) {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return nil, false
	}
	if !headers.isSet {
		return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_NONE}, true
	}
	// Time-based conditions are rare on writes; let the lock path handle them.
	if !headers.ifModifiedSince.IsZero() || !headers.ifUnmodifiedSince.IsZero() {
		return nil, false
	}
	hasMatch := headers.ifMatch != ""
	hasNoneMatch := headers.ifNoneMatch != ""
	switch {
	case hasMatch && !hasNoneMatch:
		if headers.ifMatch == "*" {
			return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_EXISTS}, true
		}
		if etag, single := singleStrongETag(headers.ifMatch); single {
			return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etag: etag}, true
		}
		return nil, false
	case hasNoneMatch && !hasMatch:
		if headers.ifNoneMatch == "*" {
			return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}, true
		}
		if etag, single := singleStrongETag(headers.ifNoneMatch); single {
			return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etag: etag}, true
		}
		return nil, false
	default:
		// Both If-Match and If-None-Match present — leave to the lock path.
		return nil, false
	}
}

// singleStrongETag returns the normalized ETag when the header carries exactly
// one strong ETag, and false for ETag lists or weak ("W/") ETags, which the
// fast path does not replicate.
func singleStrongETag(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if strings.Contains(v, ",") {
		return "", false
	}
	if strings.HasPrefix(v, "W/") || strings.HasPrefix(v, "w/") {
		return "", false
	}
	return strings.Trim(v, `"`), true
}

// mkFileRouted builds a file entry the way filer_pb.MkFile does, applies fn, and
// routes the CreateEntry (with an optional precondition) to the given owner filer
// so its local per-path lock serializes the write. Used by callers that build an
// object via a modifier (e.g. multipart completion) but want the routed,
// distributed-lock-free write path.
func (s3a *S3ApiServer) mkFileRouted(owner pb.ServerAddress, dir, name string, chunks []*filer_pb.FileChunk, cond *filer_pb.WriteCondition, fn func(*filer_pb.Entry)) s3err.ErrorCode {
	now := time.Now().Unix()
	entry := &filer_pb.Entry{
		Name:        name,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    now,
			Crtime:   now,
			FileMode: uint32(0770),
			Uid:      filer_pb.OS_UID,
			Gid:      filer_pb.OS_GID,
		},
		Chunks: chunks,
	}
	if fn != nil {
		fn(entry)
	}
	resp, err := s3a.createEntryOnFiler(owner, &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry:     entry,
		Condition: cond,
	})
	switch {
	case err != nil:
		glog.Errorf("mkFileRouted: %s/%s on %s: %v", dir, name, owner, err)
		return s3err.ErrInternalError
	case resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED:
		return s3err.ErrPreconditionFailed
	case resp.ErrorCode != filer_pb.FilerError_OK:
		if code, ok := filerErrorCodeToS3Error(resp.ErrorCode); ok {
			return code
		}
		glog.Errorf("mkFileRouted: %s/%s unexpected code %v", dir, name, resp.ErrorCode)
		return s3err.ErrInternalError
	case resp.Error != "":
		glog.Errorf("mkFileRouted: %s/%s: %s", dir, name, resp.Error)
		return s3err.ErrInternalError
	default:
		return s3err.ErrNone
	}
}

// createEntryOnFiler sends a CreateEntry directly to the given owner filer so
// its local per-path lock serializes the write. The raw response is returned so
// the caller can distinguish PRECONDITION_FAILED from other outcomes.
func (s3a *S3ApiServer) createEntryOnFiler(owner pb.ServerAddress, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	var resp *filer_pb.CreateEntryResponse
	err := pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.CreateEntry(context.Background(), req)
		return e
	})
	return resp, err
}

// filerErrorCodeToS3Error maps a routed response's machine-readable FilerError
// to the same S3 error the lock path produces via filerErrorToS3Error, so the
// fast path keeps identical semantics. ok is false for codes it does not map,
// signalling the caller to fall back to the lock path for exact behavior.
func filerErrorCodeToS3Error(code filer_pb.FilerError) (s3err.ErrorCode, bool) {
	switch code {
	case filer_pb.FilerError_PRECONDITION_FAILED:
		return s3err.ErrPreconditionFailed, true
	case filer_pb.FilerError_ENTRY_NAME_TOO_LONG:
		return s3err.ErrKeyTooLongError, true
	case filer_pb.FilerError_PARENT_IS_FILE, filer_pb.FilerError_EXISTING_IS_FILE:
		return s3err.ErrExistingObjectIsFile, true
	case filer_pb.FilerError_EXISTING_IS_DIRECTORY:
		return s3err.ErrExistingObjectIsDirectory, true
	default:
		return s3err.ErrNone, false
	}
}

// buildDeleteCondition reduces a DeleteObject's If-Match header to a primitive.
// DeleteObject only honors If-Match (matching checkDeleteIfMatch), so other
// conditional headers are ignored here as they are on the existing path.
func buildDeleteCondition(r *http.Request) (*filer_pb.WriteCondition, bool) {
	ifMatch := strings.TrimSpace(r.Header.Get(s3_constants.IfMatch))
	switch {
	case ifMatch == "":
		return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_NONE}, true
	case ifMatch == "*":
		return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_EXISTS}, true
	default:
		if etag, single := singleStrongETag(ifMatch); single {
			return &filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etag: etag}, true
		}
		return nil, false
	}
}

// deleteEntryOnFiler sends a non-recursive DeleteEntry for the object directly
// to its owner filer, which serializes it and evaluates the precondition under
// the per-path lock. Flags mirror the unversioned delete path (doDeleteEntry).
func (s3a *S3ApiServer) deleteEntryOnFiler(owner pb.ServerAddress, bucket, object string, cond *filer_pb.WriteCondition) (*filer_pb.DeleteEntryResponse, error) {
	dir, name := util.NewFullPath(s3a.bucketDir(bucket), object).DirAndName()
	var resp *filer_pb.DeleteEntryResponse
	err := pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:            dir,
			Name:                 name,
			IsDeleteData:         true,
			IsRecursive:          false,
			IgnoreRecursiveError: true,
			Condition:            cond,
		})
		return e
	})
	return resp, err
}
