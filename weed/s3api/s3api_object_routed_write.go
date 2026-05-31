package s3api

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// objectWriteRouteKeyPrefix namespaces an object's full path into the ring key
// used to resolve and forward its writes. Shared by every routed builder so the
// gateway and filer hash the same key.
const objectWriteRouteKeyPrefix = "s3.object.write:"

// objectRouteKey is the ring key the gateway hashes to resolve an object's owner
// filer. It is also sent as route_key on each routed transaction, so a non-owner
// filer (reached because the gateway's ring view was stale) forwards the
// transaction to the owner. All of an object's writes share this key.
func (s3a *S3ApiServer) objectRouteKey(bucket, object string) string {
	return objectWriteRouteKeyPrefix + s3a.toFilerPath(bucket, object)
}

// routableWriteOwner returns the owner filer for an object's writes, or "" to
// keep them on the distributed lock. All writes to one object (versioned,
// suspended, non-versioned) share the owner. Any lookup error falls back.
func (s3a *S3ApiServer) routableWriteOwner(bucket, object string) pb.ServerAddress {
	if object == "" || s3a.objectWriteLockClient == nil {
		return ""
	}
	// Object-lock PUTs route: a versioned PUT creates a new version (never an
	// overwrite of a locked one), and a non-versioned overwrite is WORM-checked
	// gateway-side before dispatch. WORM-checked deletes use routedObjectOwner.
	return s3a.objectWriteLockClient.PrimaryForKey(s3a.objectRouteKey(bucket, object))
}

// routedObjectOwner is routableWriteOwner restricted to non-versioned,
// non-object-lock buckets, for the unversioned DELETE fast path.
func (s3a *S3ApiServer) routedObjectOwner(bucket, object string) (pb.ServerAddress, bool) {
	if configured, err := s3a.isVersioningConfigured(bucket); err != nil || configured {
		return "", false
	}
	// An unversioned object-lock delete enforces WORM in the lock path; keep it
	// on the lock rather than routing past the check.
	if locked, err := s3a.isObjectLockEnabled(bucket); err != nil || locked {
		return "", false
	}
	owner := s3a.routableWriteOwner(bucket, object)
	return owner, owner != ""
}

// routeWriteCondition reduces the request's conditional headers for a routed
// create. A unique version path carries no precondition and only routes when the
// request is unconditional (a conditional versioned write must check the latest,
// which the lock path does); an overwrite carries the reduced condition.
func routeWriteCondition(r *http.Request, uniqueWritePath bool) (*filer_pb.WriteCondition, bool) {
	cond, ok := buildWriteCondition(r)
	if !ok {
		return nil, false
	}
	if uniqueWritePath && cond != nil {
		return nil, false
	}
	return cond, true
}

// buildWriteCondition reduces the request's conditional headers to a
// WriteCondition. ok=false (combined headers, time conditions, ETag lists, weak
// ETags) keeps gateway-side evaluation under the lock; a nil condition with
// ok=true means unconditional.
func buildWriteCondition(r *http.Request) (*filer_pb.WriteCondition, bool) {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return nil, false
	}
	if !headers.isSet {
		return nil, true
	}
	if !headers.ifModifiedSince.IsZero() || !headers.ifUnmodifiedSince.IsZero() {
		return nil, false
	}
	hasMatch := headers.ifMatch != ""
	hasNoneMatch := headers.ifNoneMatch != ""
	switch {
	case hasMatch && !hasNoneMatch:
		if headers.ifMatch == "*" {
			return clause(filer_pb.WriteCondition_IF_EXISTS), true
		}
		if etag, single := singleStrongETag(headers.ifMatch); single {
			return etagClause(filer_pb.WriteCondition_IF_ETAG_MATCH, etag), true
		}
		return nil, false
	case hasNoneMatch && !hasMatch:
		if headers.ifNoneMatch == "*" {
			return clause(filer_pb.WriteCondition_IF_NOT_EXISTS), true
		}
		if etag, single := singleStrongETag(headers.ifNoneMatch); single {
			return etagClause(filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, etag), true
		}
		return nil, false
	default:
		return nil, false
	}
}

// buildDeleteCondition reduces a DeleteObject's If-Match header to a condition;
// DeleteObject honors only If-Match, matching checkDeleteIfMatch.
func buildDeleteCondition(r *http.Request) (*filer_pb.WriteCondition, bool) {
	ifMatch := strings.TrimSpace(r.Header.Get(s3_constants.IfMatch))
	switch {
	case ifMatch == "":
		return nil, true
	case ifMatch == "*":
		return clause(filer_pb.WriteCondition_IF_EXISTS), true
	default:
		if etag, single := singleStrongETag(ifMatch); single {
			return etagClause(filer_pb.WriteCondition_IF_ETAG_MATCH, etag), true
		}
		return nil, false
	}
}

func clause(kind filer_pb.WriteCondition_Kind) *filer_pb.WriteCondition {
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: kind}}}
}

func etagClause(kind filer_pb.WriteCondition_Kind, etag string) *filer_pb.WriteCondition {
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: kind, Etags: []string{etag}}}}
}

// singleStrongETag returns the normalized ETag when v carries exactly one strong
// ETag, and false for ETag lists or weak ("W/") ETags.
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

func (s3a *S3ApiServer) objectTxnOnFiler(owner pb.ServerAddress, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	var resp *filer_pb.ObjectTransactionResponse
	err := pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.ObjectTransaction(context.Background(), req)
		return e
	})
	return resp, err
}

// routedPut writes an entry as a PUT, optionally followed by finalize mutations,
// as one ObjectTransaction applied in order under lockKey on the owner filer.
// lockKey is normally the entry's own path; a versioned add instead passes the
// object path plus a RECOMPUTE_LATEST finalize, so the version's PUT and its
// .versions pointer flip commit atomically (the recompute scans .versions/ after
// the PUT and sees the new version).
func (s3a *S3ApiServer) routedPut(owner pb.ServerAddress, routeKey, lockKey, filePath string, entry *filer_pb.Entry, cond *filer_pb.WriteCondition, finalize []*filer_pb.ObjectMutation) (*filer_pb.ObjectTransactionResponse, error) {
	mutations := make([]*filer_pb.ObjectMutation, 0, 1+len(finalize))
	mutations = append(mutations, &filer_pb.ObjectMutation{
		Type:      filer_pb.ObjectMutation_PUT,
		Directory: path.Dir(filePath),
		Entry:     entry,
	})
	mutations = append(mutations, finalize...)
	return s3a.objectTxnOnFiler(owner, &filer_pb.ObjectTransactionRequest{
		LockKey:   lockKey,
		RouteKey:  routeKey,
		Condition: cond,
		Mutations: mutations,
	})
}

// routedMkFile builds an entry like filer_pb.MkFile and writes it through a
// routed PUT on the owner filer, for callers that would otherwise mkFile to the
// default filer (e.g. multipart completion of a non-versioned object).
func (s3a *S3ApiServer) routedMkFile(owner pb.ServerAddress, routeKey, parentDir, name string, chunks []*filer_pb.FileChunk, fn func(*filer_pb.Entry)) error {
	now := time.Now().Unix()
	entry := &filer_pb.Entry{
		Name: name,
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
	filePath := parentDir + "/" + name
	resp, err := s3a.routedPut(owner, routeKey, filePath, filePath, entry, nil, nil)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("routed mkfile %s/%s: %s", parentDir, name, resp.Error)
	}
	return nil
}

// writeMultipartObject writes a completed multipart object entry, routed to the
// owner when known (so it serializes with concurrent writes to the same key)
// and falling back to a plain mkFile otherwise. routeKey must be the same key the
// caller used to resolve owner, so owner selection and forwarding stay consistent.
func (s3a *S3ApiServer) writeMultipartObject(owner pb.ServerAddress, routeKey, dir, name string, chunks []*filer_pb.FileChunk, fn func(*filer_pb.Entry)) error {
	if owner != "" {
		return s3a.routedMkFile(owner, routeKey, dir, name, chunks, fn)
	}
	return s3a.mkFile(dir, name, chunks, fn)
}

func (s3a *S3ApiServer) routedDelete(owner pb.ServerAddress, bucket, object string, cond *filer_pb.WriteCondition) (*filer_pb.ObjectTransactionResponse, error) {
	// NewFullPath normalizes a trailing-slash directory-marker key (e.g. "dir/")
	// to the entry name "dir", matching deleteUnversionedObjectWithClient.
	fullpath := util.NewFullPath(s3a.bucketDir(bucket), object)
	dir, name := fullpath.DirAndName()
	return s3a.objectTxnOnFiler(owner, &filer_pb.ObjectTransactionRequest{
		LockKey:   string(fullpath),
		RouteKey:  s3a.objectRouteKey(bucket, object),
		Condition: cond,
		Mutations: []*filer_pb.ObjectMutation{{
			Type:         filer_pb.ObjectMutation_DELETE,
			Directory:    dir,
			Name:         name,
			IsDeleteData: true,
		}},
	})
}

// routedMetadataReplace applies a metadata-only self-copy (REPLACE directive) to
// an existing object in place via a routed PATCH_EXTENDED. The owner merges the
// new managed metadata onto a fresh read of the entry under its per-path lock —
// so a concurrent change to non-managed keys (legal hold, retention, version id)
// is preserved rather than clobbered by a whole-entry rewrite — and bumps mtime.
// updatedMetadata is the full managed-metadata set (processMetadataBytes); the
// delete list is the managed keys the replace dropped.
func (s3a *S3ApiServer) routedMetadataReplace(owner pb.ServerAddress, bucket, object string, current *filer_pb.Entry, updatedMetadata map[string][]byte) error {
	fullpath := util.NewFullPath(s3a.bucketDir(bucket), object)
	dir, name := fullpath.DirAndName()
	var del []string
	for k := range current.Extended {
		if isManagedCopyMetadataKey(k) {
			if _, keep := updatedMetadata[k]; !keep {
				del = append(del, k)
			}
		}
	}
	resp, err := s3a.objectTxnOnFiler(owner, &filer_pb.ObjectTransactionRequest{
		LockKey:  string(fullpath),
		RouteKey: s3a.objectRouteKey(bucket, object),
		Mutations: []*filer_pb.ObjectMutation{{
			Type:           filer_pb.ObjectMutation_PATCH_EXTENDED,
			Directory:      dir,
			Name:           name,
			SetExtended:    updatedMetadata,
			DeleteExtended: del,
			TouchMtime:     true,
		}},
	})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("routed metadata replace %s/%s: %s", bucket, object, resp.Error)
	}
	return nil
}
