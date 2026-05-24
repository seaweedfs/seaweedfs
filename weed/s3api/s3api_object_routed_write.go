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

// routableWriteOwner returns the owner filer for an object's writes, or "" to
// keep them on the distributed lock. All writes to one object (versioned,
// suspended, non-versioned) share the owner; object-lock buckets stay on the
// lock until WORM-guard routing. Any lookup error also falls back.
func (s3a *S3ApiServer) routableWriteOwner(bucket, object string) pb.ServerAddress {
	if object == "" || s3a.objectWriteLockClient == nil {
		return ""
	}
	if locked, err := s3a.isObjectLockEnabled(bucket); err != nil || locked {
		return ""
	}
	return s3a.objectWriteLockClient.PrimaryForKey(fmt.Sprintf("s3.object.write:%s", s3a.toFilerPath(bucket, object)))
}

// routedObjectOwner is routableWriteOwner restricted to non-versioned buckets,
// for the unversioned DELETE fast path.
func (s3a *S3ApiServer) routedObjectOwner(bucket, object string) (pb.ServerAddress, bool) {
	if configured, err := s3a.isVersioningConfigured(bucket); err != nil || configured {
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

// routedPut writes an object entry as a one-mutation ObjectTransaction on the
// owner filer. lock_key is the object's full path so the transaction shares the
// per-path lock with a concurrent create or delete of the same key.
func (s3a *S3ApiServer) routedPut(owner pb.ServerAddress, filePath string, entry *filer_pb.Entry, cond *filer_pb.WriteCondition) (*filer_pb.ObjectTransactionResponse, error) {
	return s3a.objectTxnOnFiler(owner, &filer_pb.ObjectTransactionRequest{
		LockKey:   filePath,
		Condition: cond,
		Mutations: []*filer_pb.ObjectMutation{{
			Type:      filer_pb.ObjectMutation_PUT,
			Directory: path.Dir(filePath),
			Entry:     entry,
		}},
	})
}

// routedMkFile builds an entry like filer_pb.MkFile and writes it through a
// routed PUT on the owner filer, for callers that would otherwise mkFile to the
// default filer (e.g. multipart completion of a non-versioned object).
func (s3a *S3ApiServer) routedMkFile(owner pb.ServerAddress, parentDir, name string, chunks []*filer_pb.FileChunk, fn func(*filer_pb.Entry)) error {
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
	resp, err := s3a.routedPut(owner, parentDir+"/"+name, entry, nil)
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
// and falling back to a plain mkFile otherwise.
func (s3a *S3ApiServer) writeMultipartObject(owner pb.ServerAddress, dir, name string, chunks []*filer_pb.FileChunk, fn func(*filer_pb.Entry)) error {
	if owner != "" {
		return s3a.routedMkFile(owner, dir, name, chunks, fn)
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
		Condition: cond,
		Mutations: []*filer_pb.ObjectMutation{{
			Type:         filer_pb.ObjectMutation_DELETE,
			Directory:    dir,
			Name:         name,
			IsDeleteData: true,
		}},
	})
}
