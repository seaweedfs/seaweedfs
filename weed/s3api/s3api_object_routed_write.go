package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// objectWriteRouteKeyPrefix namespaces an object's full path into the ring key
// used to resolve and forward its writes. Shared by every routed builder so the
// gateway, the admin dashboard and the filer hash the same key.
const objectWriteRouteKeyPrefix = s3_constants.ObjectWriteRouteKeyPrefix

// objectRouteKey is the ring key the gateway hashes to resolve an object's owner
// filer. It is also sent as route_key on each routed transaction, so a non-owner
// filer (reached because the gateway's ring view was stale) forwards the
// transaction to the owner. All of an object's writes share this key.
func (s3a *S3ApiServer) objectRouteKey(bucket, object string) string {
	// A trailing-slash marker key ("dir/") names the same entry as the object
	// ("dir"), so both hash and lock to one owner.
	return objectWriteRouteKeyPrefix + strings.TrimSuffix(s3a.toFilerPath(bucket, object), "/")
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
// WriteCondition evaluated under the owner's object-path lock. Every header
// form reduces to clauses (ETag lists and weak ETags compare through the same
// canonicalization the gateway applies; time conditions become unix bounds),
// composed per RFC 7232 precedence: If-Unmodified-Since is ignored when
// If-Match is present, If-Modified-Since when If-None-Match is. ok=false only
// when the headers cannot even parse; a nil condition with ok=true means
// unconditional.
func buildWriteCondition(r *http.Request) (*filer_pb.WriteCondition, bool) {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return nil, false
	}
	if !headers.isSet {
		return nil, true
	}
	var clauses []*filer_pb.WriteCondition_Clause
	if headers.ifMatch == "*" {
		clauses = append(clauses, &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_EXISTS})
	} else if headers.ifMatch != "" {
		clauses = append(clauses, &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: etagList(headers.ifMatch)})
	}
	if !headers.ifUnmodifiedSince.IsZero() && headers.ifMatch == "" {
		clauses = append(clauses, &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_UNMODIFIED_SINCE, UnixTime: headers.ifUnmodifiedSince.Unix()})
	}
	if headers.ifNoneMatch == "*" {
		clauses = append(clauses, &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS})
	} else if headers.ifNoneMatch != "" {
		clauses = append(clauses, &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, Etags: etagList(headers.ifNoneMatch)})
	}
	if !headers.ifModifiedSince.IsZero() && headers.ifNoneMatch == "" {
		clauses = append(clauses, &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_MODIFIED_SINCE, UnixTime: headers.ifModifiedSince.Unix()})
	}
	return &filer_pb.WriteCondition{Clauses: clauses}, true
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

// etagList splits a conditional ETag header into its candidates, each kept
// verbatim — the evaluator's canonicalization handles quotes and weak markers.
func etagList(v string) []string {
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// objectTxnOnFiler applies a routed transaction, preferring the object's owner
// filer but failing over to a live filer when the owner is unreachable, so a
// restarted filer's stale ring address (e.g. a rolled K8s pod's old IP) can't hang
// the write. The reached filer forwards to the real owner by route_key, keeping its
// per-path lock authoritative. Mirrors getObjectEntryRoutedByKey on the read path.
// The transaction is not replayable: it fails over only over a channel that
// never came up, never past a possibly-committed attempt — a lost response is
// ambiguous and re-sending could re-apply mutations over a write that landed in
// between. The caller resolves that ambiguity (e.g. createAfterAmbiguousRoute).
func (s3a *S3ApiServer) objectTxnOnFiler(owner pb.ServerAddress, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	// One id per logical transaction: failover retries reuse it so the owner
	// replays the recorded outcome instead of re-applying the mutations.
	if req.RequestId == "" {
		req.RequestId = uuid.NewString()
	}
	var resp *filer_pb.ObjectTransactionResponse
	txn := func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.ObjectTransaction(context.Background(), req)
		return e
	}
	if s3a.filerClient == nil {
		err := pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, txn)
		return resp, err
	}
	// Skip an owner whose recent write hit a transport error so a dead/stale
	// address isn't re-dialed every request.
	preferred := owner
	if s3a.ownerRecentlyUnreachable(owner) {
		preferred = ""
	}
	err := s3a.withFilerClientFailover(context.Background(), preferred, false, false, txn)
	return resp, err
}

// withWriteOwnerFilerClient runs fn on the object's write owner so a read
// routed to the owner sees the change without waiting for replication. An
// error fn returns on the owner is authoritative and surfaces as-is: replaying
// the mutation on a lagging replica could update a stale entry and still
// report success. Only a channel that never came up — provably nothing sent —
// lets the failover walk reach another filer; with no resolved owner the
// default filer applies the mutation.
func (s3a *S3ApiServer) withWriteOwnerFilerClient(owner pb.ServerAddress, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if owner == "" {
		return s3a.WithFilerClient(false, fn)
	}
	if s3a.filerClient == nil {
		return pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, fn)
	}
	preferred := owner
	if s3a.ownerRecentlyUnreachable(owner) {
		preferred = ""
	}
	return s3a.withFilerClientFailover(context.Background(), preferred, false, false, fn)
}

// routedPut writes an entry as a PUT, optionally followed by finalize mutations,
// as one ObjectTransaction applied in order under lockKey on the owner filer.
// lockKey is normally the entry's own path; a versioned add instead passes the
// object path plus a RECOMPUTE_LATEST finalize, so the version's PUT and its
// .versions pointer flip commit atomically (the recompute scans .versions/ after
// the PUT and sees the new version).
// putCondition, when set, guards the PUT mutation itself against putConditionKey
// (e.g. the upload directory a multipart commit must still find); the request
// condition keeps guarding the object the transaction is about.
func (s3a *S3ApiServer) routedPut(owner pb.ServerAddress, routeKey, lockKey, filePath string, entry *filer_pb.Entry, cond *filer_pb.WriteCondition, conditionKey, conditionAltKey string, conditionPreferAlt bool, putCondition *filer_pb.WriteCondition, putConditionKey string, finalize []*filer_pb.ObjectMutation) (*filer_pb.ObjectTransactionResponse, error) {
	mutations := make([]*filer_pb.ObjectMutation, 0, 1+len(finalize))
	mutations = append(mutations, &filer_pb.ObjectMutation{
		Type:         filer_pb.ObjectMutation_PUT,
		Directory:    path.Dir(filePath),
		Entry:        entry,
		Condition:    putCondition,
		ConditionKey: putConditionKey,
	})
	mutations = append(mutations, finalize...)
	return s3a.objectTxnOnFiler(owner, &filer_pb.ObjectTransactionRequest{
		LockKey:            lockKey,
		RouteKey:           routeKey,
		Condition:          cond,
		ConditionKey:       conditionKey,
		ConditionAltKey:    conditionAltKey,
		ConditionPreferAlt: conditionPreferAlt,
		Mutations:          mutations,
	})
}

// routedMkFile builds an entry like filer_pb.MkFile and writes it through a
// routed PUT on the owner filer, for callers that would otherwise mkFile to the
// default filer (e.g. multipart completion of a non-versioned object). cond is
// the request's write condition, evaluated on the object under the transaction
// lock; condAltKey falls back to the .versions directory when the object slot
// is empty (suspended or versioned buckets).
func (s3a *S3ApiServer) routedMkFile(owner pb.ServerAddress, routeKey, parentDir, name string, chunks []*filer_pb.FileChunk, fn func(*filer_pb.Entry), removal *uploadRemovalTxn, cond *filer_pb.WriteCondition, condAltKey string) error {
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
	var putCondition *filer_pb.WriteCondition
	var putConditionKey string
	var finalize []*filer_pb.ObjectMutation
	if removal != nil {
		putCondition, putConditionKey = removal.putCondition, removal.putConditionKey
		finalize = []*filer_pb.ObjectMutation{removal.mutation}
	}
	resp, err := s3a.routedPut(owner, routeKey, filePath, filePath, entry, cond, "", condAltKey, false, putCondition, putConditionKey, finalize)
	if err != nil {
		return err
	}
	if resp.ErrorCode == filer_pb.FilerError_EXISTING_IS_DIRECTORY && !isReservedDirectoryName(name) {
		// Other keys are nested under this one, so the object goes onto the
		// directory they live in — the same retry mkFile makes off the ring.
		entry.MarkPrefixObject()
		resp, err = s3a.routedPut(owner, routeKey, filePath, filePath, entry, cond, "", condAltKey, false, putCondition, putConditionKey, finalize)
		if err != nil {
			return err
		}
	}
	if resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED {
		if resp.FailedMutation == nil {
			return errObjectPrecondition
		}
		return errUploadRemoved
	}
	if resp.Error != "" {
		return fmt.Errorf("routed mkfile %s/%s: %s", parentDir, name, resp.Error)
	}
	return nil
}

// errUploadRemoved is a routed commit rejected because the upload directory's
// precondition found it already deleted, mapped to NoSuchUpload by callers.
var errUploadRemoved = errors.New("upload directory already removed")

// errObjectPrecondition is a routed commit rejected because the request's write
// condition (e.g. If-None-Match: *) found the object slot already taken by a
// racing commit, mapped to PreconditionFailed by callers.
var errObjectPrecondition = errors.New("object precondition failed")

// writeMultipartObject writes a completed multipart object entry, routed to the
// owner when known (so it serializes with concurrent writes to the same key)
// and falling back to a plain mkFile otherwise. removal carries the upload
// precondition and deletion applied in the same transaction as the routed PUT.
// routeKey must be the same key the caller used to resolve owner, so owner
// selection and forwarding stay consistent. cond is the request's write
// condition, enforced on the owner under the transaction lock; condAltKey is
// the .versions directory a conditional check falls back to when the bucket
// tracks versions.
func (s3a *S3ApiServer) writeMultipartObject(owner pb.ServerAddress, routeKey, dir, name string, chunks []*filer_pb.FileChunk, fn func(*filer_pb.Entry), removal *uploadRemovalTxn, cond *filer_pb.WriteCondition, condAltKey string) error {
	if owner != "" {
		return s3a.routedMkFile(owner, routeKey, dir, name, chunks, fn, removal, cond, condAltKey)
	}
	return s3a.mkFile(dir, name, chunks, fn)
}

// uploadRemovalTxn carries the PUT precondition and mutation that drop a
// completed upload's directory inside the object's commit transaction. The
// precondition fails the commit when a delete that does not take the object
// lock (abort, lifecycle, s3.clean.uploads) removed the directory first,
// instead of publishing the object over freed chunks.
type uploadRemovalTxn struct {
	putCondition    *filer_pb.WriteCondition
	putConditionKey string
	mutation        *filer_pb.ObjectMutation
}

// routedUploadRemoval returns the transaction parts that remove a completed
// upload's directory inside the object's commit, after freeing the part
// entries the object does not reference. It is nil when the write is not
// routed and the upload directory still needs post-commit cleanup.
func (s3a *S3ApiServer) routedUploadRemoval(ctx context.Context, owner pb.ServerAddress, uploadDirectory, bucket, uploadId string, completionState *multipartCompletionState) (*uploadRemovalTxn, error) {
	if owner == "" {
		return nil, nil
	}
	// Older filers ignore the mutation's upload-exists condition, so during an
	// upgrade this gateway-side check keeps the same guard window the
	// pre-transaction code had.
	if exists, err := s3a.exists(s3a.genUploadsFolder(bucket), uploadId, true); err == nil && !exists {
		return nil, errUploadRemoved
	}
	if err := s3a.deleteUnusedPartEntries(ctx, uploadDirectory, bucket, uploadId, completionState); err != nil {
		return nil, err
	}
	conditionKey, condition := uploadExistsCondition(uploadDirectory)
	return &uploadRemovalTxn{
		putCondition:    condition,
		putConditionKey: conditionKey,
		mutation:        s3a.removeUploadDirMutation(bucket, uploadId),
	}, nil
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

// routedSelfCopy updates metadata and attributes together, guarded by the raw
// source snapshot. UpdateEntry routes conditional writes to the owner and checks
// the condition under its path lock, so concurrent content, ACL or retention
// updates cannot be overwritten by a stale self-copy. Chunks are reused in place.
func (s3a *S3ApiServer) routedSelfCopy(ctx context.Context, owner pb.ServerAddress, bucket, object string, current, updated *filer_pb.Entry) error {
	fullpath := util.NewFullPath(s3a.bucketDir(bucket), object)
	dir, _ := fullpath.DirAndName()
	req := &filer_pb.UpdateEntryRequest{
		Directory: dir,
		Entry:     updated,
		Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
			Kind:          filer_pb.WriteCondition_IF_ENTRY_EQUAL,
			ExpectedEntry: current,
		}}},
	}
	var conditionErr error
	update := func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.UpdateEntry(ctx, client, req)
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.NotFound:
			// These are authoritative replies from a healthy owner, not a
			// transport failure. Do not replay them or mark the owner down.
			conditionErr = err
			return nil
		default:
			return err
		}
	}
	var err error
	if s3a.filerClient == nil {
		err = pb.WithFilerClient(false, 0, owner, s3a.option.GrpcDialOption, update)
	} else {
		err = s3a.withFilerClientFailover(ctx, owner, false, false, update)
	}
	if err != nil {
		return err
	}
	return conditionErr
}
