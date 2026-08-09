package s3api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var renameSourceConditionalHeaders = sourceConditionalHeaderNames{
	ifMatch:           s3_constants.AmzRenameSourceIfMatch,
	ifNoneMatch:       s3_constants.AmzRenameSourceIfNoneMatch,
	ifModifiedSince:   s3_constants.AmzRenameSourceIfModifiedSince,
	ifUnmodifiedSince: s3_constants.AmzRenameSourceIfUnmodifiedSince,
}

// RenameObjectHandler implements RenameObject:
//
//	PUT /{bucket}/{destination key}?renameObject
//	x-amz-rename-source: /{bucket}/{source key}
//
// The object is moved by the filer's AtomicRenameEntry, so its bytes are never
// read or rewritten and its metadata (ETag, tags, SSE keys) travels unchanged.
// Versioned buckets are rejected: the move would have to rebuild the .versions
// chain, and AWS itself only offers RenameObject on directory buckets, which
// cannot be versioned.
func (s3a *S3ApiServer) RenameObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket, dstObject := s3_constants.GetBucketAndObject(r)

	candidates, errCode := renameSourceCandidates(r, bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
	srcObject := s3a.pickRenameSource(bucket, candidates)

	glog.V(3).Infof("RenameObjectHandler %s: %s => %s", bucket, srcObject, dstObject)

	if len(dstObject) > s3_constants.MaxS3ObjectKeyLength {
		s3err.WriteErrorResponse(w, r, s3err.ErrKeyTooLongError)
		return
	}
	if err := s3a.validateTableBucketObjectPath(bucket, dstObject); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
		return
	}
	// A trailing slash names a directory, and renaming one would move a whole
	// subtree rather than an object.
	if strings.HasSuffix(dstObject, "/") {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	if strings.HasSuffix(srcObject, "/") {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}
	if srcObject == dstObject {
		s3err.WriteErrorResponse(w, r, s3err.ErrRenameDestinationSameAsSource)
		return
	}

	// The route's Auth middleware only authorized the destination, because that
	// is what the request URL names. The source arrives in a header and loses
	// its key, so it needs both read and delete permission checked here.
	if errCode := s3a.authorizeRenameSource(r, bucket, srcObject); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	versioningState, err := s3a.getVersioningState(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("RenameObjectHandler: versioning state for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if versioningState != "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		return
	}

	errCode = s3a.withRenameWriteLocks(bucket, srcObject, dstObject, func() s3err.ErrorCode {
		entry, err := s3a.resolveCopySourceEntry(bucket, srcObject, "", "")
		if errCode := classifyCopySourceError(entry, err); errCode != s3err.ErrNone {
			return errCode
		}
		if errCode := validateSourceConditionalHeaders(r, entry, renameSourceConditionalHeaders); errCode != s3err.ErrNone {
			return errCode
		}
		if errCode := s3a.checkConditionalHeaders(r, bucket, dstObject); errCode != s3err.ErrNone {
			return errCode
		}
		return s3a.renameObjectEntry(r.Context(), bucket, srcObject, dstObject)
	})
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	stats_collect.RecordBucketActiveTime(bucket)
	writeSuccessResponseEmpty(w, r)
}

// renameSourceCandidates reads x-amz-rename-source into the source keys it may
// mean, best guess first.
//
// AWS spells the source both ways: its CLI, Java and Rust examples pass a bare
// key, while a second CLI example and the boto3 conditional example pass
// bucket/key. A value is therefore read as a literal key first — that is the
// form AWS leads with, and it is the only reading that can never name the wrong
// object — and, when it is prefixed with the request's own bucket, as that
// bucket-qualified form second. There is no cross-bucket reading: RenameObject
// moves within one bucket, and the filer refuses to move an entry between two.
func renameSourceCandidates(r *http.Request, bucket string) ([]string, s3err.ErrorCode) {
	rawSource := r.Header.Get(s3_constants.AmzRenameSource)
	if rawSource == "" {
		return nil, s3err.ErrInvalidRenameSource
	}
	// PathUnescape, not QueryUnescape: the value is a path, where '+' is a
	// literal plus and not a space.
	source, err := url.PathUnescape(rawSource)
	if err != nil {
		source = rawSource
	}

	// NormalizeObjectKey drops the leading slash both forms may carry.
	source = s3_constants.NormalizeObjectKey(source)
	if source == "" {
		return nil, s3err.ErrInvalidRenameSource
	}

	candidates := []string{source}
	if qualified := strings.TrimPrefix(source, bucket+"/"); qualified != source && qualified != "" {
		candidates = append(candidates, qualified)
	}
	// `.`/`..` segments are collapsed by the filer's path join, so reject them
	// here as the request URL's own key already is.
	for _, candidate := range candidates {
		if !s3_constants.IsValidObjectKey(candidate) {
			return nil, s3err.ErrInvalidRenameSource
		}
	}
	return candidates, s3err.ErrNone
}

// pickRenameSource resolves which reading of the source header the bucket
// actually holds. A single candidate is returned unprobed, so the common bare
// key costs no extra lookup; when both readings are possible the one the bucket
// holds wins, and when neither does the last is reported missing.
//
// Only a proven absence moves on to the next reading. A path that holds
// something the rename cannot move — a directory, say — is still the path the
// caller named, and answering for it beats renaming a different object under
// the other reading; so is a path whose lookup merely failed, since a blip must
// not be able to redirect a rename.
func (s3a *S3ApiServer) pickRenameSource(bucket string, candidates []string) string {
	for _, candidate := range candidates[:len(candidates)-1] {
		// A trailing slash never names an object, and never reaches a usable
		// directory/name split either.
		if strings.HasSuffix(candidate, "/") {
			continue
		}
		if !renameSourceAbsent(s3a.resolveCopySourceEntry(bucket, candidate, "", "")) {
			return candidate
		}
	}
	return candidates[len(candidates)-1]
}

// renameSourceAbsent reports whether a lookup proved the candidate absent. Only
// the filer saying so counts; a lookup that failed for any other reason is not
// a proof of absence.
func renameSourceAbsent(entry *filer_pb.Entry, err error) bool {
	if entry != nil {
		return false
	}
	return err == nil || errors.Is(err, filer_pb.ErrNotFound) || status.Code(err) == codes.NotFound
}

func (s3a *S3ApiServer) authorizeRenameSource(r *http.Request, bucket, srcObject string) s3err.ErrorCode {
	if s3a.iam == nil || !s3a.iam.isEnabled() {
		return s3err.ErrNone
	}
	var identity *Identity
	if id, ok := s3_constants.GetIdentityFromContext(r).(*Identity); ok {
		identity = id
	}
	// The rename both reads the source object and removes it from its key.
	if errCode := s3a.iam.AuthorizeCopySource(r, identity, bucket, srcObject, ""); errCode != s3err.ErrNone {
		return errCode
	}
	return s3a.iam.AuthorizeObjectDelete(r, identity, bucket, srcObject, "")
}

// withRenameWriteLocks holds the object write lock of both keys across the
// precondition checks and the move. The keys are locked in a fixed order so a
// rename in the opposite direction cannot deadlock against this one.
func (s3a *S3ApiServer) withRenameWriteLocks(bucket, srcObject, dstObject string, fn func() s3err.ErrorCode) s3err.ErrorCode {
	first, second := srcObject, dstObject
	if second < first {
		first, second = second, first
	}
	return s3a.withObjectWriteLock(bucket, first, nil, func() s3err.ErrorCode {
		return s3a.withObjectWriteLock(bucket, second, nil, fn)
	})
}

func (s3a *S3ApiServer) renameObjectEntry(ctx context.Context, bucket, srcObject, dstObject string) s3err.ErrorCode {
	srcDir, srcName := util.FullPath(s3a.toFilerPath(bucket, srcObject)).DirAndName()
	dstDir, dstName := util.FullPath(s3a.toFilerPath(bucket, dstObject)).DirAndName()

	// The move overwrites an existing destination object, but a directory in
	// the way is a conflict the filer reports as an opaque error.
	if existing, err := s3a.getEntry(dstDir, dstName); err == nil && existing.IsDirectory {
		return s3err.ErrExistingObjectIsDirectory
	} else if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.Errorf("RenameObject %s: destination %s: %v", bucket, dstObject, err)
		return s3err.ErrInternalError
	}

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: srcDir,
			OldName:      srcName,
			NewDirectory: dstDir,
			NewName:      dstName,
		})
		return err
	})
	if err != nil {
		glog.Errorf("RenameObject %s: %s => %s: %v", bucket, srcObject, dstObject, err)
		if isTransientFilerError(err) {
			return s3err.ErrServiceUnavailable
		}
		return s3err.ErrInternalError
	}
	return s3err.ErrNone
}
