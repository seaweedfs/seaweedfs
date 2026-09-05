package s3api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

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
// x-amz-client-token travels with it too, so a rename whose response was lost
// answers its own retry instead of the NoSuchKey its vanished source would give.
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

	clientToken := r.Header.Get(s3_constants.AmzClientToken)
	renameSource := r.Header.Get(s3_constants.AmzRenameSource)

	errCode = s3a.withRenameWriteLocks(bucket, srcObject, dstObject, func() s3err.ErrorCode {
		entry, err := s3a.resolveCopySourceEntry(bucket, srcObject, "", "")
		srcIsPrefixObject := entry.IsPrefixObject()
		entry = prefixObjectSource(entry)
		srcErrCode := classifyCopySourceError(entry, err)

		dstEntry, errCode := s3a.lookupRenameDestination(bucket, dstObject)
		if errCode != s3err.ErrNone {
			return errCode
		}
		switch classifyRenameToken(dstEntry, clientToken, renameSource, dstObject) {
		case renameTokenReused:
			return s3err.ErrIdempotentParameterMismatch
		case renameTokenSameRequest:
			// This rename already committed and only its response was lost, so the
			// source it names is gone for good and no retry can succeed on its own.
			// A source that is back is not that retry: the move is still to be made,
			// and making it is what leaves the caller where it asked to be.
			if srcErrCode == s3err.ErrNoSuchKey {
				return s3err.ErrNone
			}
		}
		if srcErrCode != s3err.ErrNone {
			return srcErrCode
		}
		if errCode := validateSourceConditionalHeaders(r, entry, renameSourceConditionalHeaders); errCode != s3err.ErrNone {
			return errCode
		}
		if errCode := s3a.checkConditionalHeaders(r, bucket, dstObject); errCode != s3err.ErrNone {
			return errCode
		}

		// AtomicRenameEntry moves a directory by moving everything under it, and the keys
		// nested under either end of this rename are not part of what is being renamed.
		keyHoldsNestedKeys := srcIsPrefixObject || (dstEntry != nil && dstEntry.IsDirectory)
		if clientToken != "" {
			s3a.stampRenameToken(bucket, srcObject, dstObject, entry, clientToken, renameSource, keyHoldsNestedKeys)
		}
		return s3a.renameObjectEntry(r.Context(), bucket, srcObject, dstObject, entry, keyHoldsNestedKeys)
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

// renameTokenValidity bounds how long a committed rename answers for its own
// retry. An SDK gives up retrying long before this, so the window covers every
// retry there is, while a token that comes back days later no longer stands in
// for a move the caller means to happen now.
const renameTokenValidity = 24 * time.Hour

// renameToken is what a rename leaves on the object it moves, so that the same
// request replayed after its response was lost can be answered from the
// destination instead of from a source that is no longer there.
//
// Source is the x-amz-rename-source header as it was sent. A retry resends the
// request byte for byte, so equality on the raw value recognises it, and any
// other value is the token used for a different rename. Dest is the key the
// rename wrote, which tells a token that travelled here some other way - on a
// CopyObject of a renamed object, say - from one this rename left.
type renameToken struct {
	Token  string `json:"token"`
	Source string `json:"source"`
	Dest   string `json:"dest"`
	Unix   int64  `json:"unix"`
}

type renameTokenVerdict int

const (
	// renameTokenUnrelated: the destination carries no live token of this request's.
	renameTokenUnrelated renameTokenVerdict = iota
	// renameTokenSameRequest: this very rename already committed here.
	renameTokenSameRequest
	// renameTokenReused: the same token was sent for a different rename.
	renameTokenReused
)

// classifyRenameToken reads what the destination object says about a request
// carrying clientToken.
func classifyRenameToken(dstEntry *filer_pb.Entry, clientToken, renameSource, dstObject string) renameTokenVerdict {
	if clientToken == "" || dstEntry == nil {
		return renameTokenUnrelated
	}
	stamped, found := dstEntry.Extended[s3_constants.SeaweedFSRenameToken]
	if !found {
		return renameTokenUnrelated
	}
	var token renameToken
	if err := json.Unmarshal(stamped, &token); err != nil {
		glog.Warningf("RenameObject: unreadable rename token on %s: %v", dstEntry.Name, err)
		return renameTokenUnrelated
	}
	if token.Token != clientToken || token.Dest != dstObject || time.Since(time.Unix(token.Unix, 0)) > renameTokenValidity {
		return renameTokenUnrelated
	}
	if token.Source != renameSource {
		return renameTokenReused
	}
	return renameTokenSameRequest
}

// markRenameToken puts the client token on the entry the rename moves.
func markRenameToken(srcEntry *filer_pb.Entry, clientToken, renameSource, dstObject string) error {
	stamped, err := json.Marshal(renameToken{Token: clientToken, Source: renameSource, Dest: dstObject, Unix: time.Now().Unix()})
	if err != nil {
		return err
	}
	if srcEntry.Extended == nil {
		srcEntry.Extended = make(map[string][]byte)
	}
	srcEntry.Extended[s3_constants.SeaweedFSRenameToken] = stamped
	return nil
}

// stampRenameToken records the client token on the object about to be moved, so
// that the move carries it to the destination.
//
// The token goes on before the move rather than after it: a move that commits
// and then loses its token is exactly the failure the token exists to cover,
// while a token left on a move that never happened simply rides along with the
// next attempt. Failing to record it costs this rename its idempotency and
// nothing else, so the move goes ahead either way.
func (s3a *S3ApiServer) stampRenameToken(bucket, srcObject, dstObject string, srcEntry *filer_pb.Entry, clientToken, renameSource string, keyHoldsNestedKeys bool) {
	// The ETag as it stands now, read before the token joins it.
	expected := map[string][]byte{s3_constants.ExtETagKey: srcEntry.Extended[s3_constants.ExtETagKey]}

	if err := markRenameToken(srcEntry, clientToken, renameSource, dstObject); err != nil {
		glog.Errorf("RenameObject %s: rename token for %s: %v", bucket, srcObject, err)
		return
	}

	// renameKeyHoldingNestedKeys writes the destination out of this entry, so the
	// token reaches it without a write of its own.
	if keyHoldsNestedKeys {
		return
	}

	// AtomicRenameEntry moves the entry as the filer holds it, so the token has to
	// be on the source for the move to carry it. The precondition keeps the write
	// off an object another gateway replaced in the meantime.
	srcDir, _ := util.FullPath(s3a.toFilerPath(bucket, srcObject)).DirAndName()
	if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.UpdateEntry(context.Background(), client, &filer_pb.UpdateEntryRequest{
			Directory:        srcDir,
			Entry:            srcEntry,
			ExpectedExtended: expected,
		})
	}); err != nil {
		glog.Warningf("RenameObject %s: record rename token on %s: %v", bucket, srcObject, err)
	}
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

// lookupRenameDestination reads what the destination key already holds. A key
// nothing lives at is the ordinary case and comes back as a nil entry.
//
// The move overwrites an existing destination object. A directory there is not a
// conflict: it means other keys are nested under the destination key, and the
// object goes onto the directory they live in, the way a PutObject of that key
// would put it there.
func (s3a *S3ApiServer) lookupRenameDestination(bucket, dstObject string) (*filer_pb.Entry, s3err.ErrorCode) {
	dstDir, dstName := util.FullPath(s3a.toFilerPath(bucket, dstObject)).DirAndName()

	entry, err := s3a.getEntry(dstDir, dstName)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil, s3err.ErrNone
		}
		glog.Errorf("RenameObject %s: destination %s: %v", bucket, dstObject, err)
		return nil, s3err.ErrInternalError
	}
	return entry, s3err.ErrNone
}

func (s3a *S3ApiServer) renameObjectEntry(ctx context.Context, bucket, srcObject, dstObject string, srcEntry *filer_pb.Entry, keyHoldsNestedKeys bool) s3err.ErrorCode {
	srcDir, srcName := util.FullPath(s3a.toFilerPath(bucket, srcObject)).DirAndName()
	dstDir, dstName := util.FullPath(s3a.toFilerPath(bucket, dstObject)).DirAndName()

	if keyHoldsNestedKeys {
		return s3a.renameKeyHoldingNestedKeys(bucket, srcObject, dstObject, srcEntry)
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

// renameKeyHoldingNestedKeys moves an object when either key of the rename is one
// other keys are nested under. Such a key is stored on the directory those keys live
// in, which has to stay where it is, so the object's own data is written at the
// destination and then stripped off the source key - the entry survives as the plain
// directory it also is. Both keys are held under their write locks for the whole
// move, so no other S3 write interleaves; a crash between the two steps leaves the
// destination written and the source still there, which a retry settles.
func (s3a *S3ApiServer) renameKeyHoldingNestedKeys(bucket, srcObject, dstObject string, srcEntry *filer_pb.Entry) s3err.ErrorCode {
	dstPath := util.FullPath(s3a.toFilerPath(bucket, dstObject))
	dstDir, dstName := dstPath.DirAndName()

	chunks, err := s3a.copyChunks(srcEntry, string(dstPath))
	if err != nil {
		glog.Errorf("RenameObject %s: copy chunks of %s: %v", bucket, srcObject, err)
		return s3err.ErrInternalError
	}

	if err := s3a.mkFile(dstDir, dstName, chunks, func(entry *filer_pb.Entry) {
		copyEntryToTarget(entry, srcEntry)
		entry.Chunks = chunks
	}); err != nil {
		glog.Errorf("RenameObject %s: write %s: %v", bucket, dstObject, err)
		s3a.deleteOrphanedChunks(chunks)
		return filerErrorToS3Error(err)
	}

	// The destination holds copies now, so the source's own chunks go with it.
	srcDir, srcName := util.FullPath(s3a.toFilerPath(bucket, srcObject)).DirAndName()
	if err := s3a.rmObject(context.Background(), srcDir, srcName, true, false); err != nil {
		glog.Errorf("RenameObject %s: strip %s: %v", bucket, srcObject, err)
		return s3err.ErrInternalError
	}
	return s3err.ErrNone
}
