package s3api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	DirectiveCopy    = "COPY"
	DirectiveReplace = "REPLACE"
)

// AWS default when REPLACE is requested without a Content-Type.
const defaultCopyContentType = "binary/octet-stream"

// System-metadata headers that REPLACE must rewrite on the destination.
// Content-Type lives on Attributes.Mime, tagging/storage-class have their
// own handling, so they're not in this list.
var copyReplaceSystemHeaders = []string{
	"Cache-Control",
	"Content-Encoding",
	"Content-Disposition",
	"Content-Language",
	"Expires",
}

func resolveDestinationMime(reqHeader http.Header, sourceMime string, replaceMeta bool) string {
	if replaceMeta {
		if ct := reqHeader.Get("Content-Type"); ct != "" {
			return ct
		}
		return defaultCopyContentType
	}
	return sourceMime
}

// Empty means default (COPY). Anything else outside {COPY, REPLACE} must be
// rejected, not silently downgraded.
func isValidDirective(value string) bool {
	return value == "" || value == DirectiveCopy || value == DirectiveReplace
}

// hasPrefixFold reports whether s starts with prefix, ignoring case.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// classifyCopySourceError maps a copy-source lookup to an S3 error: a missing
// or directory source is NoSuchKey like AWS, but a transient store error
// becomes a retryable 5xx so a resumable copy/commit survives a blip.
func classifyCopySourceError(entry *filer_pb.Entry, err error) s3err.ErrorCode {
	if err == nil {
		if entry == nil || entry.IsDirectory {
			return s3err.ErrNoSuchKey
		}
		if deleteMarker, ok := entry.Extended[s3_constants.ExtDeleteMarkerKey]; ok && string(deleteMarker) == "true" {
			return s3err.ErrNoSuchKey
		}
		return s3err.ErrNone
	}
	if errors.Is(err, filer_pb.ErrNotFound) || status.Code(err) == codes.NotFound ||
		errors.Is(err, errInvalidVersionID) || errors.Is(err, ErrDeleteMarker) {
		return s3err.ErrNoSuchKey
	}
	if isTransientFilerError(err) {
		return s3err.ErrServiceUnavailable
	}
	return s3err.ErrInternalError
}

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	t := time.Now().UTC().Truncate(time.Millisecond)

	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	rawCopySource := r.Header.Get("X-Amz-Copy-Source")
	// Use PathUnescape (not QueryUnescape) because the copy source is a path,
	// not a query string. QueryUnescape would incorrectly convert '+' to space.
	cpSrcPath, err := url.PathUnescape(rawCopySource)
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = rawCopySource
	}

	srcBucket, srcObject, srcVersionId := pathToBucketObjectAndVersion(rawCopySource, cpSrcPath)

	glog.V(3).Infof("CopyObjectHandler %s %s (version: %s) => %s %s", srcBucket, srcObject, srcVersionId, dstBucket, dstObject)
	if len(dstObject) > s3_constants.MaxS3ObjectKeyLength {
		s3err.WriteErrorResponse(w, r, s3err.ErrKeyTooLongError)
		return
	}
	if err := s3a.validateTableBucketObjectPath(dstBucket, dstObject); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
		return
	}
	if srcBucket != "" && srcBucket != dstBucket {
		if err := s3a.validateTableBucketObjectPath(srcBucket, srcObject); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}
	}

	// Validate copy source and destination
	if err := validateCopySource(cpSrcPath, srcBucket, srcObject, srcVersionId); err != nil {
		glog.V(2).Infof("CopyObjectHandler validation error: %v", err)
		errCode := MapCopyValidationError(err)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if err := ValidateCopyDestination(dstBucket, dstObject); err != nil {
		glog.V(2).Infof("CopyObjectHandler validation error: %v", err)
		errCode := MapCopyValidationError(err)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if !isValidDirective(r.Header.Get(s3_constants.AmzUserMetaDirective)) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMetadataDirective)
		return
	}
	if !isValidDirective(r.Header.Get(s3_constants.AmzObjectTaggingDirective)) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidTagDirective)
		return
	}

	replaceMeta, replaceTagging := replaceDirective(r.Header)

	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	// CopyObject requires s3:GetObject on the source in addition to s3:PutObject
	// on the destination. The Auth middleware only checked the destination, so
	// verify the source explicitly here.
	if errCode := s3a.authorizeCopySource(r, srcBucket, srcObject, srcVersionId); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Get detailed versioning state for source bucket
	srcVersioningState, err := s3a.getVersioningState(srcBucket)
	if err != nil {
		glog.Errorf("Error checking versioning state for source bucket %s: %v", srcBucket, err)
		// A missing source bucket is NoSuchBucket like AWS; a store error must
		// stay retryable, matching the destination-bucket lookup.
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	entry, err := s3a.resolveCopySourceEntry(srcBucket, srcObject, srcVersionId, srcVersioningState)
	if errCode := classifyCopySourceError(entry, err); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Cache remote-only sources before copying; otherwise the copy path below
	// writes a destination with FileSize > 0 but no chunks/content.
	if entry.IsInRemoteOnly() {
		cacheVersionId := resolvedSourceVersionId(srcVersionId, entry)
		cachedEntry := s3a.cacheRemoteObjectForCopy(r.Context(), srcBucket, srcObject, cacheVersionId)
		if cachedEntry == nil {
			glog.Errorf("CopyObjectHandler: failed to cache remote-only source %s/%s (version %q)", srcBucket, srcObject, cacheVersionId)
			w.Header().Set("Retry-After", "5")
			s3err.WriteErrorResponse(w, r, s3err.ErrServiceUnavailable)
			return
		}
		entry = cachedEntry
	}

	sameDestination := srcBucket == dstBucket && srcObject == dstObject
	if sameDestination && !(replaceMeta || replaceTagging) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopyDest)
		return
	}

	// Validate conditional copy headers
	if err := s3a.validateConditionalCopyHeaders(r, entry); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	if errCode := s3a.checkConditionalHeaders(r, dstBucket, dstObject); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Validate encryption parameters
	if err := ValidateCopyEncryption(entry.Extended, r.Header); err != nil {
		glog.V(2).Infof("CopyObjectHandler encryption validation error: %v", err)
		errCode := MapCopyValidationError(err)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	dstVersioningState, err := s3a.getVersioningState(dstBucket)
	if err != nil {
		glog.Errorf("Error checking versioning state for destination bucket %s: %v", dstBucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if sameDestination && (replaceMeta || replaceTagging) && s3a.canUseMetadataOnlySelfCopy(entry, r, dstBucket, dstObject) {
		var dstVersionId string
		var etag string
		// A non-versioned in-place metadata replace routes to the owner as a
		// serialized PATCH (off the distributed lock); versioned/suspended (which
		// create a new version) and the no-owner bootstrap keep the lock.
		//
		// REPLACE can also change Content-Type, which lives on Attributes.Mime,
		// not Extended. The routed PATCH only carries Extended keys, so when the
		// Mime actually changes keep the lock and take the clone path below: it is
		// still metadata-only (reuses the source chunks) but can set the Mime.
		owner := s3a.objectWriteOwner(dstBucket, dstObject)
		sourceMime := entry.GetAttributes().GetMime()
		mimeChanged := resolveDestinationMime(r.Header, sourceMime, replaceMeta) != sourceMime
		routeInPlace := owner != "" && dstVersioningState == "" && !mimeChanged
		selfCopyBody := func() s3err.ErrorCode {
			currentEntry, currentErr := s3a.resolveCopySourceEntry(srcBucket, srcObject, srcVersionId, srcVersioningState)
			if errCode := classifyCopySourceError(currentEntry, currentErr); errCode != s3err.ErrNone {
				return errCode
			}
			if errCode := s3a.validateConditionalCopyHeaders(r, currentEntry); errCode != s3err.ErrNone {
				return errCode
			}
			updatedMetadata, metadataErr := processMetadataBytes(r.Header, currentEntry.Extended, replaceMeta, replaceTagging)
			if metadataErr != nil {
				glog.Errorf("CopyObjectHandler ValidateTags error %s: %v", r.URL, metadataErr)
				return s3err.ErrInvalidTag
			}
			if routeInPlace {
				if err := s3a.routedMetadataReplace(owner, dstBucket, dstObject, currentEntry, updatedMetadata); err != nil {
					return filerErrorToS3Error(err)
				}
				etag = getEtagFromEntry(currentEntry)
				return s3err.ErrNone
			}
			updatedEntry := cloneProtoEntry(currentEntry)
			updatedEntry.Extended = mergeCopyMetadata(updatedEntry.Extended, updatedMetadata)
			if updatedEntry.Attributes == nil {
				updatedEntry.Attributes = &filer_pb.FuseAttributes{}
			}
			updatedEntry.Attributes.Mime = resolveDestinationMime(r.Header, currentEntry.GetAttributes().GetMime(), replaceMeta)
			updatedEntry.Attributes.Mtime = t.Unix()
			var finErr error
			dstVersionId, etag, finErr = s3a.finalizeCopyDestination(dstBucket, dstObject, dstVersioningState, updatedEntry)
			if finErr != nil {
				return filerErrorToS3Error(finErr)
			}
			return s3err.ErrNone
		}
		var updateCode s3err.ErrorCode
		if routeInPlace {
			if updateCode = s3a.checkConditionalHeaders(r, dstBucket, dstObject); updateCode == s3err.ErrNone {
				updateCode = selfCopyBody()
			}
		} else {
			updateCode = s3a.withObjectWriteLock(dstBucket, dstObject, func() s3err.ErrorCode {
				return s3a.checkConditionalHeaders(r, dstBucket, dstObject)
			}, selfCopyBody)
		}
		if updateCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, updateCode)
			return
		}

		if dstVersionId != "" {
			w.Header().Set("x-amz-version-id", dstVersionId)
		}
		setEtag(w, etag)
		writeSuccessResponseXML(w, r, CopyObjectResult{
			ETag:         etag,
			LastModified: t,
		})
		return
	}

	// Determine whether we can reuse the source MD5 (direct copy without encryption changes).
	canReuseSourceMd5 := false
	var sourceMd5 []byte
	if entry.Attributes != nil && len(entry.Attributes.Md5) > 0 {
		sourceMd5 = append([]byte(nil), entry.Attributes.Md5...)
		srcPath := fmt.Sprintf("%s/%s", s3a.bucketDir(srcBucket), srcObject)
		dstPath := fmt.Sprintf("%s/%s", s3a.bucketDir(dstBucket), dstObject)
		state := DetectEncryptionStateWithEntry(entry, r, srcPath, dstPath)
		s3a.applyCopyBucketDefaultEncryption(state, dstBucket)
		if strategy, err := DetermineUnifiedCopyStrategy(state, entry.Extended, r); err == nil && strategy == CopyStrategyDirect {
			canReuseSourceMd5 = true
		}
	}

	// Create new entry for destination
	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: entry.Attributes.FileSize,
			Mtime:    t.Unix(),
			Crtime:   entry.Attributes.Crtime,
			Mime:     resolveDestinationMime(r.Header, entry.Attributes.Mime, replaceMeta),
		},
		Extended: make(map[string][]byte),
	}

	// Copy extended attributes from source, filtering out conflicting encryption metadata
	// Pre-compute encryption state once for efficiency
	srcHasSSEC := IsSSECEncrypted(entry.Extended)
	srcHasSSEKMS := IsSSEKMSEncrypted(entry.Extended)
	srcHasSSES3 := IsSSES3EncryptedInternal(entry.Extended)
	dstWantsSSEC := IsSSECRequest(r)
	dstWantsSSEKMS := IsSSEKMSRequest(r)
	dstWantsSSES3 := IsSSES3RequestInternal(r)

	for k, v := range entry.Extended {
		// Skip encryption-specific headers that might conflict with destination encryption type
		skipHeader := false

		// Skip orphaned SSE-S3 headers (header exists but key is missing)
		// This prevents confusion about the object's actual encryption state
		if isOrphanedSSES3Header(k, entry.Extended) {
			skipHeader = true
		}

		// Filter conflicting headers for cross-encryption or encrypted→unencrypted copies
		// This applies to both inline files (no chunks) and chunked files - fixes GitHub #7562
		if !skipHeader {
			skipHeader = shouldSkipEncryptionHeader(k,
				srcHasSSEC, srcHasSSEKMS, srcHasSSES3,
				dstWantsSSEC, dstWantsSSEKMS, dstWantsSSES3)
		}

		if !skipHeader {
			dstEntry.Extended[k] = v
		}
	}

	// Process metadata and tags and apply to destination
	// Use dstEntry.Extended (already filtered) as the source, not entry.Extended,
	// to preserve the encryption header filtering. Fixes GitHub #7562.
	processedMetadata, tagErr := processMetadataBytes(r.Header, dstEntry.Extended, replaceMeta, replaceTagging)
	if tagErr != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	// mergeCopyMetadata drops stale managed keys before applying the new set,
	// so REPLACE doesn't leak source values through the merge. Mirrors the
	// self-copy path's routedMetadataReplace.
	dstEntry.Extended = mergeCopyMetadata(dstEntry.Extended, processedMetadata)

	// For zero-size files or files without chunks, handle inline content
	// This includes encrypted inline files that need decryption/re-encryption
	if entry.Attributes.FileSize == 0 || len(entry.GetChunks()) == 0 {
		dstEntry.Chunks = nil

		// Handle inline encrypted content - fixes GitHub #7562.
		// Also run when the destination requests encryption with no content so
		// empty objects get real key metadata, not just a bare algorithm header.
		if len(entry.Content) > 0 || dstWantsSSEC || dstWantsSSEKMS || dstWantsSSES3 {
			inlineContent, inlineMetadata, inlineErr := s3a.processInlineContentForCopy(
				entry, r, dstBucket, dstObject,
				srcHasSSEC, srcHasSSEKMS, srcHasSSES3,
				dstWantsSSEC, dstWantsSSEKMS, dstWantsSSES3)
			if inlineErr != nil {
				glog.Errorf("CopyObjectHandler inline content error: %v", inlineErr)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
			dstEntry.Content = inlineContent

			// Apply inline destination metadata
			if inlineMetadata != nil {
				for k, v := range inlineMetadata {
					dstEntry.Extended[k] = v
				}
			}
		}

		if dstEntry.Attributes != nil {
			if len(dstEntry.Attributes.Md5) == 0 && canReuseSourceMd5 {
				dstEntry.Attributes.Md5 = append([]byte(nil), sourceMd5...)
			} else if uint64(len(dstEntry.Content)) == dstEntry.Attributes.FileSize {
				dstEntry.Attributes.Md5 = util.Md5(dstEntry.Content)
			}
		}
	} else {
		// Use unified copy strategy approach
		dstChunks, dstMetadata, copyErr := s3a.executeUnifiedCopyStrategy(entry, r, srcBucket, dstBucket, srcObject, dstObject)
		if copyErr != nil {
			glog.Errorf("CopyObjectHandler unified copy error: %v", copyErr)
			// Map errors to appropriate S3 errors
			errCode := s3a.mapCopyErrorToS3Error(copyErr)
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}

		dstEntry.Chunks = dstChunks

		// Apply destination-specific metadata (e.g., SSE-C IV and headers)
		if dstMetadata != nil {
			for k, v := range dstMetadata {
				dstEntry.Extended[k] = v
			}
			glog.V(2).Infof("Applied %d destination metadata entries for copy: %s", len(dstMetadata), r.URL.Path)
		}

		if dstEntry.Attributes != nil && len(dstEntry.Attributes.Md5) == 0 && canReuseSourceMd5 {
			dstEntry.Attributes.Md5 = append([]byte(nil), sourceMd5...)
		}
	}

	var dstVersionId string
	var etag string

	finalizeCode := s3a.withObjectWriteLock(dstBucket, dstObject, func() s3err.ErrorCode {
		return s3a.checkConditionalHeaders(r, dstBucket, dstObject)
	}, func() s3err.ErrorCode {
		var finalizeErr error
		dstVersionId, etag, finalizeErr = s3a.finalizeCopyDestination(dstBucket, dstObject, dstVersioningState, dstEntry)
		if finalizeErr != nil {
			return filerErrorToS3Error(finalizeErr)
		}
		return s3err.ErrNone
	})
	if finalizeCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, finalizeCode)
		return
	}

	if dstVersionId != "" {
		w.Header().Set("x-amz-version-id", dstVersionId)
	}

	setEtag(w, etag)

	response := CopyObjectResult{
		ETag:         etag,
		LastModified: t,
	}

	writeSuccessResponseXML(w, r, response)

}

func cloneProtoEntry(entry *filer_pb.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*filer_pb.Entry)
}

func copyEntryETag(entry *filer_pb.Entry) string {
	if entry == nil {
		return ""
	}
	if entry.Extended != nil {
		if etag, ok := entry.Extended[s3_constants.ExtETagKey]; ok && len(etag) > 0 {
			return string(etag)
		}
	}
	attr := filer.Attr{}
	if entry.Attributes != nil {
		attr = filer.Attr{
			FileSize: entry.Attributes.FileSize,
			Mtime:    time.Unix(entry.Attributes.Mtime, 0),
			Crtime:   time.Unix(entry.Attributes.Crtime, 0),
			Mime:     entry.Attributes.Mime,
			Md5:      entry.Attributes.Md5,
		}
	}
	return filer.ETagEntry(&filer.Entry{
		Attr:    attr,
		Chunks:  entry.Chunks,
		Content: entry.Content,
		Remote:  entry.RemoteEntry,
	})
}

func copyEntryToTarget(dst, src *filer_pb.Entry) {
	dst.IsDirectory = src.IsDirectory
	dst.Attributes = src.Attributes
	dst.Extended = src.Extended
	dst.Chunks = src.Chunks
	dst.Content = src.Content
	dst.RemoteEntry = src.RemoteEntry
	dst.HardLinkId = src.HardLinkId
	dst.HardLinkCounter = src.HardLinkCounter
	dst.Quota = src.Quota
	dst.WormEnforcedAtTsNs = src.WormEnforcedAtTsNs
}

func (s3a *S3ApiServer) finalizeCopyDestination(dstBucket, dstObject, dstVersioningState string, dstEntry *filer_pb.Entry) (versionId string, etag string, err error) {
	normalizedObject := s3_constants.NormalizeObjectKey(dstObject)
	dstPath := util.FullPath(fmt.Sprintf("%s/%s", s3a.bucketDir(dstBucket), normalizedObject))
	dstDir, dstName := dstPath.DirAndName()

	if dstEntry.Attributes == nil {
		dstEntry.Attributes = &filer_pb.FuseAttributes{}
	}
	if dstEntry.Extended == nil {
		dstEntry.Extended = make(map[string][]byte)
	}

	etag = copyEntryETag(dstEntry)

	switch dstVersioningState {
	case s3_constants.VersioningEnabled:
		versionId = s3a.generateVersionIdForObject(dstBucket, normalizedObject)
		glog.V(2).Infof("CopyObjectHandler: creating version %s for destination %s/%s", versionId, dstBucket, normalizedObject)

		dstEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
		dstEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

		versionFileName := s3a.getVersionFileName(versionId)
		versionObjectPath := normalizedObject + s3_constants.VersionsFolder + "/" + versionFileName
		bucketDir := s3a.bucketDir(dstBucket)

		if err = s3a.mkFile(bucketDir, versionObjectPath, dstEntry.Chunks, func(entry *filer_pb.Entry) {
			copyEntryToTarget(entry, dstEntry)
		}); err != nil {
			return "", "", err
		}

		// Route the pointer flip to the owner filer when known (off the
		// distributed lock); RECOMPUTE_LATEST picks the just-written version.
		if owner := s3a.objectWriteOwner(dstBucket, normalizedObject); owner != "" {
			if code := s3a.routedVersionedFinalize(owner, dstBucket, normalizedObject, isNewFormatVersionId(versionId)); code != s3err.ErrNone {
				if rollbackErr := s3a.rollbackCopyVersion(bucketDir, versionObjectPath); rollbackErr != nil {
					glog.Errorf("CopyObjectHandler: failed to rollback version %s for %s/%s after routed finalize error: %v", versionId, dstBucket, normalizedObject, rollbackErr)
				}
				return "", "", fmt.Errorf("routed finalize for %s/%s: code %d", dstBucket, normalizedObject, code)
			}
		} else if err = s3a.updateLatestVersionInDirectory(dstBucket, normalizedObject, versionId, versionFileName, dstEntry); err != nil {
			if rollbackErr := s3a.rollbackCopyVersion(bucketDir, versionObjectPath); rollbackErr != nil {
				glog.Errorf("CopyObjectHandler: failed to rollback version %s for %s/%s after latest pointer update error: %v", versionId, dstBucket, normalizedObject, rollbackErr)
			}
			glog.Errorf("CopyObjectHandler: failed to update latest version in directory: %v", err)
			return "", "", fmt.Errorf("update latest version metadata: %w", err)
		}

		return versionId, etag, nil

	case s3_constants.VersioningSuspended:
		cleanupVersioningMetadata(dstEntry.Extended)
		dstEntry.Extended[s3_constants.ExtVersionIdKey] = []byte("null")
		dstEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

		if err = s3a.mkFile(dstDir, dstName, dstEntry.Chunks, func(entry *filer_pb.Entry) {
			copyEntryToTarget(entry, dstEntry)
		}); err != nil {
			return "", "", err
		}

		if err = s3a.updateIsLatestFlagsForSuspendedVersioning(dstBucket, normalizedObject); err != nil {
			glog.Warningf("CopyObjectHandler: failed to update suspended version latest flags for %s/%s: %v", dstBucket, normalizedObject, err)
		}

		return "", etag, nil

	default:
		cleanupVersioningMetadata(dstEntry.Extended)
		dstEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

		if err = s3a.mkFile(dstDir, dstName, dstEntry.Chunks, func(entry *filer_pb.Entry) {
			copyEntryToTarget(entry, dstEntry)
		}); err != nil {
			return "", "", err
		}

		return "", etag, nil
	}
}

func (s3a *S3ApiServer) rollbackCopyVersion(bucketDir, versionObjectPath string) error {
	versionPath := util.FullPath(fmt.Sprintf("%s/%s", bucketDir, versionObjectPath))
	versionDir, versionName := versionPath.DirAndName()
	return s3a.rmObject(versionDir, versionName, true, false)
}

func (s3a *S3ApiServer) resolveCopySourceEntry(bucket, object, versionId, versioningState string) (*filer_pb.Entry, error) {
	normalizedObject := s3_constants.NormalizeObjectKey(object)

	if versionId != "" {
		return s3a.getSpecificObjectVersion(bucket, normalizedObject, versionId)
	}

	switch versioningState {
	case s3_constants.VersioningEnabled:
		return s3a.getLatestObjectVersion(bucket, normalizedObject)
	case s3_constants.VersioningSuspended:
		return s3a.resolveSuspendedCopySourceEntry(bucket, normalizedObject, "CopyObject")
	default:
		srcPath := util.FullPath(fmt.Sprintf("%s/%s", s3a.bucketDir(bucket), normalizedObject))
		dir, name := srcPath.DirAndName()
		return s3a.getEntry(dir, name)
	}
}

func mergeCopyMetadata(existing, updated map[string][]byte) map[string][]byte {
	merged := make(map[string][]byte, len(existing)+len(updated))
	for k, v := range existing {
		merged[k] = v
	}
	for k := range merged {
		if isManagedCopyMetadataKey(k) {
			delete(merged, k)
		}
	}
	for k, v := range updated {
		merged[k] = v
	}
	return merged
}

func isManagedCopyMetadataKey(key string) bool {
	switch key {
	case s3_constants.AmzStorageClass,
		s3_constants.AmzServerSideEncryption,
		s3_constants.AmzServerSideEncryptionAwsKmsKeyId,
		s3_constants.AmzServerSideEncryptionContext,
		s3_constants.AmzServerSideEncryptionBucketKeyEnabled,
		s3_constants.AmzServerSideEncryptionCustomerAlgorithm,
		s3_constants.AmzServerSideEncryptionCustomerKeyMD5,
		s3_constants.AmzTagCount:
		return true
	}
	for _, h := range copyReplaceSystemHeaders {
		if strings.EqualFold(key, h) {
			return true
		}
	}
	// Match X-Amz-Meta-* / X-Amz-Tagging case-insensitively so legacy
	// non-canonical keys (written by non-S3 paths or older versions) are
	// still recognized as managed.
	return hasPrefixFold(key, s3_constants.AmzUserMetaPrefix) ||
		hasPrefixFold(key, s3_constants.AmzObjectTagging)
}

func (s3a *S3ApiServer) resolveSuspendedCopySourceEntry(bucket, normalizedObject, operation string) (*filer_pb.Entry, error) {
	srcPath := util.FullPath(fmt.Sprintf("%s/%s", s3a.bucketDir(bucket), normalizedObject))
	dir, name := srcPath.DirAndName()
	entry, err := s3a.getEntry(dir, name)
	if err == nil {
		return entry, nil
	}
	if !errors.Is(err, filer_pb.ErrNotFound) {
		return nil, err
	}
	glog.V(2).Infof("%s: regular file not found for suspended versioning, trying latest version", operation)
	return s3a.getLatestObjectVersion(bucket, normalizedObject)
}

// authorizeCopySource enforces s3:GetObject on the CopyObject / UploadPartCopy
// source. The route's Auth middleware only verifies destination permissions
// because the request URL points at the destination; the source from the
// X-Amz-Copy-Source header must be authorized separately.
func (s3a *S3ApiServer) authorizeCopySource(r *http.Request, srcBucket, srcObject, srcVersionId string) s3err.ErrorCode {
	if s3a.iam == nil || !s3a.iam.isEnabled() {
		return s3err.ErrNone
	}
	var identity *Identity
	if id, ok := s3_constants.GetIdentityFromContext(r).(*Identity); ok {
		identity = id
	}
	return s3a.iam.AuthorizeCopySource(r, identity, srcBucket, srcObject, srcVersionId)
}

func (s3a *S3ApiServer) canUseMetadataOnlySelfCopy(entry *filer_pb.Entry, r *http.Request, bucket, object string) bool {
	srcPath := fmt.Sprintf("%s/%s", s3a.bucketDir(bucket), s3_constants.NormalizeObjectKey(object))
	state := DetectEncryptionStateWithEntry(entry, r, srcPath, srcPath)
	s3a.applyCopyBucketDefaultEncryption(state, bucket)
	strategy, err := DetermineUnifiedCopyStrategy(state, entry.Extended, r)
	return err == nil && strategy == CopyStrategyDirect
}

func pathToBucketAndObject(path string) (bucket, object string) {
	// Remove leading slash if present
	path = strings.TrimPrefix(path, "/")

	// Split by first slash to separate bucket and object
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 2 {
		bucket = parts[0]
		object = parts[1]
		return bucket, object
	} else if len(parts) == 1 && parts[0] != "" {
		// Only bucket provided, no object
		return parts[0], ""
	}
	// Empty path
	return "", ""
}

func pathToBucketObjectAndVersion(rawPath, decodedPath string) (bucket, object, versionId string) {
	pathForBucket := decodedPath

	if rawPath != "" {
		if idx := strings.Index(rawPath, "?"); idx != -1 {
			queryPart := rawPath[idx+1:]
			if values, err := url.ParseQuery(queryPart); err == nil && values.Has("versionId") {
				versionId = values.Get("versionId")

				rawPathNoQuery := rawPath[:idx]
				if unescaped, err := url.PathUnescape(rawPathNoQuery); err == nil {
					pathForBucket = unescaped
				} else {
					pathForBucket = rawPathNoQuery
				}

				bucket, object = pathToBucketAndObject(pathForBucket)
				return bucket, object, versionId
			}
		}
	}

	bucket, object = pathToBucketAndObject(pathForBucket)
	return bucket, object, versionId
}

type CopyPartResult struct {
	LastModified      time.Time `xml:"LastModified"`
	ETag              string    `xml:"ETag"`
	ChecksumCRC32     string    `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string    `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string    `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string    `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string    `xml:"ChecksumSHA256,omitempty"`
}

func buildCopyPartResult(etag string, lastModified time.Time, metadata SSEResponseMetadata) CopyPartResult {
	result := CopyPartResult{
		ETag:         etag,
		LastModified: lastModified,
	}
	switch metadata.ChecksumHeaderName {
	case s3_constants.AmzChecksumCRC32:
		result.ChecksumCRC32 = metadata.ChecksumValue
	case s3_constants.AmzChecksumCRC32C:
		result.ChecksumCRC32C = metadata.ChecksumValue
	case s3_constants.AmzChecksumCRC64NVME:
		result.ChecksumCRC64NVME = metadata.ChecksumValue
	case s3_constants.AmzChecksumSHA1:
		result.ChecksumSHA1 = metadata.ChecksumValue
	case s3_constants.AmzChecksumSHA256:
		result.ChecksumSHA256 = metadata.ChecksumValue
	}
	return result
}

// copyPartLocation returns the destination directory and filename for a
// server-side copy part under the destination bucket's .uploads folder. Copy
// parts carry a fixed "copy" suffix (rather than the random suffix
// genPartUploadPath mints for client uploads) so re-copying a part replaces
// its predecessor in place.
func (s3a *S3ApiServer) copyPartLocation(dstBucket, uploadID string, partID int) (uploadDir, partName string) {
	uploadDir = s3a.genUploadsFolder(dstBucket) + "/" + uploadID
	partName = fmt.Sprintf("%04d_%s.part", partID, "copy")
	return
}

func (s3a *S3ApiServer) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	t := time.Now().UTC().Truncate(time.Millisecond)
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingRESTMPUapi.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	rawCopySource := r.Header.Get("X-Amz-Copy-Source")

	glog.V(4).Infof("CopyObjectPart: Raw copy source header=%q", rawCopySource)

	// Use PathUnescape (not QueryUnescape) because the copy source is a path,
	// not a query string. QueryUnescape would incorrectly convert '+' to space.
	cpSrcPath, err := url.PathUnescape(rawCopySource)
	if err != nil {
		// If unescaping fails, log and use original
		glog.V(4).Infof("CopyObjectPart: Failed to unescape copy source %q: %v, using as-is", rawCopySource, err)
		cpSrcPath = rawCopySource
	}

	srcBucket, srcObject, srcVersionId := pathToBucketObjectAndVersion(rawCopySource, cpSrcPath)

	glog.V(4).Infof("CopyObjectPart: Parsed srcBucket=%q, srcObject=%q, srcVersionId=%q",
		srcBucket, srcObject, srcVersionId)

	if err := s3a.validateTableBucketObjectPath(dstBucket, dstObject); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
		return
	}
	if srcBucket != "" && srcBucket != dstBucket {
		if err := s3a.validateTableBucketObjectPath(srcBucket, srcObject); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}
	}

	// Validate the copy source as CopyObject does.
	if err := validateCopySource(cpSrcPath, srcBucket, srcObject, srcVersionId); err != nil {
		glog.V(2).Infof("CopyObjectPartHandler validation error: %v", err)
		s3err.WriteErrorResponse(w, r, MapCopyValidationError(err))
		return
	}

	// UploadPartCopy requires s3:GetObject on the source. The Auth middleware
	// only verified s3:PutObject (s3:UploadPart) on the destination.
	if errCode := s3a.authorizeCopySource(r, srcBucket, srcObject, srcVersionId); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	partIDString := r.URL.Query().Get("partNumber")
	uploadID := r.URL.Query().Get("uploadId")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}

	// Check if the upload ID is valid
	err = s3a.checkUploadId(dstObject, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	glog.V(3).Infof("CopyObjectPartHandler %s %s => %s part %d upload %s", srcBucket, srcObject, dstBucket, partID, uploadID)

	// check partID with maximum part ID for multipart objects
	if partID > s3_constants.MaxS3MultipartParts {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}
	if partID < 1 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}

	// Get detailed versioning state for source bucket
	srcVersioningState, err := s3a.getVersioningState(srcBucket)
	if err != nil {
		glog.Errorf("Error checking versioning state for source bucket %s: %v", srcBucket, err)
		// A missing source bucket is NoSuchBucket like AWS; a store error must
		// stay retryable, matching the destination-bucket lookup.
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Get the source entry with version awareness based on versioning state
	var entry *filer_pb.Entry
	if srcVersionId != "" {
		// Specific version requested - always use version-aware retrieval
		entry, err = s3a.getSpecificObjectVersion(srcBucket, srcObject, srcVersionId)
	} else if srcVersioningState == s3_constants.VersioningEnabled {
		// Versioning enabled - get latest version from .versions directory
		entry, err = s3a.getLatestObjectVersion(srcBucket, srcObject)
	} else if srcVersioningState == s3_constants.VersioningSuspended {
		// Versioning suspended - current object is stored as regular file ("null" version)
		// Try regular file first, fall back to latest version if needed
		entry, err = s3a.resolveSuspendedCopySourceEntry(srcBucket, s3_constants.NormalizeObjectKey(srcObject), "CopyObjectPart")
	} else {
		// No versioning configured - use regular retrieval
		srcPath := util.FullPath(fmt.Sprintf("%s/%s", s3a.bucketDir(srcBucket), srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
	}

	if errCode := classifyCopySourceError(entry, err); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Cache remote-only sources before copying; the part-copy paths below
	// iterate entry.GetChunks() and would otherwise produce an empty part.
	if entry.IsInRemoteOnly() {
		cacheVersionId := resolvedSourceVersionId(srcVersionId, entry)
		cachedEntry := s3a.cacheRemoteObjectForCopy(r.Context(), srcBucket, srcObject, cacheVersionId)
		if cachedEntry == nil {
			glog.Errorf("CopyObjectPartHandler: failed to cache remote-only source %s/%s (version %q)", srcBucket, srcObject, cacheVersionId)
			w.Header().Set("Retry-After", "5")
			s3err.WriteErrorResponse(w, r, s3err.ErrServiceUnavailable)
			return
		}
		entry = cachedEntry
	}

	// Validate conditional copy headers
	if err := s3a.validateConditionalCopyHeaders(r, entry); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Handle range header if present
	rangeHeader := r.Header.Get("x-amz-copy-source-range")
	var startOffset, endOffset int64
	if rangeHeader != "" {
		startOffset, endOffset, err = parseRangeHeader(rangeHeader)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return
		}
	} else {
		startOffset = 0
		if entry.Attributes.FileSize == 0 {
			endOffset = -1 // For zero-size files, use -1 as endOffset
		} else {
			endOffset = int64(entry.Attributes.FileSize) - 1
		}
	}

	// Fetch the destination upload entry to determine whether the multipart
	// upload was created with SSE configured. If either side has SSE, the
	// fast raw-byte chunk copy below would leave destination chunks tagged
	// inconsistently with the bytes on disk and trigger #8908's deterministic
	// byte corruption on GET. Re-encrypt the source bytes in that case so
	// destination chunks come out properly tagged.
	//
	// checkUploadId above only verifies that the uploadID's hash prefix
	// matches dstObject; it does NOT prove the upload directory exists.
	// Treat a missing upload entry as NoSuchUpload — falling through with
	// uploadEntry=nil would silently skip the SSE check on the destination
	// side and could send a plain-source copy through the raw-byte fast
	// path even though the destination's encryption state is unknown.
	uploadEntry, uploadEntryErr := s3a.getEntry(s3a.genUploadsFolder(dstBucket), uploadID)
	if uploadEntryErr != nil {
		if errors.Is(uploadEntryErr, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
			return
		}
		glog.Errorf("CopyObjectPartHandler: failed to fetch upload entry for %s/%s uploadID=%s: %v",
			dstBucket, dstObject, uploadID, uploadEntryErr)
		// Distinguish transient from permanent errors: gRPC Unavailable
		// (filer briefly unreachable, leader election in flight, etc.) and
		// DeadlineExceeded both indicate the client should retry rather than
		// give up. Map them to 503 ServiceUnavailable; everything else stays
		// as 500 InternalError.
		if isTransientFilerError(uploadEntryErr) {
			s3err.WriteErrorResponse(w, r, s3err.ErrServiceUnavailable)
			return
		}
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if uploadEntryHasSSE(uploadEntry) || sourceEntryHasSSE(entry) || uploadEntryHasChecksum(uploadEntry) {
		etag, sseMetadata, errCode := s3a.copyObjectPartViaReencryption(r, entry, startOffset, endOffset, dstBucket, uploadID, partID, uploadEntry)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}
		setEtag(w, "\""+strings.Trim(etag, "\"")+"\"")
		// Mirror PutObjectPartHandler: write x-amz-server-side-encryption /
		// x-amz-server-side-encryption-aws-kms-key-id headers on the response
		// so clients can see the destination's encryption state.
		s3a.setSSEResponseHeaders(w, r, sseMetadata)
		writeSuccessResponseXML(w, r, buildCopyPartResult(etag, t, sseMetadata))
		return
	}

	// Fast path: neither source nor destination has SSE. Raw byte copy is
	// safe, since the bytes on disk are plaintext on both sides.

	// Create new entry for the part
	// Calculate part size, avoiding underflow for invalid ranges
	partSize := uint64(0)
	if endOffset >= startOffset {
		partSize = uint64(endOffset - startOffset + 1)
	}

	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: partSize,
			Mtime:    t.Unix(),
			Crtime:   t.Unix(),
			Mime:     entry.Attributes.Mime,
		},
		Extended: make(map[string][]byte),
	}

	// The copied part lives under the destination bucket's .uploads folder.
	// Assign destination volumes against that real filer path so they land in
	// the destination bucket's collection. r.URL.Path is the S3 request URI
	// (e.g. /bucket/key), not a filer path, so passing it would skip the
	// filer's bucket-to-collection mapping and route the copied bytes to the
	// default collection.
	uploadDir, partName := s3a.copyPartLocation(dstBucket, uploadID, partID)
	dstPartPath := uploadDir + "/" + partName

	// Handle zero-size files or empty ranges
	if entry.Attributes.FileSize == 0 || endOffset < startOffset {
		// For zero-size files or invalid ranges, create an empty part with size 0
		dstEntry.Attributes.FileSize = 0
		dstEntry.Chunks = nil
	} else {
		// Copy chunks that overlap with the range
		dstChunks, err := s3a.copyChunksForRange(entry, startOffset, endOffset, dstPartPath)
		if err != nil {
			glog.Errorf("CopyObjectPartHandler copy chunks error: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		dstEntry.Chunks = dstChunks
	}

	// Save the part entry to the multipart uploads folder
	// Check if part exists and remove it first (allow re-copying same part)
	if exists, _ := s3a.exists(uploadDir, partName, false); exists {
		if err := s3a.rm(uploadDir, partName, false, false); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}

	if err := s3a.mkFile(uploadDir, partName, dstEntry.Chunks, func(entry *filer_pb.Entry) {
		entry.Attributes = dstEntry.Attributes
		entry.Extended = dstEntry.Extended
	}); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Calculate ETag for the part
	etag := copyEntryETag(dstEntry)
	setEtag(w, etag)

	writeSuccessResponseXML(w, r, buildCopyPartResult(etag, t, SSEResponseMetadata{}))
}

func replaceDirective(reqHeader http.Header) (replaceMeta, replaceTagging bool) {
	return reqHeader.Get(s3_constants.AmzUserMetaDirective) == DirectiveReplace, reqHeader.Get(s3_constants.AmzObjectTaggingDirective) == DirectiveReplace
}

func processMetadataBytes(reqHeader http.Header, existing map[string][]byte, replaceMeta, replaceTagging bool) (metadata map[string][]byte, err error) {
	metadata = make(map[string][]byte)

	if sc := existing[s3_constants.AmzStorageClass]; len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = sc
	}
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	// Handle destination SSE headers from the request when present.
	if sseAlgorithm := reqHeader.Get(s3_constants.AmzServerSideEncryption); sseAlgorithm != "" {
		metadata[s3_constants.AmzServerSideEncryption] = []byte(sseAlgorithm)

		if sseAlgorithm == s3_constants.SSEAlgorithmKMS {
			// KMS Key ID (optional - can use default key)
			if kmsKeyID := reqHeader.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId); kmsKeyID != "" {
				metadata[s3_constants.AmzServerSideEncryptionAwsKmsKeyId] = []byte(kmsKeyID)
			}

			// Encryption Context (optional)
			if encryptionContext := reqHeader.Get(s3_constants.AmzServerSideEncryptionContext); encryptionContext != "" {
				metadata[s3_constants.AmzServerSideEncryptionContext] = []byte(encryptionContext)
			}

			// Bucket Key Enabled (optional)
			if bucketKeyEnabled := reqHeader.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled); bucketKeyEnabled != "" {
				metadata[s3_constants.AmzServerSideEncryptionBucketKeyEnabled] = []byte(bucketKeyEnabled)
			}
		}
	} else {
		// If not explicitly setting SSE, preserve existing SSE headers from source
		for _, sseHeader := range []string{
			s3_constants.AmzServerSideEncryption,
			s3_constants.AmzServerSideEncryptionAwsKmsKeyId,
			s3_constants.AmzServerSideEncryptionContext,
			s3_constants.AmzServerSideEncryptionBucketKeyEnabled,
		} {
			if existingValue, exists := existing[sseHeader]; exists {
				metadata[sseHeader] = existingValue
			}
		}
	}

	// Handle SSE-C headers - these are always processed from request headers if present
	if sseCustomerAlgorithm := reqHeader.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); sseCustomerAlgorithm != "" {
		metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte(sseCustomerAlgorithm)

		if sseCustomerKeyMD5 := reqHeader.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); sseCustomerKeyMD5 != "" {
			metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(sseCustomerKeyMD5)
		}
	} else {
		// If not explicitly setting SSE-C, preserve existing SSE-C headers from source
		for _, ssecHeader := range []string{
			s3_constants.AmzServerSideEncryptionCustomerAlgorithm,
			s3_constants.AmzServerSideEncryptionCustomerKeyMD5,
		} {
			if existingValue, exists := existing[ssecHeader]; exists {
				metadata[ssecHeader] = existingValue
			}
		}
	}

	if replaceMeta {
		for header, values := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
				// Go's HTTP server canonicalizes headers (e.g., x-amz-meta-foo → X-Amz-Meta-Foo)
				// We store them as they come in (after canonicalization) to preserve the user's intent
				for _, value := range values {
					metadata[header] = []byte(value)
				}
			}
		}
		for _, h := range copyReplaceSystemHeaders {
			if v := reqHeader.Get(h); v != "" {
				metadata[h] = []byte(v)
			}
		}
	} else {
		// Two-pass copy keeps the result deterministic when both the
		// canonical and a legacy-cased variant of the same header live on
		// the source: canonical always wins, legacy only fills holes.
		for _, h := range copyReplaceSystemHeaders {
			if v, ok := existing[h]; ok {
				metadata[h] = v
			}
		}
		for k, v := range existing {
			for _, h := range copyReplaceSystemHeaders {
				if k == h {
					continue
				}
				if !strings.EqualFold(k, h) {
					continue
				}
				if _, present := metadata[h]; present {
					continue
				}
				metadata[h] = v
			}
		}
		// Copy existing metadata as-is
		// Note: Metadata should already be normalized during storage (X-Amz-Meta-*),
		// but we handle legacy non-canonical formats for backward compatibility
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				// Already in canonical format
				metadata[k] = v
			} else if len(k) >= 11 && strings.EqualFold(k[:11], "x-amz-meta-") {
				// Backward compatibility: migrate old non-canonical format to canonical format
				// This ensures gradual migration of metadata to consistent format
				suffix := k[11:] // Extract suffix after "x-amz-meta-"
				canonicalKey := s3_constants.AmzUserMetaPrefix + suffix

				if glog.V(3) {
					glog.Infof("Migrating legacy user metadata key %q to canonical format %q during copy", k, canonicalKey)
				}

				// Check for collision with canonical key
				if _, exists := metadata[canonicalKey]; exists {
					glog.Warningf("User metadata key collision during copy migration: canonical key %q already exists, skipping legacy key %q", canonicalKey, k)
				} else {
					metadata[canonicalKey] = v
				}
			}
		}
	}
	if replaceTagging {
		if tags := reqHeader.Get(s3_constants.AmzObjectTagging); tags != "" {
			parsedTags, err := parseTagsHeader(tags)
			if err != nil {
				return nil, err
			}
			err = ValidateTags(parsedTags)
			if err != nil {
				return nil, err
			}
			for k, v := range parsedTags {
				metadata[s3_constants.AmzObjectTagging+"-"+k] = []byte(v)
			}
		}
	} else {
		// Two passes: canonical exact-prefix wins; legacy variants only
		// fill in keys that no canonical entry already provided. Keeps
		// the result deterministic when both variants coexist on the
		// source.
		prefixLen := len(s3_constants.AmzObjectTagging)
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTagging) {
				metadata[k] = v
			}
		}
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTagging) {
				continue
			}
			if !hasPrefixFold(k, s3_constants.AmzObjectTagging) {
				continue
			}
			canonicalKey := s3_constants.AmzObjectTagging + k[prefixLen:]
			if _, present := metadata[canonicalKey]; present {
				continue
			}
			metadata[canonicalKey] = v
		}
		delete(metadata, s3_constants.AmzTagCount)
	}

	return
}

// copyChunks replicates chunks from source entry to destination entry
func (s3a *S3ApiServer) copyChunks(entry *filer_pb.Entry, dstPath string) ([]*filer_pb.FileChunk, error) {
	dstChunks := make([]*filer_pb.FileChunk, len(entry.GetChunks()))
	const defaultChunkCopyConcurrency = 4
	executor := util.NewLimitedConcurrentExecutor(defaultChunkCopyConcurrency) // Limit to configurable concurrent operations
	errChan := make(chan error, len(entry.GetChunks()))

	for i, chunk := range entry.GetChunks() {
		chunkIndex := i
		executor.Execute(func() {
			dstChunk, err := s3a.copySingleChunk(chunk, dstPath)
			if err != nil {
				errChan <- fmt.Errorf("chunk %d: %v", chunkIndex, err)
				return
			}
			dstChunks[chunkIndex] = dstChunk
			errChan <- nil
		})
	}

	// Wait for all operations to complete and check for errors
	for i := 0; i < len(entry.GetChunks()); i++ {
		if err := <-errChan; err != nil {
			return nil, err
		}
	}

	return dstChunks, nil
}

// copySingleChunk copies a single chunk from source to destination, preserving
// the source's SSE tagging (the same-key copy fast path reuses the source
// ciphertext as-is, so the destination chunk must keep the source's SSE_C /
// SSE_KMS / SSE_S3 metadata or the read path will not decrypt — see #9281).
func (s3a *S3ApiServer) copySingleChunk(chunk *filer_pb.FileChunk, dstPath string) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunkPreservingSSE(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, chunk.Size)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Stream the chunk through io.Pipe when no in-transit transformation is
	// required; this holds only ~32 KiB per copy in flight, vs. the
	// chunk-sized buffers the buffered path needs. isFullChunk=true asks
	// the source volume for compressed bytes, so a gzipped chunk is
	// forwarded to the destination without anyone having to decompress.
	if canStreamCopyChunk(chunk) {
		if err := s3a.streamCopyChunkRange(context.Background(), srcUrl, fileId, 0, int64(chunk.Size), true /*isFullChunk*/, assignResult); err != nil {
			return nil, fmt.Errorf("stream chunk: %w", err)
		}
		return dstChunk, nil
	}

	// SSE / per-chunk-cipher: bytes need to be transformed in transit, so
	// download into a buffer first.
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size), chunk.CipherKey)
	if err != nil {
		return nil, fmt.Errorf("download chunk data: %w", err)
	}

	if err := s3a.uploadChunkData(chunkData, assignResult, chunk.IsCompressed); err != nil {
		return nil, fmt.Errorf("upload chunk data: %w", err)
	}

	return dstChunk, nil
}

// copySingleChunkForRange copies a portion of a chunk for range operations
func (s3a *S3ApiServer) copySingleChunkForRange(originalChunk, rangeChunk *filer_pb.FileChunk, rangeStart, rangeEnd int64, dstPath string) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(rangeChunk, rangeChunk.Offset, rangeChunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := originalChunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, rangeChunk.Size)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Calculate the portion of the original chunk that we need to copy
	chunkStart := originalChunk.Offset
	overlapStart := max(rangeStart, chunkStart)
	offsetInChunk := overlapStart - chunkStart

	// Stream the byte range through io.Pipe when there's no in-transit
	// transformation required (see canStreamCopyChunk for the eligibility
	// rules); this is the dominant path for Harbor-style multipart
	// assemble loads, which use UploadPartCopy with a CopySourceRange.
	//
	// When the requested range happens to cover the entire source chunk
	// exactly, switch to the full-chunk fetch mode: that asks the source
	// volume for compressed bytes (Accept-Encoding: gzip) and forwards
	// them as-is, avoiding the volume-side decompression that a Range
	// fetch on a gzipped chunk would otherwise pay. Harbor's typical
	// part-size = chunk-size assemble pattern hits this branch.
	if canStreamCopyChunk(originalChunk) {
		isFullChunk := offsetInChunk == 0 && rangeChunk.Size == originalChunk.Size
		if err := s3a.streamCopyChunkRange(context.Background(), srcUrl, fileId, offsetInChunk, int64(rangeChunk.Size), isFullChunk, assignResult); err != nil {
			return nil, fmt.Errorf("stream chunk range: %w", err)
		}
		return dstChunk, nil
	}

	// Download and upload the chunk portion
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, offsetInChunk, int64(rangeChunk.Size), originalChunk.CipherKey)
	if err != nil {
		return nil, fmt.Errorf("download chunk range data: %w", err)
	}

	if err := s3a.uploadChunkData(chunkData, assignResult, originalChunk.IsCompressed); err != nil {
		return nil, fmt.Errorf("upload chunk range data: %w", err)
	}

	return dstChunk, nil
}

// assignNewVolume assigns a new volume for the chunk
func (s3a *S3ApiServer) assignNewVolume(dstPath string, expectedDataSize uint64) (*filer_pb.AssignVolumeResponse, error) {
	var assignResult *filer_pb.AssignVolumeResponse
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
			Count:            1,
			Replication:      "",
			Collection:       "",
			DiskType:         "",
			DataCenter:       s3a.option.DataCenter,
			Path:             dstPath,
			ExpectedDataSize: expectedDataSize,
		})
		if err != nil {
			return fmt.Errorf("assign volume: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("assign volume: %v", resp.Error)
		}
		assignResult = resp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return assignResult, nil
}

// parseRangeHeader parses the x-amz-copy-source-range header
func parseRangeHeader(rangeHeader string) (startOffset, endOffset int64, err error) {
	// Remove "bytes=" prefix if present
	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	startOffset, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start offset: %w", err)
	}

	endOffset, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end offset: %w", err)
	}

	return startOffset, endOffset, nil
}

// copyChunksForRange copies chunks that overlap with the specified range
func (s3a *S3ApiServer) copyChunksForRange(entry *filer_pb.Entry, startOffset, endOffset int64, dstPath string) ([]*filer_pb.FileChunk, error) {
	var relevantChunks []*filer_pb.FileChunk
	var originalChunks []*filer_pb.FileChunk

	// Find chunks that overlap with the range
	for _, chunk := range entry.GetChunks() {
		chunkStart := chunk.Offset
		chunkEnd := chunk.Offset + int64(chunk.Size)

		// Check if chunk overlaps with the range
		if chunkStart < endOffset+1 && chunkEnd > startOffset {
			// Calculate the overlap
			overlapStart := max(startOffset, chunkStart)
			overlapEnd := min(endOffset+1, chunkEnd)

			// Create a new chunk with adjusted offset and size relative to the range
			newChunk := &filer_pb.FileChunk{
				FileId:       chunk.FileId,
				Offset:       overlapStart - startOffset, // Offset relative to the range start
				Size:         uint64(overlapEnd - overlapStart),
				ModifiedTsNs: time.Now().UnixNano(),
				ETag:         chunk.ETag,
				IsCompressed: chunk.IsCompressed,
				CipherKey:    chunk.CipherKey,
				Fid:          chunk.Fid,
			}
			relevantChunks = append(relevantChunks, newChunk)
			originalChunks = append(originalChunks, chunk)
		}
	}

	// Copy the relevant chunks using a specialized method for range copies
	dstChunks := make([]*filer_pb.FileChunk, len(relevantChunks))
	const defaultChunkCopyConcurrency = 4
	executor := util.NewLimitedConcurrentExecutor(defaultChunkCopyConcurrency)
	errChan := make(chan error, len(relevantChunks))

	for i, chunk := range relevantChunks {
		chunkIndex := i
		originalChunk := originalChunks[i] // Get the corresponding original chunk
		executor.Execute(func() {
			dstChunk, err := s3a.copySingleChunkForRange(originalChunk, chunk, startOffset, endOffset, dstPath)
			if err != nil {
				errChan <- fmt.Errorf("chunk %d: %v", chunkIndex, err)
				return
			}
			dstChunks[chunkIndex] = dstChunk
			errChan <- nil
		})
	}

	// Wait for all operations to complete and check for errors
	for i := 0; i < len(relevantChunks); i++ {
		if err := <-errChan; err != nil {
			return nil, err
		}
	}

	return dstChunks, nil
}

// Helper methods for copy operations to avoid code duplication

// validateConditionalCopyHeaders validates the conditional copy headers against the source entry
func (s3a *S3ApiServer) validateConditionalCopyHeaders(r *http.Request, entry *filer_pb.Entry) s3err.ErrorCode {
	sourceETag := copyEntryETag(entry)

	// Check X-Amz-Copy-Source-If-Match
	if ifMatch := r.Header.Get(s3_constants.AmzCopySourceIfMatch); ifMatch != "" {
		// Remove quotes if present
		ifMatch = strings.Trim(ifMatch, `"`)
		sourceETag = strings.Trim(sourceETag, `"`)
		glog.V(3).Infof("CopyObjectHandler: If-Match check - expected %s, got %s", ifMatch, sourceETag)
		if ifMatch != sourceETag {
			glog.V(3).Infof("CopyObjectHandler: If-Match failed - expected %s, got %s", ifMatch, sourceETag)
			return s3err.ErrPreconditionFailed
		}
	}

	// Check X-Amz-Copy-Source-If-None-Match
	if ifNoneMatch := r.Header.Get(s3_constants.AmzCopySourceIfNoneMatch); ifNoneMatch != "" {
		// Remove quotes if present
		ifNoneMatch = strings.Trim(ifNoneMatch, `"`)
		sourceETag = strings.Trim(sourceETag, `"`)
		glog.V(3).Infof("CopyObjectHandler: If-None-Match check - comparing %s with %s", ifNoneMatch, sourceETag)
		if ifNoneMatch == sourceETag {
			glog.V(3).Infof("CopyObjectHandler: If-None-Match failed - matched %s", sourceETag)
			return s3err.ErrPreconditionFailed
		}
	}

	// Check X-Amz-Copy-Source-If-Modified-Since
	if ifModifiedSince := r.Header.Get(s3_constants.AmzCopySourceIfModifiedSince); ifModifiedSince != "" {
		t, err := parseHTTPDate(ifModifiedSince)
		if err != nil {
			glog.V(3).Infof("CopyObjectHandler: Invalid If-Modified-Since header: %v", err)
			return s3err.ErrInvalidRequest
		}
		if !time.Unix(entry.Attributes.Mtime, 0).After(t) {
			glog.V(3).Infof("CopyObjectHandler: If-Modified-Since failed")
			return s3err.ErrPreconditionFailed
		}
	}

	// Check X-Amz-Copy-Source-If-Unmodified-Since
	if ifUnmodifiedSince := r.Header.Get(s3_constants.AmzCopySourceIfUnmodifiedSince); ifUnmodifiedSince != "" {
		t, err := parseHTTPDate(ifUnmodifiedSince)
		if err != nil {
			glog.V(3).Infof("CopyObjectHandler: Invalid If-Unmodified-Since header: %v", err)
			return s3err.ErrInvalidRequest
		}
		if time.Unix(entry.Attributes.Mtime, 0).After(t) {
			glog.V(3).Infof("CopyObjectHandler: If-Unmodified-Since failed")
			return s3err.ErrPreconditionFailed
		}
	}

	return s3err.ErrNone
}

// createDestinationChunk creates a new chunk based on the source chunk with modified properties.
//
// SseType and SseMetadata are NOT copied here because most call sites
// re-encrypt the chunk with the destination's keys and then set those fields
// to match the new encryption. The same-key fast path (where the bytes are
// copied as-is and the destination should keep the source's SSE tagging) calls
// createDestinationChunkPreservingSSE instead.
func (s3a *S3ApiServer) createDestinationChunk(sourceChunk *filer_pb.FileChunk, offset int64, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{
		Offset:       offset,
		Size:         size,
		ModifiedTsNs: time.Now().UnixNano(),
		ETag:         sourceChunk.ETag,
		IsCompressed: sourceChunk.IsCompressed,
		CipherKey:    sourceChunk.CipherKey,
	}
}

// createDestinationChunkPreservingSSE returns a destination chunk that mirrors
// the source's SSE tagging in addition to the usual fields. This is used by the
// same-key copy fast paths where the on-disk bytes are reused as-is and the
// destination must therefore declare the same per-chunk SSE encryption as the
// source (otherwise detectPrimarySSEType returns "None" on read and
// GetObjectHandler serves the still-encrypted bytes raw — issue #9281).
func (s3a *S3ApiServer) createDestinationChunkPreservingSSE(sourceChunk *filer_pb.FileChunk, offset int64, size uint64) *filer_pb.FileChunk {
	dst := s3a.createDestinationChunk(sourceChunk, offset, size)
	dst.SseType = sourceChunk.SseType
	if len(sourceChunk.SseMetadata) > 0 {
		dst.SseMetadata = append([]byte(nil), sourceChunk.SseMetadata...)
	}
	return dst
}

// lookupVolumeUrl looks up the volume URL for a given file ID using the filer's LookupVolume method
func (s3a *S3ApiServer) lookupVolumeUrl(fileId string) (string, error) {
	var srcUrl string
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		vid, _, err := operation.ParseFileId(fileId)
		if err != nil {
			return fmt.Errorf("parse file ID: %w", err)
		}

		resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
			VolumeIds: []string{vid},
		})
		if err != nil {
			return fmt.Errorf("lookup volume: %w", err)
		}

		if locations, found := resp.LocationsMap[vid]; found && len(locations.Locations) > 0 {
			srcUrl = "http://" + locations.Locations[0].Url + "/" + fileId
		} else {
			return fmt.Errorf("no location found for volume %s", vid)
		}

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("lookup volume URL: %w", err)
	}
	return srcUrl, nil
}

// setChunkFileId sets the file ID on the destination chunk
func (s3a *S3ApiServer) setChunkFileId(chunk *filer_pb.FileChunk, assignResult *filer_pb.AssignVolumeResponse) error {
	chunk.FileId = assignResult.FileId
	fid, err := filer_pb.ToFileIdObject(assignResult.FileId)
	if err != nil {
		return fmt.Errorf("parse file ID: %w", err)
	}
	chunk.Fid = fid
	return nil
}

// prepareChunkCopy prepares a chunk for copying by assigning a new volume and looking up the source URL
func (s3a *S3ApiServer) prepareChunkCopy(sourceFileId, dstPath string, expectedDataSize uint64) (*filer_pb.AssignVolumeResponse, string, error) {
	// Assign new volume
	assignResult, err := s3a.assignNewVolume(dstPath, expectedDataSize)
	if err != nil {
		return nil, "", fmt.Errorf("assign volume: %w", err)
	}

	// Look up source URL
	srcUrl, err := s3a.lookupVolumeUrl(sourceFileId)
	if err != nil {
		return nil, "", fmt.Errorf("lookup source URL: %w", err)
	}

	return assignResult, srcUrl, nil
}

// uploadChunkData uploads chunk data to the destination using common upload logic
// isCompressed indicates if the data is already compressed and should not be compressed again
func (s3a *S3ApiServer) uploadChunkData(chunkData []byte, assignResult *filer_pb.AssignVolumeResponse, isCompressed bool) error {
	uploadOption := newChunkUploadOption(chunkData, assignResult, isCompressed)
	uploader, err := operation.NewUploader()
	if err != nil {
		return fmt.Errorf("create uploader: %w", err)
	}
	_, err = uploader.UploadData(context.Background(), chunkData, uploadOption)
	if err != nil {
		return fmt.Errorf("upload chunk: %w", err)
	}

	return nil
}

// multipartFramingOverhead reserves space for the multipart wrapper
// upload_content writes around chunkData (boundary + Content-Disposition +
// optional Content-Type/Content-Encoding/Content-MD5 headers + trailing
// boundary). Real-world overhead is a few hundred bytes; rounding to 1 KiB
// avoids a single grow on the buffer we hand to the multipart writer.
const multipartFramingOverhead = 1024

// newChunkUploadOption builds the operation.UploadOption used by every
// chunk-copy upload. It always sets BytesBuffer to a fresh, per-call buffer
// so upload_content does not fall back to the package-global
// valyala/bytebufferpool — that pool retains every high-water buffer for the
// process's lifetime, and under concurrent UploadPartCopy load it hoarded
// one chunk-sized buffer per concurrent upload (see #6541). The per-call
// buffer is GC'd as soon as the upload returns.
func newChunkUploadOption(chunkData []byte, assignResult *filer_pb.AssignVolumeResponse, isCompressed bool) *operation.UploadOption {
	dstUrl := fmt.Sprintf("http://%s/%s", assignResult.Location.Url, assignResult.FileId)
	return &operation.UploadOption{
		UploadUrl:         dstUrl,
		Cipher:            false, // Data is already encrypted if source had CipherKey; don't re-encrypt
		IsInputCompressed: isCompressed,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               security.EncodedJwt(assignResult.Auth),
		BytesBuffer:       bytes.NewBuffer(make([]byte, 0, len(chunkData)+multipartFramingOverhead)),
	}
}

// downloadChunkData downloads chunk data from the source URL
func (s3a *S3ApiServer) downloadChunkData(srcUrl, fileId string, offset, size int64, cipherKey []byte) ([]byte, error) {
	jwt := filer.JwtForVolumeServer(fileId)
	// Only perform HEAD request for encrypted chunks to get physical size
	if offset == 0 && len(cipherKey) > 0 {
		req, err := http.NewRequest(http.MethodHead, srcUrl, nil)
		if err == nil {
			if jwt != "" {
				req.Header.Set("Authorization", security.BearerPrefix+string(jwt))
			}
			resp, err := util_http.GetGlobalHttpClient().Do(req)
			if err == nil {
				defer util_http.CloseResponse(resp)
				if resp.StatusCode == http.StatusOK {
					contentLengthStr := resp.Header.Get("Content-Length")
					if contentLength, err := strconv.ParseInt(contentLengthStr, 10, 64); err == nil {
						// Validate contentLength fits in int32 range before comparison
						if contentLength > int64(2147483647) { // math.MaxInt32
							return nil, fmt.Errorf("content length %d exceeds maximum int32 size", contentLength)
						}
						if contentLength > size {
							size = contentLength
						}
					}
				}
			}
		}
	}
	// Validate size fits in int32 range before conversion to int
	if size > int64(2147483647) { // math.MaxInt32
		return nil, fmt.Errorf("chunk size %d exceeds maximum int32 size", size)
	}
	sizeInt := int(size)
	// Pre-size the receive buffer to the known chunk size so the streaming
	// callback below does not trigger geometric `append`-grow on a nil slice.
	// Receiving a 64 MiB chunk through 256 KiB callback ticks would otherwise
	// allocate ~2x the chunk size, and with concurrent UploadPartCopy requests
	// (Harbor-style assemble loops) this caused the runaway-RSS pattern in
	// https://github.com/seaweedfs/seaweedfs/issues/6541.
	chunkData := make([]byte, 0, sizeInt)
	shouldRetry, err := util_http.ReadUrlAsStream(context.Background(), srcUrl, jwt, nil, false, false, offset, sizeInt, func(data []byte) {
		chunkData = append(chunkData, data...)
	})
	if err != nil {
		return nil, fmt.Errorf("download chunk: %w", err)
	}
	if shouldRetry {
		return nil, fmt.Errorf("download chunk: retry needed")
	}
	return chunkData, nil
}

// copyMultipartSSECChunks handles copying multipart SSE-C objects
// Returns chunks and destination metadata that should be applied to the destination entry
func (s3a *S3ApiServer) copyMultipartSSECChunks(entry *filer_pb.Entry, copySourceKey *SSECustomerKey, destKey *SSECustomerKey, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {

	// For multipart SSE-C, always use decrypt/reencrypt path to ensure proper metadata handling
	// The standard copyChunks() doesn't preserve SSE metadata, so we need per-chunk processing

	// Different keys or key changes: decrypt and re-encrypt each chunk individually
	glog.V(2).Infof("Multipart SSE-C reencrypt copy (different keys): %s", dstPath)

	var dstChunks []*filer_pb.FileChunk
	var destIV []byte

	for _, chunk := range entry.GetChunks() {
		if chunk.GetSseType() != filer_pb.SSEType_SSE_C {
			// Non-SSE-C chunk, copy directly
			copiedChunk, err := s3a.copySingleChunk(chunk, dstPath)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to copy non-SSE-C chunk: %w", err)
			}
			dstChunks = append(dstChunks, copiedChunk)
			continue
		}

		// SSE-C chunk: decrypt with stored per-chunk metadata, re-encrypt with dest key
		copiedChunk, chunkDestIV, err := s3a.copyMultipartSSECChunk(chunk, copySourceKey, destKey, dstPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to copy SSE-C chunk %s: %w", chunk.GetFileIdString(), err)
		}

		dstChunks = append(dstChunks, copiedChunk)

		// Store the first chunk's IV as the object's IV (for single-part compatibility)
		if len(destIV) == 0 {
			destIV = chunkDestIV
		}
	}

	// Create destination metadata
	dstMetadata := make(map[string][]byte)
	if destKey != nil && len(destIV) > 0 {
		// Store the IV and SSE-C headers for single-part compatibility
		StoreSSECIVInMetadata(dstMetadata, destIV)
		dstMetadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
		dstMetadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(destKey.KeyMD5)
		glog.V(2).Infof("Prepared multipart SSE-C destination metadata: %s", dstPath)
	}

	return dstChunks, dstMetadata, nil
}

// copyMultipartSSEKMSChunks handles copying multipart SSE-KMS objects (unified with SSE-C approach)
// Returns chunks and destination metadata that should be applied to the destination entry
func (s3a *S3ApiServer) copyMultipartSSEKMSChunks(entry *filer_pb.Entry, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) ([]*filer_pb.FileChunk, map[string][]byte, error) {

	// For multipart SSE-KMS, always use decrypt/reencrypt path to ensure proper metadata handling
	// The standard copyChunks() doesn't preserve SSE metadata, so we need per-chunk processing

	// Deserialize the source's entry-level SSE-KMS key once so it can be used
	// as a fallback for legacy multipart chunks that lack per-chunk metadata.
	// New multipart SSE-KMS uploads always populate per-chunk metadata, but
	// objects written by earlier code may have only the entry-level key.
	sourceEntrySSEKey := deserializeEntrySSEKMSKey(entry.Extended)

	var dstChunks []*filer_pb.FileChunk

	for _, chunk := range entry.GetChunks() {
		if chunk.GetSseType() != filer_pb.SSEType_SSE_KMS {
			// Non-SSE-KMS chunk, copy directly
			copiedChunk, err := s3a.copySingleChunk(chunk, dstPath)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to copy non-SSE-KMS chunk: %w", err)
			}
			dstChunks = append(dstChunks, copiedChunk)
			continue
		}

		// SSE-KMS chunk: decrypt with stored per-chunk metadata (or entry-level
		// fallback for legacy data), re-encrypt with dest key.
		copiedChunk, err := s3a.copyMultipartSSEKMSChunk(chunk, sourceEntrySSEKey, destKeyID, encryptionContext, bucketKeyEnabled, dstPath, bucket)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to copy SSE-KMS chunk %s: %w", chunk.GetFileIdString(), err)
		}

		dstChunks = append(dstChunks, copiedChunk)
	}

	// Create destination metadata for SSE-KMS.
	//
	// Prefer the first dst chunk's full per-chunk key (which carries a real
	// EDK + IV minted by copyMultipartSSEKMSChunk's
	// CreateSSEKMSEncryptedReaderWithBucketKey call) so single-chunk reads on
	// the destination can unwrap the EDK on the GET path. Fall back to a stub
	// key (KeyID + context + bucket-key only) for 0-byte objects so they're
	// still recognised as SSE-KMS encrypted.
	dstMetadata := make(map[string][]byte)
	if destKeyID != "" {
		if len(dstChunks) > 0 && len(dstChunks[0].GetSseMetadata()) > 0 {
			dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = dstChunks[0].GetSseMetadata()
		} else {
			if encryptionContext == nil {
				encryptionContext = BuildEncryptionContext(bucket, dstPath, bucketKeyEnabled)
			}
			sseKey := &SSEKMSKey{
				KeyID:             destKeyID,
				EncryptionContext: encryptionContext,
				BucketKeyEnabled:  bucketKeyEnabled,
			}
			if kmsMetadata, serErr := SerializeSSEKMSMetadata(sseKey); serErr == nil {
				dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = kmsMetadata
			} else {
				glog.Errorf("Failed to serialize SSE-KMS metadata: %v", serErr)
			}
		}
	}

	return dstChunks, dstMetadata, nil
}

// copyMultipartSSEKMSChunk copies a single SSE-KMS chunk from a multipart object (unified with SSE-C approach).
//
// sourceEntrySSEKey is the source object's entry-level SSE-KMS key (deserialized
// from entry.Extended[SeaweedFSSSEKMSKey] by the caller). It's used as a
// fallback when this chunk has no per-chunk SSE-KMS metadata of its own —
// legacy multipart SSE-KMS objects may have only the entry-level key. Newer
// uploads populate per-chunk metadata, in which case this fallback is unused.
func (s3a *S3ApiServer) copyMultipartSSEKMSChunk(chunk *filer_pb.FileChunk, sourceEntrySSEKey *SSEKMSKey, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, chunk.Size)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size), chunk.CipherKey)
	if err != nil {
		return nil, fmt.Errorf("download encrypted chunk data: %w", err)
	}

	var finalData []byte

	// Prefer the chunk's own per-chunk SSE-KMS metadata; fall back to the
	// source's entry-level key for legacy multipart objects that don't have
	// per-chunk metadata. resolveChunkSSEKMSKey centralizes that selection
	// so the same logic is used everywhere a chunk needs decryption.
	sourceSSEKey, err := resolveChunkSSEKMSKey(chunk, sourceEntrySSEKey)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve SSE-KMS metadata: %w", err)
	}

	// Decrypt the chunk data using the source metadata
	decryptedReader, decErr := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sourceSSEKey)
	if decErr != nil {
		return nil, fmt.Errorf("create SSE-KMS decrypted reader: %w", decErr)
	}

	decryptedData, readErr := io.ReadAll(decryptedReader)
	if readErr != nil {
		return nil, fmt.Errorf("decrypt chunk data: %w", readErr)
	}
	finalData = decryptedData
	glog.V(4).Infof("Decrypted multipart SSE-KMS chunk: %d bytes → %d bytes", len(encryptedData), len(finalData))

	// Re-encrypt with destination key if specified
	if destKeyID != "" {
		// Build encryption context if not provided
		if encryptionContext == nil {
			encryptionContext = BuildEncryptionContext(bucket, dstPath, bucketKeyEnabled)
		}

		// Encrypt with destination key
		originalSize := len(finalData)
		encryptedReader, destSSEKey, encErr := CreateSSEKMSEncryptedReaderWithBucketKey(bytes.NewReader(finalData), destKeyID, encryptionContext, bucketKeyEnabled)
		if encErr != nil {
			return nil, fmt.Errorf("create SSE-KMS encrypted reader: %w", encErr)
		}

		reencryptedData, readErr := io.ReadAll(encryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("re-encrypt chunk data: %w", readErr)
		}
		finalData = reencryptedData

		// Create per-chunk SSE-KMS metadata for the destination chunk
		// For copy operations, reset chunk offset to 0 (similar to SSE-C approach)
		// The copied chunks form a new object structure independent of original part boundaries
		destSSEKey.ChunkOffset = 0
		kmsMetadata, err := SerializeSSEKMSMetadata(destSSEKey)
		if err != nil {
			return nil, fmt.Errorf("serialize SSE-KMS metadata: %w", err)
		}

		// Set the SSE type and metadata on destination chunk (unified approach)
		dstChunk.SseType = filer_pb.SSEType_SSE_KMS
		dstChunk.SseMetadata = kmsMetadata

		glog.V(4).Infof("Re-encrypted multipart SSE-KMS chunk: %d bytes → %d bytes", originalSize, len(finalData))
	}

	// Upload the final data
	if err := s3a.uploadChunkData(finalData, assignResult, false); err != nil {
		return nil, fmt.Errorf("upload chunk data: %w", err)
	}

	// Update chunk size
	dstChunk.Size = uint64(len(finalData))

	glog.V(3).Infof("Successfully copied multipart SSE-KMS chunk %s → %s",
		chunk.GetFileIdString(), dstChunk.GetFileIdString())

	return dstChunk, nil
}

// copyMultipartSSECChunk copies a single SSE-C chunk from a multipart object
func (s3a *S3ApiServer) copyMultipartSSECChunk(chunk *filer_pb.FileChunk, copySourceKey *SSECustomerKey, destKey *SSECustomerKey, dstPath string) (*filer_pb.FileChunk, []byte, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, chunk.Size)
	if err != nil {
		return nil, nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, nil, err
	}

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size), chunk.CipherKey)
	if err != nil {
		return nil, nil, fmt.Errorf("download encrypted chunk data: %w", err)
	}

	var finalData []byte
	var destIV []byte

	// Decrypt if source is encrypted
	if copySourceKey != nil {
		// Get the per-chunk SSE-C metadata
		if len(chunk.GetSseMetadata()) == 0 {
			return nil, nil, fmt.Errorf("SSE-C chunk missing per-chunk metadata")
		}

		// Deserialize the SSE-C metadata
		ssecMetadata, err := DeserializeSSECMetadata(chunk.GetSseMetadata())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to deserialize SSE-C metadata: %w", err)
		}

		// Decode the IV from the metadata
		chunkBaseIV, err := base64.StdEncoding.DecodeString(ssecMetadata.IV)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode chunk IV: %w", err)
		}

		// Calculate the correct IV for this chunk using within-part offset
		var chunkIV []byte
		var ivSkip int
		if ssecMetadata.PartOffset > 0 {
			chunkIV, ivSkip = calculateIVWithOffset(chunkBaseIV, ssecMetadata.PartOffset)
		} else {
			chunkIV = chunkBaseIV
			ivSkip = 0
		}

		// Decrypt the chunk data
		decryptedReader, decErr := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), copySourceKey, chunkIV)
		if decErr != nil {
			return nil, nil, fmt.Errorf("create decrypted reader: %w", decErr)
		}

		// CRITICAL: Skip intra-block bytes from CTR decryption (non-block-aligned offset handling)
		if ivSkip > 0 {
			_, skipErr := io.CopyN(io.Discard, decryptedReader, int64(ivSkip))
			if skipErr != nil {
				return nil, nil, fmt.Errorf("failed to skip intra-block bytes (%d): %w", ivSkip, skipErr)
			}
		}

		decryptedData, readErr := io.ReadAll(decryptedReader)
		if readErr != nil {
			return nil, nil, fmt.Errorf("decrypt chunk data: %w", readErr)
		}
		finalData = decryptedData
		glog.V(4).Infof("Decrypted multipart SSE-C chunk: %d bytes → %d bytes", len(encryptedData), len(finalData))
	} else {
		// Source is unencrypted
		finalData = encryptedData
	}

	// Re-encrypt if destination should be encrypted
	if destKey != nil {
		// Generate new IV for this chunk
		newIV := make([]byte, s3_constants.AESBlockSize)
		if _, err := rand.Read(newIV); err != nil {
			return nil, nil, fmt.Errorf("generate IV: %w", err)
		}
		destIV = newIV

		// Encrypt with new key and IV
		originalSize := len(finalData)
		encryptedReader, iv, encErr := CreateSSECEncryptedReader(bytes.NewReader(finalData), destKey)
		if encErr != nil {
			return nil, nil, fmt.Errorf("create encrypted reader: %w", encErr)
		}
		destIV = iv

		reencryptedData, readErr := io.ReadAll(encryptedReader)
		if readErr != nil {
			return nil, nil, fmt.Errorf("re-encrypt chunk data: %w", readErr)
		}
		finalData = reencryptedData

		// Create per-chunk SSE-C metadata for the destination chunk
		ssecMetadata, err := SerializeSSECMetadata(destIV, destKey.KeyMD5, 0) // partOffset=0 for copied chunks
		if err != nil {
			return nil, nil, fmt.Errorf("serialize SSE-C metadata: %w", err)
		}

		// Set the SSE type and metadata on destination chunk
		dstChunk.SseType = filer_pb.SSEType_SSE_C
		dstChunk.SseMetadata = ssecMetadata // Use unified metadata field

		glog.V(4).Infof("Re-encrypted multipart SSE-C chunk: %d bytes → %d bytes", originalSize, len(finalData))
	}

	// Upload the final data
	if err := s3a.uploadChunkData(finalData, assignResult, false); err != nil {
		return nil, nil, fmt.Errorf("upload chunk data: %w", err)
	}

	// Update chunk size
	dstChunk.Size = uint64(len(finalData))

	glog.V(3).Infof("Successfully copied multipart SSE-C chunk %s → %s",
		chunk.GetFileIdString(), dstChunk.GetFileIdString())

	return dstChunk, destIV, nil
}

// copyMultipartCrossEncryption handles all cross-encryption and decrypt-only copy scenarios
// This unified function supports: SSE-C↔SSE-KMS↔SSE-S3, and any→Plain
func (s3a *S3ApiServer) copyMultipartCrossEncryption(entry *filer_pb.Entry, r *http.Request, state *EncryptionState, dstBucket, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	var dstChunks []*filer_pb.FileChunk

	// Parse destination encryption parameters
	var destSSECKey *SSECustomerKey
	var destKMSKeyID string
	var destKMSEncryptionContext map[string]string
	var destKMSBucketKeyEnabled bool
	var destSSES3Key *SSES3Key

	if state.DstSSEC {
		var err error
		destSSECKey, err = ParseSSECHeaders(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse destination SSE-C headers: %w", err)
		}
	} else if state.DstSSEKMS {
		var err error
		destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, err = ParseSSEKMSCopyHeaders(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse destination SSE-KMS headers: %w", err)
		}
	} else if state.DstSSES3 {
		// Generate SSE-S3 key for destination
		var err error
		destSSES3Key, err = GenerateSSES3Key()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate SSE-S3 key: %w", err)
		}
	}

	// Parse source encryption parameters
	var sourceSSECKey *SSECustomerKey
	if state.SrcSSEC {
		var err error
		sourceSSECKey, err = ParseSSECCopySourceHeaders(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse source SSE-C headers: %w", err)
		}
	}

	// Process each chunk with unified cross-encryption logic
	for _, chunk := range entry.GetChunks() {
		var copiedChunk *filer_pb.FileChunk
		var err error

		if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
			copiedChunk, err = s3a.copyCrossEncryptionChunk(chunk, sourceSSECKey, destSSECKey, destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, destSSES3Key, dstPath, dstBucket, state)
		} else if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS {
			copiedChunk, err = s3a.copyCrossEncryptionChunk(chunk, nil, destSSECKey, destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, destSSES3Key, dstPath, dstBucket, state)
		} else if chunk.GetSseType() == filer_pb.SSEType_SSE_S3 {
			copiedChunk, err = s3a.copyCrossEncryptionChunk(chunk, nil, destSSECKey, destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, destSSES3Key, dstPath, dstBucket, state)
		} else {
			// Unencrypted chunk - may need encryption if destination requires it
			if state.DstSSEC || state.DstSSEKMS || state.DstSSES3 {
				copiedChunk, err = s3a.copyCrossEncryptionChunk(chunk, nil, destSSECKey, destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, destSSES3Key, dstPath, dstBucket, state)
			} else {
				copiedChunk, err = s3a.copySingleChunk(chunk, dstPath)
			}
		}

		if err != nil {
			return nil, nil, fmt.Errorf("failed to copy chunk %s: %w", chunk.GetFileIdString(), err)
		}

		dstChunks = append(dstChunks, copiedChunk)
	}

	// Create destination metadata based on destination encryption type
	dstMetadata := make(map[string][]byte)

	// Clear any previous encryption metadata to avoid routing conflicts
	if state.SrcSSEKMS && state.DstSSEC {
		// SSE-KMS → SSE-C: Remove SSE-KMS headers
		// These will be excluded from dstMetadata, effectively removing them
	} else if state.SrcSSEC && state.DstSSEKMS {
		// SSE-C → SSE-KMS: Remove SSE-C headers
		// These will be excluded from dstMetadata, effectively removing them
	} else if !state.DstSSEC && !state.DstSSEKMS {
		// Encrypted → Unencrypted: Remove all encryption metadata
		// These will be excluded from dstMetadata, effectively removing them
	}

	if state.DstSSEC && destSSECKey != nil {
		// For SSE-C destination, use first chunk's IV for compatibility
		if len(dstChunks) > 0 && dstChunks[0].GetSseType() == filer_pb.SSEType_SSE_C && len(dstChunks[0].GetSseMetadata()) > 0 {
			if ssecMetadata, err := DeserializeSSECMetadata(dstChunks[0].GetSseMetadata()); err == nil {
				if iv, ivErr := base64.StdEncoding.DecodeString(ssecMetadata.IV); ivErr == nil {
					StoreSSECIVInMetadata(dstMetadata, iv)
					dstMetadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
					dstMetadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(destSSECKey.KeyMD5)
				}
			}
		}
	} else if state.DstSSEKMS && destKMSKeyID != "" {
		// For SSE-KMS destination, create object-level metadata
		if destKMSEncryptionContext == nil {
			destKMSEncryptionContext = BuildEncryptionContext(dstBucket, dstPath, destKMSBucketKeyEnabled)
		}
		// Take the first dst chunk's full per-chunk metadata as the canonical
		// entry-level key — it includes a real EDK + IV minted by
		// copyCrossEncryptionChunk's CreateSSEKMSEncryptedReaderWithBucketKey
		// call. Earlier this stored only KeyID/context/bucketKey, leaving the
		// EncryptedDataKey empty; single-chunk reads then failed with
		// "Invalid ciphertext format" when KMS was asked to unwrap an empty
		// EDK (#9281).
		if len(dstChunks) > 0 && len(dstChunks[0].GetSseMetadata()) > 0 {
			dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = dstChunks[0].GetSseMetadata()
		} else {
			// 0-byte object or no SSE-KMS chunk: fall back to a stub key
			// (sufficient for the entry to be recognised as SSE-KMS).
			sseKey := &SSEKMSKey{
				KeyID:             destKMSKeyID,
				EncryptionContext: destKMSEncryptionContext,
				BucketKeyEnabled:  destKMSBucketKeyEnabled,
			}
			if kmsMetadata, serErr := SerializeSSEKMSMetadata(sseKey); serErr == nil {
				dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = kmsMetadata
			} else {
				glog.Errorf("Failed to serialize SSE-KMS metadata: %v", serErr)
			}
		}
	} else if state.DstSSES3 && destSSES3Key != nil {
		// For SSE-S3 destination, create object-level metadata
		var sses3Metadata *SSES3Key
		if len(dstChunks) == 0 {
			// Handle 0-byte files - generate IV for metadata even though there's no content to encrypt
			if entry.Attributes.FileSize != 0 {
				return nil, nil, fmt.Errorf("internal error: no chunks created for non-empty SSE-S3 destination object")
			}
			// Generate IV for 0-byte object metadata
			iv := make([]byte, s3_constants.AESBlockSize)
			if _, err := io.ReadFull(rand.Reader, iv); err != nil {
				return nil, nil, fmt.Errorf("generate IV for 0-byte object: %w", err)
			}
			destSSES3Key.IV = iv
			sses3Metadata = destSSES3Key
		} else {
			// For non-empty objects, use the first chunk's metadata
			if dstChunks[0].GetSseType() != filer_pb.SSEType_SSE_S3 || len(dstChunks[0].GetSseMetadata()) == 0 {
				return nil, nil, fmt.Errorf("internal error: first chunk is missing expected SSE-S3 metadata for destination object")
			}
			keyManager := GetSSES3KeyManager()
			var err error
			sses3Metadata, err = DeserializeSSES3Metadata(dstChunks[0].GetSseMetadata(), keyManager)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to deserialize SSE-S3 metadata from first chunk: %w", err)
			}
		}
		// Use the derived key with its IV for object-level metadata
		keyData, serErr := SerializeSSES3Metadata(sses3Metadata)
		if serErr != nil {
			return nil, nil, fmt.Errorf("failed to serialize SSE-S3 metadata: %w", serErr)
		}
		dstMetadata[s3_constants.SeaweedFSSSES3Key] = keyData
		dstMetadata[s3_constants.AmzServerSideEncryption] = []byte("AES256")
	}
	// For unencrypted destination, no metadata needed (dstMetadata remains empty)

	return dstChunks, dstMetadata, nil
}

// copyCrossEncryptionChunk handles copying a single chunk with cross-encryption support
func (s3a *S3ApiServer) copyCrossEncryptionChunk(chunk *filer_pb.FileChunk, sourceSSECKey *SSECustomerKey, destSSECKey *SSECustomerKey, destKMSKeyID string, destKMSEncryptionContext map[string]string, destKMSBucketKeyEnabled bool, destSSES3Key *SSES3Key, dstPath, dstBucket string, state *EncryptionState) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, chunk.Size)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size), chunk.CipherKey)
	if err != nil {
		return nil, fmt.Errorf("download encrypted chunk data: %w", err)
	}

	var finalData []byte

	// Step 1: Decrypt source data
	if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
		// Decrypt SSE-C source
		if len(chunk.GetSseMetadata()) == 0 {
			return nil, fmt.Errorf("SSE-C chunk missing per-chunk metadata")
		}

		ssecMetadata, err := DeserializeSSECMetadata(chunk.GetSseMetadata())
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-C metadata: %w", err)
		}

		chunkBaseIV, err := base64.StdEncoding.DecodeString(ssecMetadata.IV)
		if err != nil {
			return nil, fmt.Errorf("failed to decode chunk IV: %w", err)
		}

		// Calculate the correct IV for this chunk using within-part offset
		var chunkIV []byte
		var ivSkip int
		if ssecMetadata.PartOffset > 0 {
			chunkIV, ivSkip = calculateIVWithOffset(chunkBaseIV, ssecMetadata.PartOffset)
		} else {
			chunkIV = chunkBaseIV
			ivSkip = 0
		}

		decryptedReader, decErr := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), sourceSSECKey, chunkIV)
		if decErr != nil {
			return nil, fmt.Errorf("create SSE-C decrypted reader: %w", decErr)
		}

		// CRITICAL: Skip intra-block bytes from CTR decryption (non-block-aligned offset handling)
		if ivSkip > 0 {
			_, skipErr := io.CopyN(io.Discard, decryptedReader, int64(ivSkip))
			if skipErr != nil {
				return nil, fmt.Errorf("failed to skip intra-block bytes (%d): %w", ivSkip, skipErr)
			}
		}

		decryptedData, readErr := io.ReadAll(decryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("decrypt SSE-C chunk data: %w", readErr)
		}
		finalData = decryptedData

	} else if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS {
		// Decrypt SSE-KMS source
		if len(chunk.GetSseMetadata()) == 0 {
			return nil, fmt.Errorf("SSE-KMS chunk missing per-chunk metadata")
		}

		sourceSSEKey, err := DeserializeSSEKMSMetadata(chunk.GetSseMetadata())
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-KMS metadata: %w", err)
		}

		decryptedReader, decErr := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sourceSSEKey)
		if decErr != nil {
			return nil, fmt.Errorf("create SSE-KMS decrypted reader: %w", decErr)
		}

		decryptedData, readErr := io.ReadAll(decryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("decrypt SSE-KMS chunk data: %w", readErr)
		}
		finalData = decryptedData

	} else if chunk.GetSseType() == filer_pb.SSEType_SSE_S3 {
		// Decrypt SSE-S3 source
		if len(chunk.GetSseMetadata()) == 0 {
			return nil, fmt.Errorf("SSE-S3 chunk missing per-chunk metadata")
		}

		keyManager := GetSSES3KeyManager()
		sourceSSEKey, err := DeserializeSSES3Metadata(chunk.GetSseMetadata(), keyManager)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-S3 metadata: %w", err)
		}

		decryptedReader, decErr := CreateSSES3DecryptedReader(bytes.NewReader(encryptedData), sourceSSEKey, sourceSSEKey.IV)
		if decErr != nil {
			return nil, fmt.Errorf("create SSE-S3 decrypted reader: %w", decErr)
		}

		decryptedData, readErr := io.ReadAll(decryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("decrypt SSE-S3 chunk data: %w", readErr)
		}
		finalData = decryptedData
		glog.V(4).Infof("Decrypted SSE-S3 chunk, size: %d", len(finalData))

	} else {
		// Source is unencrypted
		finalData = encryptedData
	}

	// Step 2: Re-encrypt with destination encryption (if any)
	if state.DstSSEC && destSSECKey != nil {
		// Encrypt with SSE-C
		encryptedReader, iv, encErr := CreateSSECEncryptedReader(bytes.NewReader(finalData), destSSECKey)
		if encErr != nil {
			return nil, fmt.Errorf("create SSE-C encrypted reader: %w", encErr)
		}

		reencryptedData, readErr := io.ReadAll(encryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("re-encrypt with SSE-C: %w", readErr)
		}
		finalData = reencryptedData

		// Create per-chunk SSE-C metadata (offset=0 for cross-encryption copies)
		ssecMetadata, err := SerializeSSECMetadata(iv, destSSECKey.KeyMD5, 0)
		if err != nil {
			return nil, fmt.Errorf("serialize SSE-C metadata: %w", err)
		}

		dstChunk.SseType = filer_pb.SSEType_SSE_C
		dstChunk.SseMetadata = ssecMetadata

		previewLen := 16
		if len(finalData) < previewLen {
			previewLen = len(finalData)
		}

	} else if state.DstSSEKMS && destKMSKeyID != "" {
		// Encrypt with SSE-KMS
		if destKMSEncryptionContext == nil {
			destKMSEncryptionContext = BuildEncryptionContext(dstBucket, dstPath, destKMSBucketKeyEnabled)
		}

		encryptedReader, destSSEKey, encErr := CreateSSEKMSEncryptedReaderWithBucketKey(bytes.NewReader(finalData), destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled)
		if encErr != nil {
			return nil, fmt.Errorf("create SSE-KMS encrypted reader: %w", encErr)
		}

		reencryptedData, readErr := io.ReadAll(encryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("re-encrypt with SSE-KMS: %w", readErr)
		}
		finalData = reencryptedData

		// Create per-chunk SSE-KMS metadata (offset=0 for cross-encryption copies)
		destSSEKey.ChunkOffset = 0
		kmsMetadata, err := SerializeSSEKMSMetadata(destSSEKey)
		if err != nil {
			return nil, fmt.Errorf("serialize SSE-KMS metadata: %w", err)
		}

		dstChunk.SseType = filer_pb.SSEType_SSE_KMS
		dstChunk.SseMetadata = kmsMetadata

		glog.V(4).Infof("Re-encrypted chunk with SSE-KMS")

	} else if state.DstSSES3 && destSSES3Key != nil {
		// Encrypt with SSE-S3
		encryptedReader, iv, encErr := CreateSSES3EncryptedReader(bytes.NewReader(finalData), destSSES3Key)
		if encErr != nil {
			return nil, fmt.Errorf("create SSE-S3 encrypted reader: %w", encErr)
		}

		reencryptedData, readErr := io.ReadAll(encryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("re-encrypt with SSE-S3: %w", readErr)
		}
		finalData = reencryptedData

		// Create per-chunk SSE-S3 metadata with chunk-specific IV
		chunkSSEKey := &SSES3Key{
			Key:       destSSES3Key.Key,
			KeyID:     destSSES3Key.KeyID,
			Algorithm: destSSES3Key.Algorithm,
			IV:        iv,
		}
		sses3Metadata, err := SerializeSSES3Metadata(chunkSSEKey)
		if err != nil {
			return nil, fmt.Errorf("serialize SSE-S3 metadata: %w", err)
		}

		dstChunk.SseType = filer_pb.SSEType_SSE_S3
		dstChunk.SseMetadata = sses3Metadata

		glog.V(4).Infof("Re-encrypted chunk with SSE-S3")
	}
	// For unencrypted destination, finalData remains as decrypted plaintext

	// Upload the final data
	if err := s3a.uploadChunkData(finalData, assignResult, false); err != nil {
		return nil, fmt.Errorf("upload chunk data: %w", err)
	}

	// Update chunk size
	dstChunk.Size = uint64(len(finalData))

	glog.V(3).Infof("Successfully copied cross-encryption chunk %s → %s",
		chunk.GetFileIdString(), dstChunk.GetFileIdString())

	return dstChunk, nil
}

// copyChunksWithSSEC handles SSE-C aware copying with smart fast/slow path selection
// Returns chunks and destination metadata that should be applied to the destination entry
// dstPath is the destination object's filer path, so volume assignment targets
// the destination bucket's collection. The caller must not pass r.URL.Path (the
// S3 request URI), which would route copied chunks to the default collection.
func (s3a *S3ApiServer) copyChunksWithSSEC(entry *filer_pb.Entry, r *http.Request, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {

	// Parse SSE-C headers
	copySourceKey, err := ParseSSECCopySourceHeaders(r)
	if err != nil {
		glog.Errorf("Failed to parse SSE-C copy source headers: %v", err)
		return nil, nil, err
	}

	destKey, err := ParseSSECHeaders(r)
	if err != nil {
		glog.Errorf("Failed to parse SSE-C headers: %v", err)
		return nil, nil, err
	}

	// Check if this is a multipart SSE-C object
	isMultipartSSEC := false
	sseCChunks := 0
	for i, chunk := range entry.GetChunks() {
		glog.V(4).Infof("Chunk %d: sseType=%d, hasMetadata=%t", i, chunk.GetSseType(), len(chunk.GetSseMetadata()) > 0)
		if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
			sseCChunks++
		}
	}
	isMultipartSSEC = sseCChunks > 1

	if isMultipartSSEC {
		glog.V(2).Infof("Detected multipart SSE-C object with %d encrypted chunks for copy", sseCChunks)
		return s3a.copyMultipartSSECChunks(entry, copySourceKey, destKey, dstPath)
	}

	// Single-part SSE-C object: use original logic
	// Determine copy strategy
	strategy, err := DetermineSSECCopyStrategy(entry.Extended, copySourceKey, destKey)
	if err != nil {
		return nil, nil, err
	}

	glog.V(2).Infof("SSE-C copy strategy for single-part %s: %v", r.URL.Path, strategy)

	switch strategy {
	case SSECCopyStrategyDirect:
		// FAST PATH: Direct chunk copy
		glog.V(2).Infof("Using fast path: direct chunk copy for %s", r.URL.Path)
		chunks, err := s3a.copyChunks(entry, dstPath)
		return chunks, nil, err

	case SSECCopyStrategyDecryptEncrypt:
		// SLOW PATH: Decrypt and re-encrypt
		glog.V(2).Infof("Using slow path: decrypt/re-encrypt for %s", r.URL.Path)
		chunks, destIV, err := s3a.copyChunksWithReencryption(entry, copySourceKey, destKey, dstPath)
		if err != nil {
			return nil, nil, err
		}

		// Create destination metadata with IV and SSE-C headers
		dstMetadata := make(map[string][]byte)
		if destKey != nil && len(destIV) > 0 {
			// Store the IV
			StoreSSECIVInMetadata(dstMetadata, destIV)

			// Store SSE-C algorithm and key MD5 for proper metadata
			dstMetadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
			dstMetadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(destKey.KeyMD5)

			glog.V(2).Infof("Prepared IV and SSE-C metadata for destination copy: %s", r.URL.Path)
		}

		return chunks, dstMetadata, nil

	default:
		return nil, nil, fmt.Errorf("unknown SSE-C copy strategy: %v", strategy)
	}
}

// copyChunksWithReencryption handles the slow path: decrypt source and re-encrypt for destination
// Returns the destination chunks and the IV used for encryption (if any)
func (s3a *S3ApiServer) copyChunksWithReencryption(entry *filer_pb.Entry, copySourceKey *SSECustomerKey, destKey *SSECustomerKey, dstPath string) ([]*filer_pb.FileChunk, []byte, error) {
	dstChunks := make([]*filer_pb.FileChunk, len(entry.GetChunks()))
	const defaultChunkCopyConcurrency = 4
	executor := util.NewLimitedConcurrentExecutor(defaultChunkCopyConcurrency) // Limit to configurable concurrent operations
	errChan := make(chan error, len(entry.GetChunks()))

	// Generate a single IV for the destination object (if destination is encrypted)
	var destIV []byte
	if destKey != nil {
		destIV = make([]byte, s3_constants.AESBlockSize)
		if _, err := io.ReadFull(rand.Reader, destIV); err != nil {
			return nil, nil, fmt.Errorf("failed to generate destination IV: %w", err)
		}
	}

	for i, chunk := range entry.GetChunks() {
		chunkIndex := i
		executor.Execute(func() {
			dstChunk, err := s3a.copyChunkWithReencryption(chunk, copySourceKey, destKey, dstPath, entry.Extended, destIV)
			if err != nil {
				errChan <- fmt.Errorf("chunk %d: %v", chunkIndex, err)
				return
			}
			dstChunks[chunkIndex] = dstChunk
			errChan <- nil
		})
	}

	// Wait for all operations to complete and check for errors
	for i := 0; i < len(entry.GetChunks()); i++ {
		if err := <-errChan; err != nil {
			return nil, nil, err
		}
	}

	return dstChunks, destIV, nil
}

// copyChunkWithReencryption copies a single chunk with decrypt/re-encrypt
func (s3a *S3ApiServer) copyChunkWithReencryption(chunk *filer_pb.FileChunk, copySourceKey *SSECustomerKey, destKey *SSECustomerKey, dstPath string, srcMetadata map[string][]byte, destIV []byte) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, chunk.Size)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size), chunk.CipherKey)
	if err != nil {
		return nil, fmt.Errorf("download encrypted chunk data: %w", err)
	}

	var finalData []byte

	// Decrypt if source is encrypted
	if copySourceKey != nil {
		// Get IV from source metadata
		srcIV, err := GetSSECIVFromMetadata(srcMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to get IV from metadata: %w", err)
		}

		// Use counter offset based on chunk position in the original object
		decryptedReader, decErr := CreateSSECDecryptedReaderWithOffset(bytes.NewReader(encryptedData), copySourceKey, srcIV, uint64(chunk.Offset))
		if decErr != nil {
			return nil, fmt.Errorf("create decrypted reader: %w", decErr)
		}

		decryptedData, readErr := io.ReadAll(decryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("decrypt chunk data: %w", readErr)
		}
		finalData = decryptedData
	} else {
		// Source is unencrypted
		finalData = encryptedData
	}

	// Re-encrypt if destination should be encrypted
	if destKey != nil {
		// Use the provided destination IV with counter offset based on chunk position
		// This ensures all chunks of the same object use the same IV with different counters
		encryptedReader, encErr := CreateSSECEncryptedReaderWithOffset(bytes.NewReader(finalData), destKey, destIV, uint64(chunk.Offset))
		if encErr != nil {
			return nil, fmt.Errorf("create encrypted reader: %w", encErr)
		}

		reencryptedData, readErr := io.ReadAll(encryptedReader)
		if readErr != nil {
			return nil, fmt.Errorf("re-encrypt chunk data: %w", readErr)
		}
		finalData = reencryptedData
		dstChunk.Size = uint64(len(finalData))

		// Tag the destination chunk as SSE-C with per-chunk metadata. Without
		// this the chunk's SseType stays NONE, detectPrimarySSEType returns
		// "None" on read (it counts SSE-C chunks; an entry whose only chunk
		// is NONE shows zero), and GetObjectHandler serves the still-encrypted
		// volume bytes raw without decryption — yielding deterministic byte
		// corruption on the SSE-C copy path (issue #9281).
		ssecMetadata, metaErr := SerializeSSECMetadata(destIV, destKey.KeyMD5, chunk.Offset)
		if metaErr != nil {
			return nil, fmt.Errorf("serialize SSE-C chunk metadata: %w", metaErr)
		}
		dstChunk.SseType = filer_pb.SSEType_SSE_C
		dstChunk.SseMetadata = ssecMetadata
	}

	// Upload the processed data
	if err := s3a.uploadChunkData(finalData, assignResult, false); err != nil {
		return nil, fmt.Errorf("upload processed chunk data: %w", err)
	}

	return dstChunk, nil
}

// copyChunksWithSSEKMS handles SSE-KMS aware copying with smart fast/slow path selection
// Returns chunks and destination metadata like SSE-C for consistency
func (s3a *S3ApiServer) copyChunksWithSSEKMS(entry *filer_pb.Entry, r *http.Request, bucket string, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {

	// Parse SSE-KMS headers from copy request
	destKeyID, encryptionContext, bucketKeyEnabled, err := ParseSSEKMSCopyHeaders(r)
	if err != nil {
		return nil, nil, err
	}

	// Check if this is a multipart SSE-KMS object
	isMultipartSSEKMS := false
	sseKMSChunks := 0
	for i, chunk := range entry.GetChunks() {
		glog.V(4).Infof("Chunk %d: sseType=%d, hasKMSMetadata=%t", i, chunk.GetSseType(), len(chunk.GetSseMetadata()) > 0)
		if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS {
			sseKMSChunks++
		}
	}
	isMultipartSSEKMS = sseKMSChunks > 1

	if isMultipartSSEKMS {
		glog.V(2).Infof("Detected multipart SSE-KMS object with %d encrypted chunks for copy", sseKMSChunks)
		return s3a.copyMultipartSSEKMSChunks(entry, destKeyID, encryptionContext, bucketKeyEnabled, dstPath, bucket)
	}

	// Single-part SSE-KMS object: use existing logic
	// If no SSE-KMS headers and source is not SSE-KMS encrypted, use regular copy
	if destKeyID == "" && !IsSSEKMSEncrypted(entry.Extended) {
		chunks, err := s3a.copyChunks(entry, dstPath)
		return chunks, nil, err
	}

	// Apply bucket default encryption if no explicit key specified
	if destKeyID == "" {
		bucketMetadata, err := s3a.getBucketMetadata(bucket)
		if err != nil {
			glog.V(2).Infof("Could not get bucket metadata for default encryption: %v", err)
		} else if bucketMetadata != nil && bucketMetadata.Encryption != nil && bucketMetadata.Encryption.SseAlgorithm == "aws:kms" {
			destKeyID = bucketMetadata.Encryption.KmsKeyId
			bucketKeyEnabled = bucketMetadata.Encryption.BucketKeyEnabled
		}
	}

	// Determine copy strategy.
	//
	// DetermineSSEKMSCopyStrategy returns Direct when source and destination
	// share the same KMS key ID, but that's not enough on its own — if the
	// destination request changes the encryption context or the BucketKey
	// flag, the source ciphertext (and its embedded EDK + context) does not
	// satisfy the destination's request. Force the slow re-encrypt path in
	// that case so the destination object gets a freshly-wrapped EDK bound
	// to the requested context/flag.
	strategy, err := DetermineSSEKMSCopyStrategy(entry.Extended, destKeyID)
	if err != nil {
		return nil, nil, err
	}
	if strategy == SSEKMSCopyStrategyDirect && destKeyID != "" && !srcSSEKMSStateMatchesDest(entry.Extended, encryptionContext, bucketKeyEnabled) {
		glog.V(2).Infof("SSE-KMS direct copy rejected — encryption context or bucket-key flag differs; falling back to re-encrypt path for %s", dstPath)
		strategy = SSEKMSCopyStrategyDecryptEncrypt
	}

	glog.V(2).Infof("SSE-KMS copy strategy for %s: %v", dstPath, strategy)

	switch strategy {
	case SSEKMSCopyStrategyDirect:
		// FAST PATH: Direct chunk copy (same key or both unencrypted)
		glog.V(2).Infof("Using fast path: direct chunk copy for %s", dstPath)
		chunks, err := s3a.copyChunks(entry, dstPath)
		// For direct copy, generate destination metadata if we're encrypting to SSE-KMS
		var dstMetadata map[string][]byte
		if destKeyID != "" {
			dstMetadata = make(map[string][]byte)
			if encryptionContext == nil {
				encryptionContext = BuildEncryptionContext(bucket, dstPath, bucketKeyEnabled)
			}
			// Direct (same-key) fast path: chunks were copied as-is and now
			// carry the source's per-chunk SSE-KMS metadata (preserved by
			// createDestinationChunkPreservingSSE in copySingleChunk). Use
			// the first chunk's full key as entry-level so single-chunk
			// reads can unwrap the EDK on the GET path. Earlier this stored
			// only KeyID/context/bucketKey, which made single-chunk reads
			// fail at GET with "Invalid ciphertext format" (#9281).
			if len(chunks) > 0 && len(chunks[0].GetSseMetadata()) > 0 {
				dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = chunks[0].GetSseMetadata()
				glog.V(3).Infof("Set entry-level SSE-KMS key from first dst chunk for direct copy: keyID=%s", destKeyID)
			} else {
				sseKey := &SSEKMSKey{
					KeyID:             destKeyID,
					EncryptionContext: encryptionContext,
					BucketKeyEnabled:  bucketKeyEnabled,
				}
				if kmsMetadata, serializeErr := SerializeSSEKMSMetadata(sseKey); serializeErr == nil {
					dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = kmsMetadata
					glog.V(3).Infof("Generated SSE-KMS metadata for direct copy: keyID=%s", destKeyID)
				} else {
					glog.Errorf("Failed to serialize SSE-KMS metadata for direct copy: %v", serializeErr)
				}
			}
		}
		return chunks, dstMetadata, err

	case SSEKMSCopyStrategyDecryptEncrypt:
		// SLOW PATH: Decrypt source and re-encrypt for destination
		glog.V(2).Infof("Using slow path: decrypt/re-encrypt for %s", dstPath)
		return s3a.copyChunksWithSSEKMSReencryption(entry, destKeyID, encryptionContext, bucketKeyEnabled, dstPath, bucket)

	default:
		return nil, nil, fmt.Errorf("unknown SSE-KMS copy strategy: %v", strategy)
	}
}

// deserializeEntrySSEKMSKey returns the SSE-KMS key serialized into
// entry.Extended[SeaweedFSSSEKMSKey], or nil if the entry is not SSE-KMS
// encrypted. Errors are logged and treated as "not present" so the caller
// can fall back to per-chunk metadata or fail safely.
func deserializeEntrySSEKMSKey(entryExtended map[string][]byte) *SSEKMSKey {
	keyData, ok := entryExtended[s3_constants.SeaweedFSSSEKMSKey]
	if !ok || len(keyData) == 0 {
		return nil
	}
	k, err := DeserializeSSEKMSMetadata(keyData)
	if err != nil {
		glog.V(2).Infof("deserializeEntrySSEKMSKey: failed to deserialize entry-level SSE-KMS key: %v", err)
		return nil
	}
	return k
}

// resolveChunkSSEKMSKey picks the right SSE-KMS key to decrypt a chunk with:
// the chunk's own per-chunk metadata if present (the post-#9211 layout for
// new uploads), else the source object's entry-level key (legacy multipart
// objects). Returns nil + an error if neither is available; the caller can
// then surface a clear "missing metadata" error to the client. The selection
// must mirror the encryption side: each chunk is encrypted with the key
// recorded in its per-chunk metadata at write time, and entry-level metadata
// is the legacy fallback for parts that were written before per-chunk keys
// existed.
func resolveChunkSSEKMSKey(chunk *filer_pb.FileChunk, entryFallback *SSEKMSKey) (*SSEKMSKey, error) {
	if len(chunk.GetSseMetadata()) > 0 {
		return DeserializeSSEKMSMetadata(chunk.GetSseMetadata())
	}
	if entryFallback != nil {
		glog.V(2).Infof("resolveChunkSSEKMSKey: chunk %s has no per-chunk SSE-KMS metadata; falling back to entry-level key (legacy multipart object)", chunk.GetFileIdString())
		return entryFallback, nil
	}
	return nil, fmt.Errorf("SSE-KMS chunk %s missing per-chunk metadata and no entry-level key available", chunk.GetFileIdString())
}

// srcSSEKMSStateMatchesDest reports whether the source object's stored SSE-KMS
// state (encryption context + bucket-key flag) matches the destination request.
// Used to gate the SSE-KMS direct copy fast path: if either differs the source
// ciphertext can't satisfy the destination's request and we must re-encrypt.
func srcSSEKMSStateMatchesDest(srcMetadata map[string][]byte, dstContext map[string]string, dstBucketKeyEnabled bool) bool {
	srcKey := deserializeEntrySSEKMSKey(srcMetadata)
	if srcKey == nil {
		// Source isn't SSE-KMS encrypted (or its key data is malformed —
		// we conservatively let CanDirectCopySSEKMS make the call there).
		return true
	}
	if srcKey.BucketKeyEnabled != dstBucketKeyEnabled {
		return false
	}
	if !encryptionContextEqual(srcKey.EncryptionContext, dstContext) {
		return false
	}
	return true
}

// encryptionContextEqual treats nil and empty maps as equivalent so a request
// that omits the context header doesn't spuriously diverge from a stored one
// that was serialised as an empty map. reflect.DeepEqual returns false for
// nil-vs-empty, so the empty-case shortcut at the top is required.
func encryptionContextEqual(a, b map[string]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// copyChunksWithSSEKMSReencryption handles the slow path: decrypt source and re-encrypt for destination
// Returns chunks and destination metadata like SSE-C for consistency
func (s3a *S3ApiServer) copyChunksWithSSEKMSReencryption(entry *filer_pb.Entry, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	var dstChunks []*filer_pb.FileChunk

	// Deserialize the source's entry-level SSE-KMS key once. Used as the
	// per-chunk fallback for legacy multipart objects (see resolveChunkSSEKMSKey).
	sourceSSEKey := deserializeEntrySSEKMSKey(entry.Extended)
	if sourceSSEKey != nil {
		glog.V(3).Infof("Extracted source SSE-KMS key: keyID=%s, bucketKey=%t", sourceSSEKey.KeyID, sourceSSEKey.BucketKeyEnabled)
	}

	// Process chunks
	for _, chunk := range entry.GetChunks() {
		dstChunk, err := s3a.copyChunkWithSSEKMSReencryption(chunk, sourceSSEKey, destKeyID, encryptionContext, bucketKeyEnabled, dstPath, bucket)
		if err != nil {
			return nil, nil, fmt.Errorf("copy chunk with SSE-KMS re-encryption: %w", err)
		}
		dstChunks = append(dstChunks, dstChunk)
	}

	// Generate destination metadata for SSE-KMS encryption.
	//
	// For multi-chunk objects (isMultipartSSEKMS=true on read), the read path
	// uses per-chunk metadata (already set by copyChunkWithSSEKMSReencryption
	// after #9281). For single-chunk objects (isMultipartSSEKMS=false), the
	// read path falls back to the entry-level SSE-KMS key — so it must be a
	// fully-formed key (with EncryptedDataKey + IV), not just KeyID. Earlier
	// this stored only KeyID/context/bucketKey, leaving EncryptedDataKey
	// empty; reads then failed with "Invalid ciphertext format" when KMS was
	// asked to unwrap an empty EDK.
	//
	// Take the first destination chunk's full per-chunk metadata as the
	// canonical entry-level key — it includes a real EDK + IV minted by
	// CreateSSEKMSEncryptedReaderWithBucketKey.
	dstMetadata := make(map[string][]byte)
	if destKeyID != "" {
		if len(dstChunks) > 0 && len(dstChunks[0].GetSseMetadata()) > 0 {
			dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = dstChunks[0].GetSseMetadata()
			glog.V(3).Infof("Set entry-level SSE-KMS key from first dst chunk: keyID=%s, bucketKey=%t", destKeyID, bucketKeyEnabled)
		} else {
			// 0-byte (no chunks) or no SSE-KMS chunk to crib metadata from:
			// fall back to a stub key so the destination entry is still
			// recognised as SSE-KMS encrypted on read. Mirrors the fallback
			// in copyChunksWithSSEKMS direct branch and copyMultipartCrossEncryption.
			if encryptionContext == nil {
				encryptionContext = BuildEncryptionContext(bucket, dstPath, bucketKeyEnabled)
			}
			sseKey := &SSEKMSKey{
				KeyID:             destKeyID,
				EncryptionContext: encryptionContext,
				BucketKeyEnabled:  bucketKeyEnabled,
			}
			if kmsMetadata, serErr := SerializeSSEKMSMetadata(sseKey); serErr == nil {
				dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = kmsMetadata
			} else {
				glog.Errorf("Failed to serialize SSE-KMS metadata for 0-byte destination: %v", serErr)
			}
		}
	}

	return dstChunks, dstMetadata, nil
}

// copyChunkWithSSEKMSReencryption copies a single chunk with SSE-KMS decrypt/re-encrypt
func (s3a *S3ApiServer) copyChunkWithSSEKMSReencryption(chunk *filer_pb.FileChunk, sourceSSEKey *SSEKMSKey, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath, chunk.Size)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Download chunk data
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size), chunk.CipherKey)
	if err != nil {
		return nil, fmt.Errorf("download chunk data: %w", err)
	}

	var finalData []byte

	// Decrypt source data if it's SSE-KMS encrypted.
	// Multipart SSE-KMS sources have a different EDK + IV per chunk; the
	// per-chunk metadata is the only place those values live, so we MUST use
	// the chunk's own metadata for decryption rather than the entry-level
	// sourceSSEKey (which only matches single-part objects). Earlier this
	// always decrypted with the entry-level key, which produced deterministic
	// wrong bytes on a multipart-source COPY (issue #9281). Use the shared
	// resolveChunkSSEKMSKey helper which centralises this selection.
	var chunkSSEKey *SSEKMSKey
	if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS || sourceSSEKey != nil {
		var resolveErr error
		chunkSSEKey, resolveErr = resolveChunkSSEKMSKey(chunk, sourceSSEKey)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve SSE-KMS metadata: %w", resolveErr)
		}
	}
	if chunkSSEKey != nil {
		decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(chunkData), chunkSSEKey)
		if err != nil {
			return nil, fmt.Errorf("create SSE-KMS decrypted reader: %w", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			return nil, fmt.Errorf("decrypt chunk data: %w", err)
		}
		finalData = decryptedData
		glog.V(4).Infof("Decrypted chunk data: %d bytes → %d bytes", len(chunkData), len(finalData))
	} else {
		// Source is not SSE-KMS encrypted, use data as-is
		finalData = chunkData
	}

	// Re-encrypt if destination should be SSE-KMS encrypted
	if destKeyID != "" {
		// Encryption context should already be provided by the caller
		// But ensure we have a fallback for robustness
		if encryptionContext == nil {
			encryptionContext = BuildEncryptionContext(bucket, dstPath, bucketKeyEnabled)
		}

		encryptedReader, destSSEKey, err := CreateSSEKMSEncryptedReaderWithBucketKey(bytes.NewReader(finalData), destKeyID, encryptionContext, bucketKeyEnabled)
		if err != nil {
			return nil, fmt.Errorf("create SSE-KMS encrypted reader: %w", err)
		}

		reencryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			return nil, fmt.Errorf("re-encrypt chunk data: %w", err)
		}

		originalSize := len(finalData)
		finalData = reencryptedData
		glog.V(4).Infof("Re-encrypted chunk data: %d bytes → %d bytes", originalSize, len(finalData))
		dstChunk.Size = uint64(len(finalData))

		// Tag the destination chunk as SSE-KMS with per-chunk metadata. Without
		// this, the chunk's SseType stays NONE, detectPrimarySSEType returns
		// "None" on read, and GetObjectHandler serves still-encrypted volume
		// bytes raw without decryption — yielding deterministic byte
		// corruption on the SSE-KMS copy path (issue #9281).
		//
		// CreateSSEKMSEncryptedReaderWithBucketKey returns destSSEKey freshly
		// populated with KeyID, EncryptionContext, EncryptedDataKey, IV and
		// BucketKey state, with the encryption stream initialised at counter 0
		// for THIS chunk's bytes (each chunk gets its own random IV, not a
		// base-IV-plus-offset scheme). ChunkOffset must therefore stay 0 on
		// read; setting it to chunk.Offset would advance the decryption IV by
		// chunk.Offset/16 blocks past the position the encryption was at,
		// producing deterministic garbage on chunks whose chunk.Offset > 0.
		destSSEKey.ChunkOffset = 0
		kmsMetadata, metaErr := SerializeSSEKMSMetadata(destSSEKey)
		if metaErr != nil {
			return nil, fmt.Errorf("serialize SSE-KMS chunk metadata: %w", metaErr)
		}
		dstChunk.SseType = filer_pb.SSEType_SSE_KMS
		dstChunk.SseMetadata = kmsMetadata
	}

	// Upload the processed data
	if err := s3a.uploadChunkData(finalData, assignResult, false); err != nil {
		return nil, fmt.Errorf("upload processed chunk data: %w", err)
	}

	glog.V(3).Infof("Successfully processed SSE-KMS chunk re-encryption: src_key=%s, dst_key=%s, size=%d→%d",
		getKeyIDString(sourceSSEKey), destKeyID, len(chunkData), len(finalData))

	return dstChunk, nil
}

// getKeyIDString safely gets the KeyID from an SSEKMSKey, handling nil cases
func getKeyIDString(key *SSEKMSKey) string {
	if key == nil {
		return "none"
	}
	if key.KeyID == "" {
		return "default"
	}
	return key.KeyID
}

// EncryptionHeaderContext holds encryption type information and header classifications
type EncryptionHeaderContext struct {
	SrcSSEC, SrcSSEKMS, SrcSSES3                bool
	DstSSEC, DstSSEKMS, DstSSES3                bool
	IsSSECHeader, IsSSEKMSHeader, IsSSES3Header bool
}

// newEncryptionHeaderContext creates a context for encryption header processing
func newEncryptionHeaderContext(headerKey string, srcSSEC, srcSSEKMS, srcSSES3, dstSSEC, dstSSEKMS, dstSSES3 bool) *EncryptionHeaderContext {
	return &EncryptionHeaderContext{
		SrcSSEC: srcSSEC, SrcSSEKMS: srcSSEKMS, SrcSSES3: srcSSES3,
		DstSSEC: dstSSEC, DstSSEKMS: dstSSEKMS, DstSSES3: dstSSES3,
		IsSSECHeader:   isSSECHeader(headerKey),
		IsSSEKMSHeader: isSSEKMSHeader(headerKey, srcSSEKMS, dstSSEKMS),
		IsSSES3Header:  isSSES3Header(headerKey, srcSSES3, dstSSES3),
	}
}

// isSSECHeader checks if the header is SSE-C specific
func isSSECHeader(headerKey string) bool {
	return headerKey == s3_constants.AmzServerSideEncryptionCustomerAlgorithm ||
		headerKey == s3_constants.AmzServerSideEncryptionCustomerKeyMD5 ||
		headerKey == s3_constants.SeaweedFSSSEIV
}

// isSSEKMSHeader checks if the header is SSE-KMS specific
func isSSEKMSHeader(headerKey string, srcSSEKMS, dstSSEKMS bool) bool {
	return (headerKey == s3_constants.AmzServerSideEncryption && (srcSSEKMS || dstSSEKMS)) ||
		headerKey == s3_constants.AmzServerSideEncryptionAwsKmsKeyId ||
		headerKey == s3_constants.SeaweedFSSSEKMSKey ||
		headerKey == s3_constants.SeaweedFSSSEKMSKeyID ||
		headerKey == s3_constants.SeaweedFSSSEKMSEncryption ||
		headerKey == s3_constants.SeaweedFSSSEKMSBucketKeyEnabled ||
		headerKey == s3_constants.SeaweedFSSSEKMSEncryptionContext ||
		headerKey == s3_constants.SeaweedFSSSEKMSBaseIV
}

// isSSES3Header checks if the header is SSE-S3 specific
func isSSES3Header(headerKey string, srcSSES3, dstSSES3 bool) bool {
	return (headerKey == s3_constants.AmzServerSideEncryption && (srcSSES3 || dstSSES3)) ||
		headerKey == s3_constants.SeaweedFSSSES3Key ||
		headerKey == s3_constants.SeaweedFSSSES3Encryption ||
		headerKey == s3_constants.SeaweedFSSSES3BaseIV ||
		headerKey == s3_constants.SeaweedFSSSES3KeyData
}

// shouldSkipCrossEncryptionHeader handles cross-encryption copy scenarios
func (ctx *EncryptionHeaderContext) shouldSkipCrossEncryptionHeader() bool {
	// SSE-C to SSE-KMS: skip SSE-C headers
	if ctx.SrcSSEC && ctx.DstSSEKMS && ctx.IsSSECHeader {
		return true
	}

	// SSE-KMS to SSE-C: skip SSE-KMS headers
	if ctx.SrcSSEKMS && ctx.DstSSEC && ctx.IsSSEKMSHeader {
		return true
	}

	// SSE-C to SSE-S3: skip SSE-C headers
	if ctx.SrcSSEC && ctx.DstSSES3 && ctx.IsSSECHeader {
		return true
	}

	// SSE-S3 to SSE-C: skip SSE-S3 headers
	if ctx.SrcSSES3 && ctx.DstSSEC && ctx.IsSSES3Header {
		return true
	}

	// SSE-KMS to SSE-S3: skip SSE-KMS headers
	if ctx.SrcSSEKMS && ctx.DstSSES3 && ctx.IsSSEKMSHeader {
		return true
	}

	// SSE-S3 to SSE-KMS: skip SSE-S3 headers
	if ctx.SrcSSES3 && ctx.DstSSEKMS && ctx.IsSSES3Header {
		return true
	}

	return false
}

// shouldSkipEncryptedToUnencryptedHeader handles encrypted to unencrypted copy scenarios
func (ctx *EncryptionHeaderContext) shouldSkipEncryptedToUnencryptedHeader() bool {
	// Skip all encryption headers when copying from encrypted to unencrypted
	hasSourceEncryption := ctx.SrcSSEC || ctx.SrcSSEKMS || ctx.SrcSSES3
	hasDestinationEncryption := ctx.DstSSEC || ctx.DstSSEKMS || ctx.DstSSES3
	isAnyEncryptionHeader := ctx.IsSSECHeader || ctx.IsSSEKMSHeader || ctx.IsSSES3Header

	return hasSourceEncryption && !hasDestinationEncryption && isAnyEncryptionHeader
}

// cleanupVersioningMetadata removes versioning-related metadata from Extended attributes
// when copying to non-versioned or suspended-versioning buckets.
// This prevents objects in non-versioned buckets from carrying invalid versioning metadata.
// It also removes the source ETag to prevent metadata inconsistency, as a new ETag will be
// calculated for the destination object.
func cleanupVersioningMetadata(metadata map[string][]byte) {
	delete(metadata, s3_constants.ExtVersionIdKey)
	delete(metadata, s3_constants.ExtDeleteMarkerKey)
	delete(metadata, s3_constants.ExtIsLatestKey)
	delete(metadata, s3_constants.ExtETagKey)
}

// isOrphanedSSES3Header checks if a header is an orphaned SSE-S3 encryption header.
// An orphaned header is one where the encryption indicator exists but the actual key is missing.
// This can happen when an object was previously encrypted but then copied without encryption,
// leaving behind the header but removing the key. These orphaned headers should be stripped
// during copy operations to prevent confusion about the object's actual encryption state.
// Fixes GitHub issue #7562.
func isOrphanedSSES3Header(headerKey string, metadata map[string][]byte) bool {
	if headerKey != s3_constants.AmzServerSideEncryption {
		return false
	}

	// The header is AmzServerSideEncryption. Check if its value indicates SSE-S3.
	if string(metadata[headerKey]) == "AES256" {
		// It's an SSE-S3 header. It's orphaned if the actual encryption key is missing.
		_, hasKey := metadata[s3_constants.SeaweedFSSSES3Key]
		return !hasKey
	}

	return false
}

// shouldSkipEncryptionHeader determines if a header should be skipped when copying extended attributes
// based on the source and destination encryption types. This consolidates the repetitive logic for
// filtering encryption-related headers during copy operations.
func shouldSkipEncryptionHeader(headerKey string,
	srcSSEC, srcSSEKMS, srcSSES3 bool,
	dstSSEC, dstSSEKMS, dstSSES3 bool) bool {

	// Create context to reduce complexity and improve testability
	ctx := newEncryptionHeaderContext(headerKey, srcSSEC, srcSSEKMS, srcSSES3, dstSSEC, dstSSEKMS, dstSSES3)

	// If it's not an encryption header, don't skip it
	if !ctx.IsSSECHeader && !ctx.IsSSEKMSHeader && !ctx.IsSSES3Header {
		return false
	}

	// Handle cross-encryption scenarios (different encryption types)
	if ctx.shouldSkipCrossEncryptionHeader() {
		return true
	}

	// Handle encrypted to unencrypted scenarios
	if ctx.shouldSkipEncryptedToUnencryptedHeader() {
		return true
	}

	// Default: don't skip the header
	return false
}

// processInlineContentForCopy handles encryption/decryption for inline content during copy
// This fixes GitHub #7562 where small files stored inline weren't properly decrypted/re-encrypted
func (s3a *S3ApiServer) processInlineContentForCopy(
	entry *filer_pb.Entry, r *http.Request, dstBucket, dstObject string,
	srcSSEC, srcSSEKMS, srcSSES3 bool,
	dstSSEC, dstSSEKMS, dstSSES3 bool) ([]byte, map[string][]byte, error) {

	content := entry.Content
	var dstMetadata map[string][]byte

	// Check if source is encrypted and needs decryption
	srcEncrypted := srcSSEC || srcSSEKMS || srcSSES3

	// Check if destination needs encryption (explicit request or bucket default)
	dstNeedsEncryption := dstSSEC || dstSSEKMS || dstSSES3
	if !dstNeedsEncryption {
		// Check bucket default encryption
		bucketMetadata, err := s3a.getBucketMetadata(dstBucket)
		if err == nil && bucketMetadata != nil && bucketMetadata.Encryption != nil {
			switch bucketMetadata.Encryption.SseAlgorithm {
			case "aws:kms":
				dstSSEKMS = true
				dstNeedsEncryption = true
			case "AES256":
				dstSSES3 = true
				dstNeedsEncryption = true
			}
		}
	}

	// Decrypt source content if encrypted
	if srcEncrypted {
		decryptedContent, decErr := s3a.decryptInlineContent(entry, srcSSEC, srcSSEKMS, srcSSES3, r)
		if decErr != nil {
			return nil, nil, fmt.Errorf("failed to decrypt inline content: %w", decErr)
		}
		content = decryptedContent
		glog.V(3).Infof("Decrypted inline content: %d bytes", len(content))
	}

	// Re-encrypt if destination needs encryption
	if dstNeedsEncryption {
		encryptedContent, encMetadata, encErr := s3a.encryptInlineContent(content, dstBucket, dstObject, dstSSEC, dstSSEKMS, dstSSES3, r)
		if encErr != nil {
			return nil, nil, fmt.Errorf("failed to encrypt inline content: %w", encErr)
		}
		content = encryptedContent
		dstMetadata = encMetadata
		glog.V(3).Infof("Encrypted inline content: %d bytes", len(content))
	}

	return content, dstMetadata, nil
}

// decryptInlineContent decrypts inline content from an encrypted source
func (s3a *S3ApiServer) decryptInlineContent(entry *filer_pb.Entry, srcSSEC, srcSSEKMS, srcSSES3 bool, r *http.Request) ([]byte, error) {
	content := entry.Content

	if srcSSES3 {
		// Get SSE-S3 key from metadata
		keyData, exists := entry.Extended[s3_constants.SeaweedFSSSES3Key]
		if !exists {
			return nil, fmt.Errorf("SSE-S3 key not found in metadata")
		}

		keyManager := GetSSES3KeyManager()
		sseKey, err := DeserializeSSES3Metadata(keyData, keyManager)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-S3 key: %w", err)
		}

		// Get IV
		iv := sseKey.IV
		if len(iv) == 0 {
			return nil, fmt.Errorf("SSE-S3 IV not found")
		}

		// Decrypt content
		decryptedReader, err := CreateSSES3DecryptedReader(bytes.NewReader(content), sseKey, iv)
		if err != nil {
			return nil, fmt.Errorf("failed to create SSE-S3 decrypted reader: %w", err)
		}
		return io.ReadAll(decryptedReader)

	} else if srcSSEKMS {
		// Get SSE-KMS key from metadata
		keyData, exists := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]
		if !exists {
			return nil, fmt.Errorf("SSE-KMS key not found in metadata")
		}

		sseKey, err := DeserializeSSEKMSMetadata(keyData)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-KMS key: %w", err)
		}

		// Decrypt content
		decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(content), sseKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create SSE-KMS decrypted reader: %w", err)
		}
		return io.ReadAll(decryptedReader)

	} else if srcSSEC {
		// Get SSE-C key from request headers
		sourceKey, err := ParseSSECCopySourceHeaders(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SSE-C copy source headers: %w", err)
		}

		// Get IV from metadata
		iv, err := GetSSECIVFromMetadata(entry.Extended)
		if err != nil {
			return nil, fmt.Errorf("failed to get SSE-C IV: %w", err)
		}

		// Decrypt content
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(content), sourceKey, iv)
		if err != nil {
			return nil, fmt.Errorf("failed to create SSE-C decrypted reader: %w", err)
		}
		return io.ReadAll(decryptedReader)
	}

	// Source not encrypted, return as-is
	return content, nil
}

// encryptInlineContent encrypts inline content for the destination
func (s3a *S3ApiServer) encryptInlineContent(content []byte, dstBucket, dstObject string,
	dstSSEC, dstSSEKMS, dstSSES3 bool, r *http.Request) ([]byte, map[string][]byte, error) {

	dstMetadata := make(map[string][]byte)

	if dstSSES3 {
		// Generate SSE-S3 key
		keyManager := GetSSES3KeyManager()
		key, err := keyManager.GetOrCreateKey("")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate SSE-S3 key: %w", err)
		}

		// Encrypt content
		encryptedReader, iv, err := CreateSSES3EncryptedReader(bytes.NewReader(content), key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create SSE-S3 encrypted reader: %w", err)
		}

		encryptedContent, err := io.ReadAll(encryptedReader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read encrypted content: %w", err)
		}

		// Store IV on key and serialize metadata
		key.IV = iv
		keyData, err := SerializeSSES3Metadata(key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to serialize SSE-S3 metadata: %w", err)
		}

		dstMetadata[s3_constants.SeaweedFSSSES3Key] = keyData
		dstMetadata[s3_constants.AmzServerSideEncryption] = []byte("AES256")

		return encryptedContent, dstMetadata, nil

	} else if dstSSEKMS {
		// Parse SSE-KMS headers
		keyID, encryptionContext, bucketKeyEnabled, err := ParseSSEKMSCopyHeaders(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse SSE-KMS headers: %w", err)
		}

		// Build encryption context if needed
		if encryptionContext == nil {
			encryptionContext = BuildEncryptionContext(dstBucket, dstObject, bucketKeyEnabled)
		}

		// Encrypt content
		encryptedReader, sseKey, err := CreateSSEKMSEncryptedReaderWithBucketKey(
			bytes.NewReader(content), keyID, encryptionContext, bucketKeyEnabled)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create SSE-KMS encrypted reader: %w", err)
		}

		encryptedContent, err := io.ReadAll(encryptedReader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read encrypted content: %w", err)
		}

		// Serialize metadata
		keyData, err := SerializeSSEKMSMetadata(sseKey)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to serialize SSE-KMS metadata: %w", err)
		}

		dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = keyData
		dstMetadata[s3_constants.AmzServerSideEncryption] = []byte("aws:kms")

		return encryptedContent, dstMetadata, nil

	} else if dstSSEC {
		// Parse SSE-C headers
		destKey, err := ParseSSECHeaders(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse SSE-C headers: %w", err)
		}

		// Encrypt content
		encryptedReader, iv, err := CreateSSECEncryptedReader(bytes.NewReader(content), destKey)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create SSE-C encrypted reader: %w", err)
		}

		encryptedContent, err := io.ReadAll(encryptedReader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read encrypted content: %w", err)
		}

		// Store IV in metadata
		StoreSSECIVInMetadata(dstMetadata, iv)
		dstMetadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
		dstMetadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(destKey.KeyMD5)

		return encryptedContent, dstMetadata, nil
	}

	// No encryption needed
	return content, nil, nil
}
