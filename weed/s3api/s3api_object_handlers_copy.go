package s3api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"modernc.org/strutil"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	DirectiveCopy    = "COPY"
	DirectiveReplace = "REPLACE"
)

func quoteETag(etag string) string {
	etag = strings.Trim(etag, "\"")
	if etag == "" {
		return ""
	}
	return strconv.Quote(etag)
}

func rawETag(etag string) string {
	return strings.Trim(etag, "\"")
}

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {

	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	rawCopySource := r.Header.Get("X-Amz-Copy-Source")
	cpSrcPath, err := url.QueryUnescape(rawCopySource)
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = rawCopySource
	}

	srcBucket, srcObject, srcVersionId := pathToBucketObjectAndVersion(rawCopySource, cpSrcPath)

	glog.V(3).Infof("CopyObjectHandler %s %s (version: %s) => %s %s", srcBucket, srcObject, srcVersionId, dstBucket, dstObject)

	// Validate copy source and destination
	if err := ValidateCopySource(cpSrcPath, srcBucket, srcObject); err != nil {
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

	replaceMeta, replaceTagging := replaceDirective(r.Header)

	if (srcBucket == dstBucket && srcObject == dstObject || cpSrcPath == "") && (replaceMeta || replaceTagging) {
		fullPath := util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, dstBucket, dstObject))
		dir, name := fullPath.DirAndName()
		entry, err := s3a.getEntry(dir, name)
		if err != nil || entry.IsDirectory {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		entry.Extended, err = processMetadataBytes(r.Header, entry.Extended, replaceMeta, replaceTagging)
		entry.Attributes.Mtime = time.Now().Unix()
		if err != nil {
			glog.Errorf("CopyObjectHandler ValidateTags error %s: %v", r.URL, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidTag)
			return
		}
		err = s3a.touch(dir, name, entry)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		sameObjectEtag := quoteETag(filer.ETag(entry))
		setEtag(w, sameObjectEtag)
		writeSuccessResponseXML(w, r, CopyObjectResult{
			ETag:         sameObjectEtag,
			LastModified: time.Now().UTC(),
		})
		return
	}

	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	// Get detailed versioning state for source bucket
	srcVersioningState, err := s3a.getVersioningState(srcBucket)
	if err != nil {
		glog.Errorf("Error checking versioning state for source bucket %s: %v", srcBucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
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
		srcPath := util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, srcBucket, srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
		if err != nil {
			// If regular file doesn't exist, try latest version as fallback
			glog.V(2).Infof("CopyObject: regular file not found for suspended versioning, trying latest version")
			entry, err = s3a.getLatestObjectVersion(srcBucket, srcObject)
		}
	} else {
		// No versioning configured - use regular retrieval
		srcPath := util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, srcBucket, srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
	}

	if err != nil || entry.IsDirectory {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	if srcBucket == dstBucket && srcObject == dstObject {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopyDest)
		return
	}

	// Validate conditional copy headers
	if err := s3a.validateConditionalCopyHeaders(r, entry); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Validate encryption parameters
	if err := ValidateCopyEncryption(entry.Extended, r.Header); err != nil {
		glog.V(2).Infof("CopyObjectHandler encryption validation error: %v", err)
		errCode := MapCopyValidationError(err)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Create new entry for destination
	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: entry.Attributes.FileSize,
			Mtime:    time.Now().Unix(),
			Crtime:   entry.Attributes.Crtime,
			Mime:     entry.Attributes.Mime,
			Md5:      entry.Attributes.Md5, // Copy MD5 for correct ETag calculation
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

	// Apply processed metadata to destination entry
	for k, v := range processedMetadata {
		dstEntry.Extended[k] = v
	}

	// For zero-size files or files without chunks, handle inline content
	// This includes encrypted inline files that need decryption/re-encryption
	if entry.Attributes.FileSize == 0 || len(entry.GetChunks()) == 0 {
		dstEntry.Chunks = nil

		// Handle inline encrypted content - fixes GitHub #7562
		if len(entry.Content) > 0 {
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
	} else {
		// Use unified copy strategy approach
		dstChunks, dstMetadata, copyErr := s3a.executeUnifiedCopyStrategy(entry, r, dstBucket, srcObject, dstObject)
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
	}

	// Check if destination bucket has versioning enabled
	// Only create versions if versioning is explicitly "Enabled", not "Suspended" or unconfigured
	dstVersioningState, err := s3a.getVersioningState(dstBucket)
	if err != nil {
		glog.Errorf("Error checking versioning state for destination bucket %s: %v", dstBucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var dstVersionId string
	var etag string

	if shouldCreateVersionForCopy(dstVersioningState) {
		// For versioned destination, create a new version using appropriate format
		dstVersionId = s3a.generateVersionIdForObject(dstBucket, dstObject)
		glog.V(2).Infof("CopyObjectHandler: creating version %s for destination %s/%s", dstVersionId, dstBucket, dstObject)

		// Add version metadata to the entry
		if dstEntry.Extended == nil {
			dstEntry.Extended = make(map[string][]byte)
		}
		dstEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(dstVersionId)

		// Calculate ETag for versioning
		filerEntry := &filer.Entry{
			FullPath: util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, dstBucket, dstObject)),
			Attr: filer.Attr{
				FileSize: dstEntry.Attributes.FileSize,
				Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
				Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
				Mime:     dstEntry.Attributes.Mime,
			},
			Chunks: dstEntry.Chunks,
		}
		etagRaw := rawETag(filer.ETagEntry(filerEntry))
		dstEntry.Extended[s3_constants.ExtETagKey] = []byte(etagRaw)
		etag = quoteETag(etagRaw)

		// Create version file
		versionFileName := s3a.getVersionFileName(dstVersionId)
		versionObjectPath := dstObject + ".versions/" + versionFileName
		bucketDir := s3a.option.BucketsPath + "/" + dstBucket

		if err := s3a.mkFile(bucketDir, versionObjectPath, dstEntry.Chunks, func(entry *filer_pb.Entry) {
			entry.Attributes = dstEntry.Attributes
			entry.Extended = dstEntry.Extended
		}); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		// Update the .versions directory metadata
		// Pass dstEntry to cache its metadata for single-scan list efficiency
		err = s3a.updateLatestVersionInDirectory(dstBucket, dstObject, dstVersionId, versionFileName, dstEntry)
		if err != nil {
			glog.Errorf("CopyObjectHandler: failed to update latest version in directory: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		// Set version ID in response header
		w.Header().Set("x-amz-version-id", dstVersionId)
	} else {
		// For non-versioned destination, use regular copy
		// Remove any versioning-related metadata from source that shouldn't carry over
		cleanupVersioningMetadata(dstEntry.Extended)

		// Calculate ETag before creating the file so we can store it in Extended metadata
		// Use the Md5 attribute from source if available for consistent ETag calculation
		filerEntry := &filer.Entry{
			FullPath: util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, dstBucket, dstObject)),
			Attr: filer.Attr{
				FileSize: dstEntry.Attributes.FileSize,
				Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
				Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
				Mime:     dstEntry.Attributes.Mime,
				Md5:      dstEntry.Attributes.Md5, // Use copied Md5 for correct ETag
			},
			Chunks: dstEntry.Chunks,
		}
		etagRaw := rawETag(filer.ETagEntry(filerEntry))
		dstEntry.Extended[s3_constants.ExtETagKey] = []byte(etagRaw)
		etag = quoteETag(etagRaw)

		dstPath := util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, dstBucket, dstObject))
		dstDir, dstName := dstPath.DirAndName()

		// Check if destination exists and remove it first (S3 copy overwrites)
		if exists, _ := s3a.exists(dstDir, dstName, false); exists {
			if err := s3a.rm(dstDir, dstName, false, false); err != nil {
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
		}

		// Create the new file
		if err := s3a.mkFile(dstDir, dstName, dstEntry.Chunks, func(entry *filer_pb.Entry) {
			entry.Attributes = dstEntry.Attributes
			entry.Extended = dstEntry.Extended
		}); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}

	setEtag(w, etag)

	response := CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, r, response)

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
				if unescaped, err := url.QueryUnescape(rawPathNoQuery); err == nil {
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
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

func (s3a *S3ApiServer) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingRESTMPUapi.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	rawCopySource := r.Header.Get("X-Amz-Copy-Source")

	glog.V(4).Infof("CopyObjectPart: Raw copy source header=%q", rawCopySource)

	// Try URL unescaping - AWS SDK sends URL-encoded copy sources
	cpSrcPath, err := url.QueryUnescape(rawCopySource)
	if err != nil {
		// If unescaping fails, log and use original
		glog.V(4).Infof("CopyObjectPart: Failed to unescape copy source %q: %v, using as-is", rawCopySource, err)
		cpSrcPath = rawCopySource
	}

	srcBucket, srcObject, srcVersionId := pathToBucketObjectAndVersion(rawCopySource, cpSrcPath)

	glog.V(4).Infof("CopyObjectPart: Parsed srcBucket=%q, srcObject=%q, srcVersionId=%q",
		srcBucket, srcObject, srcVersionId)

	// If source object is empty or bucket is empty, reply back invalid copy source.
	// Note: srcObject can be "/" for root-level objects, but empty string means parsing failed
	if srcObject == "" || srcBucket == "" {
		glog.Errorf("CopyObjectPart: Invalid copy source - srcBucket=%q, srcObject=%q (original header: %q)",
			srcBucket, srcObject, r.Header.Get("X-Amz-Copy-Source"))
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
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

	// Get detailed versioning state for source bucket
	srcVersioningState, err := s3a.getVersioningState(srcBucket)
	if err != nil {
		glog.Errorf("Error checking versioning state for source bucket %s: %v", srcBucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
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
		srcPath := util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, srcBucket, srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
		if err != nil {
			// If regular file doesn't exist, try latest version as fallback
			glog.V(2).Infof("CopyObjectPart: regular file not found for suspended versioning, trying latest version")
			entry, err = s3a.getLatestObjectVersion(srcBucket, srcObject)
		}
	} else {
		// No versioning configured - use regular retrieval
		srcPath := util.FullPath(fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, srcBucket, srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
	}

	if err != nil || entry.IsDirectory {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
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

	// Create new entry for the part
	// Calculate part size, avoiding underflow for invalid ranges
	partSize := uint64(0)
	if endOffset >= startOffset {
		partSize = uint64(endOffset - startOffset + 1)
	}

	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: partSize,
			Mtime:    time.Now().Unix(),
			Crtime:   time.Now().Unix(),
			Mime:     entry.Attributes.Mime,
		},
		Extended: make(map[string][]byte),
	}

	// Handle zero-size files or empty ranges
	if entry.Attributes.FileSize == 0 || endOffset < startOffset {
		// For zero-size files or invalid ranges, create an empty part with size 0
		dstEntry.Attributes.FileSize = 0
		dstEntry.Chunks = nil
	} else {
		// Copy chunks that overlap with the range
		dstChunks, err := s3a.copyChunksForRange(entry, startOffset, endOffset, r.URL.Path)
		if err != nil {
			glog.Errorf("CopyObjectPartHandler copy chunks error: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		dstEntry.Chunks = dstChunks
	}

	// Save the part entry to the multipart uploads folder
	uploadDir := s3a.genUploadsFolder(dstBucket) + "/" + uploadID
	partName := fmt.Sprintf("%04d_%s.part", partID, "copy")

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
	partPath := util.FullPath(uploadDir + "/" + partName)
	filerEntry := &filer.Entry{
		FullPath: partPath,
		Attr: filer.Attr{
			FileSize: dstEntry.Attributes.FileSize,
			Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
			Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
			Mime:     dstEntry.Attributes.Mime,
		},
		Chunks: dstEntry.Chunks,
	}

	etag := quoteETag(filer.ETagEntry(filerEntry))
	setEtag(w, etag)

	response := CopyPartResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, r, response)
}

func replaceDirective(reqHeader http.Header) (replaceMeta, replaceTagging bool) {
	return reqHeader.Get(s3_constants.AmzUserMetaDirective) == DirectiveReplace, reqHeader.Get(s3_constants.AmzObjectTaggingDirective) == DirectiveReplace
}

func processMetadata(reqHeader, existing http.Header, replaceMeta, replaceTagging bool, getTags func(parentDirectoryPath string, entryName string) (tags map[string]string, err error), dir, name string) (err error) {
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) == 0 {
		if sc := existing.Get(s3_constants.AmzStorageClass); len(sc) > 0 {
			reqHeader.Set(s3_constants.AmzStorageClass, sc)
		}
	}

	if !replaceMeta {
		for header := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
				delete(reqHeader, header)
			}
		}
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				reqHeader[k] = v
			}
		}
	}

	if !replaceTagging {
		for header, _ := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzObjectTagging) {
				delete(reqHeader, header)
			}
		}

		found := false
		for k, _ := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTaggingPrefix) {
				found = true
				break
			}
		}

		if found {
			tags, err := getTags(dir, name)
			if err != nil {
				return err
			}

			var tagArr []string
			for k, v := range tags {
				tagArr = append(tagArr, fmt.Sprintf("%s=%s", k, v))
			}
			tagStr := strutil.JoinFields(tagArr, "&")
			reqHeader.Set(s3_constants.AmzObjectTagging, tagStr)
		}
	}
	return
}

func processMetadataBytes(reqHeader http.Header, existing map[string][]byte, replaceMeta, replaceTagging bool) (metadata map[string][]byte, err error) {
	metadata = make(map[string][]byte)

	if sc := existing[s3_constants.AmzStorageClass]; len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = sc
	}
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	// Handle SSE-KMS headers - these are always processed from request headers if present
	if sseAlgorithm := reqHeader.Get(s3_constants.AmzServerSideEncryption); sseAlgorithm == "aws:kms" {
		metadata[s3_constants.AmzServerSideEncryption] = []byte(sseAlgorithm)

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
	} else {
		// If not explicitly setting SSE-KMS, preserve existing SSE headers from source
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
	} else {
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
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTagging) {
				metadata[k] = v
			}
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

// copySingleChunk copies a single chunk from source to destination
func (s3a *S3ApiServer) copySingleChunk(chunk *filer_pb.FileChunk, dstPath string) (*filer_pb.FileChunk, error) {
	// Create destination chunk
	dstChunk := s3a.createDestinationChunk(chunk, chunk.Offset, chunk.Size)

	// Prepare chunk copy (assign new volume and get source URL)
	fileId := chunk.GetFileIdString()
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Download and upload the chunk
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
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath)
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
func (s3a *S3ApiServer) assignNewVolume(dstPath string) (*filer_pb.AssignVolumeResponse, error) {
	var assignResult *filer_pb.AssignVolumeResponse
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: "",
			Collection:  "",
			DiskType:    "",
			DataCenter:  s3a.option.DataCenter,
			Path:        dstPath,
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

// min returns the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two int64 values
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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
