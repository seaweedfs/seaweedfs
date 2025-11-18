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

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {

	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject, srcVersionId := pathToBucketObjectAndVersion(cpSrcPath)

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
		fullPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
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
		writeSuccessResponseXML(w, r, CopyObjectResult{
			ETag:         filer.ETag(entry),
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
		srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
		if err != nil {
			// If regular file doesn't exist, try latest version as fallback
			glog.V(2).Infof("CopyObject: regular file not found for suspended versioning, trying latest version")
			entry, err = s3a.getLatestObjectVersion(srcBucket, srcObject)
		}
	} else {
		// No versioning configured - use regular retrieval
		srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
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
		},
		Extended: make(map[string][]byte),
	}

	// Copy extended attributes from source, filtering out conflicting encryption metadata
	for k, v := range entry.Extended {
		// Skip encryption-specific headers that might conflict with destination encryption type
		skipHeader := false

		// If we're doing cross-encryption, skip conflicting headers
		if len(entry.GetChunks()) > 0 {
			// Detect source and destination encryption types
			srcHasSSEC := IsSSECEncrypted(entry.Extended)
			srcHasSSEKMS := IsSSEKMSEncrypted(entry.Extended)
			srcHasSSES3 := IsSSES3EncryptedInternal(entry.Extended)
			dstWantsSSEC := IsSSECRequest(r)
			dstWantsSSEKMS := IsSSEKMSRequest(r)
			dstWantsSSES3 := IsSSES3RequestInternal(r)

			// Use helper function to determine if header should be skipped
			skipHeader = shouldSkipEncryptionHeader(k,
				srcHasSSEC, srcHasSSEKMS, srcHasSSES3,
				dstWantsSSEC, dstWantsSSEKMS, dstWantsSSES3)
		}

		if !skipHeader {
			dstEntry.Extended[k] = v
		}
	}

	// Process metadata and tags and apply to destination
	processedMetadata, tagErr := processMetadataBytes(r.Header, entry.Extended, replaceMeta, replaceTagging)
	if tagErr != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	// Apply processed metadata to destination entry
	for k, v := range processedMetadata {
		dstEntry.Extended[k] = v
	}

	// For zero-size files or files without chunks, use the original approach
	if entry.Attributes.FileSize == 0 || len(entry.GetChunks()) == 0 {
		// Just copy the entry structure without chunks for zero-size files
		dstEntry.Chunks = nil
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

	// Check if destination bucket has versioning configured
	dstVersioningConfigured, err := s3a.isVersioningConfigured(dstBucket)
	if err != nil {
		glog.Errorf("Error checking versioning status for destination bucket %s: %v", dstBucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var dstVersionId string
	var etag string

	if dstVersioningConfigured {
		// For versioned destination, create a new version
		dstVersionId = generateVersionId()
		glog.V(2).Infof("CopyObjectHandler: creating version %s for destination %s/%s", dstVersionId, dstBucket, dstObject)

		// Add version metadata to the entry
		if dstEntry.Extended == nil {
			dstEntry.Extended = make(map[string][]byte)
		}
		dstEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(dstVersionId)

		// Calculate ETag for versioning
		filerEntry := &filer.Entry{
			FullPath: util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject)),
			Attr: filer.Attr{
				FileSize: dstEntry.Attributes.FileSize,
				Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
				Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
				Mime:     dstEntry.Attributes.Mime,
			},
			Chunks: dstEntry.Chunks,
		}
		etag = filer.ETagEntry(filerEntry)
		if !strings.HasPrefix(etag, "\"") {
			etag = "\"" + etag + "\""
		}
		dstEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

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
		err = s3a.updateLatestVersionInDirectory(dstBucket, dstObject, dstVersionId, versionFileName)
		if err != nil {
			glog.Errorf("CopyObjectHandler: failed to update latest version in directory: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		// Set version ID in response header
		w.Header().Set("x-amz-version-id", dstVersionId)
	} else {
		// For non-versioned destination, use regular copy
		dstPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
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

		// Calculate ETag
		filerEntry := &filer.Entry{
			FullPath: dstPath,
			Attr: filer.Attr{
				FileSize: dstEntry.Attributes.FileSize,
				Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
				Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
				Mime:     dstEntry.Attributes.Mime,
			},
			Chunks: dstEntry.Chunks,
		}
		etag = filer.ETagEntry(filerEntry)
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
		object = "/" + parts[1]
		return bucket, object
	} else if len(parts) == 1 && parts[0] != "" {
		// Only bucket provided, no object
		return parts[0], ""
	}
	// Empty path
	return "", ""
}

func pathToBucketObjectAndVersion(path string) (bucket, object, versionId string) {
	// Parse versionId from query string if present ONLY at path boundaries
	// Format: /bucket/object?versionId=version-id
	// Must ensure we're not matching "?versionId=" that's part of the object name itself

	// Look for ?versionId= that comes after the bucket/object path
	// The key insight: a real query parameter must either be at the end or followed by &
	if idx := strings.Index(path, "?versionId="); idx != -1 {
		// Extract everything after "?versionId="
		afterMarker := path[idx+len("?versionId="):]
		// Check if this looks like a real query parameter (ends string or has &)
		endIdx := strings.Index(afterMarker, "&")
		if endIdx == -1 {
			// versionId goes to end of string
			versionId = afterMarker
			path = path[:idx]
		} else {
			// versionId ends at &
			versionId = afterMarker[:endIdx]
			path = path[:idx]
		}
	}

	bucket, object = pathToBucketAndObject(path)
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
	cpSrcPath := r.Header.Get("X-Amz-Copy-Source")

	glog.V(2).Infof("CopyObjectPart: Raw copy source header=%q (len=%d)", cpSrcPath, len(cpSrcPath))

	// Try URL unescaping - AWS SDK sends URL-encoded copy sources
	unescapedPath, err := url.QueryUnescape(cpSrcPath)
	if err != nil {
		// If unescaping fails, log and use original
		glog.V(2).Infof("CopyObjectPart: Failed to unescape copy source %q: %v, using as-is", cpSrcPath, err)
		unescapedPath = cpSrcPath
	}
	cpSrcPath = unescapedPath

	glog.V(2).Infof("CopyObjectPart: After unescape=%q (len=%d, bytes=%v)", cpSrcPath, len(cpSrcPath), []byte(cpSrcPath))

	srcBucket, srcObject, srcVersionId := pathToBucketObjectAndVersion(cpSrcPath)

	glog.V(2).Infof("CopyObjectPart: Parsed srcBucket=%q (len=%d), srcObject=%q (len=%d), srcVersionId=%q",
		srcBucket, len(srcBucket), srcObject, len(srcObject), srcVersionId)

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
		srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
		dir, name := srcPath.DirAndName()
		entry, err = s3a.getEntry(dir, name)
		if err != nil {
			// If regular file doesn't exist, try latest version as fallback
			glog.V(2).Infof("CopyObjectPart: regular file not found for suspended versioning, trying latest version")
			entry, err = s3a.getLatestObjectVersion(srcBucket, srcObject)
		}
	} else {
		// No versioning configured - use regular retrieval
		srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
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
	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: uint64(endOffset - startOffset + 1),
			Mtime:    time.Now().Unix(),
			Crtime:   time.Now().Unix(),
			Mime:     entry.Attributes.Mime,
		},
		Extended: make(map[string][]byte),
	}

	// Handle zero-size files or empty ranges
	if entry.Attributes.FileSize == 0 || endOffset < startOffset {
		// For zero-size files or invalid ranges, create an empty part
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

	etag := filer.ETagEntry(filerEntry)
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
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
	if err != nil {
		return nil, fmt.Errorf("download chunk data: %w", err)
	}

	if err := s3a.uploadChunkData(chunkData, assignResult); err != nil {
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
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, offsetInChunk, int64(rangeChunk.Size))
	if err != nil {
		return nil, fmt.Errorf("download chunk range data: %w", err)
	}

	if err := s3a.uploadChunkData(chunkData, assignResult); err != nil {
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

// copyChunksForRange copies chunks that overlap with the specified range
func (s3a *S3ApiServer) copyChunksForRange(entry *filer_pb.Entry, startOffset, endOffset int64, dstPath string) ([]*filer_pb.FileChunk, error) {
	var relevantChunks []*filer_pb.FileChunk

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
		}
	}

	// Copy the relevant chunks using a specialized method for range copies
	dstChunks := make([]*filer_pb.FileChunk, len(relevantChunks))
	const defaultChunkCopyConcurrency = 4
	executor := util.NewLimitedConcurrentExecutor(defaultChunkCopyConcurrency)
	errChan := make(chan error, len(relevantChunks))

	// Create a map to track original chunks for each relevant chunk
	originalChunks := make([]*filer_pb.FileChunk, len(relevantChunks))
	relevantIndex := 0
	for _, chunk := range entry.GetChunks() {
		chunkStart := chunk.Offset
		chunkEnd := chunk.Offset + int64(chunk.Size)

		// Check if chunk overlaps with the range
		if chunkStart < endOffset+1 && chunkEnd > startOffset {
			originalChunks[relevantIndex] = chunk
			relevantIndex++
		}
	}

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
	// Calculate ETag for the source entry
	srcPath := util.FullPath(fmt.Sprintf("%s/%s", r.URL.Path, entry.Name))
	filerEntry := &filer.Entry{
		FullPath: srcPath,
		Attr: filer.Attr{
			FileSize: entry.Attributes.FileSize,
			Mtime:    time.Unix(entry.Attributes.Mtime, 0),
			Crtime:   time.Unix(entry.Attributes.Crtime, 0),
			Mime:     entry.Attributes.Mime,
		},
		Chunks: entry.Chunks,
	}
	sourceETag := filer.ETagEntry(filerEntry)

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
		t, err := time.Parse(time.RFC1123, ifModifiedSince)
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
		t, err := time.Parse(time.RFC1123, ifUnmodifiedSince)
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

// createDestinationChunk creates a new chunk based on the source chunk with modified properties
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
func (s3a *S3ApiServer) prepareChunkCopy(sourceFileId, dstPath string) (*filer_pb.AssignVolumeResponse, string, error) {
	// Assign new volume
	assignResult, err := s3a.assignNewVolume(dstPath)
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
func (s3a *S3ApiServer) uploadChunkData(chunkData []byte, assignResult *filer_pb.AssignVolumeResponse) error {
	dstUrl := fmt.Sprintf("http://%s/%s", assignResult.Location.Url, assignResult.FileId)

	uploadOption := &operation.UploadOption{
		UploadUrl:         dstUrl,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               security.EncodedJwt(assignResult.Auth),
	}
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

// downloadChunkData downloads chunk data from the source URL
func (s3a *S3ApiServer) downloadChunkData(srcUrl, fileId string, offset, size int64) ([]byte, error) {
	jwt := filer.JwtForVolumeServer(fileId)
	var chunkData []byte
	shouldRetry, err := util_http.ReadUrlAsStream(context.Background(), srcUrl, jwt, nil, false, false, offset, int(size), func(data []byte) {
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

		// SSE-KMS chunk: decrypt with stored per-chunk metadata, re-encrypt with dest key
		copiedChunk, err := s3a.copyMultipartSSEKMSChunk(chunk, destKeyID, encryptionContext, bucketKeyEnabled, dstPath, bucket)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to copy SSE-KMS chunk %s: %w", chunk.GetFileIdString(), err)
		}

		dstChunks = append(dstChunks, copiedChunk)
	}

	// Create destination metadata for SSE-KMS
	dstMetadata := make(map[string][]byte)
	if destKeyID != "" {
		// Store SSE-KMS metadata for single-part compatibility
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

	return dstChunks, dstMetadata, nil
}

// copyMultipartSSEKMSChunk copies a single SSE-KMS chunk from a multipart object (unified with SSE-C approach)
func (s3a *S3ApiServer) copyMultipartSSEKMSChunk(chunk *filer_pb.FileChunk, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) (*filer_pb.FileChunk, error) {
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

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
	if err != nil {
		return nil, fmt.Errorf("download encrypted chunk data: %w", err)
	}

	var finalData []byte

	// Decrypt source data using stored SSE-KMS metadata (same pattern as SSE-C)
	if len(chunk.GetSseMetadata()) == 0 {
		return nil, fmt.Errorf("SSE-KMS chunk missing per-chunk metadata")
	}

	// Deserialize the SSE-KMS metadata (reusing unified metadata structure)
	sourceSSEKey, err := DeserializeSSEKMSMetadata(chunk.GetSseMetadata())
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize SSE-KMS metadata: %w", err)
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

		glog.V(4).Infof("Re-encrypted multipart SSE-KMS chunk: %d bytes → %d bytes", len(finalData)-len(reencryptedData)+len(finalData), len(finalData))
	}

	// Upload the final data
	if err := s3a.uploadChunkData(finalData, assignResult); err != nil {
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
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath)
	if err != nil {
		return nil, nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, nil, err
	}

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
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

		glog.V(4).Infof("Re-encrypted multipart SSE-C chunk: %d bytes → %d bytes", len(finalData)-len(reencryptedData)+len(finalData), len(finalData))
	}

	// Upload the final data
	if err := s3a.uploadChunkData(finalData, assignResult); err != nil {
		return nil, nil, fmt.Errorf("upload chunk data: %w", err)
	}

	// Update chunk size
	dstChunk.Size = uint64(len(finalData))

	glog.V(3).Infof("Successfully copied multipart SSE-C chunk %s → %s",
		chunk.GetFileIdString(), dstChunk.GetFileIdString())

	return dstChunk, destIV, nil
}

// copyMultipartCrossEncryption handles all cross-encryption and decrypt-only copy scenarios
// This unified function supports: SSE-C↔SSE-KMS, SSE-C→Plain, SSE-KMS→Plain
func (s3a *S3ApiServer) copyMultipartCrossEncryption(entry *filer_pb.Entry, r *http.Request, state *EncryptionState, dstBucket, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	var dstChunks []*filer_pb.FileChunk

	// Parse destination encryption parameters
	var destSSECKey *SSECustomerKey
	var destKMSKeyID string
	var destKMSEncryptionContext map[string]string
	var destKMSBucketKeyEnabled bool

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
	} else {
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
			copiedChunk, err = s3a.copyCrossEncryptionChunk(chunk, sourceSSECKey, destSSECKey, destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, dstPath, dstBucket, state)
		} else if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS {
			copiedChunk, err = s3a.copyCrossEncryptionChunk(chunk, nil, destSSECKey, destKMSKeyID, destKMSEncryptionContext, destKMSBucketKeyEnabled, dstPath, dstBucket, state)
		} else {
			// Unencrypted chunk, copy directly
			copiedChunk, err = s3a.copySingleChunk(chunk, dstPath)
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
	// For unencrypted destination, no metadata needed (dstMetadata remains empty)

	return dstChunks, dstMetadata, nil
}

// copyCrossEncryptionChunk handles copying a single chunk with cross-encryption support
func (s3a *S3ApiServer) copyCrossEncryptionChunk(chunk *filer_pb.FileChunk, sourceSSECKey *SSECustomerKey, destSSECKey *SSECustomerKey, destKMSKeyID string, destKMSEncryptionContext map[string]string, destKMSBucketKeyEnabled bool, dstPath, dstBucket string, state *EncryptionState) (*filer_pb.FileChunk, error) {
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

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
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
		previewLen := 16
		if len(finalData) < previewLen {
			previewLen = len(finalData)
		}

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
		previewLen := 16
		if len(finalData) < previewLen {
			previewLen = len(finalData)
		}

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
	}
	// For unencrypted destination, finalData remains as decrypted plaintext

	// Upload the final data
	if err := s3a.uploadChunkData(finalData, assignResult); err != nil {
		return nil, fmt.Errorf("upload chunk data: %w", err)
	}

	// Update chunk size
	dstChunk.Size = uint64(len(finalData))

	glog.V(3).Infof("Successfully copied cross-encryption chunk %s → %s",
		chunk.GetFileIdString(), dstChunk.GetFileIdString())

	return dstChunk, nil
}

// getEncryptionTypeString returns a string representation of encryption type for logging
func (s3a *S3ApiServer) getEncryptionTypeString(isSSEC, isSSEKMS, isSSES3 bool) string {
	if isSSEC {
		return s3_constants.SSETypeC
	} else if isSSEKMS {
		return s3_constants.SSETypeKMS
	} else if isSSES3 {
		return s3_constants.SSETypeS3
	}
	return "Plain"
}

// copyChunksWithSSEC handles SSE-C aware copying with smart fast/slow path selection
// Returns chunks and destination metadata that should be applied to the destination entry
func (s3a *S3ApiServer) copyChunksWithSSEC(entry *filer_pb.Entry, r *http.Request) ([]*filer_pb.FileChunk, map[string][]byte, error) {

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
		return s3a.copyMultipartSSECChunks(entry, copySourceKey, destKey, r.URL.Path)
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
		chunks, err := s3a.copyChunks(entry, r.URL.Path)
		return chunks, nil, err

	case SSECCopyStrategyDecryptEncrypt:
		// SLOW PATH: Decrypt and re-encrypt
		glog.V(2).Infof("Using slow path: decrypt/re-encrypt for %s", r.URL.Path)
		chunks, destIV, err := s3a.copyChunksWithReencryption(entry, copySourceKey, destKey, r.URL.Path)
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
	assignResult, srcUrl, err := s3a.prepareChunkCopy(fileId, dstPath)
	if err != nil {
		return nil, err
	}

	// Set file ID on destination chunk
	if err := s3a.setChunkFileId(dstChunk, assignResult); err != nil {
		return nil, err
	}

	// Download encrypted chunk data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
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

		// Update chunk size to include IV
		dstChunk.Size = uint64(len(finalData))
	}

	// Upload the processed data
	if err := s3a.uploadChunkData(finalData, assignResult); err != nil {
		return nil, fmt.Errorf("upload processed chunk data: %w", err)
	}

	return dstChunk, nil
}

// copyChunksWithSSEKMS handles SSE-KMS aware copying with smart fast/slow path selection
// Returns chunks and destination metadata like SSE-C for consistency
func (s3a *S3ApiServer) copyChunksWithSSEKMS(entry *filer_pb.Entry, r *http.Request, bucket string) ([]*filer_pb.FileChunk, map[string][]byte, error) {

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
		return s3a.copyMultipartSSEKMSChunks(entry, destKeyID, encryptionContext, bucketKeyEnabled, r.URL.Path, bucket)
	}

	// Single-part SSE-KMS object: use existing logic
	// If no SSE-KMS headers and source is not SSE-KMS encrypted, use regular copy
	if destKeyID == "" && !IsSSEKMSEncrypted(entry.Extended) {
		chunks, err := s3a.copyChunks(entry, r.URL.Path)
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

	// Determine copy strategy
	strategy, err := DetermineSSEKMSCopyStrategy(entry.Extended, destKeyID)
	if err != nil {
		return nil, nil, err
	}

	glog.V(2).Infof("SSE-KMS copy strategy for %s: %v", r.URL.Path, strategy)

	switch strategy {
	case SSEKMSCopyStrategyDirect:
		// FAST PATH: Direct chunk copy (same key or both unencrypted)
		glog.V(2).Infof("Using fast path: direct chunk copy for %s", r.URL.Path)
		chunks, err := s3a.copyChunks(entry, r.URL.Path)
		// For direct copy, generate destination metadata if we're encrypting to SSE-KMS
		var dstMetadata map[string][]byte
		if destKeyID != "" {
			dstMetadata = make(map[string][]byte)
			if encryptionContext == nil {
				encryptionContext = BuildEncryptionContext(bucket, r.URL.Path, bucketKeyEnabled)
			}
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
		return chunks, dstMetadata, err

	case SSEKMSCopyStrategyDecryptEncrypt:
		// SLOW PATH: Decrypt source and re-encrypt for destination
		glog.V(2).Infof("Using slow path: decrypt/re-encrypt for %s", r.URL.Path)
		return s3a.copyChunksWithSSEKMSReencryption(entry, destKeyID, encryptionContext, bucketKeyEnabled, r.URL.Path, bucket)

	default:
		return nil, nil, fmt.Errorf("unknown SSE-KMS copy strategy: %v", strategy)
	}
}

// copyChunksWithSSEKMSReencryption handles the slow path: decrypt source and re-encrypt for destination
// Returns chunks and destination metadata like SSE-C for consistency
func (s3a *S3ApiServer) copyChunksWithSSEKMSReencryption(entry *filer_pb.Entry, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	var dstChunks []*filer_pb.FileChunk

	// Extract and deserialize source SSE-KMS metadata
	var sourceSSEKey *SSEKMSKey
	if keyData, exists := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]; exists {
		var err error
		sourceSSEKey, err = DeserializeSSEKMSMetadata(keyData)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to deserialize source SSE-KMS metadata: %w", err)
		}
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

	// Generate destination metadata for SSE-KMS encryption (consistent with SSE-C pattern)
	dstMetadata := make(map[string][]byte)
	if destKeyID != "" {
		// Build encryption context if not provided
		if encryptionContext == nil {
			encryptionContext = BuildEncryptionContext(bucket, dstPath, bucketKeyEnabled)
		}

		// Create SSE-KMS key structure for destination metadata
		sseKey := &SSEKMSKey{
			KeyID:             destKeyID,
			EncryptionContext: encryptionContext,
			BucketKeyEnabled:  bucketKeyEnabled,
			// Note: EncryptedDataKey will be generated during actual encryption
			// IV is also generated per chunk during encryption
		}

		// Serialize SSE-KMS metadata for storage
		kmsMetadata, err := SerializeSSEKMSMetadata(sseKey)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize destination SSE-KMS metadata: %w", err)
		}

		dstMetadata[s3_constants.SeaweedFSSSEKMSKey] = kmsMetadata
		glog.V(3).Infof("Generated destination SSE-KMS metadata: keyID=%s, bucketKey=%t", destKeyID, bucketKeyEnabled)
	}

	return dstChunks, dstMetadata, nil
}

// copyChunkWithSSEKMSReencryption copies a single chunk with SSE-KMS decrypt/re-encrypt
func (s3a *S3ApiServer) copyChunkWithSSEKMSReencryption(chunk *filer_pb.FileChunk, sourceSSEKey *SSEKMSKey, destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, dstPath, bucket string) (*filer_pb.FileChunk, error) {
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

	// Download chunk data
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
	if err != nil {
		return nil, fmt.Errorf("download chunk data: %w", err)
	}

	var finalData []byte

	// Decrypt source data if it's SSE-KMS encrypted
	if sourceSSEKey != nil {
		// For SSE-KMS, the encrypted chunk data contains IV + encrypted content
		// Use the source SSE key to decrypt the chunk data
		decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(chunkData), sourceSSEKey)
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

		encryptedReader, _, err := CreateSSEKMSEncryptedReaderWithBucketKey(bytes.NewReader(finalData), destKeyID, encryptionContext, bucketKeyEnabled)
		if err != nil {
			return nil, fmt.Errorf("create SSE-KMS encrypted reader: %w", err)
		}

		reencryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			return nil, fmt.Errorf("re-encrypt chunk data: %w", err)
		}

		// Store original decrypted data size for logging
		originalSize := len(finalData)
		finalData = reencryptedData
		glog.V(4).Infof("Re-encrypted chunk data: %d bytes → %d bytes", originalSize, len(finalData))

		// Update chunk size to include IV and encryption overhead
		dstChunk.Size = uint64(len(finalData))
	}

	// Upload the processed data
	if err := s3a.uploadChunkData(finalData, assignResult); err != nil {
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
