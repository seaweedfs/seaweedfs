package s3api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// corsHeaders defines the CORS headers that need to be preserved
// Package-level constant to avoid repeated allocations
var corsHeaders = []string{
	"Access-Control-Allow-Origin",
	"Access-Control-Allow-Methods",
	"Access-Control-Allow-Headers",
	"Access-Control-Expose-Headers",
	"Access-Control-Max-Age",
	"Access-Control-Allow-Credentials",
}

// zeroBuf is a reusable buffer of zero bytes for padding operations
// Package-level to avoid per-call allocations in writeZeroBytes
var zeroBuf = make([]byte, 32*1024)

// countingWriter wraps an io.Writer to count bytes written
type countingWriter struct {
	w       io.Writer
	written int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.written += int64(n)
	return n, err
}

// adjustRangeForPart adjusts a client's Range header to absolute offsets within a part.
// Parameters:
//   - partStartOffset: the absolute start offset of the part in the object
//   - partEndOffset: the absolute end offset of the part in the object
//   - clientRangeHeader: the Range header value from the client (e.g., "bytes=0-99")
//
// Returns:
//   - adjustedStart: the adjusted absolute start offset
//   - adjustedEnd: the adjusted absolute end offset
//   - error: nil on success, error if the range is invalid
func adjustRangeForPart(partStartOffset, partEndOffset int64, clientRangeHeader string) (adjustedStart, adjustedEnd int64, err error) {
	// Validate inputs
	if partStartOffset > partEndOffset {
		return 0, 0, fmt.Errorf("invalid part boundaries: start %d > end %d", partStartOffset, partEndOffset)
	}

	// If no range header, return the full part
	if clientRangeHeader == "" || !strings.HasPrefix(clientRangeHeader, "bytes=") {
		return partStartOffset, partEndOffset, nil
	}

	// Parse client's range request (relative to the part)
	rangeSpec := clientRangeHeader[6:] // Remove "bytes=" prefix
	parts := strings.Split(rangeSpec, "-")

	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	partSize := partEndOffset - partStartOffset + 1
	var clientStart, clientEnd int64

	// Parse start offset
	if parts[0] != "" {
		clientStart, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range start: %w", err)
		}
	}

	// Parse end offset
	if parts[1] != "" {
		clientEnd, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range end: %w", err)
		}
	} else {
		// No end specified, read to end of part
		clientEnd = partSize - 1
	}

	// Handle suffix-range (e.g., "bytes=-100" means last 100 bytes)
	// When parts[0] is empty, the parsed clientEnd value represents the suffix length,
	// not the actual end position. We compute the actual start/end from the suffix length.
	if parts[0] == "" {
		suffixLength := clientEnd // clientEnd temporarily holds the suffix length
		if suffixLength > partSize {
			suffixLength = partSize
		}
		clientStart = partSize - suffixLength
		clientEnd = partSize - 1 // Now clientEnd holds the actual end position
	}

	// Validate range is within part boundaries
	if clientStart < 0 || clientStart >= partSize {
		return 0, 0, fmt.Errorf("range start %d out of bounds for part size %d", clientStart, partSize)
	}
	if clientEnd >= partSize {
		clientEnd = partSize - 1
	}
	if clientStart > clientEnd {
		return 0, 0, fmt.Errorf("range start %d > end %d", clientStart, clientEnd)
	}

	// Adjust to absolute offsets in the object
	adjustedStart = partStartOffset + clientStart
	adjustedEnd = partStartOffset + clientEnd

	return adjustedStart, adjustedEnd, nil
}

// parseAndValidateRange parses the Range header and validates it against the object size.
// It also handles SeaweedFS-specific directory object checks.
// Returns:
//   - offset: the absolute start offset in the object
//   - size: the number of bytes to read
//   - isRangeRequest: true if the client requested a range
//   - err: nil on success, StreamError on failure (wraps S3 error response)
func (s3a *S3ApiServer) parseAndValidateRange(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry, totalSize int64, bucket, object string) (offset, size int64, isRangeRequest bool, err *StreamError) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" || !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, totalSize, false, nil
	}

	rangeSpec := rangeHeader[6:]
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return 0, totalSize, false, nil
	}

	// S3 semantics: directories (without trailing "/") should return 404
	if entry.IsDirectory {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return 0, 0, false, newStreamErrorWithResponse(fmt.Errorf("directory object %s/%s cannot be retrieved", bucket, object))
	}

	var startOffset, endOffset int64
	if parts[0] == "" && parts[1] != "" {
		// Suffix range: bytes=-N (last N bytes)
		if suffixLen, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
			// RFC 7233: suffix range on empty object or zero-length suffix is unsatisfiable
			if totalSize == 0 || suffixLen <= 0 {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
				return 0, 0, false, newStreamErrorWithResponse(fmt.Errorf("invalid suffix range for empty object"))
			}
			if suffixLen > totalSize {
				suffixLen = totalSize
			}
			startOffset = totalSize - suffixLen
			endOffset = totalSize - 1
		} else {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return 0, 0, false, newStreamErrorWithResponse(fmt.Errorf("invalid suffix range"))
		}
	} else {
		// Regular range or open-ended range
		startOffset = 0
		endOffset = totalSize - 1

		if parts[0] != "" {
			if parsed, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				startOffset = parsed
			}
		}
		if parts[1] != "" {
			if parsed, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				endOffset = parsed
			}
		}

		// Special case: range requests on empty files should return 416
		if totalSize == 0 {
			w.Header().Set("Content-Range", "bytes */0")
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return 0, 0, false, newStreamErrorWithResponse(fmt.Errorf("range request on empty file %s/%s", bucket, object))
		}

		// Validate range
		if startOffset < 0 || startOffset >= totalSize {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return 0, 0, false, newStreamErrorWithResponse(fmt.Errorf("invalid range start: %d >= %d, range: %s", startOffset, totalSize, rangeHeader))
		}

		if endOffset >= totalSize {
			endOffset = totalSize - 1
		}

		if endOffset < startOffset {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return 0, 0, false, newStreamErrorWithResponse(fmt.Errorf("invalid range: end before start"))
		}
	}

	return startOffset, endOffset - startOffset + 1, true, nil
}

// StreamError is returned when streaming functions encounter errors.
// It tracks whether an HTTP response has already been written to prevent
// double WriteHeader calls that would create malformed S3 error responses.
type StreamError struct {
	// Err is the underlying error
	Err error
	// ResponseWritten indicates if HTTP headers/status have been written to ResponseWriter
	ResponseWritten bool
}

func (e *StreamError) Error() string {
	return e.Err.Error()
}

func (e *StreamError) Unwrap() error {
	return e.Err
}

// newStreamError creates a StreamError for cases where response hasn't been written yet
func newStreamError(err error) *StreamError {
	return &StreamError{Err: err, ResponseWritten: false}
}

// newStreamErrorWithResponse creates a StreamError for cases where response was already written
func newStreamErrorWithResponse(err error) *StreamError {
	return &StreamError{Err: err, ResponseWritten: true}
}

func mimeDetect(r *http.Request, dataReader io.Reader) io.ReadCloser {
	mimeBuffer := make([]byte, 512)
	size, _ := dataReader.Read(mimeBuffer)
	if size > 0 {
		r.Header.Set("Content-Type", http.DetectContentType(mimeBuffer[:size]))
		return io.NopCloser(io.MultiReader(bytes.NewReader(mimeBuffer[:size]), dataReader))
	}
	return io.NopCloser(dataReader)
}

func urlEscapeObject(object string) string {
	normalized := s3_constants.NormalizeObjectKey(object)
	// Ensure leading slash for filer paths
	if normalized != "" && !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	return urlPathEscape(normalized)
}

func entryUrlEncode(dir string, entry string, encodingTypeUrl bool) (dirName string, entryName string, prefix string) {
	if !encodingTypeUrl {
		return dir, entry, entry
	}
	return urlPathEscape(dir), url.QueryEscape(entry), urlPathEscape(entry)
}

func urlPathEscape(object string) string {
	var escapedParts []string
	for _, part := range strings.Split(object, "/") {
		escapedParts = append(escapedParts, strings.ReplaceAll(url.PathEscape(part), "+", "%2B"))
	}
	return strings.Join(escapedParts, "/")
}

func removeDuplicateSlashes(object string) string {
	result := strings.Builder{}
	result.Grow(len(object))

	isLastSlash := false
	for _, r := range object {
		switch r {
		case '/':
			if !isLastSlash {
				result.WriteRune(r)
			}
			isLastSlash = true
		default:
			result.WriteRune(r)
			isLastSlash = false
		}
	}
	return result.String()
}

// hasChildren checks if a path has any child objects (is a directory with contents)
//
// This helper function is used to distinguish implicit directories from regular files or empty directories.
// An implicit directory is one that exists only because it has children, not because it was explicitly created.
//
// Implementation:
//   - Lists the directory with Limit=1 to check for at least one child
//   - Returns true if any child exists, false otherwise
//   - Efficient: only fetches one entry to minimize overhead
//
// Used by HeadObjectHandler to implement AWS S3-compatible implicit directory behavior:
//   - If a 0-byte object or directory has children → it's an implicit directory → HEAD returns 404
//   - If a 0-byte object or directory has no children → it's empty → HEAD returns 200
//
// Examples:
//
//	hasChildren("bucket", "dataset") where "dataset/file.txt" exists → true
//	hasChildren("bucket", "empty-dir") where no children exist → false
//
// Performance: ~1-5ms per call (one gRPC LIST request with Limit=1)
func (s3a *S3ApiServer) hasChildren(bucket, prefix string) bool {
	// Clean up prefix: remove leading slashes
	cleanPrefix := strings.TrimPrefix(prefix, "/")

	// The directory to list is bucketDir + cleanPrefix
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	fullPath := bucketDir + "/" + cleanPrefix

	// Try to list one child object in the directory
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory:          fullPath,
			Limit:              1,
			InclusiveStartFrom: true,
		}

		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return err
		}

		// Check if we got at least one entry
		_, err = stream.Recv()
		if err == io.EOF {
			return io.EOF // No children
		}
		if err != nil {
			return err
		}
		return nil
	})

	// If we got an entry (not EOF), then it has children
	return err == nil
}

// checkDirectoryObject checks if the object is a directory object (ends with "/") and if it exists
// Returns: (entry, isDirectoryObject, error)
// - entry: the directory entry if found and is a directory
// - isDirectoryObject: true if the request was for a directory object (ends with "/")
// - error: any error encountered while checking
func (s3a *S3ApiServer) checkDirectoryObject(bucket, object string) (*filer_pb.Entry, bool, error) {
	if !strings.HasSuffix(object, "/") {
		return nil, false, nil // Not a directory object
	}

	bucketDir := s3a.option.BucketsPath + "/" + bucket
	cleanObject := strings.TrimSuffix(object, "/")

	if cleanObject == "" {
		return nil, true, nil // Root level directory object, but we don't handle it
	}

	// Check if directory exists
	dirEntry, err := s3a.getEntry(bucketDir, cleanObject)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil, true, nil // Directory object requested but doesn't exist
		}
		return nil, true, err // Other errors should be propagated
	}

	if !dirEntry.IsDirectory {
		return nil, true, nil // Exists but not a directory
	}

	return dirEntry, true, nil
}

// resolveObjectEntry resolves the object entry for conditional checks,
// handling versioned buckets by resolving the latest version.
func (s3a *S3ApiServer) resolveObjectEntry(bucket, object string) (*filer_pb.Entry, error) {
	// Check if versioning is configured
	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.Errorf("resolveObjectEntry: error checking versioning config for %s: %v", bucket, err)
		return nil, err
	}

	if versioningConfigured {
		// For versioned buckets, we must use getLatestObjectVersion to correctly
		// find the latest versioned object (in .versions/) or null version.
		// Standard getEntry would fail to find objects moved to .versions/.
		// Use 1 retry (fast path) for conditional checks to avoid backoff latency.
		return s3a.doGetLatestObjectVersion(bucket, object, 1)
	}

	// For non-versioned buckets, verify directly
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	return s3a.getEntry(bucketDir, object)
}

// serveDirectoryContent serves the content of a directory object directly
func (s3a *S3ApiServer) serveDirectoryContent(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry) {
	// Defensive nil checks - entry and attributes should never be nil, but guard against it
	if entry == nil || entry.Attributes == nil {
		glog.Errorf("serveDirectoryContent: entry or attributes is nil")
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Set content type - use stored MIME type or default
	contentType := entry.Attributes.Mime
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)

	// Set content length - use FileSize for accuracy, especially for large files
	contentLength := int64(entry.Attributes.FileSize)
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))

	// Set last modified
	w.Header().Set("Last-Modified", time.Unix(entry.Attributes.Mtime, 0).UTC().Format(http.TimeFormat))

	// Set ETag
	w.Header().Set("ETag", "\""+filer.ETag(entry)+"\"")

	// For HEAD requests, don't write body
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Write content
	w.WriteHeader(http.StatusOK)
	if len(entry.Content) > 0 {
		if _, err := w.Write(entry.Content); err != nil {
			glog.Errorf("serveDirectoryContent: failed to write response: %v", err)
		}
	}
}

// handleDirectoryObjectRequest is a helper function that handles directory object requests
// for both GET and HEAD operations, eliminating code duplication
func (s3a *S3ApiServer) handleDirectoryObjectRequest(w http.ResponseWriter, r *http.Request, bucket, object, handlerName string) bool {
	// Check if this is a directory object and handle it directly
	if dirEntry, isDirectoryObject, err := s3a.checkDirectoryObject(bucket, object); err != nil {
		glog.Errorf("%s: error checking directory object %s/%s: %v", handlerName, bucket, object, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return true // Request was handled (with error)
	} else if dirEntry != nil {
		glog.V(2).Infof("%s: directory object %s/%s found, serving content", handlerName, bucket, object)
		s3a.serveDirectoryContent(w, r, dirEntry)
		return true // Request was handled successfully
	} else if isDirectoryObject {
		// Directory object but doesn't exist
		glog.V(2).Infof("%s: directory object %s/%s not found", handlerName, bucket, object)
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return true // Request was handled (with not found)
	}

	return false // Not a directory object, continue with normal processing
}

func newListEntry(entry *filer_pb.Entry, key string, dir string, name string, bucketPrefix string, fetchOwner bool, isDirectory bool, encodingTypeUrl bool, iam AccountManager) (listEntry ListEntry) {
	storageClass := "STANDARD"
	if v, ok := entry.Extended[s3_constants.AmzStorageClass]; ok {
		storageClass = string(v)
	}
	keyFormat := "%s/%s"
	if isDirectory {
		keyFormat += "/"
	}
	if key == "" {
		key = fmt.Sprintf(keyFormat, dir, name)[len(bucketPrefix):]
	}
	if encodingTypeUrl {
		key = urlPathEscape(key)
	}
	// Determine ETag: prioritize ExtETagKey for versioned objects (supports multipart ETags),
	// then fall back to filer.ETag() which uses Md5 attribute or calculates from chunks
	var etag string
	if entry.Extended != nil {
		if etagBytes, hasETag := entry.Extended[s3_constants.ExtETagKey]; hasETag {
			etag = string(etagBytes)
		}
	}
	if etag == "" {
		etag = "\"" + filer.ETag(entry) + "\""
	}
	listEntry = ListEntry{
		Key:          key,
		LastModified: time.Unix(entry.Attributes.Mtime, 0).UTC(),
		ETag:         etag,
		Size:         int64(filer.FileSize(entry)),
		StorageClass: StorageClass(storageClass),
	}
	if fetchOwner {
		// Extract owner from S3 metadata (Extended attributes) instead of file system attributes
		var ownerID, displayName string
		if entry.Extended != nil {
			if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
				ownerID = string(ownerBytes)
			}
		}

		// Fallback to anonymous if no S3 owner found
		if ownerID == "" {
			ownerID = s3_constants.AccountAnonymousId
			displayName = "anonymous"
		} else {
			// Get the proper display name from IAM system
			displayName = iam.GetAccountNameById(ownerID)
			// Fallback to ownerID if no display name found
			if displayName == "" {
				displayName = ownerID
			}
		}

		listEntry.Owner = &CanonicalUser{
			ID:          ownerID,
			DisplayName: displayName,
		}
	}
	return listEntry
}

func (s3a *S3ApiServer) toFilerPath(bucket, object string) string {
	// Returns the raw file path - no URL escaping needed
	// The path is used directly, not embedded in a URL
	object = s3_constants.NormalizeObjectKey(object)
	return fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, object)
}

// hasConditionalHeaders checks if the request has any conditional headers
// This is a lightweight check to avoid unnecessary function calls
func (s3a *S3ApiServer) hasConditionalHeaders(r *http.Request) bool {
	return r.Header.Get(s3_constants.IfMatch) != "" ||
		r.Header.Get(s3_constants.IfNoneMatch) != "" ||
		r.Header.Get(s3_constants.IfModifiedSince) != "" ||
		r.Header.Get(s3_constants.IfUnmodifiedSince) != ""
}

// processConditionalHeaders checks conditional headers and writes an error response if a condition fails.
// It returns the result of the check and a boolean indicating if the request has been handled.
func (s3a *S3ApiServer) processConditionalHeaders(w http.ResponseWriter, r *http.Request, bucket, object, handlerName string) (ConditionalHeaderResult, bool) {
	if !s3a.hasConditionalHeaders(r) {
		return ConditionalHeaderResult{ErrorCode: s3err.ErrNone}, false
	}

	result := s3a.checkConditionalHeadersForReads(r, bucket, object)
	if result.ErrorCode != s3err.ErrNone {
		glog.V(3).Infof("%s: Conditional header check failed for %s/%s with error %v", handlerName, bucket, object, result.ErrorCode)

		// For 304 Not Modified responses, include the ETag header
		if result.ErrorCode == s3err.ErrNotModified && result.ETag != "" {
			w.Header().Set("ETag", result.ETag)
		}

		s3err.WriteErrorResponse(w, r, result.ErrorCode)
		return result, true // request handled
	}
	return result, false // request not handled
}

func (s3a *S3ApiServer) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectHandler %s %s", bucket, object)

	// Check for SOSAPI virtual objects (system.xml, capacity.xml)
	// These are dynamically generated and don't exist on disk
	if s3a.handleSOSAPIGetObject(w, r, bucket, object) {
		return // SOSAPI request was handled
	}

	// TTFB Profiling: Track all stages until first byte
	tStart := time.Now()
	var (
		conditionalHeadersTime time.Duration
		versioningCheckTime    time.Duration
		entryFetchTime         time.Duration
		streamTime             time.Duration
	)
	defer func() {
		totalTime := time.Since(tStart)
		glog.V(4).Infof("GET TTFB PROFILE %s/%s: total=%v | conditional=%v, versioning=%v, entryFetch=%v, stream=%v",
			bucket, object, totalTime, conditionalHeadersTime, versioningCheckTime, entryFetchTime, streamTime)
	}()

	// Handle directory objects with shared logic
	if s3a.handleDirectoryObjectRequest(w, r, bucket, object, "GetObjectHandler") {
		return // Directory object request was handled
	}

	// Check conditional headers and handle early return if conditions fail
	tConditional := time.Now()
	result, handled := s3a.processConditionalHeaders(w, r, bucket, object, "GetObjectHandler")
	conditionalHeadersTime = time.Since(tConditional)
	if handled {
		return
	}

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	var (
		entry                *filer_pb.Entry // Declare entry at function scope for SSE processing
		versioningConfigured bool
		err                  error
	)

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	tVersioning := time.Now()
	// Note: We need to check this even if versionId is empty, because versioned buckets
	// handle even "get latest version" requests differently (through .versions directory)
	versioningConfigured, err = s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	glog.V(3).Infof("GetObject: bucket %s, object %s, versioningConfigured=%v, versionId=%s", bucket, object, versioningConfigured, versionId)

	if versioningConfigured {
		// Handle versioned GET - check if specific version requested
		var targetVersionId string

		if versionId != "" {
			// Request for specific version - must look in .versions directory
			glog.V(3).Infof("GetObject: requesting specific version %s for %s/%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
			if err != nil {
				glog.Errorf("Failed to get specific version %s: %v", versionId, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
			targetVersionId = versionId
		} else {
			// Request for latest version - OPTIMIZATION:
			// Check if .versions/ directory exists quickly (no retries) to decide path
			// - If .versions/ exists: real versions available, use getLatestObjectVersion
			// - If .versions/ doesn't exist (ErrNotFound): only null version at regular path, use it directly
			// - If transient error: fall back to getLatestObjectVersion which has retry logic
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			normalizedObject := s3_constants.NormalizeObjectKey(object)
			versionsDir := normalizedObject + s3_constants.VersionsFolder

			// Quick check (no retries) for .versions/ directory
			versionsEntry, versionsErr := s3a.getEntry(bucketDir, versionsDir)

			if versionsErr == nil && versionsEntry != nil {
				// .versions/ exists, meaning real versions are stored there
				// Use getLatestObjectVersion which will properly find the newest version
				entry, err = s3a.getLatestObjectVersion(bucket, object)
				if err != nil {
					glog.Errorf("GetObject: Failed to get latest version for %s/%s: %v", bucket, object, err)
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			} else if errors.Is(versionsErr, filer_pb.ErrNotFound) {
				// .versions/ doesn't exist (confirmed not found), check regular path for null version
				regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
				if regularErr == nil && regularEntry != nil {
					// Found object at regular path - this is the null version
					entry = regularEntry
					targetVersionId = "null"
				} else {
					// No object at regular path either - object doesn't exist
					glog.V(3).Infof("GetObject: object not found at regular path or .versions for %s/%s", bucket, object)
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			} else {
				// Transient error checking .versions/, fall back to getLatestObjectVersion with retries
				glog.V(2).Infof("GetObject: transient error checking .versions for %s/%s: %v, falling back to getLatestObjectVersion", bucket, object, versionsErr)
				entry, err = s3a.getLatestObjectVersion(bucket, object)
				if err != nil {
					glog.Errorf("GetObject: Failed to get latest version for %s/%s: %v", bucket, object, err)
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			}
			// Extract version ID if not already set
			if targetVersionId == "" {
				if entry.Extended != nil {
					if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
						targetVersionId = string(versionIdBytes)
					}
				}
				// If no version ID found in entry, this is a pre-versioning object
				if targetVersionId == "" {
					targetVersionId = "null"
				}
			}
		}

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}

		// For versioned objects, log the target version
		if targetVersionId == "null" {
			glog.V(2).Infof("GetObject: pre-versioning object %s/%s", bucket, object)
		} else {
			glog.V(2).Infof("GetObject: version %s for %s/%s", targetVersionId, bucket, object)
		}

		// Set version ID in response header
		w.Header().Set("x-amz-version-id", targetVersionId)

		// Add object lock metadata to response headers if present
		s3a.addObjectLockHeadersToResponse(w, entry)
	}

	versioningCheckTime = time.Since(tVersioning)

	// Fetch the correct entry for SSE processing (respects versionId)
	// This consolidates entry lookups to avoid multiple filer calls
	tEntryFetch := time.Now()
	var objectEntryForSSE *filer_pb.Entry

	// Optimization: Reuse already-fetched entry to avoid redundant metadata fetches
	if versioningConfigured {
		// For versioned objects, reuse the already-fetched entry
		objectEntryForSSE = entry
	} else {
		// For non-versioned objects, try to reuse entry from conditional header check
		if result.Entry != nil {
			// Reuse entry fetched during conditional header check (optimization)
			objectEntryForSSE = result.Entry
			glog.V(3).Infof("GetObjectHandler: Reusing entry from conditional header check for %s/%s", bucket, object)
		} else {
			// Fetch entry for SSE processing
			// This is needed for all SSE types (SSE-C, SSE-KMS, SSE-S3) to:
			// 1. Detect encryption from object metadata (SSE-KMS/SSE-S3 don't send headers on GET)
			// 2. Add proper response headers
			// 3. Handle Range requests on encrypted objects
			var fetchErr error
			objectEntryForSSE, fetchErr = s3a.fetchObjectEntry(bucket, object)
			if fetchErr != nil {
				glog.Warningf("GetObjectHandler: failed to get entry for %s/%s: %v", bucket, object, fetchErr)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
			if objectEntryForSSE == nil {
				// Not found, return error early to avoid another lookup in proxyToFiler
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}
	}
	entryFetchTime = time.Since(tEntryFetch)

	// Safety check: entry must be valid before tag-based policy evaluation
	if objectEntryForSSE == nil {
		glog.Errorf("GetObjectHandler: objectEntryForSSE is nil for %s/%s (should not happen)", bucket, object)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Handle remote storage objects: cache to local cluster if object is remote-only
	// This uses singleflight to deduplicate concurrent caching requests for the same object
	// On cache error, gracefully falls back to streaming from remote
	if objectEntryForSSE.IsInRemoteOnly() {
		objectEntryForSSE = s3a.cacheRemoteObjectWithDedup(r.Context(), bucket, object, objectEntryForSSE)
	}

	// Re-check bucket policy with object entry for tag-based conditions (e.g., s3:ExistingObjectTag)
	if errCode := s3a.recheckPolicyWithObjectEntry(r, bucket, object, string(s3_constants.ACTION_READ), objectEntryForSSE.Extended, "GetObjectHandler"); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Check if PartNumber query parameter is present (for multipart GET requests)
	partNumberStr := r.URL.Query().Get("partNumber")
	if partNumberStr == "" {
		partNumberStr = r.URL.Query().Get("PartNumber")
	}

	// If PartNumber is specified, set headers and modify Range to read only that part
	// This replicates the filer handler logic
	if partNumberStr != "" {
		if partNumber, parseErr := strconv.Atoi(partNumberStr); parseErr == nil && partNumber > 0 {
			// Get actual parts count from metadata (not chunk count)
			partsCount, partInfo := s3a.getMultipartInfo(objectEntryForSSE, partNumber)

			// Validate part number
			if partNumber > partsCount {
				glog.Warningf("GetObject: Invalid part number %d, object has %d parts", partNumber, partsCount)
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
				return
			}

			// Set parts count header
			w.Header().Set(s3_constants.AmzMpPartsCount, strconv.Itoa(partsCount))
			glog.V(3).Infof("GetObject: Set PartsCount=%d for multipart GET with PartNumber=%d", partsCount, partNumber)

			// Calculate the byte range for this part
			// Note: ETag is NOT overridden - AWS S3 returns the complete object's ETag
			// even when requesting a specific part via PartNumber
			var startOffset, endOffset int64
			if partInfo != nil {
				// Use part boundaries from metadata (accurate for multi-chunk parts)
				startOffset = objectEntryForSSE.Chunks[partInfo.StartChunk].Offset
				lastChunk := objectEntryForSSE.Chunks[partInfo.EndChunk-1]
				endOffset = lastChunk.Offset + int64(lastChunk.Size) - 1
			} else {
				// Fallback: assume 1:1 part-to-chunk mapping (backward compatibility)
				chunkIndex := partNumber - 1
				if chunkIndex >= len(objectEntryForSSE.Chunks) {
					glog.Warningf("GetObject: Part %d chunk index %d out of range (chunks: %d)", partNumber, chunkIndex, len(objectEntryForSSE.Chunks))
					s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
					return
				}
				partChunk := objectEntryForSSE.Chunks[chunkIndex]
				startOffset = partChunk.Offset
				endOffset = partChunk.Offset + int64(partChunk.Size) - 1
			}

			// Check if client supplied a Range header - if so, apply it within the part's boundaries
			// S3 allows both partNumber and Range together, where Range applies within the selected part
			clientRangeHeader := r.Header.Get("Range")
			if clientRangeHeader != "" {
				adjustedStart, adjustedEnd, rangeErr := adjustRangeForPart(startOffset, endOffset, clientRangeHeader)
				if rangeErr != nil {
					glog.Warningf("GetObject: Invalid Range for part %d: %v", partNumber, rangeErr)
					s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
					return
				}
				startOffset = adjustedStart
				endOffset = adjustedEnd
				glog.V(3).Infof("GetObject: Client Range %s applied to part %d, adjusted to bytes=%d-%d", clientRangeHeader, partNumber, startOffset, endOffset)
			}

			// Set Range header to read the requested bytes (full part or client-specified range within part)
			rangeHeader := fmt.Sprintf("bytes=%d-%d", startOffset, endOffset)
			r.Header.Set("Range", rangeHeader)
			glog.V(3).Infof("GetObject: Set Range header for part %d: %s", partNumber, rangeHeader)
		}
	}

	// NEW OPTIMIZATION: Stream directly from volume servers, bypassing filer proxy
	// This eliminates the 19ms filer proxy overhead
	// SSE decryption is handled inline during streaming

	// Detect SSE encryption type
	primarySSEType := s3a.detectPrimarySSEType(objectEntryForSSE)

	// Stream directly from volume servers with SSE support
	tStream := time.Now()
	err = s3a.streamFromVolumeServersWithSSE(w, r, objectEntryForSSE, primarySSEType, bucket, object, versionId)
	streamTime = time.Since(tStream)
	if err != nil {
		glog.Errorf("GetObjectHandler: failed to stream %s/%s from volume servers: %v", bucket, object, err)
		// Check if the streaming function already wrote an HTTP response
		var streamErr *StreamError
		if errors.As(err, &streamErr) && streamErr.ResponseWritten {
			// Response already written (headers + status code), don't write again
			// to avoid "superfluous response.WriteHeader call" and malformed S3 error bodies
			return
		}
		// Response not yet written - safe to write S3 error response
		// Check if error is due to volume server rate limiting (HTTP 429)
		if errors.Is(err, util_http.ErrTooManyRequests) {
			s3err.WriteErrorResponse(w, r, s3err.ErrRequestBytesExceed)
		} else {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}
}

// streamFromVolumeServers streams object data directly from volume servers, bypassing filer proxy
// This eliminates the ~19ms filer proxy overhead by reading chunks directly
func (s3a *S3ApiServer) streamFromVolumeServers(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry, sseType string, bucket, object, versionId string) error {
	// Profiling: Track overall and stage timings
	t0 := time.Now()
	var (
		rangeParseTime   time.Duration
		headerSetTime    time.Duration
		chunkResolveTime time.Duration
		streamPrepTime   time.Duration
		streamExecTime   time.Duration
	)
	defer func() {
		totalTime := time.Since(t0)
		glog.V(2).Infof("  └─ streamFromVolumeServers: total=%v, rangeParse=%v, headerSet=%v, chunkResolve=%v, streamPrep=%v, streamExec=%v",
			totalTime, rangeParseTime, headerSetTime, chunkResolveTime, streamPrepTime, streamExecTime)
	}()

	if entry == nil {
		// Early validation error: write S3-compliant XML error response
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return newStreamErrorWithResponse(fmt.Errorf("entry is nil"))
	}

	// Get file size
	totalSize := int64(filer.FileSize(entry))

	// Parse Range header if present
	tRangeParse := time.Now()
	offset, size, isRangeRequest, rangeErr := s3a.parseAndValidateRange(w, r, entry, totalSize, bucket, object)
	if rangeErr != nil {
		return rangeErr
	}
	rangeParseTime = time.Since(tRangeParse)

	// For small files stored inline in entry.Content - validate BEFORE setting headers
	if len(entry.Content) > 0 && totalSize == int64(len(entry.Content)) {
		if isRangeRequest {
			// Safely convert int64 to int for slice indexing - validate BEFORE WriteHeader
			// Use MaxInt32 for portability across 32-bit and 64-bit platforms
			if offset < 0 || offset > int64(math.MaxInt32) || size < 0 || size > int64(math.MaxInt32) {
				// Early validation error: write S3-compliant error response
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
				return newStreamErrorWithResponse(fmt.Errorf("range too large for platform: offset=%d, size=%d", offset, size))
			}
			start := int(offset)
			end := start + int(size)
			// Bounds check (should already be validated, but double-check) - BEFORE WriteHeader
			if start < 0 || start > len(entry.Content) || end > len(entry.Content) || end < start {
				// Early validation error: write S3-compliant error response
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
				return newStreamErrorWithResponse(fmt.Errorf("invalid range for inline content: start=%d, end=%d, len=%d", start, end, len(entry.Content)))
			}
			// Validation passed - now set headers and write
			s3a.setResponseHeaders(w, r, entry, totalSize)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+size-1, totalSize))
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusPartialContent)
			written, err := w.Write(entry.Content[start:end])
			if written > 0 {
				BucketTrafficSent(int64(written), r)
			}
			return err
		}
		// Non-range request for inline content
		s3a.setResponseHeaders(w, r, entry, totalSize)
		w.WriteHeader(http.StatusOK)
		written, err := w.Write(entry.Content)
		if written > 0 {
			BucketTrafficSent(int64(written), r)
		}
		return err
	}

	// Get chunks and validate BEFORE setting headers
	chunks := entry.GetChunks()
	glog.V(4).Infof("streamFromVolumeServers: entry has %d chunks, totalSize=%d, isRange=%v, offset=%d, size=%d",
		len(chunks), totalSize, isRangeRequest, offset, size)

	if len(chunks) == 0 {
		// Check if this is a remote-only entry that needs caching
		// This handles the case where initial caching attempt timed out or failed
		if entry.IsInRemoteOnly() {
			glog.V(1).Infof("streamFromVolumeServers: entry is remote-only, attempting to cache before streaming")
			// Try to cache the remote object synchronously (like filer does)
			cachedEntry := s3a.cacheRemoteObjectForStreaming(r, entry, bucket, object, versionId)
			if cachedEntry != nil && len(cachedEntry.GetChunks()) > 0 {
				chunks = cachedEntry.GetChunks()
				entry = cachedEntry
				glog.V(1).Infof("streamFromVolumeServers: successfully cached remote object, got %d chunks", len(chunks))
			} else {
				// Caching failed - return error to client
				glog.Errorf("streamFromVolumeServers: failed to cache remote object for streaming")
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return newStreamErrorWithResponse(fmt.Errorf("failed to cache remote object for streaming"))
			}
		} else if totalSize > 0 && len(entry.Content) == 0 {
			// Not a remote entry but has size without content - this is a data integrity issue
			glog.Errorf("streamFromVolumeServers: Data integrity error - entry reports size %d but has no content or chunks", totalSize)
			// Write S3-compliant XML error response
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return newStreamErrorWithResponse(fmt.Errorf("data integrity error: size %d reported but no content available", totalSize))
		} else {
			// Empty object - set headers and write status
			s3a.setResponseHeaders(w, r, entry, totalSize)
			w.WriteHeader(http.StatusOK)
			return nil
		}
	}

	// Log chunk details (verbose only - high frequency)
	if glog.V(4) {
		for i, chunk := range chunks {
			glog.Infof("  GET Chunk[%d]: fid=%s, offset=%d, size=%d", i, chunk.GetFileIdString(), chunk.Offset, chunk.Size)
		}
	}

	// CRITICAL: Resolve chunks and prepare stream BEFORE WriteHeader
	// This ensures we can write proper error responses if these operations fail
	ctx := r.Context()
	lookupFileIdFn := s3a.createLookupFileIdFunction()

	// Resolve chunk manifests with the requested range
	tChunkResolve := time.Now()
	resolvedChunks, _, err := filer.ResolveChunkManifest(ctx, lookupFileIdFn, chunks, offset, offset+size)
	chunkResolveTime = time.Since(tChunkResolve)
	if err != nil {
		glog.Errorf("streamFromVolumeServers: failed to resolve chunks: %v", err)
		// Write S3-compliant XML error response
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return newStreamErrorWithResponse(fmt.Errorf("failed to resolve chunks: %v", err))
	}

	// Prepare streaming function with simple master client wrapper
	tStreamPrep := time.Now()
	// Use filerClient directly (not wrapped) so it can support cache invalidation
	streamFn, err := filer.PrepareStreamContentWithThrottler(
		ctx,
		s3a.filerClient,
		filer.JwtForVolumeServer, // Use filer's JWT function (loads config once, generates JWT locally)
		resolvedChunks,
		offset,
		size,
		0, // no throttling
	)
	streamPrepTime = time.Since(tStreamPrep)
	if err != nil {
		glog.Errorf("streamFromVolumeServers: failed to prepare stream: %v", err)
		// Write S3-compliant XML error response
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return newStreamErrorWithResponse(fmt.Errorf("failed to prepare stream: %v", err))
	}

	// All validation and preparation successful - NOW set headers and write status
	tHeaderSet := time.Now()
	s3a.setResponseHeaders(w, r, entry, totalSize)

	// Override/add range-specific headers if this is a range request
	if isRangeRequest {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+size-1, totalSize))
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	headerSetTime = time.Since(tHeaderSet)

	// Now write status code (headers are all set, stream is ready)
	if isRangeRequest {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Track time to first byte metric
	TimeToFirstByte(r.Method, t0, r)

	// Stream directly to response with counting wrapper
	tStreamExec := time.Now()
	glog.V(4).Infof("streamFromVolumeServers: starting streamFn, offset=%d, size=%d", offset, size)
	cw := &countingWriter{w: w}
	err = streamFn(cw)
	streamExecTime = time.Since(tStreamExec)
	// Track traffic even on partial writes for accurate egress accounting
	if cw.written > 0 {
		BucketTrafficSent(cw.written, r)
	}
	if err != nil {
		glog.Errorf("streamFromVolumeServers: streamFn failed after writing %d bytes: %v", cw.written, err)
		// Streaming error after WriteHeader was called - response already partially written
		return newStreamErrorWithResponse(err)
	}
	glog.V(4).Infof("streamFromVolumeServers: streamFn completed successfully, wrote %d bytes", cw.written)
	return nil
}

// Shared HTTP client for volume server requests (connection pooling)
var volumeServerHTTPClient = &http.Client{
	Timeout: 5 * time.Minute,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	},
}

// createLookupFileIdFunction creates a reusable lookup function for resolving volume URLs
// Uses FilerClient's vidMap cache to eliminate per-chunk gRPC overhead
func (s3a *S3ApiServer) createLookupFileIdFunction() func(context.Context, string) ([]string, error) {
	// Return the FilerClient's lookup function which uses the battle-tested vidMap cache
	return s3a.filerClient.GetLookupFileIdFunction()
}

// streamFromVolumeServersWithSSE handles streaming with inline SSE decryption
func (s3a *S3ApiServer) streamFromVolumeServersWithSSE(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry, sseType string, bucket, object, versionId string) error {
	// If not encrypted, use fast path without decryption
	if sseType == "" || sseType == "None" {
		return s3a.streamFromVolumeServers(w, r, entry, sseType, bucket, object, versionId)
	}

	// Profiling: Track SSE decryption stages
	t0 := time.Now()
	var (
		rangeParseTime   time.Duration
		keyValidateTime  time.Duration
		headerSetTime    time.Duration
		streamFetchTime  time.Duration
		decryptSetupTime time.Duration
		copyTime         time.Duration
	)
	defer func() {
		totalTime := time.Since(t0)
		glog.V(2).Infof("  └─ streamFromVolumeServersWithSSE (%s): total=%v, rangeParse=%v, keyValidate=%v, headerSet=%v, streamFetch=%v, decryptSetup=%v, copy=%v",
			sseType, totalTime, rangeParseTime, keyValidateTime, headerSetTime, streamFetchTime, decryptSetupTime, copyTime)
	}()

	glog.V(2).Infof("streamFromVolumeServersWithSSE: Handling %s encrypted object with inline decryption", sseType)

	// Parse Range header BEFORE key validation
	totalSize := int64(filer.FileSize(entry))
	tRangeParse := time.Now()
	offset, size, isRangeRequest, rangeErr := s3a.parseAndValidateRange(w, r, entry, totalSize, bucket, object)
	if rangeErr != nil {
		return rangeErr
	}
	if isRangeRequest {
		glog.V(2).Infof("streamFromVolumeServersWithSSE: Range request bytes %d-%d/%d (size=%d)", offset, offset+size-1, totalSize, size)
	}
	rangeParseTime = time.Since(tRangeParse)

	// Validate SSE keys BEFORE streaming
	tKeyValidate := time.Now()
	var decryptionKey interface{}
	switch sseType {
	case s3_constants.SSETypeC:
		customerKey, err := ParseSSECHeaders(r)
		if err != nil {
			s3err.WriteErrorResponse(w, r, MapSSECErrorToS3Error(err))
			return newStreamErrorWithResponse(err)
		}
		if customerKey == nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyMissing)
			return newStreamErrorWithResponse(fmt.Errorf("SSE-C key required"))
		}
		// Validate key MD5
		if entry.Extended != nil {
			storedKeyMD5 := string(entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5])
			if storedKeyMD5 != "" && customerKey.KeyMD5 != storedKeyMD5 {
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return newStreamErrorWithResponse(fmt.Errorf("SSE-C key mismatch"))
			}
		}
		decryptionKey = customerKey
	case s3_constants.SSETypeKMS:
		// Extract KMS key from metadata (stored as raw bytes, matching filer behavior)
		if entry.Extended == nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return newStreamErrorWithResponse(fmt.Errorf("no SSE-KMS metadata"))
		}
		kmsMetadataBytes := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]
		sseKMSKey, err := DeserializeSSEKMSMetadata(kmsMetadataBytes)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return newStreamErrorWithResponse(err)
		}
		decryptionKey = sseKMSKey
	case s3_constants.SSETypeS3:
		// Extract S3 key from metadata (stored as raw bytes, matching filer behavior)
		if entry.Extended == nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return newStreamErrorWithResponse(fmt.Errorf("no SSE-S3 metadata"))
		}
		keyData := entry.Extended[s3_constants.SeaweedFSSSES3Key]
		keyManager := GetSSES3KeyManager()
		sseS3Key, err := DeserializeSSES3Metadata(keyData, keyManager)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return newStreamErrorWithResponse(err)
		}
		decryptionKey = sseS3Key
	}
	keyValidateTime = time.Since(tKeyValidate)

	// Set response headers
	// IMPORTANT: Set ALL headers BEFORE calling WriteHeader (headers are ignored after WriteHeader)
	tHeaderSet := time.Now()
	s3a.setResponseHeaders(w, r, entry, totalSize)
	s3a.addSSEResponseHeadersFromEntry(w, r, entry, sseType)

	// Override/add range-specific headers if this is a range request
	if isRangeRequest {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+size-1, totalSize))
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	headerSetTime = time.Since(tHeaderSet)

	// Now write status code (headers are all set)
	if isRangeRequest {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Track time to first byte metric
	TimeToFirstByte(r.Method, t0, r)

	// Full Range Optimization: Use ViewFromChunks to only fetch/decrypt needed chunks
	tDecryptSetup := time.Now()

	// Use range-aware chunk resolution (like filer does)
	if isRangeRequest {
		glog.V(2).Infof("Using range-aware SSE decryption for offset=%d size=%d", offset, size)
		streamFetchTime = 0 // No full stream fetch in range-aware path
		written, err := s3a.streamDecryptedRangeFromChunks(r.Context(), w, entry, offset, size, sseType, decryptionKey)
		decryptSetupTime = time.Since(tDecryptSetup)
		copyTime = decryptSetupTime // Streaming is included in decrypt setup for range-aware path
		// Track traffic even on partial writes for accurate egress accounting
		if written > 0 {
			BucketTrafficSent(written, r)
		}
		if err != nil {
			// Error after WriteHeader - response already written
			return newStreamErrorWithResponse(err)
		}
		return nil
	}

	// Full object path: Optimize multipart vs single-part
	var decryptedReader io.Reader
	var err error

	switch sseType {
	case s3_constants.SSETypeC:
		customerKey := decryptionKey.(*SSECustomerKey)

		// Check if this is a multipart object (multiple chunks with SSE-C metadata)
		isMultipartSSEC := false
		ssecChunks := 0
		for _, chunk := range entry.GetChunks() {
			if chunk.GetSseType() == filer_pb.SSEType_SSE_C && len(chunk.GetSseMetadata()) > 0 {
				ssecChunks++
			}
		}
		isMultipartSSEC = ssecChunks > 1
		glog.V(3).Infof("SSE-C decryption: KeyMD5=%s, entry has %d chunks, isMultipart=%v, ssecChunks=%d",
			customerKey.KeyMD5, len(entry.GetChunks()), isMultipartSSEC, ssecChunks)

		if isMultipartSSEC {
			// For multipart, skip getEncryptedStreamFromVolumes and fetch chunks directly
			// This saves one filer lookup/pipe creation
			decryptedReader, err = s3a.createMultipartSSECDecryptedReaderDirect(r.Context(), nil, customerKey, entry)
			glog.V(2).Infof("Using multipart SSE-C decryption for object with %d chunks (no prefetch)", len(entry.GetChunks()))
		} else {
			// For single-part, get encrypted stream and decrypt
			tStreamFetch := time.Now()
			encryptedReader, streamErr := s3a.getEncryptedStreamFromVolumes(r.Context(), entry)
			streamFetchTime = time.Since(tStreamFetch)
			if streamErr != nil {
				// Error after WriteHeader - response already written
				return newStreamErrorWithResponse(streamErr)
			}
			defer encryptedReader.Close()

			iv := entry.Extended[s3_constants.SeaweedFSSSEIV]
			if len(iv) == 0 {
				// Error after WriteHeader - response already written
				return newStreamErrorWithResponse(fmt.Errorf("SSE-C IV not found in entry metadata"))
			}
			glog.V(2).Infof("SSE-C decryption: IV length=%d, KeyMD5=%s", len(iv), customerKey.KeyMD5)
			decryptedReader, err = CreateSSECDecryptedReader(encryptedReader, customerKey, iv)
		}

	case s3_constants.SSETypeKMS:
		sseKMSKey := decryptionKey.(*SSEKMSKey)

		// Check if this is a multipart object (multiple chunks with SSE-KMS metadata)
		isMultipartSSEKMS := false
		ssekmsChunks := 0
		for _, chunk := range entry.GetChunks() {
			if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS && len(chunk.GetSseMetadata()) > 0 {
				ssekmsChunks++
			}
		}
		isMultipartSSEKMS = ssekmsChunks > 1
		glog.V(3).Infof("SSE-KMS decryption: isMultipart=%v, ssekmsChunks=%d", isMultipartSSEKMS, ssekmsChunks)

		if isMultipartSSEKMS {
			// For multipart, skip getEncryptedStreamFromVolumes and fetch chunks directly
			decryptedReader, err = s3a.createMultipartSSEKMSDecryptedReaderDirect(r.Context(), nil, entry)
			glog.V(2).Infof("Using multipart SSE-KMS decryption for object with %d chunks (no prefetch)", len(entry.GetChunks()))
		} else {
			// For single-part, get encrypted stream and decrypt
			tStreamFetch := time.Now()
			encryptedReader, streamErr := s3a.getEncryptedStreamFromVolumes(r.Context(), entry)
			streamFetchTime = time.Since(tStreamFetch)
			if streamErr != nil {
				// Error after WriteHeader - response already written
				return newStreamErrorWithResponse(streamErr)
			}
			defer encryptedReader.Close()

			glog.V(2).Infof("SSE-KMS decryption: KeyID=%s, IV length=%d", sseKMSKey.KeyID, len(sseKMSKey.IV))
			decryptedReader, err = CreateSSEKMSDecryptedReader(encryptedReader, sseKMSKey)
		}

	case s3_constants.SSETypeS3:
		sseS3Key := decryptionKey.(*SSES3Key)

		// Check if this is a multipart object (multiple chunks with SSE-S3 metadata)
		isMultipartSSES3 := false
		sses3Chunks := 0
		for _, chunk := range entry.GetChunks() {
			if chunk.GetSseType() == filer_pb.SSEType_SSE_S3 && len(chunk.GetSseMetadata()) > 0 {
				sses3Chunks++
			}
		}
		isMultipartSSES3 = sses3Chunks > 1
		glog.V(3).Infof("SSE-S3 decryption: isMultipart=%v, sses3Chunks=%d", isMultipartSSES3, sses3Chunks)

		if isMultipartSSES3 {
			// For multipart, skip getEncryptedStreamFromVolumes and fetch chunks directly
			decryptedReader, err = s3a.createMultipartSSES3DecryptedReaderDirect(r.Context(), nil, entry)
			glog.V(2).Infof("Using multipart SSE-S3 decryption for object with %d chunks (no prefetch)", len(entry.GetChunks()))
		} else {
			// For single-part, get encrypted stream and decrypt
			tStreamFetch := time.Now()
			encryptedReader, streamErr := s3a.getEncryptedStreamFromVolumes(r.Context(), entry)
			streamFetchTime = time.Since(tStreamFetch)
			if streamErr != nil {
				// Error after WriteHeader - response already written
				return newStreamErrorWithResponse(streamErr)
			}
			defer encryptedReader.Close()

			keyManager := GetSSES3KeyManager()
			iv, ivErr := GetSSES3IV(entry, sseS3Key, keyManager)
			if ivErr != nil {
				// Error after WriteHeader - response already written
				return newStreamErrorWithResponse(fmt.Errorf("failed to get SSE-S3 IV: %w", ivErr))
			}
			glog.V(2).Infof("SSE-S3 decryption: KeyID=%s, IV length=%d", sseS3Key.KeyID, len(iv))
			decryptedReader, err = CreateSSES3DecryptedReader(encryptedReader, sseS3Key, iv)
		}
	}
	decryptSetupTime = time.Since(tDecryptSetup)

	if err != nil {
		glog.Errorf("SSE decryption error (%s): %v", sseType, err)
		// Error after WriteHeader - response already written
		return newStreamErrorWithResponse(fmt.Errorf("failed to create decrypted reader: %w", err))
	}

	// Close the decrypted reader to avoid leaking HTTP bodies
	if closer, ok := decryptedReader.(io.Closer); ok {
		defer func() {
			if closeErr := closer.Close(); closeErr != nil {
				glog.V(3).Infof("Error closing decrypted reader: %v", closeErr)
			}
		}()
	}

	// Stream full decrypted object to client
	tCopy := time.Now()
	buf := make([]byte, 128*1024)
	copied, copyErr := io.CopyBuffer(w, decryptedReader, buf)
	copyTime = time.Since(tCopy)
	// Track traffic even on partial writes for accurate egress accounting
	if copied > 0 {
		BucketTrafficSent(copied, r)
	}
	if copyErr != nil {
		glog.Errorf("Failed to copy full object: copied %d bytes: %v", copied, copyErr)
		// Error after WriteHeader - response already written
		return newStreamErrorWithResponse(copyErr)
	}
	glog.V(3).Infof("Full object request: copied %d bytes", copied)
	return nil
}

// streamDecryptedRangeFromChunks streams a range of decrypted data by only fetching needed chunks
// This implements the filer's ViewFromChunks approach for optimal range performance
// Returns the number of bytes written and any error
func (s3a *S3ApiServer) streamDecryptedRangeFromChunks(ctx context.Context, w io.Writer, entry *filer_pb.Entry, offset int64, size int64, sseType string, decryptionKey interface{}) (int64, error) {
	// Use filer's ViewFromChunks to resolve only needed chunks for the range
	lookupFileIdFn := s3a.createLookupFileIdFunction()
	chunkViews := filer.ViewFromChunks(ctx, lookupFileIdFn, entry.GetChunks(), offset, size)

	totalWritten := int64(0)
	targetOffset := offset

	// Stream each chunk view
	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunkView := x.Value

		// Handle gaps between chunks (write zeros)
		if targetOffset < chunkView.ViewOffset {
			gap := chunkView.ViewOffset - targetOffset
			glog.V(4).Infof("Writing %d zero bytes for gap [%d,%d)", gap, targetOffset, chunkView.ViewOffset)
			if err := writeZeroBytes(w, gap); err != nil {
				return totalWritten, fmt.Errorf("failed to write zero padding: %w", err)
			}
			totalWritten += gap
			targetOffset = chunkView.ViewOffset
		}

		// Find the corresponding FileChunk for this chunkView
		var fileChunk *filer_pb.FileChunk
		for _, chunk := range entry.GetChunks() {
			if chunk.GetFileIdString() == chunkView.FileId {
				fileChunk = chunk
				break
			}
		}
		if fileChunk == nil {
			return totalWritten, fmt.Errorf("chunk %s not found in entry", chunkView.FileId)
		}

		// Fetch and decrypt this chunk view
		var decryptedChunkReader io.Reader
		var err error

		switch sseType {
		case s3_constants.SSETypeC:
			decryptedChunkReader, err = s3a.decryptSSECChunkView(ctx, fileChunk, chunkView, decryptionKey.(*SSECustomerKey))
		case s3_constants.SSETypeKMS:
			decryptedChunkReader, err = s3a.decryptSSEKMSChunkView(ctx, fileChunk, chunkView)
		case s3_constants.SSETypeS3:
			decryptedChunkReader, err = s3a.decryptSSES3ChunkView(ctx, fileChunk, chunkView, entry)
		default:
			// Non-encrypted chunk
			decryptedChunkReader, err = s3a.fetchChunkViewData(ctx, chunkView)
		}

		if err != nil {
			return totalWritten, fmt.Errorf("failed to decrypt chunk view %s: %w", chunkView.FileId, err)
		}

		// Copy the decrypted chunk data
		written, copyErr := io.Copy(w, decryptedChunkReader)
		if closer, ok := decryptedChunkReader.(io.Closer); ok {
			closeErr := closer.Close()
			if closeErr != nil {
				glog.Warningf("streamDecryptedRangeFromChunks: failed to close decrypted chunk reader: %v", closeErr)
			}
		}
		if copyErr != nil {
			glog.Errorf("streamDecryptedRangeFromChunks: copy error after writing %d bytes (expected %d): %v", written, chunkView.ViewSize, copyErr)
			return totalWritten, fmt.Errorf("failed to copy decrypted chunk data: %w", copyErr)
		}

		if written != int64(chunkView.ViewSize) {
			glog.Errorf("streamDecryptedRangeFromChunks: size mismatch - wrote %d bytes but expected %d", written, chunkView.ViewSize)
			return totalWritten, fmt.Errorf("size mismatch: wrote %d bytes but expected %d for chunk %s", written, chunkView.ViewSize, chunkView.FileId)
		}

		totalWritten += written
		targetOffset += written
		glog.V(2).Infof("streamDecryptedRangeFromChunks: Wrote %d bytes from chunk %s [%d,%d), totalWritten=%d, targetSize=%d", written, chunkView.FileId, chunkView.ViewOffset, chunkView.ViewOffset+int64(chunkView.ViewSize), totalWritten, size)
	}

	// Handle trailing zeros if needed
	remaining := size - totalWritten
	if remaining > 0 {
		glog.V(4).Infof("Writing %d trailing zero bytes", remaining)
		if err := writeZeroBytes(w, remaining); err != nil {
			return totalWritten, fmt.Errorf("failed to write trailing zeros: %w", err)
		}
		totalWritten += remaining
	}

	glog.V(3).Infof("Completed range-aware SSE decryption: wrote %d bytes for range [%d,%d)", totalWritten, offset, offset+size)
	return totalWritten, nil
}

// writeZeroBytes writes n zero bytes to writer using the package-level zero buffer
func writeZeroBytes(w io.Writer, n int64) error {
	for n > 0 {
		toWrite := min(n, int64(len(zeroBuf)))
		written, err := w.Write(zeroBuf[:toWrite])
		if err != nil {
			return err
		}
		n -= int64(written)
	}
	return nil
}

// decryptSSECChunkView decrypts a specific chunk view with SSE-C
//
// IV Handling for SSE-C:
// ----------------------
// SSE-C multipart encryption differs from SSE-KMS/SSE-S3:
//
// 1. Encryption: CreateSSECEncryptedReader generates a RANDOM IV per part
//   - Each part starts with a fresh random IV (NOT derived from a base IV)
//   - CTR counter starts from 0 for each part: counter₀, counter₁, counter₂, ...
//   - PartOffset is stored in metadata to describe where this chunk sits in that encrypted stream
//
// 2. Decryption: Use the stored per-part IV and advance the CTR by PartOffset
//   - CreateSSECDecryptedReaderWithOffset internally uses calculateIVWithOffset to advance
//     the CTR counter to reach PartOffset within the per-part encrypted stream
//   - calculateIVWithOffset is applied to the per-part IV, NOT to derive a global base IV
//   - Do NOT compute a single base IV for all parts (unlike SSE-KMS/SSE-S3)
//
// This contrasts with SSE-KMS/SSE-S3 which use: base IV + calculateIVWithOffset(ChunkOffset)
func (s3a *S3ApiServer) decryptSSECChunkView(ctx context.Context, fileChunk *filer_pb.FileChunk, chunkView *filer.ChunkView, customerKey *SSECustomerKey) (io.Reader, error) {
	// For multipart SSE-C, each chunk has its own IV in chunk.SseMetadata
	if fileChunk.GetSseType() == filer_pb.SSEType_SSE_C && len(fileChunk.GetSseMetadata()) > 0 {
		ssecMetadata, err := DeserializeSSECMetadata(fileChunk.GetSseMetadata())
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-C metadata: %w", err)
		}
		chunkIV, err := base64.StdEncoding.DecodeString(ssecMetadata.IV)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IV: %w", err)
		}

		// Fetch FULL encrypted chunk
		// Note: Fetching full chunk is necessary for proper CTR decryption stream
		fullChunkReader, err := s3a.fetchFullChunk(ctx, chunkView.FileId)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch full chunk: %w", err)
		}

		partOffset := ssecMetadata.PartOffset
		if partOffset < 0 {
			fullChunkReader.Close()
			return nil, fmt.Errorf("invalid SSE-C part offset %d for chunk %s", partOffset, chunkView.FileId)
		}

		// Use stored IV and advance CTR stream by PartOffset within the encrypted stream
		decryptedReader, decryptErr := CreateSSECDecryptedReaderWithOffset(fullChunkReader, customerKey, chunkIV, uint64(partOffset))
		if decryptErr != nil {
			fullChunkReader.Close()
			return nil, fmt.Errorf("failed to create decrypted reader: %w", decryptErr)
		}

		// Skip to the position we need in the decrypted stream
		if chunkView.OffsetInChunk > 0 {
			_, err = io.CopyN(io.Discard, decryptedReader, chunkView.OffsetInChunk)
			if err != nil {
				if closer, ok := decryptedReader.(io.Closer); ok {
					closer.Close()
				}
				return nil, fmt.Errorf("failed to skip to offset %d: %w", chunkView.OffsetInChunk, err)
			}
		}

		// Return a reader that only reads ViewSize bytes with proper cleanup
		limitedReader := io.LimitReader(decryptedReader, int64(chunkView.ViewSize))
		return &rc{Reader: limitedReader, Closer: fullChunkReader}, nil
	}

	// Single-part SSE-C: use object-level IV (should not hit this in range path, but handle it)
	encryptedReader, err := s3a.fetchChunkViewData(ctx, chunkView)
	if err != nil {
		return nil, err
	}
	// For single-part, the IV is stored at object level, already handled in non-range path
	return encryptedReader, nil
}

// decryptSSEKMSChunkView decrypts a specific chunk view with SSE-KMS
//
// IV Handling for SSE-KMS:
// ------------------------
// SSE-KMS (and SSE-S3) use a fundamentally different IV scheme than SSE-C:
//
// 1. Encryption: Uses a BASE IV + offset calculation
//   - Base IV is generated once for the entire object
//   - For each chunk at position N: adjustedIV = calculateIVWithOffset(baseIV, N)
//   - This shifts the CTR counter to counterₙ where n = N/16
//   - ChunkOffset is stored in metadata and IS applied during encryption
//
// 2. Decryption: Apply the same offset calculation
//   - Use calculateIVWithOffset(baseIV, ChunkOffset) to reconstruct the encryption IV
//   - Also handle ivSkip for non-block-aligned offsets (intra-block positioning)
//   - This ensures decryption uses the same CTR counter sequence as encryption
//
// This contrasts with SSE-C which uses random IVs without offset calculation.
func (s3a *S3ApiServer) decryptSSEKMSChunkView(ctx context.Context, fileChunk *filer_pb.FileChunk, chunkView *filer.ChunkView) (io.Reader, error) {
	if fileChunk.GetSseType() == filer_pb.SSEType_SSE_KMS && len(fileChunk.GetSseMetadata()) > 0 {
		sseKMSKey, err := DeserializeSSEKMSMetadata(fileChunk.GetSseMetadata())
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize SSE-KMS metadata: %w", err)
		}

		// Fetch FULL encrypted chunk
		fullChunkReader, err := s3a.fetchFullChunk(ctx, chunkView.FileId)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch full chunk: %w", err)
		}

		// IMPORTANT: Calculate adjusted IV using ChunkOffset
		// SSE-KMS uses base IV + offset calculation (unlike SSE-C which uses random IVs)
		// This reconstructs the same IV that was used during encryption
		var adjustedIV []byte
		var ivSkip int
		if sseKMSKey.ChunkOffset > 0 {
			adjustedIV, ivSkip = calculateIVWithOffset(sseKMSKey.IV, sseKMSKey.ChunkOffset)
		} else {
			adjustedIV = sseKMSKey.IV
			ivSkip = 0
		}

		adjustedKey := &SSEKMSKey{
			KeyID:             sseKMSKey.KeyID,
			EncryptedDataKey:  sseKMSKey.EncryptedDataKey,
			EncryptionContext: sseKMSKey.EncryptionContext,
			BucketKeyEnabled:  sseKMSKey.BucketKeyEnabled,
			IV:                adjustedIV,
			ChunkOffset:       sseKMSKey.ChunkOffset,
		}

		decryptedReader, decryptErr := CreateSSEKMSDecryptedReader(fullChunkReader, adjustedKey)
		if decryptErr != nil {
			fullChunkReader.Close()
			return nil, fmt.Errorf("failed to create KMS decrypted reader: %w", decryptErr)
		}

		// CRITICAL: Skip intra-block bytes from CTR decryption (non-block-aligned offset handling)
		if ivSkip > 0 {
			_, err = io.CopyN(io.Discard, decryptedReader, int64(ivSkip))
			if err != nil {
				if closer, ok := decryptedReader.(io.Closer); ok {
					closer.Close()
				}
				return nil, fmt.Errorf("failed to skip intra-block bytes (%d): %w", ivSkip, err)
			}
		}

		// Skip to position and limit to ViewSize
		if chunkView.OffsetInChunk > 0 {
			_, err = io.CopyN(io.Discard, decryptedReader, chunkView.OffsetInChunk)
			if err != nil {
				if closer, ok := decryptedReader.(io.Closer); ok {
					closer.Close()
				}
				return nil, fmt.Errorf("failed to skip to offset: %w", err)
			}
		}

		limitedReader := io.LimitReader(decryptedReader, int64(chunkView.ViewSize))
		return &rc{Reader: limitedReader, Closer: fullChunkReader}, nil
	}

	// Non-KMS encrypted chunk
	return s3a.fetchChunkViewData(ctx, chunkView)
}

// decryptSSES3ChunkView decrypts a specific chunk view with SSE-S3
//
// IV Handling for SSE-S3:
// -----------------------
// SSE-S3 uses the same BASE IV + offset scheme as SSE-KMS, but with a subtle difference:
//
// 1. Encryption: Uses BASE IV + offset, but stores the ADJUSTED IV
//   - Base IV is generated once for the entire object
//   - For each chunk at position N: adjustedIV, skip = calculateIVWithOffset(baseIV, N)
//   - The ADJUSTED IV (not base IV) is stored in chunk metadata
//   - ChunkOffset calculation is performed during encryption
//
// 2. Decryption: Use the stored adjusted IV directly
//   - The stored IV is already block-aligned and ready to use
//   - No need to call calculateIVWithOffset again (unlike SSE-KMS)
//   - Decrypt full chunk from start, then skip to OffsetInChunk in plaintext
//
// This differs from:
//   - SSE-C: Uses random IV per chunk, no offset calculation
//   - SSE-KMS: Stores base IV, requires calculateIVWithOffset during decryption
func (s3a *S3ApiServer) decryptSSES3ChunkView(ctx context.Context, fileChunk *filer_pb.FileChunk, chunkView *filer.ChunkView, entry *filer_pb.Entry) (io.Reader, error) {
	// For multipart SSE-S3, each chunk has its own IV in chunk.SseMetadata
	if fileChunk.GetSseType() == filer_pb.SSEType_SSE_S3 && len(fileChunk.GetSseMetadata()) > 0 {
		keyManager := GetSSES3KeyManager()

		// Deserialize per-chunk SSE-S3 metadata to get chunk-specific IV
		chunkSSES3Metadata, err := DeserializeSSES3Metadata(fileChunk.GetSseMetadata(), keyManager)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize chunk SSE-S3 metadata: %w", err)
		}

		// Fetch FULL encrypted chunk (necessary for proper CTR decryption stream)
		fullChunkReader, err := s3a.fetchFullChunk(ctx, chunkView.FileId)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch full chunk: %w", err)
		}

		// IMPORTANT: Use the stored IV directly - it's already block-aligned
		// During encryption, CreateSSES3EncryptedReaderWithBaseIV called:
		//   adjustedIV, skip := calculateIVWithOffset(baseIV, partOffset)
		// and stored the adjustedIV in metadata. We use it as-is for decryption.
		// No need to call calculateIVWithOffset again (unlike SSE-KMS which stores base IV).
		iv := chunkSSES3Metadata.IV

		glog.V(4).Infof("Decrypting multipart SSE-S3 chunk %s with chunk-specific IV length=%d",
			chunkView.FileId, len(iv))

		// Decrypt the full chunk starting from offset 0
		decryptedReader, decryptErr := CreateSSES3DecryptedReader(fullChunkReader, chunkSSES3Metadata, iv)
		if decryptErr != nil {
			fullChunkReader.Close()
			return nil, fmt.Errorf("failed to create SSE-S3 decrypted reader: %w", decryptErr)
		}

		// Skip to position within the decrypted chunk (plaintext offset, not ciphertext offset)
		if chunkView.OffsetInChunk > 0 {
			_, err = io.CopyN(io.Discard, decryptedReader, chunkView.OffsetInChunk)
			if err != nil {
				if closer, ok := decryptedReader.(io.Closer); ok {
					closer.Close()
				}
				return nil, fmt.Errorf("failed to skip to offset %d: %w", chunkView.OffsetInChunk, err)
			}
		}

		limitedReader := io.LimitReader(decryptedReader, int64(chunkView.ViewSize))
		return &rc{Reader: limitedReader, Closer: fullChunkReader}, nil
	}

	// Single-part SSE-S3: use object-level IV and key (fallback path)
	keyData := entry.Extended[s3_constants.SeaweedFSSSES3Key]
	keyManager := GetSSES3KeyManager()
	sseS3Key, err := DeserializeSSES3Metadata(keyData, keyManager)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize SSE-S3 metadata: %w", err)
	}

	// Fetch FULL encrypted chunk
	fullChunkReader, err := s3a.fetchFullChunk(ctx, chunkView.FileId)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch full chunk: %w", err)
	}

	// Get base IV for single-part object
	iv, err := GetSSES3IV(entry, sseS3Key, keyManager)
	if err != nil {
		fullChunkReader.Close()
		return nil, fmt.Errorf("failed to get SSE-S3 IV: %w", err)
	}

	glog.V(4).Infof("Decrypting single-part SSE-S3 chunk %s with entry-level IV length=%d",
		chunkView.FileId, len(iv))

	decryptedReader, decryptErr := CreateSSES3DecryptedReader(fullChunkReader, sseS3Key, iv)
	if decryptErr != nil {
		fullChunkReader.Close()
		return nil, fmt.Errorf("failed to create S3 decrypted reader: %w", decryptErr)
	}

	// Skip to position and limit to ViewSize
	if chunkView.OffsetInChunk > 0 {
		_, err = io.CopyN(io.Discard, decryptedReader, chunkView.OffsetInChunk)
		if err != nil {
			if closer, ok := decryptedReader.(io.Closer); ok {
				closer.Close()
			}
			return nil, fmt.Errorf("failed to skip to offset: %w", err)
		}
	}

	limitedReader := io.LimitReader(decryptedReader, int64(chunkView.ViewSize))
	return &rc{Reader: limitedReader, Closer: fullChunkReader}, nil
}

// fetchFullChunk fetches the complete encrypted chunk from volume server
func (s3a *S3ApiServer) fetchFullChunk(ctx context.Context, fileId string) (io.ReadCloser, error) {
	// Lookup the volume server URLs for this chunk
	lookupFileIdFn := s3a.createLookupFileIdFunction()
	urlStrings, err := lookupFileIdFn(ctx, fileId)
	if err != nil || len(urlStrings) == 0 {
		return nil, fmt.Errorf("failed to lookup chunk %s: %w", fileId, err)
	}

	// Use the first URL
	chunkUrl := urlStrings[0]

	// Generate JWT for volume server authentication (uses config loaded once at startup)
	jwt := filer.JwtForVolumeServer(fileId)

	// Create request WITHOUT Range header to get full chunk
	req, err := http.NewRequestWithContext(ctx, "GET", chunkUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set JWT for authentication
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+jwt)
	}

	// Use shared HTTP client
	resp, err := volumeServerHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chunk: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code %d for chunk %s", resp.StatusCode, fileId)
	}

	return resp.Body, nil
}

// fetchChunkViewData fetches encrypted data for a chunk view (with range)
func (s3a *S3ApiServer) fetchChunkViewData(ctx context.Context, chunkView *filer.ChunkView) (io.ReadCloser, error) {
	// Lookup the volume server URLs for this chunk
	lookupFileIdFn := s3a.createLookupFileIdFunction()
	urlStrings, err := lookupFileIdFn(ctx, chunkView.FileId)
	if err != nil || len(urlStrings) == 0 {
		return nil, fmt.Errorf("failed to lookup chunk %s: %w", chunkView.FileId, err)
	}

	// Use the first URL (already contains complete URL with fileId)
	chunkUrl := urlStrings[0]

	// Generate JWT for volume server authentication (uses config loaded once at startup)
	jwt := filer.JwtForVolumeServer(chunkView.FileId)

	// Create request with Range header for the chunk view
	// chunkUrl already contains the complete URL including fileId
	req, err := http.NewRequestWithContext(ctx, "GET", chunkUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set Range header to fetch only the needed portion of the chunk
	if !chunkView.IsFullChunk() {
		rangeEnd := chunkView.OffsetInChunk + int64(chunkView.ViewSize) - 1
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunkView.OffsetInChunk, rangeEnd))
	}

	// Set JWT for authentication
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+jwt)
	}

	// Use shared HTTP client with connection pooling
	resp, err := volumeServerHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chunk: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code %d for chunk %s", resp.StatusCode, chunkView.FileId)
	}

	return resp.Body, nil
}

// getEncryptedStreamFromVolumes gets raw encrypted data stream from volume servers
func (s3a *S3ApiServer) getEncryptedStreamFromVolumes(ctx context.Context, entry *filer_pb.Entry) (io.ReadCloser, error) {
	// Handle inline content
	if len(entry.Content) > 0 {
		return io.NopCloser(bytes.NewReader(entry.Content)), nil
	}

	// Handle empty files
	chunks := entry.GetChunks()
	if len(chunks) == 0 {
		return io.NopCloser(bytes.NewReader([]byte{})), nil
	}

	// Reuse shared lookup function to keep volume lookup logic in one place
	lookupFileIdFn := s3a.createLookupFileIdFunction()

	// Resolve chunks
	totalSize := int64(filer.FileSize(entry))
	resolvedChunks, _, err := filer.ResolveChunkManifest(ctx, lookupFileIdFn, chunks, 0, totalSize)
	if err != nil {
		return nil, err
	}

	// Create streaming reader - use filerClient directly for cache invalidation support
	streamFn, err := filer.PrepareStreamContentWithThrottler(
		ctx,
		s3a.filerClient,
		filer.JwtForVolumeServer, // Use filer's JWT function (loads config once, generates JWT locally)
		resolvedChunks,
		0,
		totalSize,
		0,
	)
	if err != nil {
		return nil, err
	}

	// Create a pipe to get io.ReadCloser
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		if err := streamFn(pipeWriter); err != nil {
			glog.Errorf("getEncryptedStreamFromVolumes: streaming error: %v", err)
			pipeWriter.CloseWithError(err)
		}
	}()

	return pipeReader, nil
}

// addSSEResponseHeadersFromEntry adds appropriate SSE response headers based on entry metadata
func (s3a *S3ApiServer) addSSEResponseHeadersFromEntry(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry, sseType string) {
	if entry == nil || entry.Extended == nil {
		return
	}

	switch sseType {
	case s3_constants.SSETypeC:
		// SSE-C: Echo back algorithm and key MD5
		if algo, exists := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; exists {
			w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, string(algo))
		}
		if keyMD5, exists := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists {
			w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, string(keyMD5))
		}

	case s3_constants.SSETypeKMS:
		// SSE-KMS: Return algorithm and key ID
		w.Header().Set(s3_constants.AmzServerSideEncryption, "aws:kms")
		if kmsMetadataBytes, exists := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]; exists {
			sseKMSKey, err := DeserializeSSEKMSMetadata(kmsMetadataBytes)
			if err == nil {
				AddSSEKMSResponseHeaders(w, sseKMSKey)
			}
		}

	case s3_constants.SSETypeS3:
		// SSE-S3: Return algorithm
		w.Header().Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmAES256)
	}
}

// setResponseHeaders sets all standard HTTP response headers from entry metadata
func (s3a *S3ApiServer) setResponseHeaders(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry, totalSize int64) {
	// Safety check: entry must be valid
	if entry == nil {
		glog.Errorf("setResponseHeaders: entry is nil")
		return
	}

	// Set content length and accept ranges
	w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
	w.Header().Set("Accept-Ranges", "bytes")

	// Set ETag (but don't overwrite if already set, e.g., for part-specific GET requests)
	if w.Header().Get("ETag") == "" {
		etag := filer.ETag(entry)
		if etag != "" {
			w.Header().Set("ETag", "\""+etag+"\"")
		}
	}

	// Set Last-Modified in RFC1123 format
	if entry.Attributes != nil {
		modTime := time.Unix(entry.Attributes.Mtime, 0).UTC()
		w.Header().Set("Last-Modified", modTime.Format(http.TimeFormat))
	}

	// Set Content-Type
	mimeType := ""
	if entry.Attributes != nil && entry.Attributes.Mime != "" {
		mimeType = entry.Attributes.Mime
	}
	if mimeType == "" {
		// Try to detect from entry name
		if entry.Name != "" {
			ext := filepath.Ext(entry.Name)
			if ext != "" {
				mimeType = mime.TypeByExtension(ext)
			}
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	// Set custom headers from entry.Extended (user metadata)
	// Use direct map assignment to preserve original header casing (matches proxy behavior)
	if entry.Extended != nil {
		for k, v := range entry.Extended {
			// Skip internal SeaweedFS headers
			if !strings.HasPrefix(k, "xattr-") && !s3_constants.IsSeaweedFSInternalHeader(k) {
				// Support backward compatibility: migrate old non-canonical format to canonical format
				// OLD: "x-amz-meta-foo" → NEW: "X-Amz-Meta-foo" (preserving suffix case)
				headerKey := k
				if len(k) >= 11 && strings.EqualFold(k[:11], "x-amz-meta-") {
					// Normalize to AWS S3 format: "X-Amz-Meta-" prefix with lowercase suffix
					// AWS S3 returns user metadata with the suffix in lowercase
					suffix := k[len("x-amz-meta-"):]
					headerKey = s3_constants.AmzUserMetaPrefix + strings.ToLower(suffix)
					if glog.V(4) && k != headerKey {
						glog.Infof("Normalizing user metadata header %q to %q in response", k, headerKey)
					}
				}
				w.Header()[headerKey] = []string{string(v)}
			}
		}
	}

	// Set tag count header (matches filer logic)
	if entry.Extended != nil {
		tagCount := 0
		for k := range entry.Extended {
			if strings.HasPrefix(k, s3_constants.AmzObjectTagging+"-") {
				tagCount++
			}
		}
		if tagCount > 0 {
			w.Header().Set(s3_constants.AmzTagCount, strconv.Itoa(tagCount))
		}
	}

	// Apply S3 passthrough headers from query parameters
	// AWS S3 supports overriding response headers via query parameters like:
	// ?response-cache-control=no-cache&response-content-type=application/json
	// This allows presigned URLs to control how browsers handle the downloaded content
	if r != nil {
		for queryParam, headerValue := range r.URL.Query() {
			if normalizedHeader, ok := s3_constants.PassThroughHeaders[strings.ToLower(queryParam)]; ok && len(headerValue) > 0 && headerValue[0] != "" {
				w.Header().Set(normalizedHeader, headerValue[0])
			}
		}
	}
}

// HeadObjectHandler handles S3 HEAD object requests
//
// Special behavior for implicit directories:
// When a HEAD request is made on a path without a trailing slash, and that path represents
// a directory with children (either a 0-byte file marker or an actual directory), this handler
// returns 404 Not Found instead of 200 OK. This behavior improves compatibility with s3fs and
// matches AWS S3's handling of implicit directories.
//
// Rationale:
//   - AWS S3 typically doesn't create directory markers when files are uploaded (e.g., uploading
//     "dataset/file.txt" doesn't create a marker at "dataset")
//   - Some S3 clients (like PyArrow with s3fs) create directory markers, which can confuse s3fs
//   - s3fs's info() method calls HEAD first; if it succeeds with size=0, s3fs incorrectly reports
//     the object as a file instead of checking for children
//   - By returning 404 for implicit directories, we force s3fs to fall back to LIST-based discovery,
//     which correctly identifies directories by checking for children
//
// Examples:
//
//	HEAD /bucket/dataset (no trailing slash, has children) → 404 Not Found (implicit directory)
//	HEAD /bucket/dataset/ (trailing slash) → 200 OK (explicit directory request)
//	HEAD /bucket/empty.txt (0-byte file, no children) → 200 OK (legitimate empty file)
//	HEAD /bucket/file.txt (regular file) → 200 OK (normal operation)
//
// This behavior only applies to:
//   - Non-versioned buckets (versioned buckets use different semantics)
//   - Paths without trailing slashes (trailing slash indicates explicit directory request)
//   - Objects that are either 0-byte files or actual directories
//   - Objects that have at least one child (checked via hasChildren)
func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("HeadObjectHandler %s %s", bucket, object)

	// Check for SOSAPI virtual objects (system.xml, capacity.xml)
	// These are dynamically generated and don't exist on disk
	if s3a.handleSOSAPIHeadObject(w, r, bucket, object) {
		return // SOSAPI request was handled
	}

	// Handle directory objects with shared logic
	if s3a.handleDirectoryObjectRequest(w, r, bucket, object, "HeadObjectHandler") {
		return // Directory object request was handled
	}

	// Check conditional headers and handle early return if conditions fail
	result, handled := s3a.processConditionalHeaders(w, r, bucket, object, "HeadObjectHandler")
	if handled {
		return
	}

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	var (
		entry                *filer_pb.Entry // Declare entry at function scope for SSE processing
		versioningConfigured bool
		err                  error
	)

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	// Note: We need to check this even if versionId is empty, because versioned buckets
	// handle even "get latest version" requests differently (through .versions directory)
	versioningConfigured, err = s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if versioningConfigured {
		// Handle versioned HEAD - all versions are stored in .versions directory
		var targetVersionId string

		if versionId != "" {
			// Request for specific version
			glog.V(2).Infof("HeadObject: requesting specific version %s for %s/%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
			if err != nil {
				glog.Errorf("Failed to get specific version %s for %s/%s: %v", versionId, bucket, object, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
			targetVersionId = versionId
		} else {
			// Request for latest version - OPTIMIZATION:
			// Check if .versions/ directory exists quickly (no retries) to decide path
			// - If .versions/ exists: real versions available, use getLatestObjectVersion
			// - If .versions/ doesn't exist (ErrNotFound): only null version at regular path, use it directly
			// - If transient error: fall back to getLatestObjectVersion which has retry logic
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			normalizedObject := s3_constants.NormalizeObjectKey(object)
			versionsDir := normalizedObject + s3_constants.VersionsFolder

			// Quick check (no retries) for .versions/ directory
			versionsEntry, versionsErr := s3a.getEntry(bucketDir, versionsDir)

			if versionsErr == nil && versionsEntry != nil {
				// .versions/ exists, meaning real versions are stored there
				// Use getLatestObjectVersion which will properly find the newest version
				entry, err = s3a.getLatestObjectVersion(bucket, object)
				if err != nil {
					glog.Errorf("HeadObject: Failed to get latest version for %s/%s: %v", bucket, object, err)
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			} else if errors.Is(versionsErr, filer_pb.ErrNotFound) {
				// .versions/ doesn't exist (confirmed not found), check regular path for null version
				regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
				if regularErr == nil && regularEntry != nil {
					// Found object at regular path - this is the null version
					entry = regularEntry
					targetVersionId = "null"
				} else {
					// No object at regular path either - object doesn't exist
					glog.V(3).Infof("HeadObject: object not found at regular path or .versions for %s/%s", bucket, object)
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			} else {
				// Transient error checking .versions/, fall back to getLatestObjectVersion with retries
				glog.V(2).Infof("HeadObject: transient error checking .versions for %s/%s: %v, falling back to getLatestObjectVersion", bucket, object, versionsErr)
				entry, err = s3a.getLatestObjectVersion(bucket, object)
				if err != nil {
					glog.Errorf("HeadObject: Failed to get latest version for %s/%s: %v", bucket, object, err)
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			}
			// Extract version ID if not already set
			if targetVersionId == "" {
				if entry.Extended != nil {
					if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
						targetVersionId = string(versionIdBytes)
					}
				}
				// If no version ID found in entry, this is a pre-versioning object
				if targetVersionId == "" {
					targetVersionId = "null"
				}
			}
		}

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}

		// For versioned objects, log the target version
		if targetVersionId == "null" {
			glog.V(2).Infof("HeadObject: pre-versioning object %s/%s", bucket, object)
		} else {
			glog.V(2).Infof("HeadObject: version %s for %s/%s", targetVersionId, bucket, object)
		}

		// Set version ID in response header
		w.Header().Set("x-amz-version-id", targetVersionId)

		// Add object lock metadata to response headers if present
		s3a.addObjectLockHeadersToResponse(w, entry)
	}

	// Fetch the correct entry for SSE processing (respects versionId)
	// For versioned objects, reuse already-fetched entry; for non-versioned, try to reuse from conditional check
	var objectEntryForSSE *filer_pb.Entry
	if versioningConfigured {
		objectEntryForSSE = entry
	} else {
		// For non-versioned objects, try to reuse entry from conditional header check
		if result.Entry != nil {
			// Reuse entry fetched during conditional header check (optimization)
			objectEntryForSSE = result.Entry
			glog.V(3).Infof("HeadObjectHandler: Reusing entry from conditional header check for %s/%s", bucket, object)
		} else {
			// Fetch entry for SSE processing
			// This is needed for all SSE types (SSE-C, SSE-KMS, SSE-S3) to:
			// 1. Detect encryption from object metadata (SSE-KMS/SSE-S3 don't send headers on HEAD)
			// 2. Add proper response headers
			var fetchErr error
			objectEntryForSSE, fetchErr = s3a.fetchObjectEntry(bucket, object)
			if fetchErr != nil {
				glog.Warningf("HeadObjectHandler: failed to get entry for %s/%s: %v", bucket, object, fetchErr)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
			if objectEntryForSSE == nil {
				// Not found, return error early to avoid another lookup in proxyToFiler
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}
	}

	// Safety check: entry must be valid
	if objectEntryForSSE == nil {
		glog.Errorf("HeadObjectHandler: objectEntryForSSE is nil for %s/%s (should not happen)", bucket, object)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Re-check bucket policy with object entry for tag-based conditions (e.g., s3:ExistingObjectTag)
	if errCode := s3a.recheckPolicyWithObjectEntry(r, bucket, object, string(s3_constants.ACTION_READ), objectEntryForSSE.Extended, "HeadObjectHandler"); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Implicit Directory Handling for s3fs Compatibility
	// ====================================================
	//
	// Background:
	//   Some S3 clients (like PyArrow with s3fs) create directory markers when writing datasets.
	//   These can be either:
	//   1. 0-byte files with directory MIME type (e.g., "application/octet-stream")
	//   2. Actual directories in the filer (created by PyArrow's write_dataset)
	//
	// Problem:
	//   s3fs's info() method calls HEAD on the path. If HEAD returns 200 with size=0,
	//   s3fs incorrectly reports it as a file (type='file', size=0) instead of checking
	//   for children. This causes PyArrow to fail with "Parquet file size is 0 bytes".
	//
	// Solution:
	//   For non-versioned objects without trailing slash, if the object is a 0-byte file
	//   or directory AND has children, return 404 instead of 200. This forces s3fs to
	//   fall back to LIST-based discovery, which correctly identifies it as a directory.
	//
	// AWS S3 Compatibility:
	//   AWS S3 typically doesn't create directory markers for implicit directories, so
	//   HEAD on "dataset" (when only "dataset/file.txt" exists) returns 404. Our behavior
	//   matches this by returning 404 for implicit directories with children.
	//
	// Edge Cases Handled:
	//   - Empty files (0-byte, no children) → 200 OK (legitimate empty file)
	//   - Empty directories (no children) → 200 OK (legitimate empty directory)
	//   - Explicit directory requests (trailing slash) → 200 OK (handled earlier)
	//   - Versioned objects → Skip this check (different semantics)
	//
	// Performance:
	//   Only adds overhead for 0-byte files or directories without trailing slash.
	//   Cost: One LIST operation with Limit=1 (~1-5ms).
	//
	if !versioningConfigured && !strings.HasSuffix(object, "/") {
		// Check if this is an implicit directory (either a 0-byte file or actual directory with children)
		// PyArrow may create 0-byte files when writing datasets, or the filer may have actual directories
		if objectEntryForSSE.Attributes != nil {
			isZeroByteFile := objectEntryForSSE.Attributes.FileSize == 0 && !objectEntryForSSE.IsDirectory
			isActualDirectory := objectEntryForSSE.IsDirectory

			if isZeroByteFile || isActualDirectory {
				// Check if it has children (making it an implicit directory)
				if s3a.hasChildren(bucket, object) {
					// This is an implicit directory with children
					// Return 404 to force clients (like s3fs) to use LIST-based discovery
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			}
		}
	}

	// For HEAD requests, we already have all metadata - just set headers directly
	totalSize := int64(filer.FileSize(objectEntryForSSE))
	s3a.setResponseHeaders(w, r, objectEntryForSSE, totalSize)

	// Check if PartNumber query parameter is present (for multipart objects)
	// This logic matches the filer handler for consistency
	partNumberStr := r.URL.Query().Get("partNumber")
	if partNumberStr == "" {
		partNumberStr = r.URL.Query().Get("PartNumber")
	}

	// If PartNumber is specified, set headers (matching filer logic)
	if partNumberStr != "" {
		if partNumber, parseErr := strconv.Atoi(partNumberStr); parseErr == nil && partNumber > 0 {
			// Get actual parts count from metadata (not chunk count)
			partsCount, _ := s3a.getMultipartInfo(objectEntryForSSE, partNumber)

			// Validate part number
			if partNumber > partsCount {
				glog.Warningf("HeadObject: Invalid part number %d, object has %d parts", partNumber, partsCount)
				s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
				return
			}

			// Set parts count header
			// Note: ETag is NOT overridden - AWS S3 returns the complete object's ETag
			// even when requesting a specific part via PartNumber
			w.Header().Set(s3_constants.AmzMpPartsCount, strconv.Itoa(partsCount))
			glog.V(3).Infof("HeadObject: Set PartsCount=%d for part %d", partsCount, partNumber)
		}
	}

	// Detect and handle SSE
	glog.V(3).Infof("HeadObjectHandler: Retrieved entry for %s/%s - %d chunks", bucket, object, len(objectEntryForSSE.Chunks))
	sseType := s3a.detectPrimarySSEType(objectEntryForSSE)
	glog.V(2).Infof("HeadObjectHandler: Detected SSE type: %s", sseType)
	if sseType != "" && sseType != "None" {
		// Validate SSE headers for encrypted objects
		switch sseType {
		case s3_constants.SSETypeC:
			customerKey, err := ParseSSECHeaders(r)
			if err != nil {
				s3err.WriteErrorResponse(w, r, MapSSECErrorToS3Error(err))
				return
			}
			if customerKey == nil {
				s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyMissing)
				return
			}
			// Validate key MD5
			if objectEntryForSSE.Extended != nil {
				storedKeyMD5 := string(objectEntryForSSE.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5])
				if storedKeyMD5 != "" && customerKey.KeyMD5 != storedKeyMD5 {
					s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
					return
				}
			}
		}
		// Add SSE response headers
		s3a.addSSEResponseHeadersFromEntry(w, r, objectEntryForSSE, sseType)
	}

	w.WriteHeader(http.StatusOK)
}

func captureCORSHeaders(w http.ResponseWriter, headersToCapture []string) map[string]string {
	captured := make(map[string]string)
	for _, corsHeader := range headersToCapture {
		if value := w.Header().Get(corsHeader); value != "" {
			captured[corsHeader] = value
		}
	}
	return captured
}

func restoreCORSHeaders(w http.ResponseWriter, capturedCORSHeaders map[string]string) {
	for corsHeader, value := range capturedCORSHeaders {
		w.Header().Set(corsHeader, value)
	}
}

// writeFinalResponse handles the common response writing logic shared between
// passThroughResponse and handleSSECResponse
func writeFinalResponse(w http.ResponseWriter, proxyResponse *http.Response, bodyReader io.Reader, capturedCORSHeaders map[string]string) (statusCode int, bytesTransferred int64) {
	// Restore CORS headers that were set by middleware
	restoreCORSHeaders(w, capturedCORSHeaders)

	if proxyResponse.Header.Get("Content-Range") != "" && proxyResponse.StatusCode == 200 {
		statusCode = http.StatusPartialContent
	} else {
		statusCode = proxyResponse.StatusCode
	}
	w.WriteHeader(statusCode)

	// Stream response data
	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)
	bytesTransferred, err := io.CopyBuffer(w, bodyReader, buf)
	if err != nil {
		glog.V(1).Infof("response read %d bytes: %v", bytesTransferred, err)
	}
	return statusCode, bytesTransferred
}

// fetchObjectEntry fetches the filer entry for an object
// Returns nil if not found (not an error), or propagates other errors
func (s3a *S3ApiServer) fetchObjectEntry(bucket, object string) (*filer_pb.Entry, error) {
	objectPath := fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, object)
	fetchedEntry, fetchErr := s3a.getEntry("", objectPath)
	if fetchErr != nil {
		if errors.Is(fetchErr, filer_pb.ErrNotFound) {
			return nil, nil // Not found is not an error for SSE check
		}
		return nil, fetchErr // Propagate other errors
	}
	return fetchedEntry, nil
}

// fetchObjectEntryRequired fetches the filer entry for an object
// Returns an error if the object is not found or any other error occurs
func (s3a *S3ApiServer) fetchObjectEntryRequired(bucket, object string) (*filer_pb.Entry, error) {
	objectPath := fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, object)
	fetchedEntry, fetchErr := s3a.getEntry("", objectPath)
	if fetchErr != nil {
		return nil, fetchErr // Return error for both not-found and other errors
	}
	return fetchedEntry, nil
}

// copyResponseHeaders copies headers from proxy response to the response writer,
// excluding internal SeaweedFS headers and optionally excluding body-related headers
func copyResponseHeaders(w http.ResponseWriter, proxyResponse *http.Response, excludeBodyHeaders bool) {
	for k, v := range proxyResponse.Header {
		// Always exclude internal SeaweedFS headers
		if s3_constants.IsSeaweedFSInternalHeader(k) {
			continue
		}
		// Optionally exclude body-related headers that might change after decryption
		if excludeBodyHeaders && (k == "Content-Length" || k == "Content-Encoding") {
			continue
		}
		w.Header()[k] = v
	}
}

func passThroughResponse(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
	// Capture existing CORS headers that may have been set by middleware
	capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

	// Copy headers from proxy response (excluding internal SeaweedFS headers)
	copyResponseHeaders(w, proxyResponse, false)

	return writeFinalResponse(w, proxyResponse, proxyResponse.Body, capturedCORSHeaders)
}

// handleSSECResponse handles SSE-C decryption and response processing
func (s3a *S3ApiServer) handleSSECResponse(r *http.Request, proxyResponse *http.Response, w http.ResponseWriter, entry *filer_pb.Entry) (statusCode int, bytesTransferred int64) {
	// Check if the object has SSE-C metadata
	sseAlgorithm := proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	sseKeyMD5 := proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)
	isObjectEncrypted := sseAlgorithm != "" && sseKeyMD5 != ""

	// Parse SSE-C headers from request once (avoid duplication)
	customerKey, err := ParseSSECHeaders(r)
	if err != nil {
		errCode := MapSSECErrorToS3Error(err)
		s3err.WriteErrorResponse(w, r, errCode)
		return http.StatusBadRequest, 0
	}

	if isObjectEncrypted {
		// This object was encrypted with SSE-C, validate customer key
		if customerKey == nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyMissing)
			return http.StatusBadRequest, 0
		}

		// SSE-C MD5 is base64 and case-sensitive
		if customerKey.KeyMD5 != sseKeyMD5 {
			// For GET/HEAD requests, AWS S3 returns 403 Forbidden for a key mismatch.
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return http.StatusForbidden, 0
		}

		// SSE-C encrypted objects support HTTP Range requests
		// The IV is stored in metadata and CTR mode allows seeking to any offset
		// Range requests will be handled by the filer layer with proper offset-based decryption

		// Check if this is a chunked or small content SSE-C object
		// Use the entry parameter passed from the caller (avoids redundant lookup)
		if entry != nil {
			// Check for SSE-C chunks
			sseCChunks := 0
			for _, chunk := range entry.GetChunks() {
				if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
					sseCChunks++
				}
			}

			if sseCChunks >= 1 {

				// Handle chunked SSE-C objects - each chunk needs independent decryption
				multipartReader, decErr := s3a.createMultipartSSECDecryptedReader(r, proxyResponse, entry)
				if decErr != nil {
					glog.Errorf("Failed to create multipart SSE-C decrypted reader: %v", decErr)
					s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
					return http.StatusInternalServerError, 0
				}

				// Capture existing CORS headers
				capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

				// Copy headers from proxy response (excluding internal SeaweedFS headers)
				copyResponseHeaders(w, proxyResponse, false)

				// Set proper headers for range requests
				rangeHeader := r.Header.Get("Range")
				if rangeHeader != "" {

					// Parse range header (e.g., "bytes=0-99")
					if len(rangeHeader) > 6 && rangeHeader[:6] == "bytes=" {
						rangeSpec := rangeHeader[6:]
						parts := strings.Split(rangeSpec, "-")
						if len(parts) == 2 {
							startOffset, endOffset := int64(0), int64(-1)
							if parts[0] != "" {
								startOffset, _ = strconv.ParseInt(parts[0], 10, 64)
							}
							if parts[1] != "" {
								endOffset, _ = strconv.ParseInt(parts[1], 10, 64)
							}

							if endOffset >= startOffset {
								// Specific range - set proper Content-Length and Content-Range headers
								rangeLength := endOffset - startOffset + 1
								totalSize := proxyResponse.Header.Get("Content-Length")

								w.Header().Set("Content-Length", strconv.FormatInt(rangeLength, 10))
								w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%s", startOffset, endOffset, totalSize))
								// writeFinalResponse will set status to 206 if Content-Range is present
							}
						}
					}
				}

				return writeFinalResponse(w, proxyResponse, multipartReader, capturedCORSHeaders)
			} else if len(entry.GetChunks()) == 0 && len(entry.Content) > 0 {
				// Small content SSE-C object stored directly in entry.Content

				// Fall through to traditional single-object SSE-C handling below
			}
		}

		// Single-part SSE-C object: Get IV from proxy response headers (stored during upload)
		ivBase64 := proxyResponse.Header.Get(s3_constants.SeaweedFSSSEIVHeader)
		if ivBase64 == "" {
			glog.Errorf("SSE-C encrypted single-part object missing IV in metadata")
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return http.StatusInternalServerError, 0
		}

		iv, err := base64.StdEncoding.DecodeString(ivBase64)
		if err != nil {
			glog.Errorf("Failed to decode IV from metadata: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return http.StatusInternalServerError, 0
		}

		// Create decrypted reader with IV from metadata
		decryptedReader, decErr := CreateSSECDecryptedReader(proxyResponse.Body, customerKey, iv)
		if decErr != nil {
			glog.Errorf("Failed to create SSE-C decrypted reader: %v", decErr)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return http.StatusInternalServerError, 0
		}

		// Capture existing CORS headers that may have been set by middleware
		capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

		// Copy headers from proxy response (excluding body-related headers that might change and internal SeaweedFS headers)
		copyResponseHeaders(w, proxyResponse, true)

		// Set correct Content-Length for SSE-C (only for full object requests)
		// With IV stored in metadata, the encrypted length equals the original length
		if proxyResponse.Header.Get("Content-Range") == "" {
			// Full object request: encrypted length equals original length (IV not in stream)
			if contentLengthStr := proxyResponse.Header.Get("Content-Length"); contentLengthStr != "" {
				// Content-Length is already correct since IV is stored in metadata, not in data stream
				w.Header().Set("Content-Length", contentLengthStr)
			}
		}
		// For range requests, let the actual bytes transferred determine the response length

		// Add SSE-C response headers
		w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, sseAlgorithm)
		w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, sseKeyMD5)

		return writeFinalResponse(w, proxyResponse, decryptedReader, capturedCORSHeaders)
	} else {
		// Object is not encrypted, but check if customer provided SSE-C headers unnecessarily
		if customerKey != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyNotNeeded)
			return http.StatusBadRequest, 0
		}

		// Normal pass-through response
		return passThroughResponse(proxyResponse, w)
	}
}

// addObjectLockHeadersToResponse extracts object lock metadata from entry Extended attributes
// and adds the appropriate S3 headers to the response
func (s3a *S3ApiServer) addObjectLockHeadersToResponse(w http.ResponseWriter, entry *filer_pb.Entry) {
	if entry == nil || entry.Extended == nil {
		return
	}

	// Check if this entry has any object lock metadata (indicating it's from an object lock enabled bucket)
	hasObjectLockMode := false
	hasRetentionDate := false

	// Add object lock mode header if present
	if modeBytes, exists := entry.Extended[s3_constants.ExtObjectLockModeKey]; exists && len(modeBytes) > 0 {
		w.Header().Set(s3_constants.AmzObjectLockMode, string(modeBytes))
		hasObjectLockMode = true
	}

	// Add retention until date header if present
	if dateBytes, exists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; exists && len(dateBytes) > 0 {
		dateStr := string(dateBytes)
		// Convert Unix timestamp to ISO8601 format for S3 compatibility
		if timestamp, err := strconv.ParseInt(dateStr, 10, 64); err == nil {
			retainUntilDate := time.Unix(timestamp, 0).UTC()
			w.Header().Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))
			hasRetentionDate = true
		} else {
			glog.Errorf("addObjectLockHeadersToResponse: failed to parse retention until date from stored metadata (dateStr: %s): %v", dateStr, err)
		}
	}

	// Add legal hold header - AWS S3 behavior: always include legal hold for object lock enabled buckets
	if legalHoldBytes, exists := entry.Extended[s3_constants.ExtLegalHoldKey]; exists && len(legalHoldBytes) > 0 {
		// Return stored S3 standard "ON"/"OFF" values directly
		w.Header().Set(s3_constants.AmzObjectLockLegalHold, string(legalHoldBytes))
	} else if hasObjectLockMode || hasRetentionDate {
		// If this entry has object lock metadata (indicating object lock enabled bucket)
		// but no legal hold specifically set, default to "OFF" as per AWS S3 behavior
		w.Header().Set(s3_constants.AmzObjectLockLegalHold, s3_constants.LegalHoldOff)
	}
}

// addSSEHeadersToResponse converts stored SSE metadata from entry.Extended to HTTP response headers
// Uses intelligent prioritization: only set headers for the PRIMARY encryption type to avoid conflicts
func (s3a *S3ApiServer) addSSEHeadersToResponse(proxyResponse *http.Response, entry *filer_pb.Entry) {
	if entry == nil || entry.Extended == nil {
		return
	}

	// Determine the primary encryption type by examining chunks (most reliable)
	primarySSEType := s3a.detectPrimarySSEType(entry)

	// Only set headers for the PRIMARY encryption type
	switch primarySSEType {
	case s3_constants.SSETypeC:
		// Add only SSE-C headers
		if algorithmBytes, exists := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; exists && len(algorithmBytes) > 0 {
			proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, string(algorithmBytes))
		}

		if keyMD5Bytes, exists := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists && len(keyMD5Bytes) > 0 {
			proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, string(keyMD5Bytes))
		}

		if ivBytes, exists := entry.Extended[s3_constants.SeaweedFSSSEIV]; exists && len(ivBytes) > 0 {
			ivBase64 := base64.StdEncoding.EncodeToString(ivBytes)
			proxyResponse.Header.Set(s3_constants.SeaweedFSSSEIVHeader, ivBase64)
		}

	case s3_constants.SSETypeKMS:
		// Add only SSE-KMS headers
		if sseAlgorithm, exists := entry.Extended[s3_constants.AmzServerSideEncryption]; exists && len(sseAlgorithm) > 0 {
			proxyResponse.Header.Set(s3_constants.AmzServerSideEncryption, string(sseAlgorithm))
		}

		if kmsKeyID, exists := entry.Extended[s3_constants.AmzServerSideEncryptionAwsKmsKeyId]; exists && len(kmsKeyID) > 0 {
			proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, string(kmsKeyID))
		}

	case s3_constants.SSETypeS3:
		// Add only SSE-S3 headers
		proxyResponse.Header.Set(s3_constants.AmzServerSideEncryption, SSES3Algorithm)

	default:
		// Unencrypted or unknown - don't set any SSE headers
	}

	glog.V(3).Infof("addSSEHeadersToResponse: processed %d extended metadata entries", len(entry.Extended))
}

// detectPrimarySSEType determines the primary SSE type by examining chunk metadata
func (s3a *S3ApiServer) detectPrimarySSEType(entry *filer_pb.Entry) string {
	// Safety check: handle nil entry
	if entry == nil {
		return "None"
	}

	if len(entry.GetChunks()) == 0 {
		// No chunks - check object-level metadata only (single objects or smallContent)
		hasSSEC := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] != nil
		hasSSEKMS := entry.Extended[s3_constants.AmzServerSideEncryption] != nil

		// Check for SSE-S3: algorithm is AES256 but no customer key
		if hasSSEKMS && !hasSSEC {
			// Distinguish SSE-S3 from SSE-KMS: check the algorithm value and the presence of a KMS key ID
			sseAlgo := string(entry.Extended[s3_constants.AmzServerSideEncryption])
			switch sseAlgo {
			case s3_constants.SSEAlgorithmAES256:
				// Could be SSE-S3 or SSE-KMS, check for KMS key ID
				if _, hasKMSKey := entry.Extended[s3_constants.AmzServerSideEncryptionAwsKmsKeyId]; hasKMSKey {
					return s3_constants.SSETypeKMS
				}
				// No KMS key, this is SSE-S3
				return s3_constants.SSETypeS3
			case s3_constants.SSEAlgorithmKMS:
				return s3_constants.SSETypeKMS
			default:
				// Unknown or unsupported algorithm
				return "None"
			}
		} else if hasSSEC && !hasSSEKMS {
			return s3_constants.SSETypeC
		} else if hasSSEC && hasSSEKMS {
			// Both present - this should only happen during cross-encryption copies
			// Use content to determine actual encryption state
			if len(entry.Content) > 0 {
				// smallContent - check if it's encrypted (heuristic: random-looking data)
				return s3_constants.SSETypeC // Default to SSE-C for mixed case
			} else {
				// No content, both headers - default to SSE-C
				return s3_constants.SSETypeC
			}
		}
		return "None"
	}

	// Count chunk types to determine primary (multipart objects)
	ssecChunks := 0
	ssekmsChunks := 0
	sses3Chunks := 0

	for _, chunk := range entry.GetChunks() {
		switch chunk.GetSseType() {
		case filer_pb.SSEType_SSE_C:
			ssecChunks++
		case filer_pb.SSEType_SSE_KMS:
			if len(chunk.GetSseMetadata()) > 0 {
				ssekmsChunks++
			}
		case filer_pb.SSEType_SSE_S3:
			if len(chunk.GetSseMetadata()) > 0 {
				sses3Chunks++
			}
		}
	}

	// Primary type is the one with more chunks
	// Note: Tie-breaking follows precedence order SSE-C > SSE-KMS > SSE-S3
	// Mixed encryption in an object indicates potential corruption and should not occur in normal operation
	if ssecChunks > ssekmsChunks && ssecChunks > sses3Chunks {
		return s3_constants.SSETypeC
	} else if ssekmsChunks > ssecChunks && ssekmsChunks > sses3Chunks {
		return s3_constants.SSETypeKMS
	} else if sses3Chunks > ssecChunks && sses3Chunks > ssekmsChunks {
		return s3_constants.SSETypeS3
	} else if ssecChunks > 0 {
		// Equal number or ties - precedence: SSE-C first
		return s3_constants.SSETypeC
	} else if ssekmsChunks > 0 {
		return s3_constants.SSETypeKMS
	} else if sses3Chunks > 0 {
		return s3_constants.SSETypeS3
	}

	return "None"
}

// createMultipartSSECDecryptedReaderDirect creates a reader that decrypts each chunk independently for multipart SSE-C objects (direct volume path)
// Note: encryptedStream parameter is unused (always nil) as this function fetches chunks directly to avoid double I/O.
// It's kept in the signature for API consistency with non-Direct versions.
func (s3a *S3ApiServer) createMultipartSSECDecryptedReaderDirect(ctx context.Context, encryptedStream io.ReadCloser, customerKey *SSECustomerKey, entry *filer_pb.Entry) (io.Reader, error) {
	// Sort chunks by offset to ensure correct order
	chunks := entry.GetChunks()
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetOffset() < chunks[j].GetOffset()
	})

	// Create readers for each chunk, decrypting them independently
	var readers []io.Reader

	for _, chunk := range chunks {
		// Get this chunk's encrypted data
		chunkReader, err := s3a.createEncryptedChunkReader(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %v", err)
		}

		// Handle based on chunk's encryption type
		if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
			// Check if this chunk has per-chunk SSE-C metadata
			if len(chunk.GetSseMetadata()) == 0 {
				chunkReader.Close()
				return nil, fmt.Errorf("SSE-C chunk %s missing per-chunk metadata", chunk.GetFileIdString())
			}

			// Deserialize the SSE-C metadata
			ssecMetadata, err := DeserializeSSECMetadata(chunk.GetSseMetadata())
			if err != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to deserialize SSE-C metadata for chunk %s: %v", chunk.GetFileIdString(), err)
			}

			// Decode the IV from the metadata
			chunkIV, err := base64.StdEncoding.DecodeString(ssecMetadata.IV)
			if err != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to decode IV for SSE-C chunk %s: %v", chunk.GetFileIdString(), err)
			}

			glog.V(4).Infof("Decrypting SSE-C chunk %s with IV=%x, PartOffset=%d",
				chunk.GetFileIdString(), chunkIV[:8], ssecMetadata.PartOffset)

			// Note: SSE-C multipart behavior (differs from SSE-KMS/SSE-S3):
			// - Upload: CreateSSECEncryptedReader generates RANDOM IV per part (no base IV + offset)
			// - Metadata: PartOffset tracks position within the encrypted stream
			// - Decryption: Use stored IV and advance CTR stream by PartOffset
			//
			// This differs from:
			// - SSE-KMS/SSE-S3: Use base IV + calculateIVWithOffset(partOffset) during encryption
			// - CopyObject: Applies calculateIVWithOffset to SSE-C (which may be incorrect)
			//
			// TODO: Investigate CopyObject SSE-C PartOffset handling for consistency
			partOffset := ssecMetadata.PartOffset
			if partOffset < 0 {
				chunkReader.Close()
				return nil, fmt.Errorf("invalid SSE-C part offset %d for chunk %s", partOffset, chunk.GetFileIdString())
			}
			decryptedChunkReader, decErr := CreateSSECDecryptedReaderWithOffset(chunkReader, customerKey, chunkIV, uint64(partOffset))
			if decErr != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to decrypt chunk: %v", decErr)
			}

			// Use the streaming decrypted reader directly
			readers = append(readers, struct {
				io.Reader
				io.Closer
			}{
				Reader: decryptedChunkReader,
				Closer: chunkReader,
			})
			glog.V(4).Infof("Added streaming decrypted reader for SSE-C chunk %s", chunk.GetFileIdString())
		} else {
			// Non-SSE-C chunk, use as-is
			readers = append(readers, chunkReader)
			glog.V(4).Infof("Added non-encrypted reader for chunk %s", chunk.GetFileIdString())
		}
	}

	// Close the original encrypted stream since we're reading chunks individually
	if encryptedStream != nil {
		encryptedStream.Close()
	}

	return NewMultipartSSEReader(readers), nil
}

// createMultipartSSEKMSDecryptedReaderDirect creates a reader that decrypts each chunk independently for multipart SSE-KMS objects (direct volume path)
// Note: encryptedStream parameter is unused (always nil) as this function fetches chunks directly to avoid double I/O.
// It's kept in the signature for API consistency with non-Direct versions.
func (s3a *S3ApiServer) createMultipartSSEKMSDecryptedReaderDirect(ctx context.Context, encryptedStream io.ReadCloser, entry *filer_pb.Entry) (io.Reader, error) {
	// Sort chunks by offset to ensure correct order
	chunks := entry.GetChunks()
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetOffset() < chunks[j].GetOffset()
	})

	// Create readers for each chunk, decrypting them independently
	var readers []io.Reader

	for _, chunk := range chunks {
		// Get this chunk's encrypted data
		chunkReader, err := s3a.createEncryptedChunkReader(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %v", err)
		}

		// Handle based on chunk's encryption type
		if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS {
			// Check if this chunk has per-chunk SSE-KMS metadata
			if len(chunk.GetSseMetadata()) == 0 {
				chunkReader.Close()
				return nil, fmt.Errorf("SSE-KMS chunk %s missing per-chunk metadata", chunk.GetFileIdString())
			}

			// Use the per-chunk SSE-KMS metadata
			kmsKey, err := DeserializeSSEKMSMetadata(chunk.GetSseMetadata())
			if err != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to deserialize SSE-KMS metadata for chunk %s: %v", chunk.GetFileIdString(), err)
			}

			glog.V(4).Infof("Decrypting SSE-KMS chunk %s with KeyID=%s",
				chunk.GetFileIdString(), kmsKey.KeyID)

			// Create decrypted reader for this chunk
			decryptedChunkReader, decErr := CreateSSEKMSDecryptedReader(chunkReader, kmsKey)
			if decErr != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to decrypt chunk: %v", decErr)
			}

			// Use the streaming decrypted reader directly
			readers = append(readers, struct {
				io.Reader
				io.Closer
			}{
				Reader: decryptedChunkReader,
				Closer: chunkReader,
			})
			glog.V(4).Infof("Added streaming decrypted reader for SSE-KMS chunk %s", chunk.GetFileIdString())
		} else {
			// Non-SSE-KMS chunk, use as-is
			readers = append(readers, chunkReader)
			glog.V(4).Infof("Added non-encrypted reader for chunk %s", chunk.GetFileIdString())
		}
	}

	// Close the original encrypted stream since we're reading chunks individually
	if encryptedStream != nil {
		encryptedStream.Close()
	}

	return NewMultipartSSEReader(readers), nil
}

// createMultipartSSES3DecryptedReaderDirect creates a reader that decrypts each chunk independently for multipart SSE-S3 objects (direct volume path)
// Note: encryptedStream parameter is unused (always nil) as this function fetches chunks directly to avoid double I/O.
// It's kept in the signature for API consistency with non-Direct versions.
func (s3a *S3ApiServer) createMultipartSSES3DecryptedReaderDirect(ctx context.Context, encryptedStream io.ReadCloser, entry *filer_pb.Entry) (io.Reader, error) {
	// Sort chunks by offset to ensure correct order
	chunks := entry.GetChunks()
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetOffset() < chunks[j].GetOffset()
	})

	// Create readers for each chunk, decrypting them independently
	var readers []io.Reader

	// Get key manager and SSE-S3 key from entry metadata
	keyManager := GetSSES3KeyManager()
	keyData := entry.Extended[s3_constants.SeaweedFSSSES3Key]
	sseS3Key, err := DeserializeSSES3Metadata(keyData, keyManager)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize SSE-S3 key from entry metadata: %v", err)
	}

	for _, chunk := range chunks {
		// Get this chunk's encrypted data
		chunkReader, err := s3a.createEncryptedChunkReader(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %v", err)
		}

		// Handle based on chunk's encryption type
		if chunk.GetSseType() == filer_pb.SSEType_SSE_S3 {
			// Check if this chunk has per-chunk SSE-S3 metadata
			if len(chunk.GetSseMetadata()) == 0 {
				chunkReader.Close()
				return nil, fmt.Errorf("SSE-S3 chunk %s missing per-chunk metadata", chunk.GetFileIdString())
			}

			// Deserialize the per-chunk SSE-S3 metadata to get the IV
			chunkSSES3Metadata, err := DeserializeSSES3Metadata(chunk.GetSseMetadata(), keyManager)
			if err != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to deserialize SSE-S3 metadata for chunk %s: %v", chunk.GetFileIdString(), err)
			}

			// Use the IV from the chunk metadata
			iv := chunkSSES3Metadata.IV
			glog.V(4).Infof("Decrypting SSE-S3 chunk %s with KeyID=%s, IV length=%d",
				chunk.GetFileIdString(), sseS3Key.KeyID, len(iv))

			// Create decrypted reader for this chunk
			decryptedChunkReader, decErr := CreateSSES3DecryptedReader(chunkReader, sseS3Key, iv)
			if decErr != nil {
				chunkReader.Close()
				return nil, fmt.Errorf("failed to decrypt SSE-S3 chunk: %v", decErr)
			}

			// Use the streaming decrypted reader directly
			readers = append(readers, struct {
				io.Reader
				io.Closer
			}{
				Reader: decryptedChunkReader,
				Closer: chunkReader,
			})
			glog.V(4).Infof("Added streaming decrypted reader for SSE-S3 chunk %s", chunk.GetFileIdString())
		} else {
			// Non-SSE-S3 chunk, use as-is
			readers = append(readers, chunkReader)
			glog.V(4).Infof("Added non-encrypted reader for chunk %s", chunk.GetFileIdString())
		}
	}

	// Close the original encrypted stream since we're reading chunks individually
	if encryptedStream != nil {
		encryptedStream.Close()
	}

	return NewMultipartSSEReader(readers), nil
}

// createEncryptedChunkReader creates a reader for a single encrypted chunk
// Context propagation ensures cancellation if the S3 client disconnects
func (s3a *S3ApiServer) createEncryptedChunkReader(ctx context.Context, chunk *filer_pb.FileChunk) (io.ReadCloser, error) {
	// Get chunk URL
	srcUrl, err := s3a.lookupVolumeUrl(chunk.GetFileIdString())
	if err != nil {
		return nil, fmt.Errorf("lookup volume URL for chunk %s: %v", chunk.GetFileIdString(), err)
	}

	// Create HTTP request with context for cancellation propagation
	req, err := http.NewRequestWithContext(ctx, "GET", srcUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request for chunk: %v", err)
	}

	// Attach volume server JWT for authentication (uses config loaded once at startup)
	jwt := filer.JwtForVolumeServer(chunk.GetFileIdString())
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+jwt)
	}

	// Use shared HTTP client with connection pooling
	resp, err := volumeServerHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute HTTP request for chunk: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP request for chunk failed: %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// MultipartSSEReader wraps multiple readers and ensures all underlying readers are properly closed
type MultipartSSEReader struct {
	multiReader io.Reader
	readers     []io.Reader
}

// SSERangeReader applies range logic to an underlying reader
type SSERangeReader struct {
	reader    io.Reader
	offset    int64  // bytes to skip from the beginning
	remaining int64  // bytes remaining to read (-1 for unlimited)
	skipped   int64  // bytes already skipped
	skipBuf   []byte // reusable buffer for skipping bytes (avoids per-call allocation)
}

// NewMultipartSSEReader creates a new multipart reader that can properly close all underlying readers
func NewMultipartSSEReader(readers []io.Reader) *MultipartSSEReader {
	return &MultipartSSEReader{
		multiReader: io.MultiReader(readers...),
		readers:     readers,
	}
}

// Read implements the io.Reader interface
func (m *MultipartSSEReader) Read(p []byte) (n int, err error) {
	return m.multiReader.Read(p)
}

// Close implements the io.Closer interface and closes all underlying readers that support closing
func (m *MultipartSSEReader) Close() error {
	var lastErr error
	for i, reader := range m.readers {
		if closer, ok := reader.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				glog.V(2).Infof("Error closing reader %d: %v", i, err)
				lastErr = err // Keep track of the last error, but continue closing others
			}
		}
	}
	return lastErr
}

// Read implements the io.Reader interface for SSERangeReader
func (r *SSERangeReader) Read(p []byte) (n int, err error) {
	// Skip bytes iteratively (no recursion) until we reach the offset
	for r.skipped < r.offset {
		skipNeeded := r.offset - r.skipped

		// Lazily allocate skip buffer on first use, reuse thereafter
		if r.skipBuf == nil {
			// Use a fixed 32KB buffer for skipping (avoids per-call allocation)
			r.skipBuf = make([]byte, 32*1024)
		}

		// Determine how much to skip in this iteration
		bufSize := int64(len(r.skipBuf))
		if skipNeeded < bufSize {
			bufSize = skipNeeded
		}

		skipRead, skipErr := r.reader.Read(r.skipBuf[:bufSize])
		r.skipped += int64(skipRead)

		if skipErr != nil {
			return 0, skipErr
		}

		// Guard against infinite loop: io.Reader may return (0, nil)
		// which is permitted by the interface contract for non-empty buffers.
		// If we get zero bytes without an error, treat it as an unexpected EOF.
		if skipRead == 0 {
			return 0, io.ErrUnexpectedEOF
		}
	}

	// If we have a remaining limit and it's reached
	if r.remaining == 0 {
		return 0, io.EOF
	}

	// Calculate how much to read
	readSize := len(p)
	if r.remaining > 0 && int64(readSize) > r.remaining {
		readSize = int(r.remaining)
	}

	// Read the data
	n, err = r.reader.Read(p[:readSize])
	if r.remaining > 0 {
		r.remaining -= int64(n)
	}

	return n, err
}

// createMultipartSSECDecryptedReader creates a decrypted reader for multipart SSE-C objects
// Each chunk has its own IV and encryption key from the original multipart parts
func (s3a *S3ApiServer) createMultipartSSECDecryptedReader(r *http.Request, proxyResponse *http.Response, entry *filer_pb.Entry) (io.Reader, error) {
	ctx := r.Context()

	// Parse SSE-C headers from the request for decryption key
	customerKey, err := ParseSSECHeaders(r)
	if err != nil {
		return nil, fmt.Errorf("invalid SSE-C headers for multipart decryption: %v", err)
	}

	// Entry is passed from caller to avoid redundant filer lookup

	// Sort chunks by offset to ensure correct order
	chunks := entry.GetChunks()
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetOffset() < chunks[j].GetOffset()
	})

	// Check for Range header to optimize chunk processing
	var startOffset, endOffset int64 = 0, -1
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Parse range header (e.g., "bytes=0-99")
		if len(rangeHeader) > 6 && rangeHeader[:6] == "bytes=" {
			rangeSpec := rangeHeader[6:]
			parts := strings.Split(rangeSpec, "-")
			if len(parts) == 2 {
				if parts[0] != "" {
					startOffset, _ = strconv.ParseInt(parts[0], 10, 64)
				}
				if parts[1] != "" {
					endOffset, _ = strconv.ParseInt(parts[1], 10, 64)
				}
			}
		}
	}

	// Filter chunks to only those needed for the range request
	var neededChunks []*filer_pb.FileChunk
	for _, chunk := range chunks {
		chunkStart := chunk.GetOffset()
		chunkEnd := chunkStart + int64(chunk.GetSize()) - 1

		// Check if this chunk overlaps with the requested range
		if endOffset == -1 {
			// No end specified, take all chunks from startOffset
			if chunkEnd >= startOffset {
				neededChunks = append(neededChunks, chunk)
			}
		} else {
			// Specific range: check for overlap
			if chunkStart <= endOffset && chunkEnd >= startOffset {
				neededChunks = append(neededChunks, chunk)
			}
		}
	}

	// Create readers for only the needed chunks
	var readers []io.Reader

	for _, chunk := range neededChunks {

		// Get this chunk's encrypted data
		chunkReader, err := s3a.createEncryptedChunkReader(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %v", err)
		}

		if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
			// For SSE-C chunks, extract the IV from the stored per-chunk metadata (unified approach)
			if len(chunk.GetSseMetadata()) > 0 {
				// Deserialize the SSE-C metadata stored in the unified metadata field
				ssecMetadata, decErr := DeserializeSSECMetadata(chunk.GetSseMetadata())
				if decErr != nil {
					chunkReader.Close()
					return nil, fmt.Errorf("failed to deserialize SSE-C metadata for chunk %s: %v", chunk.GetFileIdString(), decErr)
				}

				// Decode the IV from the metadata
				iv, ivErr := base64.StdEncoding.DecodeString(ssecMetadata.IV)
				if ivErr != nil {
					chunkReader.Close()
					return nil, fmt.Errorf("failed to decode IV for SSE-C chunk %s: %v", chunk.GetFileIdString(), ivErr)
				}

				partOffset := ssecMetadata.PartOffset
				if partOffset < 0 {
					chunkReader.Close()
					return nil, fmt.Errorf("invalid SSE-C part offset %d for chunk %s", partOffset, chunk.GetFileIdString())
				}

				// Use stored IV and advance CTR stream by PartOffset within the encrypted stream
				decryptedReader, decErr := CreateSSECDecryptedReaderWithOffset(chunkReader, customerKey, iv, uint64(partOffset))
				if decErr != nil {
					chunkReader.Close()
					return nil, fmt.Errorf("failed to create SSE-C decrypted reader for chunk %s: %v", chunk.GetFileIdString(), decErr)
				}
				readers = append(readers, decryptedReader)
			} else {
				chunkReader.Close()
				return nil, fmt.Errorf("SSE-C chunk %s missing required metadata", chunk.GetFileIdString())
			}
		} else {
			// Non-SSE-C chunk, use as-is
			readers = append(readers, chunkReader)
		}
	}

	multiReader := NewMultipartSSEReader(readers)

	// Apply range logic if a range was requested
	if rangeHeader != "" && startOffset >= 0 {
		if endOffset == -1 {
			// Open-ended range (e.g., "bytes=100-")
			return &SSERangeReader{
				reader:    multiReader,
				offset:    startOffset,
				remaining: -1, // Read until EOF
			}, nil
		} else {
			// Specific range (e.g., "bytes=0-99")
			rangeLength := endOffset - startOffset + 1
			return &SSERangeReader{
				reader:    multiReader,
				offset:    startOffset,
				remaining: rangeLength,
			}, nil
		}
	}

	return multiReader, nil
}

// PartBoundaryInfo holds information about a part's chunk boundaries
type PartBoundaryInfo struct {
	PartNumber int    `json:"part"`
	StartChunk int    `json:"start"`
	EndChunk   int    `json:"end"` // exclusive
	ETag       string `json:"etag"`
}

// rc is a helper type that wraps a Reader and Closer for proper resource cleanup
type rc struct {
	io.Reader
	io.Closer
}

// getMultipartInfo retrieves multipart metadata for a given part number
// Returns: (partsCount, partInfo)
// - partsCount: total number of parts in the multipart object
// - partInfo: boundary information for the requested part (nil if not found or not a multipart object)
func (s3a *S3ApiServer) getMultipartInfo(entry *filer_pb.Entry, partNumber int) (int, *PartBoundaryInfo) {
	if entry == nil {
		return 0, nil
	}
	if entry.Extended == nil {
		// Not a multipart object or no metadata
		return len(entry.GetChunks()), nil
	}

	// Try to get parts count from metadata
	partsCount := len(entry.GetChunks()) // default fallback
	if partsCountBytes, exists := entry.Extended[s3_constants.SeaweedFSMultipartPartsCount]; exists {
		if count, err := strconv.Atoi(string(partsCountBytes)); err == nil && count > 0 {
			partsCount = count
		}
	}

	// Try to get part boundaries from metadata
	if boundariesJSON, exists := entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries]; exists {
		var boundaries []PartBoundaryInfo
		if err := json.Unmarshal(boundariesJSON, &boundaries); err == nil {
			// Find the requested part
			for i := range boundaries {
				if boundaries[i].PartNumber == partNumber {
					return partsCount, &boundaries[i]
				}
			}
		}
	}

	// No part boundaries metadata or part not found
	return partsCount, nil
}

// buildRemoteObjectPath builds the filer directory and object name from S3 bucket/object.
// This is shared by all remote object caching functions.
func (s3a *S3ApiServer) buildRemoteObjectPath(bucket, object string) (dir, name string) {
	dir = s3a.option.BucketsPath + "/" + bucket
	name = s3_constants.NormalizeObjectKey(object)
	if idx := strings.LastIndex(name, "/"); idx > 0 {
		dir = dir + "/" + name[:idx]
		name = name[idx+1:]
	}
	return dir, name
}

// doCacheRemoteObject calls the filer's CacheRemoteObjectToLocalCluster gRPC endpoint.
// This is the core caching function used by both cacheRemoteObjectWithDedup and cacheRemoteObjectForStreaming.
func (s3a *S3ApiServer) doCacheRemoteObject(ctx context.Context, dir, name string) (*filer_pb.Entry, error) {
	var cachedEntry *filer_pb.Entry
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, cacheErr := client.CacheRemoteObjectToLocalCluster(ctx, &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory: dir,
			Name:      name,
		})
		if cacheErr != nil {
			return cacheErr
		}
		if resp != nil && resp.Entry != nil {
			cachedEntry = resp.Entry
		}
		return nil
	})
	return cachedEntry, err
}

// cacheRemoteObjectWithDedup caches a remote-only object to the local cluster.
// The filer server handles singleflight deduplication, so all clients (S3, HTTP, Hadoop) benefit.
// On cache error, returns the original entry (will retry in streamFromVolumeServers).
// Uses a bounded timeout to avoid blocking requests indefinitely.
func (s3a *S3ApiServer) cacheRemoteObjectWithDedup(ctx context.Context, bucket, object string, entry *filer_pb.Entry) *filer_pb.Entry {
	const cacheTimeout = 30 * time.Second
	cacheCtx, cancel := context.WithTimeout(ctx, cacheTimeout)
	defer cancel()

	dir, name := s3a.buildRemoteObjectPath(bucket, object)
	glog.V(2).Infof("cacheRemoteObjectWithDedup: caching %s/%s (remote size: %d)", bucket, object, entry.RemoteEntry.RemoteSize)

	cachedEntry, err := s3a.doCacheRemoteObject(cacheCtx, dir, name)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			glog.V(1).Infof("cacheRemoteObjectWithDedup: timeout caching %s/%s after %v (will retry in streaming)", bucket, object, cacheTimeout)
		} else {
			glog.Warningf("cacheRemoteObjectWithDedup: failed to cache %s/%s: %v (will retry in streaming)", bucket, object, err)
		}
		return entry
	}

	if cachedEntry != nil && len(cachedEntry.GetChunks()) > 0 {
		glog.V(1).Infof("cacheRemoteObjectWithDedup: successfully cached %s/%s (%d chunks)", bucket, object, len(cachedEntry.GetChunks()))
		entry.Chunks = cachedEntry.Chunks
	}

	return entry
}

// cacheRemoteObjectForStreaming caches a remote-only object to the local cluster for streaming.
// This is called from streamFromVolumeServers when the initial caching attempt timed out or failed.
// Uses the request context (no artificial timeout) to allow the caching to complete.
// For versioned objects, versionId determines the correct path in .versions/ directory.
func (s3a *S3ApiServer) cacheRemoteObjectForStreaming(r *http.Request, entry *filer_pb.Entry, bucket, object, versionId string) *filer_pb.Entry {
	var dir, name string
	if versionId != "" && versionId != "null" {
		// This is a specific version - entry is located at /buckets/<bucket>/<object>.versions/v_<versionId>
		normalizedObject := s3_constants.NormalizeObjectKey(object)
		dir = s3a.option.BucketsPath + "/" + bucket + "/" + normalizedObject + s3_constants.VersionsFolder
		name = s3a.getVersionFileName(versionId)
	} else {
		// Non-versioned object or "null" version - lives at the main path
		dir, name = s3a.buildRemoteObjectPath(bucket, object)
	}

	glog.V(1).Infof("cacheRemoteObjectForStreaming: caching %s/%s (remote size: %d, versionId: %s)", dir, name, entry.RemoteEntry.RemoteSize, versionId)

	cachedEntry, err := s3a.doCacheRemoteObject(r.Context(), dir, name)
	if err != nil {
		glog.Errorf("cacheRemoteObjectForStreaming: failed to cache %s/%s: %v", dir, name, err)
		return nil
	}

	if cachedEntry != nil && len(cachedEntry.GetChunks()) > 0 {
		glog.V(1).Infof("cacheRemoteObjectForStreaming: successfully cached %s/%s (%d chunks)", dir, name, len(cachedEntry.GetChunks()))
		return cachedEntry
	}

	return nil
}
