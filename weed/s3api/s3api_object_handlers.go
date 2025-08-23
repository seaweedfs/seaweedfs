package s3api

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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
	t := urlPathEscape(removeDuplicateSlashes(object))
	if strings.HasPrefix(t, "/") {
		return t
	}
	return "/" + t
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
	cleanObject := strings.TrimSuffix(strings.TrimPrefix(object, "/"), "/")

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

// serveDirectoryContent serves the content of a directory object directly
func (s3a *S3ApiServer) serveDirectoryContent(w http.ResponseWriter, r *http.Request, entry *filer_pb.Entry) {
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
	listEntry = ListEntry{
		Key:          key,
		LastModified: time.Unix(entry.Attributes.Mtime, 0).UTC(),
		ETag:         "\"" + filer.ETag(entry) + "\"",
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

func (s3a *S3ApiServer) toFilerUrl(bucket, object string) string {
	object = urlPathEscape(removeDuplicateSlashes(object))
	destUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, bucket, object)
	return destUrl
}

func (s3a *S3ApiServer) GetObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectHandler %s %s", bucket, object)

	// Handle directory objects with shared logic
	if s3a.handleDirectoryObjectRequest(w, r, bucket, object, "GetObjectHandler") {
		return // Directory object request was handled
	}

	// Check conditional headers for read operations
	result := s3a.checkConditionalHeadersForReads(r, bucket, object)
	if result.ErrorCode != s3err.ErrNone {
		glog.V(3).Infof("GetObjectHandler: Conditional header check failed for %s/%s with error %v", bucket, object, result.ErrorCode)

		// For 304 Not Modified responses, include the ETag header
		if result.ErrorCode == s3err.ErrNotModified && result.ETag != "" {
			w.Header().Set("ETag", result.ETag)
		}

		s3err.WriteErrorResponse(w, r, result.ErrorCode)
		return
	}

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(1).Infof("GetObject: bucket %s, object %s, versioningConfigured=%v, versionId=%s", bucket, object, versioningConfigured, versionId)

	var destUrl string

	if versioningConfigured {
		// Handle versioned GET - all versions are stored in .versions directory
		var targetVersionId string
		var entry *filer_pb.Entry

		if versionId != "" {
			// Request for specific version
			glog.V(2).Infof("GetObject: requesting specific version %s for %s%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
			if err != nil {
				glog.Errorf("Failed to get specific version %s: %v", versionId, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
			targetVersionId = versionId
		} else {
			// Request for latest version
			glog.V(1).Infof("GetObject: requesting latest version for %s%s", bucket, object)
			entry, err = s3a.getLatestObjectVersion(bucket, object)
			if err != nil {
				glog.Errorf("GetObject: Failed to get latest version for %s%s: %v", bucket, object, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
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

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}

		// Determine the actual file path based on whether this is a versioned or pre-versioning object
		if targetVersionId == "null" {
			// Pre-versioning object - stored as regular file
			destUrl = s3a.toFilerUrl(bucket, object)
			glog.V(2).Infof("GetObject: pre-versioning object URL: %s", destUrl)
		} else {
			// Versioned object - stored in .versions directory
			versionObjectPath := object + ".versions/" + s3a.getVersionFileName(targetVersionId)
			destUrl = s3a.toFilerUrl(bucket, versionObjectPath)
			glog.V(2).Infof("GetObject: version %s URL: %s", targetVersionId, destUrl)
		}

		// Set version ID in response header
		w.Header().Set("x-amz-version-id", targetVersionId)

		// Add object lock metadata to response headers if present
		s3a.addObjectLockHeadersToResponse(w, entry)
	} else {
		// Handle regular GET (non-versioned)
		destUrl = s3a.toFilerUrl(bucket, object)
	}

	// Check if this is a range request to an SSE object and modify the approach
	originalRangeHeader := r.Header.Get("Range")
	var sseObject = false

	// Pre-check if this object is SSE encrypted to avoid filer range conflicts
	if originalRangeHeader != "" {
		bucket, object := s3_constants.GetBucketAndObject(r)
		objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)
		if objectEntry, err := s3a.getEntry("", objectPath); err == nil {
			primarySSEType := s3a.detectPrimarySSEType(objectEntry)
			if primarySSEType == s3_constants.SSETypeC || primarySSEType == s3_constants.SSETypeKMS {
				sseObject = true
				// Temporarily remove Range header to get full encrypted data from filer
				r.Header.Del("Range")

			}
		}
	}

	s3a.proxyToFiler(w, r, destUrl, false, func(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
		// Restore the original Range header for SSE processing
		if sseObject && originalRangeHeader != "" {
			r.Header.Set("Range", originalRangeHeader)

		}

		// Add SSE metadata headers based on object metadata before SSE processing
		bucket, object := s3_constants.GetBucketAndObject(r)
		objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)
		if objectEntry, err := s3a.getEntry("", objectPath); err == nil {
			s3a.addSSEHeadersToResponse(proxyResponse, objectEntry)
		}

		// Handle SSE decryption (both SSE-C and SSE-KMS) if needed
		return s3a.handleSSEResponse(r, proxyResponse, w)
	})
}

func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("HeadObjectHandler %s %s", bucket, object)

	// Handle directory objects with shared logic
	if s3a.handleDirectoryObjectRequest(w, r, bucket, object, "HeadObjectHandler") {
		return // Directory object request was handled
	}

	// Check conditional headers for read operations
	result := s3a.checkConditionalHeadersForReads(r, bucket, object)
	if result.ErrorCode != s3err.ErrNone {
		glog.V(3).Infof("HeadObjectHandler: Conditional header check failed for %s/%s with error %v", bucket, object, result.ErrorCode)

		// For 304 Not Modified responses, include the ETag header
		if result.ErrorCode == s3err.ErrNotModified && result.ETag != "" {
			w.Header().Set("ETag", result.ETag)
		}

		s3err.WriteErrorResponse(w, r, result.ErrorCode)
		return
	}

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	// Check if versioning is configured for the bucket (Enabled or Suspended)
	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var destUrl string

	if versioningConfigured {
		// Handle versioned HEAD - all versions are stored in .versions directory
		var targetVersionId string
		var entry *filer_pb.Entry

		if versionId != "" {
			// Request for specific version
			glog.V(2).Infof("HeadObject: requesting specific version %s for %s%s", versionId, bucket, object)
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
			if err != nil {
				glog.Errorf("Failed to get specific version %s: %v", versionId, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
			targetVersionId = versionId
		} else {
			// Request for latest version
			glog.V(2).Infof("HeadObject: requesting latest version for %s%s", bucket, object)
			entry, err = s3a.getLatestObjectVersion(bucket, object)
			if err != nil {
				glog.Errorf("Failed to get latest version: %v", err)
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
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

		// Check if this is a delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}

		// Determine the actual file path based on whether this is a versioned or pre-versioning object
		if targetVersionId == "null" {
			// Pre-versioning object - stored as regular file
			destUrl = s3a.toFilerUrl(bucket, object)
			glog.V(2).Infof("HeadObject: pre-versioning object URL: %s", destUrl)
		} else {
			// Versioned object - stored in .versions directory
			versionObjectPath := object + ".versions/" + s3a.getVersionFileName(targetVersionId)
			destUrl = s3a.toFilerUrl(bucket, versionObjectPath)
			glog.V(2).Infof("HeadObject: version %s URL: %s", targetVersionId, destUrl)
		}

		// Set version ID in response header
		w.Header().Set("x-amz-version-id", targetVersionId)

		// Add object lock metadata to response headers if present
		s3a.addObjectLockHeadersToResponse(w, entry)
	} else {
		// Handle regular HEAD (non-versioned)
		destUrl = s3a.toFilerUrl(bucket, object)
	}

	s3a.proxyToFiler(w, r, destUrl, false, func(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
		// Handle SSE validation (both SSE-C and SSE-KMS) for HEAD requests
		return s3a.handleSSEResponse(r, proxyResponse, w)
	})
}

func (s3a *S3ApiServer) proxyToFiler(w http.ResponseWriter, r *http.Request, destUrl string, isWrite bool, responseFn func(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64)) {

	glog.V(3).Infof("s3 proxying %s to %s", r.Method, destUrl)
	start := time.Now()

	proxyReq, err := http.NewRequest(r.Method, destUrl, r.Body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", destUrl, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("Accept-Encoding", "identity")
	for k, v := range r.URL.Query() {
		if _, ok := s3_constants.PassThroughHeaders[strings.ToLower(k)]; ok {
			proxyReq.Header[k] = v
		}
		if k == "partNumber" {
			proxyReq.Header[s3_constants.SeaweedFSPartNumber] = v
		}
	}
	for header, values := range r.Header {
		proxyReq.Header[header] = values
	}
	if proxyReq.ContentLength == 0 && r.ContentLength != 0 {
		proxyReq.ContentLength = r.ContentLength
	}

	// ensure that the Authorization header is overriding any previous
	// Authorization header which might be already present in proxyReq
	s3a.maybeAddFilerJwtAuthorization(proxyReq, isWrite)
	resp, postErr := s3a.client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	defer util_http.CloseResponse(resp)

	if resp.StatusCode == http.StatusPreconditionFailed {
		s3err.WriteErrorResponse(w, r, s3err.ErrPreconditionFailed)
		return
	}

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
		return
	}

	if r.Method == http.MethodDelete {
		if resp.StatusCode == http.StatusNotFound {
			// this is normal
			responseStatusCode, _ := responseFn(resp, w)
			s3err.PostLog(r, responseStatusCode, s3err.ErrNone)
			return
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	TimeToFirstByte(r.Method, start, r)
	if resp.Header.Get(s3_constants.SeaweedFSIsDirectoryKey) == "true" {
		responseStatusCode, _ := responseFn(resp, w)
		s3err.PostLog(r, responseStatusCode, s3err.ErrNone)
		return
	}

	if resp.StatusCode == http.StatusInternalServerError {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// when HEAD a directory, it should be reported as no such key
	// https://github.com/seaweedfs/seaweedfs/issues/3457
	if resp.ContentLength == -1 && resp.StatusCode != http.StatusNotModified {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	if resp.StatusCode == http.StatusBadRequest {
		resp_body, _ := io.ReadAll(resp.Body)
		switch string(resp_body) {
		case "InvalidPart":
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		default:
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		}
		resp.Body.Close()
		return
	}

	setUserMetadataKeyToLowercase(resp)

	responseStatusCode, bytesTransferred := responseFn(resp, w)
	BucketTrafficSent(bytesTransferred, r)

	s3err.PostLog(r, responseStatusCode, s3err.ErrNone)
}

func setUserMetadataKeyToLowercase(resp *http.Response) {
	for key, value := range resp.Header {
		if strings.HasPrefix(key, s3_constants.AmzUserMetaPrefix) {
			resp.Header[strings.ToLower(key)] = value
			delete(resp.Header, key)
		}
	}
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

func passThroughResponse(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
	// Capture existing CORS headers that may have been set by middleware
	capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

	// Copy headers from proxy response
	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}

	return writeFinalResponse(w, proxyResponse, proxyResponse.Body, capturedCORSHeaders)
}

// handleSSECResponse handles SSE-C decryption and response processing
func (s3a *S3ApiServer) handleSSECResponse(r *http.Request, proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
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
		bucket, object := s3_constants.GetBucketAndObject(r)
		objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)
		if entry, err := s3a.getEntry("", objectPath); err == nil {
			// Check for SSE-C chunks
			sseCChunks := 0
			for _, chunk := range entry.GetChunks() {
				if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
					sseCChunks++
				}
			}

			if sseCChunks >= 1 {

				// Handle chunked SSE-C objects - each chunk needs independent decryption
				multipartReader, decErr := s3a.createMultipartSSECDecryptedReader(r, proxyResponse)
				if decErr != nil {
					glog.Errorf("Failed to create multipart SSE-C decrypted reader: %v", decErr)
					s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
					return http.StatusInternalServerError, 0
				}

				// Capture existing CORS headers
				capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

				// Copy headers from proxy response
				for k, v := range proxyResponse.Header {
					w.Header()[k] = v
				}

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

		// Copy headers from proxy response (excluding body-related headers that might change)
		for k, v := range proxyResponse.Header {
			if k != "Content-Length" && k != "Content-Encoding" {
				w.Header()[k] = v
			}
		}

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

// handleSSEResponse handles both SSE-C and SSE-KMS decryption/validation and response processing
func (s3a *S3ApiServer) handleSSEResponse(r *http.Request, proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
	// Check what the client is expecting based on request headers
	clientExpectsSSEC := IsSSECRequest(r)

	// Check what the stored object has in headers (may be conflicting after copy)
	kmsMetadataHeader := proxyResponse.Header.Get(s3_constants.SeaweedFSSSEKMSKeyHeader)
	sseAlgorithm := proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)

	// Get actual object state by examining chunks (most reliable for cross-encryption)
	bucket, object := s3_constants.GetBucketAndObject(r)
	objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)
	actualObjectType := "Unknown"
	if objectEntry, err := s3a.getEntry("", objectPath); err == nil {
		actualObjectType = s3a.detectPrimarySSEType(objectEntry)
	}

	// Route based on ACTUAL object type (from chunks) rather than conflicting headers
	if actualObjectType == s3_constants.SSETypeC && clientExpectsSSEC {
		// Object is SSE-C and client expects SSE-C → SSE-C handler
		return s3a.handleSSECResponse(r, proxyResponse, w)
	} else if actualObjectType == s3_constants.SSETypeKMS && !clientExpectsSSEC {
		// Object is SSE-KMS and client doesn't expect SSE-C → SSE-KMS handler
		return s3a.handleSSEKMSResponse(r, proxyResponse, w, kmsMetadataHeader)
	} else if actualObjectType == "None" && !clientExpectsSSEC {
		// Object is unencrypted and client doesn't expect SSE-C → pass through
		return passThroughResponse(proxyResponse, w)
	} else if actualObjectType == s3_constants.SSETypeC && !clientExpectsSSEC {
		// Object is SSE-C but client doesn't provide SSE-C headers → Error
		s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyMissing)
		return http.StatusBadRequest, 0
	} else if actualObjectType == s3_constants.SSETypeKMS && clientExpectsSSEC {
		// Object is SSE-KMS but client provides SSE-C headers → Error
		s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyMissing)
		return http.StatusBadRequest, 0
	} else if actualObjectType == "None" && clientExpectsSSEC {
		// Object is unencrypted but client provides SSE-C headers → Error
		s3err.WriteErrorResponse(w, r, s3err.ErrSSECustomerKeyMissing)
		return http.StatusBadRequest, 0
	}

	// Fallback for edge cases - use original logic with header-based detection
	if clientExpectsSSEC && sseAlgorithm != "" {
		return s3a.handleSSECResponse(r, proxyResponse, w)
	} else if !clientExpectsSSEC && kmsMetadataHeader != "" {
		return s3a.handleSSEKMSResponse(r, proxyResponse, w, kmsMetadataHeader)
	} else {
		return passThroughResponse(proxyResponse, w)
	}
}

// handleSSEKMSResponse handles SSE-KMS decryption and response processing
func (s3a *S3ApiServer) handleSSEKMSResponse(r *http.Request, proxyResponse *http.Response, w http.ResponseWriter, kmsMetadataHeader string) (statusCode int, bytesTransferred int64) {
	// Deserialize SSE-KMS metadata
	kmsMetadataBytes, err := base64.StdEncoding.DecodeString(kmsMetadataHeader)
	if err != nil {
		glog.Errorf("Failed to decode SSE-KMS metadata: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return http.StatusInternalServerError, 0
	}

	sseKMSKey, err := DeserializeSSEKMSMetadata(kmsMetadataBytes)
	if err != nil {
		glog.Errorf("Failed to deserialize SSE-KMS metadata: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return http.StatusInternalServerError, 0
	}

	// For HEAD requests, we don't need to decrypt the body, just add response headers
	if r.Method == "HEAD" {
		// Capture existing CORS headers that may have been set by middleware
		capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

		// Copy headers from proxy response
		for k, v := range proxyResponse.Header {
			w.Header()[k] = v
		}

		// Add SSE-KMS response headers
		AddSSEKMSResponseHeaders(w, sseKMSKey)

		return writeFinalResponse(w, proxyResponse, proxyResponse.Body, capturedCORSHeaders)
	}

	// For GET requests, check if this is a multipart SSE-KMS object
	// We need to check the object structure to determine if it's multipart encrypted
	isMultipartSSEKMS := false

	if sseKMSKey != nil {
		// Get the object entry to check chunk structure
		bucket, object := s3_constants.GetBucketAndObject(r)
		objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)
		if entry, err := s3a.getEntry("", objectPath); err == nil {
			// Check for multipart SSE-KMS
			sseKMSChunks := 0
			for _, chunk := range entry.GetChunks() {
				if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS && len(chunk.GetSseMetadata()) > 0 {
					sseKMSChunks++
				}
			}
			isMultipartSSEKMS = sseKMSChunks > 1

			glog.Infof("SSE-KMS object detection: chunks=%d, sseKMSChunks=%d, isMultipartSSEKMS=%t",
				len(entry.GetChunks()), sseKMSChunks, isMultipartSSEKMS)
		}
	}

	var decryptedReader io.Reader
	if isMultipartSSEKMS {
		// Handle multipart SSE-KMS objects - each chunk needs independent decryption
		multipartReader, decErr := s3a.createMultipartSSEKMSDecryptedReader(r, proxyResponse)
		if decErr != nil {
			glog.Errorf("Failed to create multipart SSE-KMS decrypted reader: %v", decErr)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return http.StatusInternalServerError, 0
		}
		decryptedReader = multipartReader
		glog.V(3).Infof("Using multipart SSE-KMS decryption for object")
	} else {
		// Handle single-part SSE-KMS objects
		singlePartReader, decErr := CreateSSEKMSDecryptedReader(proxyResponse.Body, sseKMSKey)
		if decErr != nil {
			glog.Errorf("Failed to create SSE-KMS decrypted reader: %v", decErr)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return http.StatusInternalServerError, 0
		}
		decryptedReader = singlePartReader
		glog.V(3).Infof("Using single-part SSE-KMS decryption for object")
	}

	// Capture existing CORS headers that may have been set by middleware
	capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

	// Copy headers from proxy response (excluding body-related headers that might change)
	for k, v := range proxyResponse.Header {
		if k != "Content-Length" && k != "Content-Encoding" {
			w.Header()[k] = v
		}
	}

	// Set correct Content-Length for SSE-KMS
	if proxyResponse.Header.Get("Content-Range") == "" {
		// For full object requests, encrypted length equals original length
		if contentLengthStr := proxyResponse.Header.Get("Content-Length"); contentLengthStr != "" {
			w.Header().Set("Content-Length", contentLengthStr)
		}
	}

	// Add SSE-KMS response headers
	AddSSEKMSResponseHeaders(w, sseKMSKey)

	return writeFinalResponse(w, proxyResponse, decryptedReader, capturedCORSHeaders)
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

	default:
		// Unencrypted or unknown - don't set any SSE headers
	}

	glog.V(3).Infof("addSSEHeadersToResponse: processed %d extended metadata entries", len(entry.Extended))
}

// detectPrimarySSEType determines the primary SSE type by examining chunk metadata
func (s3a *S3ApiServer) detectPrimarySSEType(entry *filer_pb.Entry) string {
	if len(entry.GetChunks()) == 0 {
		// No chunks - check object-level metadata only (single objects or smallContent)
		hasSSEC := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] != nil
		hasSSEKMS := entry.Extended[s3_constants.AmzServerSideEncryption] != nil

		if hasSSEC && !hasSSEKMS {
			return s3_constants.SSETypeC
		} else if hasSSEKMS && !hasSSEC {
			return s3_constants.SSETypeKMS
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

	for _, chunk := range entry.GetChunks() {
		switch chunk.GetSseType() {
		case filer_pb.SSEType_SSE_C:
			ssecChunks++
		case filer_pb.SSEType_SSE_KMS:
			ssekmsChunks++
		}
	}

	// Primary type is the one with more chunks
	if ssecChunks > ssekmsChunks {
		return s3_constants.SSETypeC
	} else if ssekmsChunks > ssecChunks {
		return s3_constants.SSETypeKMS
	} else if ssecChunks > 0 {
		// Equal number, prefer SSE-C (shouldn't happen in practice)
		return s3_constants.SSETypeC
	}

	return "None"
}

// createMultipartSSEKMSDecryptedReader creates a reader that decrypts each chunk independently for multipart SSE-KMS objects
func (s3a *S3ApiServer) createMultipartSSEKMSDecryptedReader(r *http.Request, proxyResponse *http.Response) (io.Reader, error) {
	// Get the object path from the request
	bucket, object := s3_constants.GetBucketAndObject(r)
	objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)

	// Get the object entry from filer to access chunk information
	entry, err := s3a.getEntry("", objectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get object entry for multipart SSE-KMS decryption: %v", err)
	}

	// Sort chunks by offset to ensure correct order
	chunks := entry.GetChunks()
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetOffset() < chunks[j].GetOffset()
	})

	// Create readers for each chunk, decrypting them independently
	var readers []io.Reader

	for i, chunk := range chunks {
		glog.Infof("Processing chunk %d/%d: fileId=%s, offset=%d, size=%d, sse_type=%d",
			i+1, len(entry.GetChunks()), chunk.GetFileIdString(), chunk.GetOffset(), chunk.GetSize(), chunk.GetSseType())

		// Get this chunk's encrypted data
		chunkReader, err := s3a.createEncryptedChunkReader(chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %v", err)
		}

		// Get SSE-KMS metadata for this chunk
		var chunkSSEKMSKey *SSEKMSKey

		// Check if this chunk has per-chunk SSE-KMS metadata (new architecture)
		if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS && len(chunk.GetSseMetadata()) > 0 {
			// Use the per-chunk SSE-KMS metadata
			kmsKey, err := DeserializeSSEKMSMetadata(chunk.GetSseMetadata())
			if err != nil {
				glog.Errorf("Failed to deserialize per-chunk SSE-KMS metadata for chunk %s: %v", chunk.GetFileIdString(), err)
			} else {
				// ChunkOffset is already set from the stored metadata (PartOffset)
				chunkSSEKMSKey = kmsKey
				glog.Infof("Using per-chunk SSE-KMS metadata for chunk %s: keyID=%s, IV=%x, partOffset=%d",
					chunk.GetFileIdString(), kmsKey.KeyID, kmsKey.IV[:8], kmsKey.ChunkOffset)
			}
		}

		// Fallback to object-level metadata (legacy support)
		if chunkSSEKMSKey == nil {
			objectMetadataHeader := proxyResponse.Header.Get(s3_constants.SeaweedFSSSEKMSKeyHeader)
			if objectMetadataHeader != "" {
				kmsMetadataBytes, decodeErr := base64.StdEncoding.DecodeString(objectMetadataHeader)
				if decodeErr == nil {
					kmsKey, _ := DeserializeSSEKMSMetadata(kmsMetadataBytes)
					if kmsKey != nil {
						// For object-level metadata (legacy), use absolute file offset as fallback
						kmsKey.ChunkOffset = chunk.GetOffset()
						chunkSSEKMSKey = kmsKey
					}
					glog.Infof("Using fallback object-level SSE-KMS metadata for chunk %s with offset %d", chunk.GetFileIdString(), chunk.GetOffset())
				}
			}
		}

		if chunkSSEKMSKey == nil {
			return nil, fmt.Errorf("no SSE-KMS metadata found for chunk %s in multipart object", chunk.GetFileIdString())
		}

		// Create decrypted reader for this chunk
		decryptedChunkReader, decErr := CreateSSEKMSDecryptedReader(chunkReader, chunkSSEKMSKey)
		if decErr != nil {
			chunkReader.Close() // Close the chunk reader if decryption fails
			return nil, fmt.Errorf("failed to decrypt chunk: %v", decErr)
		}

		// Use the streaming decrypted reader directly instead of reading into memory
		readers = append(readers, decryptedChunkReader)
		glog.V(4).Infof("Added streaming decrypted reader for chunk %s in multipart SSE-KMS object", chunk.GetFileIdString())
	}

	// Combine all decrypted chunk readers into a single stream with proper resource management
	multiReader := NewMultipartSSEReader(readers)
	glog.V(3).Infof("Created multipart SSE-KMS decrypted reader with %d chunks", len(readers))

	return multiReader, nil
}

// createEncryptedChunkReader creates a reader for a single encrypted chunk
func (s3a *S3ApiServer) createEncryptedChunkReader(chunk *filer_pb.FileChunk) (io.ReadCloser, error) {
	// Get chunk URL
	srcUrl, err := s3a.lookupVolumeUrl(chunk.GetFileIdString())
	if err != nil {
		return nil, fmt.Errorf("lookup volume URL for chunk %s: %v", chunk.GetFileIdString(), err)
	}

	// Create HTTP request for chunk data
	req, err := http.NewRequest("GET", srcUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request for chunk: %v", err)
	}

	// Execute request
	resp, err := http.DefaultClient.Do(req)
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
	offset    int64 // bytes to skip from the beginning
	remaining int64 // bytes remaining to read (-1 for unlimited)
	skipped   int64 // bytes already skipped
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

	// If we need to skip bytes and haven't skipped enough yet
	if r.skipped < r.offset {
		skipNeeded := r.offset - r.skipped
		skipBuf := make([]byte, min(int64(len(p)), skipNeeded))
		skipRead, skipErr := r.reader.Read(skipBuf)
		r.skipped += int64(skipRead)

		if skipErr != nil {
			return 0, skipErr
		}

		// If we still need to skip more, recurse
		if r.skipped < r.offset {
			return r.Read(p)
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
func (s3a *S3ApiServer) createMultipartSSECDecryptedReader(r *http.Request, proxyResponse *http.Response) (io.Reader, error) {
	// Parse SSE-C headers from the request for decryption key
	customerKey, err := ParseSSECHeaders(r)
	if err != nil {
		return nil, fmt.Errorf("invalid SSE-C headers for multipart decryption: %v", err)
	}

	// Get the object path from the request
	bucket, object := s3_constants.GetBucketAndObject(r)
	objectPath := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)

	// Get the object entry from filer to access chunk information
	entry, err := s3a.getEntry("", objectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get object entry for multipart SSE-C decryption: %v", err)
	}

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
		chunkReader, err := s3a.createEncryptedChunkReader(chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %v", err)
		}

		if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
			// For SSE-C chunks, extract the IV from the stored per-chunk metadata (unified approach)
			if len(chunk.GetSseMetadata()) > 0 {
				// Deserialize the SSE-C metadata stored in the unified metadata field
				ssecMetadata, decErr := DeserializeSSECMetadata(chunk.GetSseMetadata())
				if decErr != nil {
					return nil, fmt.Errorf("failed to deserialize SSE-C metadata for chunk %s: %v", chunk.GetFileIdString(), decErr)
				}

				// Decode the IV from the metadata
				iv, ivErr := base64.StdEncoding.DecodeString(ssecMetadata.IV)
				if ivErr != nil {
					return nil, fmt.Errorf("failed to decode IV for SSE-C chunk %s: %v", chunk.GetFileIdString(), ivErr)
				}

				// Calculate the correct IV for this chunk using within-part offset
				var chunkIV []byte
				if ssecMetadata.PartOffset > 0 {
					chunkIV = calculateIVWithOffset(iv, ssecMetadata.PartOffset)
				} else {
					chunkIV = iv
				}

				decryptedReader, decErr := CreateSSECDecryptedReader(chunkReader, customerKey, chunkIV)
				if decErr != nil {
					return nil, fmt.Errorf("failed to create SSE-C decrypted reader for chunk %s: %v", chunk.GetFileIdString(), decErr)
				}
				readers = append(readers, decryptedReader)
				glog.Infof("Created SSE-C decrypted reader for chunk %s using stored metadata", chunk.GetFileIdString())
			} else {
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
