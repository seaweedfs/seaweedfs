package s3api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

	s3a.proxyToFiler(w, r, destUrl, false, passThroughResponse)
}

func (s3a *S3ApiServer) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("HeadObjectHandler %s %s", bucket, object)

	// Handle directory objects with shared logic
	if s3a.handleDirectoryObjectRequest(w, r, bucket, object, "HeadObjectHandler") {
		return // Directory object request was handled
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

	s3a.proxyToFiler(w, r, destUrl, false, passThroughResponse)
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

func passThroughResponse(proxyResponse *http.Response, w http.ResponseWriter) (statusCode int, bytesTransferred int64) {
	// Capture existing CORS headers that may have been set by middleware
	capturedCORSHeaders := captureCORSHeaders(w, corsHeaders)

	// Copy headers from proxy response
	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}

	// Restore CORS headers that were set by middleware
	restoreCORSHeaders(w, capturedCORSHeaders)

	if proxyResponse.Header.Get("Content-Range") != "" && proxyResponse.StatusCode == 200 {
		w.WriteHeader(http.StatusPartialContent)
		statusCode = http.StatusPartialContent
	} else {
		statusCode = proxyResponse.StatusCode
	}
	w.WriteHeader(statusCode)
	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)
	bytesTransferred, err := io.CopyBuffer(w, proxyResponse.Body, buf)
	if err != nil {
		glog.V(1).Infof("passthrough response read %d bytes: %v", bytesTransferred, err)
	}
	return statusCode, bytesTransferred
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
