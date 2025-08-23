package s3api

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pquerna/cachecontrol/cacheobject"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

// Object lock validation errors
var (
	ErrObjectLockVersioningRequired          = errors.New("object lock headers can only be used on versioned buckets")
	ErrInvalidObjectLockMode                 = errors.New("invalid object lock mode")
	ErrInvalidLegalHoldStatus                = errors.New("invalid legal hold status")
	ErrInvalidRetentionDateFormat            = errors.New("invalid retention until date format")
	ErrRetentionDateMustBeFuture             = errors.New("retain until date must be in the future")
	ErrObjectLockModeRequiresDate            = errors.New("object lock mode requires retention until date")
	ErrRetentionDateRequiresMode             = errors.New("retention until date requires object lock mode")
	ErrGovernanceBypassVersioningRequired    = errors.New("governance bypass header can only be used on versioned buckets")
	ErrInvalidObjectLockDuration             = errors.New("object lock duration must be greater than 0 days")
	ErrObjectLockDurationExceeded            = errors.New("object lock duration exceeds maximum allowed days")
	ErrObjectLockConfigurationMissingEnabled = errors.New("object lock configuration must specify ObjectLockEnabled")
	ErrInvalidObjectLockEnabledValue         = errors.New("invalid object lock enabled value")
	ErrRuleMissingDefaultRetention           = errors.New("rule configuration must specify DefaultRetention")
	ErrDefaultRetentionMissingMode           = errors.New("default retention must specify Mode")
	ErrInvalidDefaultRetentionMode           = errors.New("invalid default retention mode")
	ErrDefaultRetentionMissingPeriod         = errors.New("default retention must specify either Days or Years")
	ErrDefaultRetentionBothDaysAndYears      = errors.New("default retention cannot specify both Days and Years")
	ErrDefaultRetentionDaysOutOfRange        = errors.New("default retention days must be between 0 and 36500")
	ErrDefaultRetentionYearsOutOfRange       = errors.New("default retention years must be between 0 and 100")
)

// hasExplicitEncryption checks if any explicit encryption was provided in the request.
// This helper improves readability and makes the encryption check condition more explicit.
func hasExplicitEncryption(customerKey *SSECustomerKey, sseKMSKey *SSEKMSKey, sseS3Key *SSES3Key) bool {
	return customerKey != nil || sseKMSKey != nil || sseS3Key != nil
}

// BucketDefaultEncryptionResult holds the result of bucket default encryption processing
type BucketDefaultEncryptionResult struct {
	DataReader io.Reader
	SSES3Key   *SSES3Key
	SSEKMSKey  *SSEKMSKey
}

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectHandler %s %s", bucket, object)

	_, err := validateContentMd5(r.Header)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidDigest)
		return
	}

	// Check conditional headers
	if errCode := s3a.checkConditionalHeaders(r, bucket, object); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if r.Header.Get("Cache-Control") != "" {
		if _, err = cacheobject.ParseRequestCacheControl(r.Header.Get("Cache-Control")); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidDigest)
			return
		}
	}

	if r.Header.Get("Expires") != "" {
		if _, err = time.Parse(http.TimeFormat, r.Header.Get("Expires")); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrMalformedDate)
			return
		}
	}

	dataReader, s3ErrCode := getRequestDataReader(s3a, r)
	if s3ErrCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, s3ErrCode)
		return
	}
	defer dataReader.Close()

	objectContentType := r.Header.Get("Content-Type")
	if strings.HasSuffix(object, "/") && r.ContentLength <= 1024 {
		if err := s3a.mkdir(
			s3a.option.BucketsPath, bucket+strings.TrimSuffix(object, "/"),
			func(entry *filer_pb.Entry) {
				if objectContentType == "" {
					objectContentType = s3_constants.FolderMimeType
				}
				if r.ContentLength > 0 {
					entry.Content, _ = io.ReadAll(r.Body)
				}
				entry.Attributes.Mime = objectContentType

				// Set object owner for directory objects (same as regular objects)
				s3a.setObjectOwnerFromRequest(r, entry)
			}); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	} else {
		// Get detailed versioning state for the bucket
		versioningState, err := s3a.getVersioningState(bucket)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
				return
			}
			glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		versioningEnabled := (versioningState == s3_constants.VersioningEnabled)
		versioningConfigured := (versioningState != "")

		glog.V(1).Infof("PutObjectHandler: bucket %s, object %s, versioningState=%s", bucket, object, versioningState)

		// Validate object lock headers before processing
		if err := s3a.validateObjectLockHeaders(r, versioningEnabled); err != nil {
			glog.V(2).Infof("PutObjectHandler: object lock header validation failed for bucket %s, object %s: %v", bucket, object, err)
			s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
			return
		}

		// For non-versioned buckets, check if existing object has object lock protections
		// that would prevent overwrite (PUT operations overwrite existing objects in non-versioned buckets)
		if !versioningConfigured {
			governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
			if err := s3a.enforceObjectLockProtections(r, bucket, object, "", governanceBypassAllowed); err != nil {
				glog.V(2).Infof("PutObjectHandler: object lock permissions check failed for %s/%s: %v", bucket, object, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}
		}

		if versioningState == s3_constants.VersioningEnabled {
			// Handle enabled versioning - create new versions with real version IDs
			glog.V(1).Infof("PutObjectHandler: using versioned PUT for %s/%s", bucket, object)
			versionId, etag, errCode := s3a.putVersionedObject(r, bucket, object, dataReader, objectContentType)
			if errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			// Set version ID in response header
			if versionId != "" {
				w.Header().Set("x-amz-version-id", versionId)
			}

			// Set ETag in response
			setEtag(w, etag)
		} else if versioningState == s3_constants.VersioningSuspended {
			// Handle suspended versioning - overwrite with "null" version ID but preserve existing versions
			glog.V(1).Infof("PutObjectHandler: using suspended versioning PUT for %s/%s", bucket, object)
			etag, errCode := s3a.putSuspendedVersioningObject(r, bucket, object, dataReader, objectContentType)
			if errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			// Note: Suspended versioning should NOT return x-amz-version-id header according to AWS S3 spec
			// The object is stored with "null" version internally but no version header is returned

			// Set ETag in response
			setEtag(w, etag)
		} else {
			// Handle regular PUT (never configured versioning)
			glog.V(1).Infof("PutObjectHandler: using regular PUT for %s/%s", bucket, object)
			uploadUrl := s3a.toFilerUrl(bucket, object)
			if objectContentType == "" {
				dataReader = mimeDetect(r, dataReader)
			}

			etag, errCode, sseType := s3a.putToFiler(r, uploadUrl, dataReader, "", bucket, 1)

			if errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			// No version ID header for never-configured versioning
			setEtag(w, etag)

			// Set SSE response headers based on encryption type used
			if sseType == s3_constants.SSETypeS3 {
				w.Header().Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmAES256)
			}
		}
	}
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3UploadedObjectsCounter.WithLabelValues(bucket).Inc()

	writeSuccessResponseEmpty(w, r)
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, uploadUrl string, dataReader io.Reader, destination string, bucket string, partNumber int) (etag string, code s3err.ErrorCode, sseType string) {
	// Calculate unique offset for each part to prevent IV reuse in multipart uploads
	// This is critical for CTR mode encryption security
	partOffset := calculatePartOffset(partNumber)

	// Handle all SSE encryption types in a unified manner to eliminate repetitive dataReader assignments
	sseResult, sseErrorCode := s3a.handleAllSSEEncryption(r, dataReader, partOffset)
	if sseErrorCode != s3err.ErrNone {
		return "", sseErrorCode, ""
	}

	// Extract results from unified SSE handling
	dataReader = sseResult.DataReader
	customerKey := sseResult.CustomerKey
	sseIV := sseResult.SSEIV
	sseKMSKey := sseResult.SSEKMSKey
	sseKMSMetadata := sseResult.SSEKMSMetadata
	sseS3Key := sseResult.SSES3Key
	sseS3Metadata := sseResult.SSES3Metadata

	// Apply bucket default encryption if no explicit encryption was provided
	// This implements AWS S3 behavior where bucket default encryption automatically applies
	if !hasExplicitEncryption(customerKey, sseKMSKey, sseS3Key) {
		glog.V(4).Infof("putToFiler: no explicit encryption detected, checking for bucket default encryption")

		// Apply bucket default encryption and get the result
		encryptionResult, applyErr := s3a.applyBucketDefaultEncryption(bucket, r, dataReader)
		if applyErr != nil {
			glog.Errorf("Failed to apply bucket default encryption: %v", applyErr)
			return "", s3err.ErrInternalError, ""
		}

		// Update variables based on the result
		dataReader = encryptionResult.DataReader
		sseS3Key = encryptionResult.SSES3Key
		sseKMSKey = encryptionResult.SSEKMSKey

		// If SSE-S3 was applied by bucket default, prepare metadata (if not already done)
		if sseS3Key != nil && len(sseS3Metadata) == 0 {
			var metaErr error
			sseS3Metadata, metaErr = SerializeSSES3Metadata(sseS3Key)
			if metaErr != nil {
				glog.Errorf("Failed to serialize SSE-S3 metadata for bucket default encryption: %v", metaErr)
				return "", s3err.ErrInternalError, ""
			}
		}
	} else {
		glog.V(4).Infof("putToFiler: explicit encryption already applied, skipping bucket default encryption")
	}

	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)

	proxyReq, err := http.NewRequest(http.MethodPut, uploadUrl, body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", uploadUrl, err)
		return "", s3err.ErrInternalError, ""
	}

	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	if destination != "" {
		proxyReq.Header.Set(s3_constants.SeaweedStorageDestinationHeader, destination)
	}

	if s3a.option.FilerGroup != "" {
		query := proxyReq.URL.Query()
		query.Add("collection", s3a.getCollectionName(bucket))
		proxyReq.URL.RawQuery = query.Encode()
	}

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	// Set object owner header for filer to extract
	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
	if amzAccountId != "" {
		proxyReq.Header.Set(s3_constants.ExtAmzOwnerKey, amzAccountId)
		glog.V(2).Infof("putToFiler: setting owner header %s for object %s", amzAccountId, uploadUrl)
	}

	// Set SSE-C metadata headers for the filer if encryption was applied
	if customerKey != nil && len(sseIV) > 0 {
		proxyReq.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
		proxyReq.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, customerKey.KeyMD5)
		// Store IV in a custom header that the filer can use to store in entry metadata
		proxyReq.Header.Set(s3_constants.SeaweedFSSSEIVHeader, base64.StdEncoding.EncodeToString(sseIV))
	}

	// Set SSE-KMS metadata headers for the filer if KMS encryption was applied
	if sseKMSKey != nil {
		// Use already-serialized SSE-KMS metadata from helper function
		// Store serialized KMS metadata in a custom header that the filer can use
		proxyReq.Header.Set(s3_constants.SeaweedFSSSEKMSKeyHeader, base64.StdEncoding.EncodeToString(sseKMSMetadata))

		glog.V(3).Infof("putToFiler: storing SSE-KMS metadata for object %s with keyID %s", uploadUrl, sseKMSKey.KeyID)
	} else {
		glog.V(4).Infof("putToFiler: no SSE-KMS encryption detected")
	}

	// Set SSE-S3 metadata headers for the filer if S3 encryption was applied
	if sseS3Key != nil && len(sseS3Metadata) > 0 {
		// Store serialized S3 metadata in a custom header that the filer can use
		proxyReq.Header.Set(s3_constants.SeaweedFSSSES3Key, base64.StdEncoding.EncodeToString(sseS3Metadata))
		glog.V(3).Infof("putToFiler: storing SSE-S3 metadata for object %s with keyID %s", uploadUrl, sseS3Key.KeyID)
	}

	// ensure that the Authorization header is overriding any previous
	// Authorization header which might be already present in proxyReq
	s3a.maybeAddFilerJwtAuthorization(proxyReq, true)
	resp, postErr := s3a.client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		if strings.Contains(postErr.Error(), s3err.ErrMsgPayloadChecksumMismatch) {
			return "", s3err.ErrInvalidDigest, ""
		}
		return "", s3err.ErrInternalError, ""
	}
	defer resp.Body.Close()

	etag = fmt.Sprintf("%x", hash.Sum(nil))

	resp_body, ra_err := io.ReadAll(resp.Body)
	if ra_err != nil {
		glog.Errorf("upload to filer response read %d: %v", resp.StatusCode, ra_err)
		return etag, s3err.ErrInternalError, ""
	}
	var ret weed_server.FilerPostResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.Errorf("failing to read upload to %s : %v", uploadUrl, string(resp_body))
		return "", s3err.ErrInternalError, ""
	}
	if ret.Error != "" {
		glog.Errorf("upload to filer error: %v", ret.Error)
		return "", filerErrorToS3Error(ret.Error), ""
	}

	stats_collect.RecordBucketActiveTime(bucket)

	// Return the SSE type determined by the unified handler
	return etag, s3err.ErrNone, sseResult.SSEType
}

func setEtag(w http.ResponseWriter, etag string) {
	if etag != "" {
		if strings.HasPrefix(etag, "\"") {
			w.Header()["ETag"] = []string{etag}
		} else {
			w.Header()["ETag"] = []string{"\"" + etag + "\""}
		}
	}
}

func filerErrorToS3Error(errString string) s3err.ErrorCode {
	switch {
	case strings.HasPrefix(errString, "existing ") && strings.HasSuffix(errString, "is a directory"):
		return s3err.ErrExistingObjectIsDirectory
	case strings.HasSuffix(errString, "is a file"):
		return s3err.ErrExistingObjectIsFile
	default:
		return s3err.ErrInternalError
	}
}

func (s3a *S3ApiServer) maybeAddFilerJwtAuthorization(r *http.Request, isWrite bool) {
	encodedJwt := s3a.maybeGetFilerJwtAuthorizationToken(isWrite)

	if encodedJwt == "" {
		return
	}

	r.Header.Set("Authorization", "BEARER "+string(encodedJwt))
}

func (s3a *S3ApiServer) maybeGetFilerJwtAuthorizationToken(isWrite bool) string {
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForFilerServer(s3a.filerGuard.SigningKey, s3a.filerGuard.ExpiresAfterSec)
	} else {
		encodedJwt = security.GenJwtForFilerServer(s3a.filerGuard.ReadSigningKey, s3a.filerGuard.ReadExpiresAfterSec)
	}
	return string(encodedJwt)
}

// setObjectOwnerFromRequest sets the object owner metadata based on the authenticated user
func (s3a *S3ApiServer) setObjectOwnerFromRequest(r *http.Request, entry *filer_pb.Entry) {
	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
	if amzAccountId != "" {
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
		glog.V(2).Infof("setObjectOwnerFromRequest: set object owner to %s", amzAccountId)
	}
}

// putVersionedObject handles PUT operations for versioned buckets using the new layout
// where all versions (including latest) are stored in the .versions directory
func (s3a *S3ApiServer) putSuspendedVersioningObject(r *http.Request, bucket, object string, dataReader io.Reader, objectContentType string) (etag string, errCode s3err.ErrorCode) {
	// For suspended versioning, store as regular object (version ID "null") but preserve existing versions
	glog.V(2).Infof("putSuspendedVersioningObject: creating null version for %s/%s", bucket, object)

	uploadUrl := s3a.toFilerUrl(bucket, object)
	if objectContentType == "" {
		dataReader = mimeDetect(r, dataReader)
	}

	etag, errCode, _ = s3a.putToFiler(r, uploadUrl, dataReader, "", bucket, 1)
	if errCode != s3err.ErrNone {
		glog.Errorf("putSuspendedVersioningObject: failed to upload object: %v", errCode)
		return "", errCode
	}

	// Get the uploaded entry to add version metadata indicating this is "null" version
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	entry, err := s3a.getEntry(bucketDir, object)
	if err != nil {
		glog.Errorf("putSuspendedVersioningObject: failed to get object entry: %v", err)
		return "", s3err.ErrInternalError
	}

	// Add metadata to indicate this is a "null" version for suspended versioning
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	entry.Extended[s3_constants.ExtVersionIdKey] = []byte("null")

	// Set object owner for suspended versioning objects
	s3a.setObjectOwnerFromRequest(r, entry)

	// Extract and store object lock metadata from request headers (if any)
	if err := s3a.extractObjectLockMetadataFromRequest(r, entry); err != nil {
		glog.Errorf("putSuspendedVersioningObject: failed to extract object lock metadata: %v", err)
		return "", s3err.ErrInvalidRequest
	}

	// Update the entry with metadata
	err = s3a.mkFile(bucketDir, object, entry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = entry.Extended
		updatedEntry.Attributes = entry.Attributes
		updatedEntry.Chunks = entry.Chunks
	})
	if err != nil {
		glog.Errorf("putSuspendedVersioningObject: failed to update object metadata: %v", err)
		return "", s3err.ErrInternalError
	}

	// Update all existing versions/delete markers to set IsLatest=false since "null" is now latest
	err = s3a.updateIsLatestFlagsForSuspendedVersioning(bucket, object)
	if err != nil {
		glog.Warningf("putSuspendedVersioningObject: failed to update IsLatest flags: %v", err)
		// Don't fail the request, but log the warning
	}

	glog.V(2).Infof("putSuspendedVersioningObject: successfully created null version for %s/%s", bucket, object)
	return etag, s3err.ErrNone
}

// updateIsLatestFlagsForSuspendedVersioning sets IsLatest=false on all existing versions/delete markers
// when a new "null" version becomes the latest during suspended versioning
func (s3a *S3ApiServer) updateIsLatestFlagsForSuspendedVersioning(bucket, object string) error {
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := object + ".versions"
	versionsDir := bucketDir + "/" + versionsObjectPath

	glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: updating flags for %s%s", bucket, object)

	// Check if .versions directory exists
	_, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		// No .versions directory exists, nothing to update
		glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: no .versions directory for %s%s", bucket, object)
		return nil
	}

	// List all entries in .versions directory
	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err != nil {
		return fmt.Errorf("failed to list versions directory: %v", err)
	}

	glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: found %d entries to update", len(entries))

	// Update each version/delete marker to set IsLatest=false
	for _, entry := range entries {
		if entry.Extended == nil {
			continue
		}

		// Check if this entry has a version ID (it should be a version or delete marker)
		versionIdBytes, hasVersionId := entry.Extended[s3_constants.ExtVersionIdKey]
		if !hasVersionId {
			continue
		}

		versionId := string(versionIdBytes)
		glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: setting IsLatest=false for version %s", versionId)

		// Update the entry to set IsLatest=false (we don't explicitly store this flag,
		// it's determined by comparison with latest version metadata)
		// We need to clear the latest version metadata from the .versions directory
		// so that our getObjectVersionList function will correctly show IsLatest=false
	}

	// Clear the latest version metadata from .versions directory since "null" is now latest
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err == nil && versionsEntry.Extended != nil {
		// Remove latest version metadata so all versions show IsLatest=false
		delete(versionsEntry.Extended, s3_constants.ExtLatestVersionIdKey)
		delete(versionsEntry.Extended, s3_constants.ExtLatestVersionFileNameKey)

		// Update the .versions directory entry
		err = s3a.mkFile(bucketDir, versionsObjectPath, versionsEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
			updatedEntry.Extended = versionsEntry.Extended
			updatedEntry.Attributes = versionsEntry.Attributes
			updatedEntry.Chunks = versionsEntry.Chunks
		})
		if err != nil {
			return fmt.Errorf("failed to update .versions directory metadata: %v", err)
		}

		glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: cleared latest version metadata for %s%s", bucket, object)
	}

	return nil
}

func (s3a *S3ApiServer) putVersionedObject(r *http.Request, bucket, object string, dataReader io.Reader, objectContentType string) (versionId string, etag string, errCode s3err.ErrorCode) {
	// Generate version ID
	versionId = generateVersionId()

	glog.V(2).Infof("putVersionedObject: creating version %s for %s/%s", versionId, bucket, object)

	// Create the version file name
	versionFileName := s3a.getVersionFileName(versionId)

	// Upload directly to the versions directory
	// We need to construct the object path relative to the bucket
	versionObjectPath := object + ".versions/" + versionFileName
	versionUploadUrl := s3a.toFilerUrl(bucket, versionObjectPath)

	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)
	if objectContentType == "" {
		body = mimeDetect(r, body)
	}

	glog.V(2).Infof("putVersionedObject: uploading %s/%s version %s to %s", bucket, object, versionId, versionUploadUrl)

	etag, errCode, _ = s3a.putToFiler(r, versionUploadUrl, body, "", bucket, 1)
	if errCode != s3err.ErrNone {
		glog.Errorf("putVersionedObject: failed to upload version: %v", errCode)
		return "", "", errCode
	}

	// Get the uploaded entry to add versioning metadata
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionEntry, err := s3a.getEntry(bucketDir, versionObjectPath)
	if err != nil {
		glog.Errorf("putVersionedObject: failed to get version entry: %v", err)
		return "", "", s3err.ErrInternalError
	}

	// Add versioning metadata to this version
	if versionEntry.Extended == nil {
		versionEntry.Extended = make(map[string][]byte)
	}
	versionEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)

	// Store ETag with quotes for S3 compatibility
	if !strings.HasPrefix(etag, "\"") {
		etag = "\"" + etag + "\""
	}
	versionEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

	// Set object owner for versioned objects
	s3a.setObjectOwnerFromRequest(r, versionEntry)

	// Extract and store object lock metadata from request headers
	if err := s3a.extractObjectLockMetadataFromRequest(r, versionEntry); err != nil {
		glog.Errorf("putVersionedObject: failed to extract object lock metadata: %v", err)
		return "", "", s3err.ErrInvalidRequest
	}

	// Update the version entry with metadata
	err = s3a.mkFile(bucketDir, versionObjectPath, versionEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = versionEntry.Extended
		updatedEntry.Attributes = versionEntry.Attributes
		updatedEntry.Chunks = versionEntry.Chunks
	})
	if err != nil {
		glog.Errorf("putVersionedObject: failed to update version metadata: %v", err)
		return "", "", s3err.ErrInternalError
	}

	// Update the .versions directory metadata to indicate this is the latest version
	err = s3a.updateLatestVersionInDirectory(bucket, object, versionId, versionFileName)
	if err != nil {
		glog.Errorf("putVersionedObject: failed to update latest version in directory: %v", err)
		return "", "", s3err.ErrInternalError
	}

	glog.V(2).Infof("putVersionedObject: successfully created version %s for %s/%s", versionId, bucket, object)
	return versionId, etag, s3err.ErrNone
}

// updateLatestVersionInDirectory updates the .versions directory metadata to indicate the latest version
func (s3a *S3ApiServer) updateLatestVersionInDirectory(bucket, object, versionId, versionFileName string) error {
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := object + ".versions"

	// Get the current .versions directory entry
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		glog.Errorf("updateLatestVersionInDirectory: failed to get .versions entry: %v", err)
		return fmt.Errorf("failed to get .versions entry: %w", err)
	}

	// Add or update the latest version metadata
	if versionsEntry.Extended == nil {
		versionsEntry.Extended = make(map[string][]byte)
	}
	versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey] = []byte(versionId)
	versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey] = []byte(versionFileName)

	// Update the .versions directory entry with metadata
	err = s3a.mkFile(bucketDir, versionsObjectPath, versionsEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = versionsEntry.Extended
		updatedEntry.Attributes = versionsEntry.Attributes
		updatedEntry.Chunks = versionsEntry.Chunks
	})
	if err != nil {
		glog.Errorf("updateLatestVersionInDirectory: failed to update .versions directory metadata: %v", err)
		return fmt.Errorf("failed to update .versions directory metadata: %w", err)
	}

	return nil
}

// extractObjectLockMetadataFromRequest extracts object lock headers from PUT requests
// and applies bucket default retention if no explicit retention is provided
func (s3a *S3ApiServer) extractObjectLockMetadataFromRequest(r *http.Request, entry *filer_pb.Entry) error {
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	// Extract explicit object lock mode (GOVERNANCE or COMPLIANCE)
	explicitMode := r.Header.Get(s3_constants.AmzObjectLockMode)
	if explicitMode != "" {
		entry.Extended[s3_constants.ExtObjectLockModeKey] = []byte(explicitMode)
		glog.V(2).Infof("extractObjectLockMetadataFromRequest: storing explicit object lock mode: %s", explicitMode)
	}

	// Extract explicit retention until date
	explicitRetainUntilDate := r.Header.Get(s3_constants.AmzObjectLockRetainUntilDate)
	if explicitRetainUntilDate != "" {
		// Parse the ISO8601 date and convert to Unix timestamp for storage
		parsedTime, err := time.Parse(time.RFC3339, explicitRetainUntilDate)
		if err != nil {
			glog.Errorf("extractObjectLockMetadataFromRequest: failed to parse retention until date, expected format: %s, error: %v", time.RFC3339, err)
			return ErrInvalidRetentionDateFormat
		}
		entry.Extended[s3_constants.ExtRetentionUntilDateKey] = []byte(strconv.FormatInt(parsedTime.Unix(), 10))
		glog.V(2).Infof("extractObjectLockMetadataFromRequest: storing explicit retention until date (timestamp: %d)", parsedTime.Unix())
	}

	// Extract legal hold status
	if legalHold := r.Header.Get(s3_constants.AmzObjectLockLegalHold); legalHold != "" {
		// Store S3 standard "ON"/"OFF" values directly
		if legalHold == s3_constants.LegalHoldOn || legalHold == s3_constants.LegalHoldOff {
			entry.Extended[s3_constants.ExtLegalHoldKey] = []byte(legalHold)
			glog.V(2).Infof("extractObjectLockMetadataFromRequest: storing legal hold: %s", legalHold)
		} else {
			glog.Errorf("extractObjectLockMetadataFromRequest: unexpected legal hold value provided, expected 'ON' or 'OFF'")
			return ErrInvalidLegalHoldStatus
		}
	}

	// Apply bucket default retention if no explicit retention was provided
	// This implements AWS S3 behavior where bucket default retention automatically applies to new objects
	if explicitMode == "" && explicitRetainUntilDate == "" {
		bucket, _ := s3_constants.GetBucketAndObject(r)
		if err := s3a.applyBucketDefaultRetention(bucket, entry); err != nil {
			glog.V(2).Infof("extractObjectLockMetadataFromRequest: skipping bucket default retention for %s: %v", bucket, err)
			// Don't fail the upload if default retention can't be applied - this matches AWS behavior
		}
	}

	return nil
}

// applyBucketDefaultEncryption applies bucket default encryption settings to a new object
// This implements AWS S3 behavior where bucket default encryption automatically applies to new objects
// when no explicit encryption headers are provided in the upload request.
// Returns the modified dataReader and encryption keys instead of using pointer parameters for better code clarity.
func (s3a *S3ApiServer) applyBucketDefaultEncryption(bucket string, r *http.Request, dataReader io.Reader) (*BucketDefaultEncryptionResult, error) {
	// Check if bucket has default encryption configured
	encryptionConfig, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil || encryptionConfig == nil {
		// No default encryption configured, return original reader
		return &BucketDefaultEncryptionResult{DataReader: dataReader}, nil
	}

	if encryptionConfig.SseAlgorithm == "" {
		// No encryption algorithm specified
		return &BucketDefaultEncryptionResult{DataReader: dataReader}, nil
	}

	glog.V(3).Infof("applyBucketDefaultEncryption: applying default encryption %s for bucket %s", encryptionConfig.SseAlgorithm, bucket)

	switch encryptionConfig.SseAlgorithm {
	case EncryptionTypeAES256:
		// Apply SSE-S3 (AES256) encryption
		return s3a.applySSES3DefaultEncryption(dataReader)

	case EncryptionTypeKMS:
		// Apply SSE-KMS encryption
		return s3a.applySSEKMSDefaultEncryption(bucket, r, dataReader, encryptionConfig)

	default:
		return nil, fmt.Errorf("unsupported default encryption algorithm: %s", encryptionConfig.SseAlgorithm)
	}
}

// applySSES3DefaultEncryption applies SSE-S3 encryption as bucket default
func (s3a *S3ApiServer) applySSES3DefaultEncryption(dataReader io.Reader) (*BucketDefaultEncryptionResult, error) {
	// Generate SSE-S3 key
	keyManager := GetSSES3KeyManager()
	key, err := keyManager.GetOrCreateKey("")
	if err != nil {
		return nil, fmt.Errorf("failed to generate SSE-S3 key for default encryption: %v", err)
	}

	// Create encrypted reader
	encryptedReader, iv, encErr := CreateSSES3EncryptedReader(dataReader, key)
	if encErr != nil {
		return nil, fmt.Errorf("failed to create SSE-S3 encrypted reader for default encryption: %v", encErr)
	}

	// Store IV on the key object for later decryption
	key.IV = iv

	// Store key in manager for later retrieval
	keyManager.StoreKey(key)
	glog.V(3).Infof("applySSES3DefaultEncryption: applied SSE-S3 default encryption with key ID: %s", key.KeyID)

	return &BucketDefaultEncryptionResult{
		DataReader: encryptedReader,
		SSES3Key:   key,
	}, nil
}

// applySSEKMSDefaultEncryption applies SSE-KMS encryption as bucket default
func (s3a *S3ApiServer) applySSEKMSDefaultEncryption(bucket string, r *http.Request, dataReader io.Reader, encryptionConfig *s3_pb.EncryptionConfiguration) (*BucketDefaultEncryptionResult, error) {
	// Use the KMS key ID from bucket configuration, or default if not specified
	keyID := encryptionConfig.KmsKeyId
	if keyID == "" {
		keyID = "alias/aws/s3" // AWS default KMS key for S3
	}

	// Check if bucket key is enabled in configuration
	bucketKeyEnabled := encryptionConfig.BucketKeyEnabled

	// Build encryption context for KMS
	bucket, object := s3_constants.GetBucketAndObject(r)
	encryptionContext := BuildEncryptionContext(bucket, object, bucketKeyEnabled)

	// Create SSE-KMS encrypted reader
	encryptedReader, sseKey, encErr := CreateSSEKMSEncryptedReaderWithBucketKey(dataReader, keyID, encryptionContext, bucketKeyEnabled)
	if encErr != nil {
		return nil, fmt.Errorf("failed to create SSE-KMS encrypted reader for default encryption: %v", encErr)
	}

	glog.V(3).Infof("applySSEKMSDefaultEncryption: applied SSE-KMS default encryption with key ID: %s", keyID)

	return &BucketDefaultEncryptionResult{
		DataReader: encryptedReader,
		SSEKMSKey:  sseKey,
	}, nil
}

// applyBucketDefaultRetention applies bucket default retention settings to a new object
// This implements AWS S3 behavior where bucket default retention automatically applies to new objects
// when no explicit retention headers are provided in the upload request
func (s3a *S3ApiServer) applyBucketDefaultRetention(bucket string, entry *filer_pb.Entry) error {
	// Safety check - if bucket config cache is not available, skip default retention
	if s3a.bucketConfigCache == nil {
		return nil
	}

	// Get bucket configuration (getBucketConfig handles caching internally)
	bucketConfig, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return fmt.Errorf("failed to get bucket config: %v", errCode)
	}

	// Check if bucket has cached Object Lock configuration
	if bucketConfig.ObjectLockConfig == nil {
		return nil // No Object Lock configuration
	}

	objectLockConfig := bucketConfig.ObjectLockConfig

	// Check if there's a default retention rule
	if objectLockConfig.Rule == nil || objectLockConfig.Rule.DefaultRetention == nil {
		return nil // No default retention configured
	}

	defaultRetention := objectLockConfig.Rule.DefaultRetention

	// Validate default retention has required fields
	if defaultRetention.Mode == "" {
		return fmt.Errorf("default retention missing mode")
	}

	if !defaultRetention.DaysSet && !defaultRetention.YearsSet {
		return fmt.Errorf("default retention missing period")
	}

	// Calculate retention until date based on default retention period
	var retainUntilDate time.Time
	now := time.Now()

	if defaultRetention.DaysSet && defaultRetention.Days > 0 {
		retainUntilDate = now.AddDate(0, 0, defaultRetention.Days)
	} else if defaultRetention.YearsSet && defaultRetention.Years > 0 {
		retainUntilDate = now.AddDate(defaultRetention.Years, 0, 0)
	}

	// Apply default retention to the object
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	entry.Extended[s3_constants.ExtObjectLockModeKey] = []byte(defaultRetention.Mode)
	entry.Extended[s3_constants.ExtRetentionUntilDateKey] = []byte(strconv.FormatInt(retainUntilDate.Unix(), 10))

	glog.V(2).Infof("applyBucketDefaultRetention: applied default retention %s until %s for bucket %s",
		defaultRetention.Mode, retainUntilDate.Format(time.RFC3339), bucket)

	return nil
}

// validateObjectLockHeaders validates object lock headers in PUT requests
func (s3a *S3ApiServer) validateObjectLockHeaders(r *http.Request, versioningEnabled bool) error {
	// Extract object lock headers from request
	mode := r.Header.Get(s3_constants.AmzObjectLockMode)
	retainUntilDateStr := r.Header.Get(s3_constants.AmzObjectLockRetainUntilDate)
	legalHold := r.Header.Get(s3_constants.AmzObjectLockLegalHold)

	// Check if any object lock headers are present
	hasObjectLockHeaders := mode != "" || retainUntilDateStr != "" || legalHold != ""

	// Object lock headers can only be used on versioned buckets
	if hasObjectLockHeaders && !versioningEnabled {
		return ErrObjectLockVersioningRequired
	}

	// Validate object lock mode if present
	if mode != "" {
		if mode != s3_constants.RetentionModeGovernance && mode != s3_constants.RetentionModeCompliance {
			return ErrInvalidObjectLockMode
		}
	}

	// Validate retention date if present
	if retainUntilDateStr != "" {
		retainUntilDate, err := time.Parse(time.RFC3339, retainUntilDateStr)
		if err != nil {
			return ErrInvalidRetentionDateFormat
		}

		// Retention date must be in the future
		if retainUntilDate.Before(time.Now()) {
			return ErrRetentionDateMustBeFuture
		}
	}

	// If mode is specified, retention date must also be specified
	if mode != "" && retainUntilDateStr == "" {
		return ErrObjectLockModeRequiresDate
	}

	// If retention date is specified, mode must also be specified
	if retainUntilDateStr != "" && mode == "" {
		return ErrRetentionDateRequiresMode
	}

	// Validate legal hold if present
	if legalHold != "" {
		if legalHold != s3_constants.LegalHoldOn && legalHold != s3_constants.LegalHoldOff {
			return ErrInvalidLegalHoldStatus
		}
	}

	// Check for governance bypass header - only valid for versioned buckets
	bypassGovernance := r.Header.Get("x-amz-bypass-governance-retention") == "true"

	// Governance bypass headers are only valid for versioned buckets (like object lock headers)
	if bypassGovernance && !versioningEnabled {
		return ErrGovernanceBypassVersioningRequired
	}

	return nil
}

// mapValidationErrorToS3Error maps object lock validation errors to appropriate S3 error codes
func mapValidationErrorToS3Error(err error) s3err.ErrorCode {
	// Check for sentinel errors first
	switch {
	case errors.Is(err, ErrObjectLockVersioningRequired):
		// For object lock operations on non-versioned buckets, return InvalidRequest
		// This matches the test expectations
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidObjectLockMode):
		// For invalid object lock mode, return InvalidRequest
		// This matches the test expectations
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidLegalHoldStatus):
		// For invalid legal hold status in XML body, return MalformedXML
		// AWS S3 treats invalid status values in XML as malformed content
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrInvalidRetentionDateFormat):
		// For malformed retention date format, return MalformedDate
		// This matches the test expectations
		return s3err.ErrMalformedDate
	case errors.Is(err, ErrRetentionDateMustBeFuture):
		// For retention dates in the past, return InvalidRequest
		// This matches the test expectations
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrObjectLockModeRequiresDate):
		// For mode without retention date, return InvalidRequest
		// This matches the test expectations
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrRetentionDateRequiresMode):
		// For retention date without mode, return InvalidRequest
		// This matches the test expectations
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrGovernanceBypassVersioningRequired):
		// For governance bypass on non-versioned bucket, return InvalidRequest
		// This matches the test expectations
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrMalformedXML):
		// For malformed XML in request body, return MalformedXML
		// This matches the test expectations for invalid retention mode and legal hold status
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrInvalidRetentionPeriod):
		// For invalid retention period (e.g., Days <= 0), return InvalidRetentionPeriod
		// This matches the test expectations
		return s3err.ErrInvalidRetentionPeriod
	case errors.Is(err, ErrComplianceModeActive):
		// For compliance mode retention violations, return AccessDenied
		// This matches the test expectations
		return s3err.ErrAccessDenied
	case errors.Is(err, ErrGovernanceModeActive):
		// For governance mode retention violations, return AccessDenied
		// This matches the test expectations
		return s3err.ErrAccessDenied
	case errors.Is(err, ErrObjectUnderLegalHold):
		// For legal hold violations, return AccessDenied
		// This matches the test expectations
		return s3err.ErrAccessDenied
	case errors.Is(err, ErrGovernanceBypassNotPermitted):
		// For governance bypass permission violations, return AccessDenied
		// This matches the test expectations
		return s3err.ErrAccessDenied
	// Validation error constants
	case errors.Is(err, ErrObjectLockConfigurationMissingEnabled):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrInvalidObjectLockEnabledValue):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrRuleMissingDefaultRetention):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrDefaultRetentionMissingMode):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrInvalidDefaultRetentionMode):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrDefaultRetentionMissingPeriod):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrDefaultRetentionBothDaysAndYears):
		return s3err.ErrMalformedXML
	case errors.Is(err, ErrDefaultRetentionDaysOutOfRange):
		return s3err.ErrInvalidRetentionPeriod
	case errors.Is(err, ErrDefaultRetentionYearsOutOfRange):
		return s3err.ErrInvalidRetentionPeriod
	}

	// Check for error constants from the updated validation functions
	switch {
	case errors.Is(err, ErrRetentionMissingMode):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrRetentionMissingRetainUntilDate):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidRetentionModeValue):
		return s3err.ErrMalformedXML
	}

	return s3err.ErrInvalidRequest
}

// EntryGetter interface for dependency injection in tests
// Simplified to only mock the data access dependency
type EntryGetter interface {
	getEntry(parentDirectoryPath, entryName string) (*filer_pb.Entry, error)
}

// conditionalHeaders holds parsed conditional header values
type conditionalHeaders struct {
	ifMatch           string
	ifNoneMatch       string
	ifModifiedSince   time.Time
	ifUnmodifiedSince time.Time
	isSet             bool // true if any conditional headers are present
}

// parseConditionalHeaders extracts and validates conditional headers from the request
func parseConditionalHeaders(r *http.Request) (conditionalHeaders, s3err.ErrorCode) {
	headers := conditionalHeaders{
		ifMatch:     r.Header.Get(s3_constants.IfMatch),
		ifNoneMatch: r.Header.Get(s3_constants.IfNoneMatch),
	}

	ifModifiedSinceStr := r.Header.Get(s3_constants.IfModifiedSince)
	ifUnmodifiedSinceStr := r.Header.Get(s3_constants.IfUnmodifiedSince)

	// Check if any conditional headers are present
	headers.isSet = headers.ifMatch != "" || headers.ifNoneMatch != "" ||
		ifModifiedSinceStr != "" || ifUnmodifiedSinceStr != ""

	if !headers.isSet {
		return headers, s3err.ErrNone
	}

	// Parse date headers with validation
	var err error
	if ifModifiedSinceStr != "" {
		headers.ifModifiedSince, err = time.Parse(time.RFC1123, ifModifiedSinceStr)
		if err != nil {
			glog.V(3).Infof("parseConditionalHeaders: Invalid If-Modified-Since format: %v", err)
			return headers, s3err.ErrInvalidRequest
		}
	}

	if ifUnmodifiedSinceStr != "" {
		headers.ifUnmodifiedSince, err = time.Parse(time.RFC1123, ifUnmodifiedSinceStr)
		if err != nil {
			glog.V(3).Infof("parseConditionalHeaders: Invalid If-Unmodified-Since format: %v", err)
			return headers, s3err.ErrInvalidRequest
		}
	}

	return headers, s3err.ErrNone
}

// S3ApiServer implements EntryGetter interface
func (s3a *S3ApiServer) getObjectETag(entry *filer_pb.Entry) string {
	// Try to get ETag from Extended attributes first
	if etagBytes, hasETag := entry.Extended[s3_constants.ExtETagKey]; hasETag {
		return string(etagBytes)
	}
	// Fallback: calculate ETag from chunks
	return s3a.calculateETagFromChunks(entry.Chunks)
}

func (s3a *S3ApiServer) etagMatches(headerValue, objectETag string) bool {
	// Clean the object ETag
	objectETag = strings.Trim(objectETag, `"`)

	// Split header value by commas to handle multiple ETags
	etags := strings.Split(headerValue, ",")
	for _, etag := range etags {
		etag = strings.TrimSpace(etag)
		etag = strings.Trim(etag, `"`)
		if etag == objectETag {
			return true
		}
	}
	return false
}

// checkConditionalHeadersWithGetter is a testable method that accepts a simple EntryGetter
// Uses the production getObjectETag and etagMatches methods to ensure testing of real logic
func (s3a *S3ApiServer) checkConditionalHeadersWithGetter(getter EntryGetter, r *http.Request, bucket, object string) s3err.ErrorCode {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		glog.V(3).Infof("checkConditionalHeaders: Invalid date format")
		return errCode
	}
	if !headers.isSet {
		return s3err.ErrNone
	}

	// Get object entry for conditional checks.
	bucketDir := "/buckets/" + bucket
	entry, entryErr := getter.getEntry(bucketDir, object)
	objectExists := entryErr == nil

	// For PUT requests, all specified conditions must be met.
	// The evaluation order follows AWS S3 behavior for consistency.

	// 1. Check If-Match
	if headers.ifMatch != "" {
		if !objectExists {
			glog.V(3).Infof("checkConditionalHeaders: If-Match failed - object %s/%s does not exist", bucket, object)
			return s3err.ErrPreconditionFailed
		}
		// If `ifMatch` is "*", the condition is met if the object exists.
		// Otherwise, we need to check the ETag.
		if headers.ifMatch != "*" {
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			// Use production etagMatches method
			if !s3a.etagMatches(headers.ifMatch, objectETag) {
				glog.V(3).Infof("checkConditionalHeaders: If-Match failed for object %s/%s - expected ETag %s, got %s", bucket, object, headers.ifMatch, objectETag)
				return s3err.ErrPreconditionFailed
			}
		}
		glog.V(3).Infof("checkConditionalHeaders: If-Match passed for object %s/%s", bucket, object)
	}

	// 2. Check If-Unmodified-Since
	if !headers.ifUnmodifiedSince.IsZero() {
		if objectExists {
			objectModTime := time.Unix(entry.Attributes.Mtime, 0)
			if objectModTime.After(headers.ifUnmodifiedSince) {
				glog.V(3).Infof("checkConditionalHeaders: If-Unmodified-Since failed - object modified after %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
				return s3err.ErrPreconditionFailed
			}
			glog.V(3).Infof("checkConditionalHeaders: If-Unmodified-Since passed - object not modified since %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
		}
	}

	// 3. Check If-None-Match
	if headers.ifNoneMatch != "" {
		if objectExists {
			if headers.ifNoneMatch == "*" {
				glog.V(3).Infof("checkConditionalHeaders: If-None-Match=* failed - object %s/%s exists", bucket, object)
				return s3err.ErrPreconditionFailed
			}
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			// Use production etagMatches method
			if s3a.etagMatches(headers.ifNoneMatch, objectETag) {
				glog.V(3).Infof("checkConditionalHeaders: If-None-Match failed - ETag matches %s", objectETag)
				return s3err.ErrPreconditionFailed
			}
			glog.V(3).Infof("checkConditionalHeaders: If-None-Match passed - ETag %s doesn't match %s", objectETag, headers.ifNoneMatch)
		} else {
			glog.V(3).Infof("checkConditionalHeaders: If-None-Match passed - object %s/%s does not exist", bucket, object)
		}
	}

	// 4. Check If-Modified-Since
	if !headers.ifModifiedSince.IsZero() {
		if objectExists {
			objectModTime := time.Unix(entry.Attributes.Mtime, 0)
			if !objectModTime.After(headers.ifModifiedSince) {
				glog.V(3).Infof("checkConditionalHeaders: If-Modified-Since failed - object not modified since %s", r.Header.Get(s3_constants.IfModifiedSince))
				return s3err.ErrPreconditionFailed
			}
			glog.V(3).Infof("checkConditionalHeaders: If-Modified-Since passed - object modified after %s", r.Header.Get(s3_constants.IfModifiedSince))
		}
	}

	return s3err.ErrNone
}

// checkConditionalHeaders is the production method that uses the S3ApiServer as EntryGetter
func (s3a *S3ApiServer) checkConditionalHeaders(r *http.Request, bucket, object string) s3err.ErrorCode {
	return s3a.checkConditionalHeadersWithGetter(s3a, r, bucket, object)
}

// checkConditionalHeadersForReadsWithGetter is a testable method for read operations
// Uses the production getObjectETag and etagMatches methods to ensure testing of real logic
func (s3a *S3ApiServer) checkConditionalHeadersForReadsWithGetter(getter EntryGetter, r *http.Request, bucket, object string) ConditionalHeaderResult {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		glog.V(3).Infof("checkConditionalHeadersForReads: Invalid date format")
		return ConditionalHeaderResult{ErrorCode: errCode}
	}
	if !headers.isSet {
		return ConditionalHeaderResult{ErrorCode: s3err.ErrNone}
	}

	// Get object entry for conditional checks.
	bucketDir := "/buckets/" + bucket
	entry, entryErr := getter.getEntry(bucketDir, object)
	objectExists := entryErr == nil

	// If object doesn't exist, fail for If-Match and If-Unmodified-Since
	if !objectExists {
		if headers.ifMatch != "" {
			glog.V(3).Infof("checkConditionalHeadersForReads: If-Match failed - object %s/%s does not exist", bucket, object)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed}
		}
		if !headers.ifUnmodifiedSince.IsZero() {
			glog.V(3).Infof("checkConditionalHeadersForReads: If-Unmodified-Since failed - object %s/%s does not exist", bucket, object)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed}
		}
		// If-None-Match and If-Modified-Since succeed when object doesn't exist
		return ConditionalHeaderResult{ErrorCode: s3err.ErrNone}
	}

	// Object exists - check all conditions
	// The evaluation order follows AWS S3 behavior for consistency.

	// 1. Check If-Match (412 Precondition Failed if fails)
	if headers.ifMatch != "" {
		// If `ifMatch` is "*", the condition is met if the object exists.
		// Otherwise, we need to check the ETag.
		if headers.ifMatch != "*" {
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			// Use production etagMatches method
			if !s3a.etagMatches(headers.ifMatch, objectETag) {
				glog.V(3).Infof("checkConditionalHeadersForReads: If-Match failed for object %s/%s - expected ETag %s, got %s", bucket, object, headers.ifMatch, objectETag)
				return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed}
			}
		}
		glog.V(3).Infof("checkConditionalHeadersForReads: If-Match passed for object %s/%s", bucket, object)
	}

	// 2. Check If-Unmodified-Since (412 Precondition Failed if fails)
	if !headers.ifUnmodifiedSince.IsZero() {
		objectModTime := time.Unix(entry.Attributes.Mtime, 0)
		if objectModTime.After(headers.ifUnmodifiedSince) {
			glog.V(3).Infof("checkConditionalHeadersForReads: If-Unmodified-Since failed - object modified after %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
			return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed}
		}
		glog.V(3).Infof("checkConditionalHeadersForReads: If-Unmodified-Since passed - object not modified since %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
	}

	// 3. Check If-None-Match (304 Not Modified if fails)
	if headers.ifNoneMatch != "" {
		// Use production getObjectETag method
		objectETag := s3a.getObjectETag(entry)

		if headers.ifNoneMatch == "*" {
			glog.V(3).Infof("checkConditionalHeadersForReads: If-None-Match=* failed - object %s/%s exists", bucket, object)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrNotModified, ETag: objectETag}
		}
		// Use production etagMatches method
		if s3a.etagMatches(headers.ifNoneMatch, objectETag) {
			glog.V(3).Infof("checkConditionalHeadersForReads: If-None-Match failed - ETag matches %s", objectETag)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrNotModified, ETag: objectETag}
		}
		glog.V(3).Infof("checkConditionalHeadersForReads: If-None-Match passed - ETag %s doesn't match %s", objectETag, headers.ifNoneMatch)
	}

	// 4. Check If-Modified-Since (304 Not Modified if fails)
	if !headers.ifModifiedSince.IsZero() {
		objectModTime := time.Unix(entry.Attributes.Mtime, 0)
		if !objectModTime.After(headers.ifModifiedSince) {
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			glog.V(3).Infof("checkConditionalHeadersForReads: If-Modified-Since failed - object not modified since %s", r.Header.Get(s3_constants.IfModifiedSince))
			return ConditionalHeaderResult{ErrorCode: s3err.ErrNotModified, ETag: objectETag}
		}
		glog.V(3).Infof("checkConditionalHeadersForReads: If-Modified-Since passed - object modified after %s", r.Header.Get(s3_constants.IfModifiedSince))
	}

	return ConditionalHeaderResult{ErrorCode: s3err.ErrNone}
}

// checkConditionalHeadersForReads is the production method that uses the S3ApiServer as EntryGetter
func (s3a *S3ApiServer) checkConditionalHeadersForReads(r *http.Request, bucket, object string) ConditionalHeaderResult {
	return s3a.checkConditionalHeadersForReadsWithGetter(s3a, r, bucket, object)
}
