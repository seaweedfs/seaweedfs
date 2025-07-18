package s3api

import (
	"crypto/md5"
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
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

// Object lock validation errors
var (
	ErrObjectLockVersioningRequired       = errors.New("object lock headers can only be used on versioned buckets")
	ErrInvalidObjectLockMode              = errors.New("invalid object lock mode")
	ErrInvalidLegalHoldStatus             = errors.New("invalid legal hold status")
	ErrInvalidRetentionDateFormat         = errors.New("invalid retention until date format")
	ErrRetentionDateMustBeFuture          = errors.New("retention until date must be in the future")
	ErrObjectLockModeRequiresDate         = errors.New("object lock mode requires retention until date")
	ErrRetentionDateRequiresMode          = errors.New("retention until date requires object lock mode")
	ErrGovernanceBypassVersioningRequired = errors.New("governance bypass header can only be used on versioned buckets")
	ErrInvalidObjectLockDuration          = errors.New("object lock duration must be greater than 0 days")
	ErrObjectLockDurationExceeded         = errors.New("object lock duration exceeds maximum allowed days")
)

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectHandler %s %s", bucket, object)

	_, err := validateContentMd5(r.Header)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidDigest)
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
			}); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	} else {
		// Check if versioning is enabled for the bucket
		versioningEnabled, err := s3a.isVersioningEnabled(bucket)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
				return
			}
			glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		glog.V(1).Infof("PutObjectHandler: bucket %s, object %s, versioningEnabled=%v", bucket, object, versioningEnabled)

		// Validate object lock headers before processing
		if err := s3a.validateObjectLockHeaders(r, versioningEnabled); err != nil {
			glog.V(2).Infof("PutObjectHandler: object lock header validation failed for bucket %s, object %s: %v", bucket, object, err)
			s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
			return
		}

		// For non-versioned buckets, check if existing object has object lock protections
		// that would prevent overwrite (PUT operations overwrite existing objects in non-versioned buckets)
		if !versioningEnabled {
			bypassGovernance := r.Header.Get("x-amz-bypass-governance-retention") == "true"
			if err := s3a.checkObjectLockPermissions(r, bucket, object, "", bypassGovernance); err != nil {
				glog.V(2).Infof("PutObjectHandler: object lock permissions check failed for %s/%s: %v", bucket, object, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}
		}

		if versioningEnabled {
			// Handle versioned PUT
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
		} else {
			// Handle regular PUT (non-versioned)
			glog.V(1).Infof("PutObjectHandler: using regular PUT for %s/%s", bucket, object)
			uploadUrl := s3a.toFilerUrl(bucket, object)
			if objectContentType == "" {
				dataReader = mimeDetect(r, dataReader)
			}

			etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader, "", bucket)

			if errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			setEtag(w, etag)
		}
	}
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3UploadedObjectsCounter.WithLabelValues(bucket).Inc()

	writeSuccessResponseEmpty(w, r)
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, uploadUrl string, dataReader io.Reader, destination string, bucket string) (etag string, code s3err.ErrorCode) {

	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)

	proxyReq, err := http.NewRequest(http.MethodPut, uploadUrl, body)

	if err != nil {
		glog.Errorf("NewRequest %s: %v", uploadUrl, err)
		return "", s3err.ErrInternalError
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
	// ensure that the Authorization header is overriding any previous
	// Authorization header which might be already present in proxyReq
	s3a.maybeAddFilerJwtAuthorization(proxyReq, true)
	resp, postErr := s3a.client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		if strings.Contains(postErr.Error(), s3err.ErrMsgPayloadChecksumMismatch) {
			return "", s3err.ErrInvalidDigest
		}
		return "", s3err.ErrInternalError
	}
	defer resp.Body.Close()

	etag = fmt.Sprintf("%x", hash.Sum(nil))

	resp_body, ra_err := io.ReadAll(resp.Body)
	if ra_err != nil {
		glog.Errorf("upload to filer response read %d: %v", resp.StatusCode, ra_err)
		return etag, s3err.ErrInternalError
	}
	var ret weed_server.FilerPostResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.Errorf("failing to read upload to %s : %v", uploadUrl, string(resp_body))
		return "", s3err.ErrInternalError
	}
	if ret.Error != "" {
		glog.Errorf("upload to filer error: %v", ret.Error)
		return "", filerErrorToS3Error(ret.Error)
	}
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3BucketTrafficReceivedBytesCounter.WithLabelValues(bucket).Add(float64(ret.Size))
	return etag, s3err.ErrNone
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

// putVersionedObject handles PUT operations for versioned buckets using the new layout
// where all versions (including latest) are stored in the .versions directory
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

	etag, errCode = s3a.putToFiler(r, versionUploadUrl, body, "", bucket)
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

	if defaultRetention.Days == 0 && defaultRetention.Years == 0 {
		return fmt.Errorf("default retention missing period")
	}

	// Calculate retention until date based on default retention period
	var retainUntilDate time.Time
	now := time.Now()

	if defaultRetention.Days > 0 {
		retainUntilDate = now.AddDate(0, 0, defaultRetention.Days)
	} else if defaultRetention.Years > 0 {
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
	switch {
	case errors.Is(err, ErrObjectLockVersioningRequired):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidObjectLockMode):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidLegalHoldStatus):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidRetentionDateFormat):
		return s3err.ErrMalformedDate
	case errors.Is(err, ErrRetentionDateMustBeFuture),
		errors.Is(err, ErrObjectLockModeRequiresDate),
		errors.Is(err, ErrRetentionDateRequiresMode):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrGovernanceBypassVersioningRequired):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrInvalidObjectLockDuration):
		return s3err.ErrInvalidRequest
	case errors.Is(err, ErrObjectLockDurationExceeded):
		return s3err.ErrInvalidRequest
	default:
		return s3err.ErrInvalidRequest
	}
}
