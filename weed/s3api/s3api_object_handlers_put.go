package s3api

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pquerna/cachecontrol/cacheobject"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
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

		if versioningEnabled {
			// Handle versioned PUT
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

// putVersionedObject handles PUT operations for versioned buckets
func (s3a *S3ApiServer) putVersionedObject(r *http.Request, bucket, object string, dataReader io.Reader, objectContentType string) (versionId string, etag string, errCode s3err.ErrorCode) {
	// Generate version ID
	versionId = generateVersionId()

	glog.V(2).Infof("putVersionedObject: starting upload of %s/%s with version ID %s", bucket, object, versionId)

	// Create a unique temporary file name for this version
	tempObject := fmt.Sprintf("%s.tmp.%s.%d", object, versionId, time.Now().UnixNano())
	tempUploadUrl := s3a.toFilerUrl(bucket, tempObject)

	// Upload to temporary location first
	hash := md5.New()
	var body = io.TeeReader(dataReader, hash)
	if objectContentType == "" {
		body = mimeDetect(r, body)
	}

	glog.V(2).Infof("putVersionedObject: uploading %s/%s to temporary location %s", bucket, object, tempUploadUrl)

	etag, errCode = s3a.putToFiler(r, tempUploadUrl, body, "", bucket)
	if errCode != s3err.ErrNone {
		glog.Errorf("putVersionedObject: failed to upload %s/%s to temporary location: %v", bucket, object, errCode)
		return "", "", errCode
	}

	// Add versioning metadata to the temporary entry
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	tempObjectName := strings.TrimPrefix(tempObject, "/")
	tempEntry, err := s3a.getEntry(bucketDir, tempObjectName)
	if err != nil {
		glog.Errorf("putVersionedObject: failed to get temporary entry: %v", err)
		return "", "", s3err.ErrInternalError
	}

	if tempEntry.Extended == nil {
		tempEntry.Extended = make(map[string][]byte)
	}
	tempEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
	tempEntry.Extended[s3_constants.ExtIsLatestKey] = []byte("true")
	// Store ETag with quotes for S3 compatibility
	if !strings.HasPrefix(etag, "\"") {
		etag = "\"" + etag + "\""
	}
	tempEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

	// Update the temporary entry with versioning metadata
	err = s3a.mkFile(bucketDir, tempObjectName, tempEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = tempEntry.Extended
		updatedEntry.Attributes = tempEntry.Attributes
		updatedEntry.Chunks = tempEntry.Chunks
	})
	if err != nil {
		glog.Errorf("putVersionedObject: failed to update temporary entry metadata: %v", err)
		return "", "", s3err.ErrInternalError
	}

	// Now perform the atomic version creation process
	// This is where we handle the race condition properly
	err = s3a.atomicVersionCreation(bucket, object, tempObject, versionId)
	if err != nil {
		glog.Errorf("putVersionedObject: failed to create version atomically: %v", err)
		// Clean up temporary file
		s3a.rm(bucketDir, tempObjectName, true, false)
		return "", "", s3err.ErrInternalError
	}

	glog.V(2).Infof("putVersionedObject: successfully created version %s for %s/%s", versionId, bucket, object)
	return versionId, etag, s3err.ErrNone
}

// atomicVersionCreation handles the atomic creation of a version by:
// 1. Moving existing current object to versions directory (if exists)
// 2. Moving temporary file to current location
// This approach avoids race conditions by using the filer's atomic operations
func (s3a *S3ApiServer) atomicVersionCreation(bucket, object, tempObject, versionId string) error {
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	objectName := strings.TrimPrefix(object, "/")
	tempObjectName := strings.TrimPrefix(tempObject, "/")

	// Step 1: Move existing current object to versions directory (if it exists and has version metadata)
	err := s3a.moveCurrentObjectToVersions(bucket, object)
	if err != nil {
		glog.Warningf("atomicVersionCreation: failed to move current object to versions: %v", err)
		// Continue anyway - this is not fatal for version creation
	}

	// Step 2: Get the temporary entry
	tempEntry, err := s3a.getEntry(bucketDir, tempObjectName)
	if err != nil {
		return fmt.Errorf("failed to get temporary entry: %v", err)
	}

	// Step 3: Atomically move temporary file to current location
	// This is the critical atomic operation
	err = s3a.mkFile(bucketDir, objectName, tempEntry.Chunks, func(entry *filer_pb.Entry) {
		entry.Name = objectName
		entry.IsDirectory = tempEntry.IsDirectory
		entry.Attributes = tempEntry.Attributes
		entry.Extended = tempEntry.Extended
		entry.Chunks = tempEntry.Chunks
	})
	if err != nil {
		return fmt.Errorf("failed to create versioned object at current location: %v", err)
	}

	// Step 4: Clean up temporary file
	err = s3a.rm(bucketDir, tempObjectName, true, false)
	if err != nil {
		glog.Warningf("atomicVersionCreation: failed to clean up temporary file %s: %v", tempObject, err)
		// Non-fatal - the object was created successfully
	}

	return nil
}
