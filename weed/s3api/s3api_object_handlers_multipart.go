package s3api

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/pquerna/cachecontrol/cacheobject"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

const (
	maxObjectListSizeLimit = 1000  // Limit number of objects in a listObjectsResponse.
	maxUploadsList         = 10000 // Limit number of uploads in a listUploadsResponse.
	maxPartsList           = 10000 // Limit number of parts in a listPartsResponse.
)

// NewMultipartUploadHandler - New multipart upload.
func (s3a *S3ApiServer) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists, and create it if it doesn't (auto-create bucket)
	if err := s3a.checkBucket(r, bucket); err == s3err.ErrNoSuchBucket {
		// Auto-create bucket if it doesn't exist (requires Admin permission)
		if !s3a.handleAutoCreateBucket(w, r, bucket, "NewMultipartUploadHandler") {
			return
		}
	} else if err != s3err.ErrNone {
		// Other errors (like access denied) should still fail
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Check if versioning is enabled for the bucket (needed for object lock)
	versioningEnabled, err := s3a.isVersioningEnabled(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Validate object lock headers before processing
	if err := s3a.validateObjectLockHeaders(r, versioningEnabled); err != nil {
		glog.V(2).Infof("NewMultipartUploadHandler: object lock header validation failed for bucket %s, object %s: %v", bucket, object, err)
		s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
		return
	}

	// Validate Cache-Control header format if present
	if cacheControl := r.Header.Get("Cache-Control"); cacheControl != "" {
		if _, err := cacheobject.ParseRequestCacheControl(cacheControl); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
	}

	// Validate Expires header format if present
	if expires := r.Header.Get("Expires"); expires != "" {
		if _, err := time.Parse(http.TimeFormat, expires); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrMalformedDate)
			return
		}
	}

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		Metadata: make(map[string]*string),
	}

	// Parse S3 metadata from request headers
	metadata, errCode := ParseS3Metadata(r, nil, false)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
	for k, v := range metadata {
		createMultipartUploadInput.Metadata[k] = aws.String(string(v))
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		createMultipartUploadInput.ContentType = &contentType
	}
	response, errCode := s3a.createMultipartUpload(r, createMultipartUploadInput)

	glog.V(3).Info("NewMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	writeSuccessResponseXML(w, r, response)

}

// CompleteMultipartUploadHandler - Completes multipart upload.
func (s3a *S3ApiServer) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html

	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before completing multipart upload
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	parts := &CompleteMultipartUpload{}
	if err := xmlDecoder(r.Body, parts, r.ContentLength); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	// Check conditional headers before completing multipart upload
	// This implements AWS S3 behavior where conditional headers apply to CompleteMultipartUpload
	if errCode := s3a.checkConditionalHeaders(r, bucket, object); errCode != s3err.ErrNone {
		glog.V(3).Infof("CompleteMultipartUploadHandler: Conditional header check failed for %s/%s", bucket, object)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	response, errCode := s3a.completeMultipartUpload(r, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		UploadId: aws.String(uploadID),
	}, parts)

	glog.V(3).Info("CompleteMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Set version ID in HTTP header if present
	if response.VersionId != nil {
		w.Header().Set("x-amz-version-id", *response.VersionId)
	}

	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3UploadedObjectsCounter.WithLabelValues(bucket).Inc()

	writeSuccessResponseXML(w, r, response)

}

// AbortMultipartUploadHandler - Aborts multipart upload.
func (s3a *S3ApiServer) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before aborting multipart upload
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	response, errCode := s3a.abortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		UploadId: aws.String(uploadID),
	})

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Info("AbortMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)))

	//https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
	s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
	s3err.PostLog(r, http.StatusNoContent, s3err.ErrNone)

}

// ListMultipartUploadsHandler - Lists multipart uploads.
func (s3a *S3ApiServer) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before listing multipart uploads
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, encodingType := getBucketMultipartResources(r.URL.Query())
	if maxUploads < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxUploads)
		return
	}
	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(keyMarker, prefix) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
			return
		}
	}

	response, errCode := s3a.listMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucket),
		Delimiter:      aws.String(delimiter),
		EncodingType:   aws.String(encodingType),
		KeyMarker:      aws.String(keyMarker),
		MaxUploads:     aws.Int64(int64(maxUploads)),
		Prefix:         aws.String(prefix),
		UploadIdMarker: aws.String(uploadIDMarker),
	})

	glog.V(3).Infof("ListMultipartUploadsHandler %s errCode=%d", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// TODO handle encodingType

	writeSuccessResponseXML(w, r, response)
}

// ListObjectPartsHandler - Lists object parts in a multipart upload.
func (s3a *S3ApiServer) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before listing object parts
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	uploadID, partNumberMarker, maxParts, _ := getObjectResources(r.URL.Query())
	if partNumberMarker < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPartNumberMarker)
		return
	}
	if maxParts < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
		return
	}

	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	response, errCode := s3a.listObjectParts(&s3.ListPartsInput{
		Bucket:           aws.String(bucket),
		Key:              objectKey(aws.String(object)),
		MaxParts:         aws.Int64(int64(maxParts)),
		PartNumberMarker: aws.Int64(int64(partNumberMarker)),
		UploadId:         aws.String(uploadID),
	})

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Infof("ListObjectPartsHandler %s count=%d", string(s3err.EncodeXMLResponse(response)), len(response.Part))

	writeSuccessResponseXML(w, r, response)

}

// PutObjectPartHandler - Put an object part in a multipart upload.
func (s3a *S3ApiServer) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before putting object part
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	partIDString := r.URL.Query().Get("partNumber")
	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}
	if partID > s3_constants.MaxS3MultipartParts {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}
	if partID < 1 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}

	dataReader, s3ErrCode := getRequestDataReader(s3a, r)
	if s3ErrCode != s3err.ErrNone {
		glog.Errorf("PutObjectPartHandler: getRequestDataReader failed with code %v", s3ErrCode)
		s3err.WriteErrorResponse(w, r, s3ErrCode)
		return
	}
	defer dataReader.Close()

	glog.V(2).Infof("PutObjectPartHandler %s %s %04d", bucket, uploadID, partID)

	// Check for SSE-C headers in the current request first
	sseCustomerAlgorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	if sseCustomerAlgorithm != "" {
		// SSE-C part upload - headers are already present, let putToFiler handle it
	} else {
		// No SSE-C headers, check for SSE-KMS settings from upload directory
		if uploadEntry, err := s3a.getEntry(s3a.genUploadsFolder(bucket), uploadID); err == nil {
			if uploadEntry.Extended != nil {
				// Check if this upload uses SSE-KMS
				if keyIDBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSKeyID]; exists {
					keyID := string(keyIDBytes)

					// Build SSE-KMS metadata for this part
					bucketKeyEnabled := false
					if bucketKeyBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSBucketKeyEnabled]; exists && string(bucketKeyBytes) == "true" {
						bucketKeyEnabled = true
					}

					var encryptionContext map[string]string
					if contextBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSEncryptionContext]; exists {
						// Parse the stored encryption context
						if err := json.Unmarshal(contextBytes, &encryptionContext); err != nil {
							glog.Errorf("Failed to parse encryption context for upload %s: %v", uploadID, err)
							encryptionContext = BuildEncryptionContext(bucket, object, bucketKeyEnabled)
						}
					} else {
						encryptionContext = BuildEncryptionContext(bucket, object, bucketKeyEnabled)
					}

					// Get the base IV for this multipart upload
					var baseIV []byte
					if baseIVBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSBaseIV]; exists {
						// Decode the base64 encoded base IV
						decodedIV, decodeErr := base64.StdEncoding.DecodeString(string(baseIVBytes))
						if decodeErr == nil && len(decodedIV) == s3_constants.AESBlockSize {
							baseIV = decodedIV
							glog.V(4).Infof("Using stored base IV %x for multipart upload %s", baseIV[:8], uploadID)
						} else {
							glog.Errorf("Failed to decode base IV for multipart upload %s: %v (expected %d bytes, got %d)", uploadID, decodeErr, s3_constants.AESBlockSize, len(decodedIV))
						}
					}

					// Base IV is required for SSE-KMS multipart uploads - fail if missing or invalid
					if len(baseIV) == 0 {
						glog.Errorf("No valid base IV found for SSE-KMS multipart upload %s - cannot proceed with encryption", uploadID)
						s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
						return
					}

					// Add SSE-KMS headers to the request for putToFiler to handle encryption
					r.Header.Set(s3_constants.AmzServerSideEncryption, "aws:kms")
					r.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, keyID)
					if bucketKeyEnabled {
						r.Header.Set(s3_constants.AmzServerSideEncryptionBucketKeyEnabled, "true")
					}
					if len(encryptionContext) > 0 {
						if contextJSON, err := json.Marshal(encryptionContext); err == nil {
							r.Header.Set(s3_constants.AmzServerSideEncryptionContext, base64.StdEncoding.EncodeToString(contextJSON))
						}
					}

					// Pass the base IV to putToFiler via header
					r.Header.Set(s3_constants.SeaweedFSSSEKMSBaseIVHeader, base64.StdEncoding.EncodeToString(baseIV))

				} else {
					// Check if this upload uses SSE-S3
					if err := s3a.handleSSES3MultipartHeaders(r, uploadEntry, uploadID); err != nil {
						glog.Errorf("Failed to setup SSE-S3 multipart headers: %v", err)
						s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
						return
					}
				}
			}
		} else if !errors.Is(err, filer_pb.ErrNotFound) {
			// Log unexpected errors (but not "not found" which is normal for non-SSE uploads)
			glog.V(3).Infof("Could not retrieve upload entry for %s/%s: %v (may be non-SSE upload)", bucket, uploadID, err)
		}
	}

	filePath := s3a.genPartUploadPath(bucket, uploadID, partID)

	if partID == 1 && r.Header.Get("Content-Type") == "" {
		dataReader = mimeDetect(r, dataReader)
	}

	glog.V(2).Infof("PutObjectPart: bucket=%s, object=%s, uploadId=%s, partNumber=%d, size=%d",
		bucket, object, uploadID, partID, r.ContentLength)

	etag, errCode, sseMetadata := s3a.putToFiler(r, filePath, dataReader, bucket, partID)
	if errCode != s3err.ErrNone {
		glog.Errorf("PutObjectPart: putToFiler failed with error code %v for bucket=%s, object=%s, partNumber=%d",
			errCode, bucket, object, partID)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(2).Infof("PutObjectPart: SUCCESS - bucket=%s, object=%s, partNumber=%d, etag=%s, sseType=%s",
		bucket, object, partID, etag, sseMetadata.SSEType)

	setEtag(w, etag)

	// Set SSE response headers for multipart uploads
	s3a.setSSEResponseHeaders(w, r, sseMetadata)

	writeSuccessResponseEmpty(w, r)

}

func (s3a *S3ApiServer) genUploadsFolder(bucket string) string {
	return fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, s3_constants.MultipartUploadsFolder)
}

func (s3a *S3ApiServer) genPartUploadPath(bucket, uploadID string, partID int) string {
	// Returns just the file path - no filer address needed
	// Upload traffic goes directly to volume servers, not through filer
	return fmt.Sprintf("%s/%s/%04d_%s.part",
		s3a.genUploadsFolder(bucket), uploadID, partID, uuid.NewString())
}

// Generate uploadID hash string from object
func (s3a *S3ApiServer) generateUploadID(object string) string {

	object = strings.TrimPrefix(object, "/")
	h := sha1.New()
	h.Write([]byte(object))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Check object name and uploadID when processing  multipart uploading
func (s3a *S3ApiServer) checkUploadId(object string, id string) error {

	hash := s3a.generateUploadID(object)

	if !strings.HasPrefix(id, hash) {
		glog.Errorf("object %s and uploadID %s are not matched", object, id)
		return fmt.Errorf("object %s and uploadID %s are not matched", object, id)
	}
	return nil
}

// Parse bucket url queries for ?uploads
func getBucketMultipartResources(values url.Values) (prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int, encodingType string) {
	prefix = values.Get("prefix")
	keyMarker = values.Get("key-marker")
	uploadIDMarker = values.Get("upload-id-marker")
	delimiter = values.Get("delimiter")
	if values.Get("max-uploads") != "" {
		maxUploads, _ = strconv.Atoi(values.Get("max-uploads"))
	} else {
		maxUploads = maxUploadsList
	}
	encodingType = values.Get("encoding-type")
	return
}

// Parse object url queries
func getObjectResources(values url.Values) (uploadID string, partNumberMarker, maxParts int, encodingType string) {
	uploadID = values.Get("uploadId")
	partNumberMarker, _ = strconv.Atoi(values.Get("part-number-marker"))
	if values.Get("max-parts") != "" {
		maxParts, _ = strconv.Atoi(values.Get("max-parts"))
	} else {
		maxParts = maxPartsList
	}
	encodingType = values.Get("encoding-type")
	return
}

func xmlDecoder(body io.Reader, v interface{}, size int64) error {
	var lbody io.Reader
	if size > 0 {
		lbody = io.LimitReader(body, size)
	} else {
		lbody = body
	}
	d := xml.NewDecoder(lbody)
	d.CharsetReader = func(label string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	return d.Decode(v)
}

type CompleteMultipartUpload struct {
	Parts []CompletedPart `xml:"Part"`
}
type CompletedPart struct {
	ETag       string
	PartNumber int
}

// handleSSES3MultipartHeaders handles SSE-S3 multipart upload header setup to reduce nesting complexity
func (s3a *S3ApiServer) handleSSES3MultipartHeaders(r *http.Request, uploadEntry *filer_pb.Entry, uploadID string) error {
	if encryptionTypeBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSES3Encryption]; exists && string(encryptionTypeBytes) == s3_constants.SSEAlgorithmAES256 {

		// Set SSE-S3 headers to indicate server-side encryption
		r.Header.Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmAES256)

		// Retrieve and set base IV for consistent multipart encryption - REQUIRED for security
		var baseIV []byte
		if baseIVBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSES3BaseIV]; exists {
			// Decode the base64 encoded base IV
			decodedIV, decodeErr := base64.StdEncoding.DecodeString(string(baseIVBytes))
			if decodeErr != nil {
				return fmt.Errorf("failed to decode base IV for SSE-S3 multipart upload %s: %v", uploadID, decodeErr)
			}
			if len(decodedIV) != s3_constants.AESBlockSize {
				return fmt.Errorf("invalid base IV length for SSE-S3 multipart upload %s: expected %d bytes, got %d", uploadID, s3_constants.AESBlockSize, len(decodedIV))
			}
			baseIV = decodedIV
			glog.V(4).Infof("Using stored base IV %x for SSE-S3 multipart upload %s", baseIV[:8], uploadID)
		} else {
			return fmt.Errorf("no base IV found for SSE-S3 multipart upload %s - required for encryption consistency", uploadID)
		}

		// Retrieve and set key data for consistent multipart encryption - REQUIRED for decryption
		if keyDataBytes, exists := uploadEntry.Extended[s3_constants.SeaweedFSSSES3KeyData]; exists {
			// Key data is already base64 encoded, pass it directly
			keyDataStr := string(keyDataBytes)
			r.Header.Set(s3_constants.SeaweedFSSSES3KeyDataHeader, keyDataStr)
			glog.V(4).Infof("Using stored key data for SSE-S3 multipart upload %s", uploadID)
		} else {
			return fmt.Errorf("no SSE-S3 key data found for multipart upload %s - required for encryption", uploadID)
		}

		// Pass the base IV to putToFiler via header for offset calculation
		r.Header.Set(s3_constants.SeaweedFSSSES3BaseIVHeader, base64.StdEncoding.EncodeToString(baseIV))

	}
	return nil
}
