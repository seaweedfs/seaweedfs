package s3api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pquerna/cachecontrol/cacheobject"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

// Object lock validation errors
var (
	ErrObjectLockVersioningRequired          = errors.New("object lock headers can only be used on buckets with Object Lock enabled")
	ErrInvalidObjectLockMode                 = errors.New("invalid object lock mode")
	ErrInvalidLegalHoldStatus                = errors.New("invalid legal hold status")
	ErrInvalidRetentionDateFormat            = errors.New("invalid retention until date format")
	ErrRetentionDateMustBeFuture             = errors.New("retain until date must be in the future")
	ErrObjectLockModeRequiresDate            = errors.New("object lock mode requires retention until date")
	ErrRetentionDateRequiresMode             = errors.New("retention until date requires object lock mode")
	ErrGovernanceBypassVersioningRequired    = errors.New("governance bypass header can only be used on buckets with Object Lock enabled")
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

// SSEResponseMetadata holds encryption metadata needed for HTTP response headers
type SSEResponseMetadata struct {
	SSEType          string
	KMSKeyID         string
	BucketKeyEnabled bool
}

func (s3a *S3ApiServer) PutObjectHandler(w http.ResponseWriter, r *http.Request) {

	// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(2).Infof("PutObjectHandler bucket=%s object=%s size=%d", bucket, object, r.ContentLength)
	if err := s3a.validateTableBucketObjectPath(bucket, object); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
		return
	}

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

	// Check bucket policy
	if errCode, _ := s3a.checkPolicyWithEntry(r, bucket, object, string(s3_constants.ACTION_WRITE), "", nil); errCode != s3err.ErrNone {
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
		// Split the object into directory path and name
		objectWithoutSlash := strings.TrimSuffix(object, "/")
		dirName := path.Dir(objectWithoutSlash)
		entryName := path.Base(objectWithoutSlash)

		if dirName == "." {
			dirName = ""
		}
		dirName = strings.TrimPrefix(dirName, "/")

		// Construct full directory path
		fullDirPath := s3a.bucketDir(bucket)
		if dirName != "" {
			fullDirPath = fullDirPath + "/" + dirName
		}

		glog.Infof("PutObjectHandler: explicit directory marker %s/%s (contentType=%q, len=%d)",
			bucket, object, objectContentType, r.ContentLength)
		if err := s3a.mkdir(
			fullDirPath, entryName,
			func(entry *filer_pb.Entry) {
				if objectContentType == "" {
					objectContentType = s3_constants.FolderMimeType
				}
				if r.ContentLength > 0 {
					entry.Content, _ = io.ReadAll(r.Body)
				}
				entry.Attributes.Mime = objectContentType

				// Set object owner for directory objects (same as regular objects)
				s3a.setObjectOwnerFromRequest(r, bucket, entry)
			}); err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	} else {
		// Get detailed versioning state for the bucket
		versioningState, err := s3a.getVersioningState(bucket)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				// Auto-create bucket if it doesn't exist (requires Admin permission)
				if !s3a.handleAutoCreateBucket(w, r, bucket, "PutObjectHandler") {
					return
				}
				// Re-fetch versioning state to handle race conditions where
				// another process might have created the bucket with versioning enabled.
				versioningState, err = s3a.getVersioningState(bucket)
				if err != nil {
					glog.Errorf("Error re-checking versioning status for bucket %s after auto-creation: %v", bucket, err)
					s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
					return
				}
			} else {
				glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
		}

		versioningEnabled := (versioningState == s3_constants.VersioningEnabled)
		versioningConfigured := (versioningState != "")

		glog.V(3).Infof("PutObjectHandler: bucket=%s, object=%s, versioningState='%s', versioningEnabled=%v, versioningConfigured=%v", bucket, object, versioningState, versioningEnabled, versioningConfigured)

		// Check if Object Lock is enabled for this bucket
		objectLockEnabled, err := s3a.isObjectLockEnabled(bucket)
		if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			glog.Errorf("Error checking Object Lock status for bucket %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}

		// Validate object lock headers before processing
		if err := s3a.validateObjectLockHeaders(r, objectLockEnabled); err != nil {
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

		switch versioningState {
		case s3_constants.VersioningEnabled:
			// Handle enabled versioning - create new versions with real version IDs
			glog.V(3).Infof("PutObjectHandler: ENABLED versioning detected for %s/%s, calling putVersionedObject", bucket, object)
			versionId, etag, errCode, sseMetadata := s3a.putVersionedObject(r, bucket, object, dataReader, objectContentType)
			if errCode != s3err.ErrNone {
				glog.Errorf("PutObjectHandler: putVersionedObject failed with errCode=%v for %s/%s", errCode, bucket, object)
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			glog.V(3).Infof("PutObjectHandler: putVersionedObject returned versionId=%s, etag=%s for %s/%s", versionId, etag, bucket, object)

			// Set version ID in response header
			if versionId != "" {
				w.Header().Set("x-amz-version-id", versionId)
				glog.V(3).Infof("PutObjectHandler: set x-amz-version-id header to %s for %s/%s", versionId, bucket, object)
			} else {
				glog.Errorf("PutObjectHandler: CRITICAL - versionId is EMPTY for versioned bucket %s, object %s", bucket, object)
			}

			// Set ETag in response
			setEtag(w, etag)

			// Set SSE response headers for versioned objects
			s3a.setSSEResponseHeaders(w, r, sseMetadata)

		case s3_constants.VersioningSuspended:
			// Handle suspended versioning - overwrite with "null" version ID but preserve existing versions
			glog.V(3).Infof("PutObjectHandler: SUSPENDED versioning detected for %s/%s, calling putSuspendedVersioningObject", bucket, object)
			etag, errCode, sseMetadata := s3a.putSuspendedVersioningObject(r, bucket, object, dataReader, objectContentType)
			if errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			// Note: Suspended versioning should NOT return x-amz-version-id header per AWS S3 spec
			// The object is stored with "null" version internally but no version header is returned

			// Set ETag in response
			setEtag(w, etag)

			// Set SSE response headers for suspended versioning
			s3a.setSSEResponseHeaders(w, r, sseMetadata)
		default:
			// Handle regular PUT (never configured versioning)
			filePath := s3a.toFilerPath(bucket, object)
			if objectContentType == "" {
				dataReader = mimeDetect(r, dataReader)
			}

			etag, errCode, sseMetadata := s3a.putToFiler(r, filePath, dataReader, bucket, 1)

			if errCode != s3err.ErrNone {
				s3err.WriteErrorResponse(w, r, errCode)
				return
			}

			// No version ID header for never-configured versioning
			setEtag(w, etag)

			// Set SSE response headers based on encryption type used
			s3a.setSSEResponseHeaders(w, r, sseMetadata)
		}
	}
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3UploadedObjectsCounter.WithLabelValues(bucket).Inc()

	writeSuccessResponseEmpty(w, r)
}

func (s3a *S3ApiServer) putToFiler(r *http.Request, filePath string, dataReader io.Reader, bucket string, partNumber int) (etag string, code s3err.ErrorCode, sseMetadata SSEResponseMetadata) {
	// NEW OPTIMIZATION: Write directly to volume servers, bypassing filer proxy
	// This eliminates the filer proxy overhead for PUT operations
	// Note: filePath is now passed directly instead of URL (no parsing needed)

	// For SSE, encrypt with offset=0 for all parts
	// Each part is encrypted independently, then decrypted using metadata during GET
	partOffset := int64(0)

	// Handle all SSE encryption types in a unified manner
	sseResult, sseErrorCode := s3a.handleAllSSEEncryption(r, dataReader, partOffset)
	if sseErrorCode != s3err.ErrNone {
		return "", sseErrorCode, SSEResponseMetadata{}
	}

	// Extract results from unified SSE handling
	dataReader = sseResult.DataReader
	customerKey := sseResult.CustomerKey
	sseIV := sseResult.SSEIV
	sseKMSKey := sseResult.SSEKMSKey
	sseKMSMetadata := sseResult.SSEKMSMetadata
	sseS3Key := sseResult.SSES3Key
	sseS3Metadata := sseResult.SSES3Metadata
	sseType := sseResult.SSEType

	// Apply bucket default encryption if no explicit encryption was provided
	// This implements AWS S3 behavior where bucket default encryption automatically applies
	if !hasExplicitEncryption(customerKey, sseKMSKey, sseS3Key) && !s3a.cipher {
		glog.V(4).Infof("putToFiler: no explicit encryption detected, checking for bucket default encryption")

		// Apply bucket default encryption and get the result
		encryptionResult, applyErr := s3a.applyBucketDefaultEncryption(bucket, r, dataReader)
		if applyErr != nil {
			glog.Errorf("Failed to apply bucket default encryption: %v", applyErr)
			return "", s3err.ErrInternalError, SSEResponseMetadata{}
		}

		// Update variables based on the result
		dataReader = encryptionResult.DataReader
		sseS3Key = encryptionResult.SSES3Key
		sseKMSKey = encryptionResult.SSEKMSKey

		// If bucket-default encryption selected an algorithm, reflect it in SSE type
		if sseType == "" {
			if sseS3Key != nil {
				sseType = s3_constants.SSETypeS3
			} else if sseKMSKey != nil {
				sseType = s3_constants.SSETypeKMS
			}
		}

		// If SSE-S3 was applied by bucket default, prepare metadata (if not already done)
		if sseS3Key != nil && len(sseS3Metadata) == 0 {
			var metaErr error
			sseS3Metadata, metaErr = SerializeSSES3Metadata(sseS3Key)
			if metaErr != nil {
				glog.Errorf("Failed to serialize SSE-S3 metadata for bucket default encryption: %v", metaErr)
				return "", s3err.ErrInternalError, SSEResponseMetadata{}
			}
		}
	} else {
		glog.V(4).Infof("putToFiler: explicit encryption already applied, skipping bucket default encryption")
	}

	// filePath is already provided directly - no URL parsing needed
	// Step 1 & 2: Use auto-chunking to handle large files without OOM
	// This splits large uploads into 8MB chunks, preventing memory issues on both S3 API and volume servers
	const chunkSize = 8 * 1024 * 1024 // 8MB chunks (S3 standard)
	const smallFileLimit = 256 * 1024 // 256KB - store inline in filer

	collection := ""
	if s3a.option.FilerGroup != "" {
		collection = s3a.getCollectionName(bucket)
	}
	ttlSeconds := s3a.resolveBucketLifecycleTTLSeconds(bucket, filePath)

	// Create assign function for chunked upload
	assignFunc := func(ctx context.Context, count int) (*operation.VolumeAssignRequest, *operation.AssignResult, error) {
		var assignResult *filer_pb.AssignVolumeResponse
		err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.AssignVolume(ctx, &filer_pb.AssignVolumeRequest{
				Count:       int32(count),
				Replication: "",
				Collection:  collection,
				DiskType:    "",
				DataCenter:  s3a.option.DataCenter,
				Path:        filePath,
				TtlSec:      ttlSeconds,
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
			return nil, nil, err
		}

		// Convert filer_pb.AssignVolumeResponse to operation.AssignResult
		return nil, &operation.AssignResult{
			Fid:       assignResult.FileId,
			Url:       assignResult.Location.Url,
			PublicUrl: assignResult.Location.PublicUrl,
			Count:     uint64(count),
			Auth:      security.EncodedJwt(assignResult.Auth),
		}, nil
	}

	// Upload with auto-chunking
	// Use context.Background() to ensure chunk uploads complete even if HTTP request is cancelled
	// This prevents partial uploads and data corruption
	chunkResult, err := operation.UploadReaderInChunks(context.Background(), dataReader, &operation.ChunkedUploadOption{
		ChunkSize:       chunkSize,
		SmallFileLimit:  smallFileLimit,
		Collection:      collection,
		DataCenter:      s3a.option.DataCenter,
		SaveSmallInline: false, // S3 API always creates chunks, never stores inline
		MimeType:        r.Header.Get("Content-Type"),
		Cipher:          s3a.cipher, // encrypt data on volume servers
		AssignFunc:      assignFunc,
	})
	if err != nil {
		glog.Errorf("putToFiler: chunked upload failed: %v", err)

		// CRITICAL: Cleanup orphaned chunks before returning error
		// UploadReaderInChunks now returns partial results even on error,
		// allowing us to cleanup any chunks that were successfully uploaded
		// before the failure occurred
		if chunkResult != nil && len(chunkResult.FileChunks) > 0 {
			glog.Warningf("putToFiler: Upload failed, attempting to cleanup %d orphaned chunks", len(chunkResult.FileChunks))
			s3a.deleteOrphanedChunks(chunkResult.FileChunks)
		}

		if strings.Contains(err.Error(), s3err.ErrMsgPayloadChecksumMismatch) {
			return "", s3err.ErrInvalidDigest, SSEResponseMetadata{}
		}
		return "", s3err.ErrInternalError, SSEResponseMetadata{}
	}

	// Step 3: Calculate MD5 hash and add SSE metadata to chunks
	md5Sum := chunkResult.Md5Hash.Sum(nil)

	glog.V(4).Infof("putToFiler: Chunked upload SUCCESS - path=%s, chunks=%d, size=%d",
		filePath, len(chunkResult.FileChunks), chunkResult.TotalSize)

	// Log chunk details for debugging (verbose only - high frequency)
	if glog.V(4) {
		for i, chunk := range chunkResult.FileChunks {
			glog.Infof("  PUT Chunk[%d]: fid=%s, offset=%d, size=%d", i, chunk.GetFileIdString(), chunk.Offset, chunk.Size)
		}
	}

	// Add SSE metadata to all chunks if present
	for _, chunk := range chunkResult.FileChunks {
		switch {
		case customerKey != nil:
			// SSE-C: Create per-chunk metadata (matches filer logic)
			chunk.SseType = filer_pb.SSEType_SSE_C
			if len(sseIV) > 0 {
				// PartOffset tracks position within the encrypted stream
				// Since ALL uploads (single-part and multipart parts) encrypt starting from offset 0,
				// PartOffset = chunk.Offset represents where this chunk is in that encrypted stream
				// - Single-part: chunk.Offset is position in the file's encrypted stream
				// - Multipart: chunk.Offset is position in this part's encrypted stream
				ssecMetadataStruct := struct {
					Algorithm  string `json:"algorithm"`
					IV         string `json:"iv"`
					KeyMD5     string `json:"keyMD5"`
					PartOffset int64  `json:"partOffset"`
				}{
					Algorithm:  "AES256",
					IV:         base64.StdEncoding.EncodeToString(sseIV),
					KeyMD5:     customerKey.KeyMD5,
					PartOffset: chunk.Offset, // Position within the encrypted stream (always encrypted from 0)
				}
				if ssecMetadata, serErr := json.Marshal(ssecMetadataStruct); serErr == nil {
					chunk.SseMetadata = ssecMetadata
				}
			}
		case sseKMSKey != nil:
			// SSE-KMS: Create per-chunk metadata with chunk-specific offsets
			// Each chunk needs its own metadata with ChunkOffset set for proper IV calculation during decryption
			chunk.SseType = filer_pb.SSEType_SSE_KMS

			// Create a copy of the SSE-KMS key with chunk-specific offset
			chunkSSEKey := &SSEKMSKey{
				KeyID:             sseKMSKey.KeyID,
				EncryptedDataKey:  sseKMSKey.EncryptedDataKey,
				EncryptionContext: sseKMSKey.EncryptionContext,
				BucketKeyEnabled:  sseKMSKey.BucketKeyEnabled,
				IV:                sseKMSKey.IV,
				ChunkOffset:       chunk.Offset, // Set chunk-specific offset for IV calculation
			}

			// Serialize per-chunk metadata
			if chunkMetadata, serErr := SerializeSSEKMSMetadata(chunkSSEKey); serErr == nil {
				chunk.SseMetadata = chunkMetadata
			} else {
				glog.Errorf("Failed to serialize SSE-KMS metadata for chunk at offset %d: %v", chunk.Offset, serErr)
			}
		case sseS3Key != nil:
			// SSE-S3: Create per-chunk metadata with chunk-specific IVs
			// Each chunk needs its own IV calculated from the base IV + chunk offset
			chunk.SseType = filer_pb.SSEType_SSE_S3

			// Calculate chunk-specific IV using base IV and chunk offset
			chunkIV, _ := calculateIVWithOffset(sseS3Key.IV, chunk.Offset)

			// Create a copy of the SSE-S3 key with chunk-specific IV
			chunkSSEKey := &SSES3Key{
				Key:       sseS3Key.Key,
				KeyID:     sseS3Key.KeyID,
				Algorithm: sseS3Key.Algorithm,
				IV:        chunkIV, // Use chunk-specific IV
			}

			// Serialize per-chunk metadata
			if chunkMetadata, serErr := SerializeSSES3Metadata(chunkSSEKey); serErr == nil {
				chunk.SseMetadata = chunkMetadata
			} else {
				glog.Errorf("Failed to serialize SSE-S3 metadata for chunk at offset %d: %v", chunk.Offset, serErr)
			}
		}
	}

	// Step 4: Create metadata entry
	now := time.Now()
	mimeType := r.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// Create entry
	entry := &filer_pb.Entry{
		Name:        path.Base(filePath),
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Crtime:   now.Unix(),
			Mtime:    now.Unix(),
			FileMode: 0660,
			Uid:      0,
			Gid:      0,
			Mime:     mimeType,
			FileSize: uint64(chunkResult.TotalSize),
		},
		Chunks:   chunkResult.FileChunks, // All chunks from auto-chunking
		Extended: make(map[string][]byte),
	}
	if ttlSeconds > 0 {
		entry.Attributes.TtlSec = ttlSeconds
	}

	// Always set Md5 attribute for regular object uploads (PutObject)
	// This ensures the ETag is a pure MD5 hash, which AWS S3 SDKs expect
	// for PutObject responses. The composite "md5-count" format is only
	// used for multipart upload completion (CompleteMultipartUpload API),
	// not for regular PutObject even if the file is internally auto-chunked.
	entry.Attributes.Md5 = md5Sum

	// Calculate ETag - with Md5 set, this returns the pure MD5 hash
	etag = filer.ETag(entry)
	glog.V(4).Infof("putToFiler: Calculated ETag=%s for %d chunks", etag, len(chunkResult.FileChunks))

	// Store ETag in Extended attribute for future retrieval (e.g. multipart parts)
	entry.Extended[s3_constants.ExtETagKey] = []byte(etag)

	// Set object owner
	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
	if amzAccountId != "" {
		entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
		glog.V(2).Infof("putToFiler: setting owner %s for object %s", amzAccountId, filePath)
	}

	// Set version ID if present
	if versionIdHeader := r.Header.Get(s3_constants.ExtVersionIdKey); versionIdHeader != "" {
		entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionIdHeader)
		glog.V(3).Infof("putToFiler: setting version ID %s for object %s", versionIdHeader, filePath)
	}

	// Set TTL-based S3 expiry flag only if object has a TTL
	if entry.Attributes.TtlSec > 0 {
		entry.Extended[s3_constants.SeaweedFSExpiresS3] = []byte("true")
	}

	// Copy user metadata and standard headers
	for k, v := range r.Header {
		if len(v) > 0 && len(v[0]) > 0 {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				// Go's HTTP server canonicalizes headers (e.g., x-amz-meta-foo → X-Amz-Meta-Foo)
				// We store them as they come in (after canonicalization) to preserve the user's intent
				entry.Extended[k] = []byte(v[0])
			} else {
				switch k {
				case "Cache-Control", "Expires", "Content-Disposition", "Content-Encoding", "Content-Language":
					entry.Extended[k] = []byte(v[0])
				}
			}
			if k == "Response-Content-Disposition" {
				entry.Extended["Content-Disposition"] = []byte(v[0])
			}
		}
	}

	// Store the storage class from header
	if sc := r.Header.Get(s3_constants.AmzStorageClass); sc != "" {
		if !validateStorageClass(sc) {
			glog.Warningf("putToFiler: Invalid storage class '%s' for %s", sc, filePath)
			return "", s3err.ErrInvalidStorageClass, SSEResponseMetadata{}
		}
		entry.Extended[s3_constants.AmzStorageClass] = []byte(sc)
	}

	// Parse and store object tags from X-Amz-Tagging header
	// Fix for GitHub issue #7589: Tags sent during object upload were not being stored
	if tagging := r.Header.Get(s3_constants.AmzObjectTagging); tagging != "" {
		parsedTags, err := url.ParseQuery(tagging)
		if err != nil {
			glog.Warningf("putToFiler: Invalid S3 tag format in header '%s': %v", tagging, err)
			return "", s3err.ErrInvalidTag, SSEResponseMetadata{}
		}
		for key, values := range parsedTags {
			if len(values) > 1 {
				glog.Warningf("putToFiler: Duplicate tag key '%s' in header", key)
				return "", s3err.ErrInvalidTag, SSEResponseMetadata{}
			}
			value := ""
			if len(values) > 0 {
				value = values[0]
			}
			entry.Extended[s3_constants.AmzObjectTagging+"-"+key] = []byte(value)
		}
		glog.V(3).Infof("putToFiler: stored %d tags from X-Amz-Tagging header", len(parsedTags))
	}

	// Set SSE-C metadata
	if customerKey != nil && len(sseIV) > 0 {
		// Store IV as RAW bytes (matches filer behavior - filer decodes base64 headers and stores raw bytes)
		entry.Extended[s3_constants.SeaweedFSSSEIV] = sseIV
		entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
		entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(customerKey.KeyMD5)
		glog.V(3).Infof("putToFiler: storing SSE-C metadata - IV len=%d", len(sseIV))
	}

	// Set SSE-KMS metadata
	if sseKMSKey != nil {
		// Store metadata as RAW bytes (matches filer behavior - filer decodes base64 headers and stores raw bytes)
		entry.Extended[s3_constants.SeaweedFSSSEKMSKey] = sseKMSMetadata
		// Set standard SSE headers for detection
		entry.Extended[s3_constants.AmzServerSideEncryption] = []byte("aws:kms")
		entry.Extended[s3_constants.AmzServerSideEncryptionAwsKmsKeyId] = []byte(sseKMSKey.KeyID)
		glog.V(3).Infof("putToFiler: storing SSE-KMS metadata - keyID=%s, raw len=%d", sseKMSKey.KeyID, len(sseKMSMetadata))
	}

	// Set SSE-S3 metadata
	if sseS3Key != nil && len(sseS3Metadata) > 0 {
		// Store metadata as RAW bytes (matches filer behavior - filer decodes base64 headers and stores raw bytes)
		entry.Extended[s3_constants.SeaweedFSSSES3Key] = sseS3Metadata
		// Set standard SSE header for detection
		entry.Extended[s3_constants.AmzServerSideEncryption] = []byte("AES256")
		glog.V(3).Infof("putToFiler: storing SSE-S3 metadata - keyID=%s, raw len=%d", sseS3Key.KeyID, len(sseS3Metadata))
	}

	// Step 4: Save metadata to filer via gRPC
	// Use context.Background() to ensure metadata save completes even if HTTP request is cancelled
	// This matches the chunk upload behavior and prevents orphaned chunks
	glog.V(3).Infof("putToFiler: About to create entry - dir=%s, name=%s, chunks=%d, extended keys=%d",
		path.Dir(filePath), path.Base(filePath), len(entry.Chunks), len(entry.Extended))
	createErr := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		req := &filer_pb.CreateEntryRequest{
			Directory: path.Dir(filePath),
			Entry:     entry,
		}
		glog.V(3).Infof("putToFiler: Calling CreateEntry for %s", filePath)
		_, err := client.CreateEntry(context.Background(), req)
		if err != nil {
			glog.Errorf("putToFiler: CreateEntry returned error: %v", err)
		}
		return err
	})
	if createErr != nil {
		glog.Errorf("putToFiler: failed to create entry for %s: %v", filePath, createErr)

		// CRITICAL: Cleanup orphaned chunks before returning error
		// If CreateEntry fails, the uploaded chunks are orphaned and must be deleted
		// to prevent resource leaks and wasted storage
		if len(chunkResult.FileChunks) > 0 {
			glog.Warningf("putToFiler: CreateEntry failed, attempting to cleanup %d orphaned chunks", len(chunkResult.FileChunks))
			s3a.deleteOrphanedChunks(chunkResult.FileChunks)
		}

		return "", filerErrorToS3Error(createErr), SSEResponseMetadata{}
	}
	glog.V(3).Infof("putToFiler: CreateEntry SUCCESS for %s", filePath)

	glog.V(2).Infof("putToFiler: Metadata saved SUCCESS - path=%s, etag(hex)=%s, size=%d, partNumber=%d",
		filePath, etag, entry.Attributes.FileSize, partNumber)

	BucketTrafficReceived(chunkResult.TotalSize, r)

	// Build SSE response metadata with encryption details
	responseMetadata := SSEResponseMetadata{
		SSEType: sseType,
	}

	// For SSE-KMS, include key ID and bucket-key-enabled flag from stored metadata
	if sseKMSKey != nil {
		responseMetadata.KMSKeyID = sseKMSKey.KeyID
		responseMetadata.BucketKeyEnabled = sseKMSKey.BucketKeyEnabled
		glog.V(4).Infof("putToFiler: returning SSE-KMS metadata - keyID=%s, bucketKeyEnabled=%v",
			sseKMSKey.KeyID, sseKMSKey.BucketKeyEnabled)
	}

	return etag, s3err.ErrNone, responseMetadata
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

// setSSEResponseHeaders sets appropriate SSE response headers based on encryption type
func (s3a *S3ApiServer) setSSEResponseHeaders(w http.ResponseWriter, r *http.Request, sseMetadata SSEResponseMetadata) {
	switch sseMetadata.SSEType {
	case s3_constants.SSETypeS3:
		// SSE-S3: Return the encryption algorithm
		w.Header().Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmAES256)

	case s3_constants.SSETypeC:
		// SSE-C: Echo back the customer-provided algorithm and key MD5
		if algo := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); algo != "" {
			w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, algo)
		}
		if keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); keyMD5 != "" {
			w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5)
		}

	case s3_constants.SSETypeKMS:
		// SSE-KMS: Return the KMS key ID and algorithm
		w.Header().Set(s3_constants.AmzServerSideEncryption, "aws:kms")

		// Use metadata from stored encryption config (for bucket-default encryption)
		// or fall back to request headers (for explicit encryption)
		if sseMetadata.KMSKeyID != "" {
			w.Header().Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, sseMetadata.KMSKeyID)
		} else if keyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId); keyID != "" {
			w.Header().Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, keyID)
		}

		// Set bucket-key-enabled header if it was enabled
		if sseMetadata.BucketKeyEnabled {
			w.Header().Set(s3_constants.AmzServerSideEncryptionBucketKeyEnabled, "true")
		} else if bucketKeyEnabled := r.Header.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled); bucketKeyEnabled == "true" {
			w.Header().Set(s3_constants.AmzServerSideEncryptionBucketKeyEnabled, "true")
		}
	}
}

func filerErrorToS3Error(err error) s3err.ErrorCode {
	if err == nil {
		return s3err.ErrNone
	}

	errString := err.Error()

	switch {
	case errString == constants.ErrMsgBadDigest:
		return s3err.ErrBadDigest
	case errors.Is(err, weed_server.ErrReadOnly):
		// Bucket is read-only due to quota enforcement or other configuration
		// Return 403 Forbidden per S3 semantics (similar to MinIO's quota enforcement)
		// Uses errors.Is() to properly detect wrapped errors
		return s3err.ErrAccessDenied
	case strings.Contains(errString, "context canceled") || strings.Contains(errString, "code = Canceled"):
		// Client canceled the request, return client error not server error
		return s3err.ErrInvalidRequest
	case strings.HasPrefix(errString, "existing ") && strings.HasSuffix(errString, "is a directory"):
		return s3err.ErrExistingObjectIsDirectory
	case strings.HasSuffix(errString, "is a file"):
		return s3err.ErrExistingObjectIsFile
	default:
		return s3err.ErrInternalError
	}
}

// setObjectOwnerFromRequest sets the object owner metadata based on the bucket ownership policy.
// When BucketOwnerEnforced (the modern AWS default), the bucket owner owns all objects.
// Otherwise, the uploader's account ID is used (ObjectWriter mode).
func (s3a *S3ApiServer) setObjectOwnerFromRequest(r *http.Request, bucket string, entry *filer_pb.Entry) {
	var ownerId string

	// Check if bucketRegistry is available
	if s3a.bucketRegistry == nil {
		// Fallback to uploader if registry unavailable
		ownerId = r.Header.Get(s3_constants.AmzAccountId)
		glog.V(2).Infof("setObjectOwnerFromRequest: bucketRegistry unavailable, fallback to uploader %s", ownerId)
	} else {
		// Check bucket ownership policy
		bucketMetadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
		useBucketOwner := errCode == s3err.ErrNone && bucketMetadata != nil &&
			bucketMetadata.ObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced &&
			bucketMetadata.Owner != nil && bucketMetadata.Owner.ID != nil

		if useBucketOwner {
			ownerId = *bucketMetadata.Owner.ID
			glog.V(2).Infof("setObjectOwnerFromRequest: using bucket owner %s (BucketOwnerEnforced)", ownerId)
		} else {
			ownerId = r.Header.Get(s3_constants.AmzAccountId)
			if errCode != s3err.ErrNone || bucketMetadata == nil {
				glog.V(2).Infof("setObjectOwnerFromRequest: fallback to uploader %s", ownerId)
			} else if bucketMetadata.ObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced {
				glog.V(2).Infof("setObjectOwnerFromRequest: BucketOwnerEnforced but no owner found, fallback to uploader %s", ownerId)
			} else {
				glog.V(2).Infof("setObjectOwnerFromRequest: using uploader %s (ObjectWriter mode)", ownerId)
			}
		}
	}

	if ownerId != "" {
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(ownerId)
	}
}

// putSuspendedVersioningObject handles PUT operations for buckets with suspended versioning.
//
// Key architectural approach:
// Instead of creating the file and then updating its metadata (which can cause race conditions and duplicate versions),
// we set all required metadata as HTTP headers BEFORE calling putToFiler. The filer automatically stores any header
// starting with "Seaweed-" in entry.Extended during file creation, ensuring atomic metadata persistence.
//
// This approach eliminates:
// - Race conditions from read-after-write consistency delays
// - Need for retry loops and exponential backoff
// - Duplicate entries from separate create/update operations
//
// For suspended versioning, objects are stored as regular files (version ID "null") in the bucket directory,
// while existing versions from when versioning was enabled remain preserved in the .versions subdirectory.
func (s3a *S3ApiServer) putSuspendedVersioningObject(r *http.Request, bucket, object string, dataReader io.Reader, objectContentType string) (etag string, errCode s3err.ErrorCode, sseMetadata SSEResponseMetadata) {
	// Normalize object path to ensure consistency with toFilerPath behavior
	normalizedObject := removeDuplicateSlashes(object)

	glog.V(3).Infof("putSuspendedVersioningObject: START bucket=%s, object=%s, normalized=%s",
		bucket, object, normalizedObject)

	bucketDir := s3a.bucketDir(bucket)

	// Check if there's an existing null version in .versions directory and delete it
	// This ensures suspended versioning properly overwrites the null version as per S3 spec
	// Note: We only delete null versions, NOT regular versions (those should be preserved)
	versionsObjectPath := normalizedObject + s3_constants.VersionsFolder
	versionsDir := bucketDir + "/" + versionsObjectPath
	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err == nil {
		// .versions directory exists
		glog.V(3).Infof("putSuspendedVersioningObject: found %d entries in .versions for %s/%s", len(entries), bucket, object)
		for _, entry := range entries {
			if entry.Extended != nil {
				if versionIdBytes, ok := entry.Extended[s3_constants.ExtVersionIdKey]; ok {
					versionId := string(versionIdBytes)
					glog.V(3).Infof("putSuspendedVersioningObject: found version '%s' in .versions", versionId)
					if versionId == "null" {
						// Only delete null version - preserve real versioned entries
						glog.V(3).Infof("putSuspendedVersioningObject: deleting null version from .versions")
						err := s3a.rm(versionsDir, entry.Name, true, false)
						if err != nil {
							glog.Warningf("putSuspendedVersioningObject: failed to delete null version: %v", err)
						} else {
							glog.V(3).Infof("putSuspendedVersioningObject: successfully deleted null version")
						}
						break
					}
				}
			}
		}
	} else {
		glog.V(3).Infof("putSuspendedVersioningObject: no .versions directory for %s/%s", bucket, object)
	}

	filePath := s3a.toFilerPath(bucket, normalizedObject)

	body := dataReader
	if objectContentType == "" {
		body = mimeDetect(r, body)
	}

	// Set all metadata headers BEFORE calling putToFiler
	// This ensures the metadata is set during file creation, not after
	// The filer automatically stores any header starting with "Seaweed-" in entry.Extended

	// Set version ID to "null" for suspended versioning
	r.Header.Set(s3_constants.ExtVersionIdKey, "null")

	// Extract and set object lock metadata as headers
	// This handles retention mode, retention date, and legal hold
	explicitMode := r.Header.Get(s3_constants.AmzObjectLockMode)
	explicitRetainUntilDate := r.Header.Get(s3_constants.AmzObjectLockRetainUntilDate)

	if explicitMode != "" {
		r.Header.Set(s3_constants.ExtObjectLockModeKey, explicitMode)
		glog.V(2).Infof("putSuspendedVersioningObject: setting object lock mode header: %s", explicitMode)
	}

	if explicitRetainUntilDate != "" {
		// Parse and convert to Unix timestamp
		parsedTime, err := time.Parse(time.RFC3339, explicitRetainUntilDate)
		if err != nil {
			glog.Errorf("putSuspendedVersioningObject: failed to parse retention until date: %v", err)
			return "", s3err.ErrInvalidRequest, SSEResponseMetadata{}
		}
		r.Header.Set(s3_constants.ExtRetentionUntilDateKey, strconv.FormatInt(parsedTime.Unix(), 10))
		glog.V(2).Infof("putSuspendedVersioningObject: setting retention until date header (timestamp: %d)", parsedTime.Unix())
	}

	if legalHold := r.Header.Get(s3_constants.AmzObjectLockLegalHold); legalHold != "" {
		if legalHold == s3_constants.LegalHoldOn || legalHold == s3_constants.LegalHoldOff {
			r.Header.Set(s3_constants.ExtLegalHoldKey, legalHold)
			glog.V(2).Infof("putSuspendedVersioningObject: setting legal hold header: %s", legalHold)
		} else {
			glog.Errorf("putSuspendedVersioningObject: invalid legal hold value: %s", legalHold)
			return "", s3err.ErrInvalidRequest, SSEResponseMetadata{}
		}
	}

	// Apply bucket default retention if no explicit retention was provided
	if explicitMode == "" && explicitRetainUntilDate == "" {
		// Create a temporary entry to apply defaults
		tempEntry := &filer_pb.Entry{Extended: make(map[string][]byte)}
		if err := s3a.applyBucketDefaultRetention(bucket, tempEntry); err == nil {
			// Copy default retention headers from temp entry
			if modeBytes, ok := tempEntry.Extended[s3_constants.ExtObjectLockModeKey]; ok {
				r.Header.Set(s3_constants.ExtObjectLockModeKey, string(modeBytes))
				glog.V(2).Infof("putSuspendedVersioningObject: applied bucket default retention mode: %s", string(modeBytes))
			}
			if dateBytes, ok := tempEntry.Extended[s3_constants.ExtRetentionUntilDateKey]; ok {
				r.Header.Set(s3_constants.ExtRetentionUntilDateKey, string(dateBytes))
				glog.V(2).Infof("putSuspendedVersioningObject: applied bucket default retention date")
			}
		}
	}

	// Upload the file using putToFiler - this will create the file with version metadata
	etag, errCode, sseMetadata = s3a.putToFiler(r, filePath, body, bucket, 1)
	if errCode != s3err.ErrNone {
		glog.Errorf("putSuspendedVersioningObject: failed to upload object: %v", errCode)
		return "", errCode, SSEResponseMetadata{}
	}

	// Update all existing versions/delete markers to set IsLatest=false since "null" is now latest
	err = s3a.updateIsLatestFlagsForSuspendedVersioning(bucket, normalizedObject)
	if err != nil {
		glog.Warningf("putSuspendedVersioningObject: failed to update IsLatest flags: %v", err)
		// Don't fail the request, but log the warning
	}

	glog.V(2).Infof("putSuspendedVersioningObject: successfully created null version for %s/%s", bucket, object)

	return etag, s3err.ErrNone, sseMetadata
}

// updateIsLatestFlagsForSuspendedVersioning sets IsLatest=false on all existing versions/delete markers
// when a new "null" version becomes the latest during suspended versioning
func (s3a *S3ApiServer) updateIsLatestFlagsForSuspendedVersioning(bucket, object string) error {
	bucketDir := s3a.bucketDir(bucket)
	versionsObjectPath := object + s3_constants.VersionsFolder
	versionsDir := bucketDir + "/" + versionsObjectPath

	glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: updating flags for %s/%s", bucket, object)

	// Check if .versions directory exists
	_, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		// No .versions directory exists, nothing to update
		glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: no .versions directory for %s/%s", bucket, object)
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

		glog.V(2).Infof("updateIsLatestFlagsForSuspendedVersioning: cleared latest version metadata for %s/%s", bucket, object)
	}

	return nil
}

func (s3a *S3ApiServer) putVersionedObject(r *http.Request, bucket, object string, dataReader io.Reader, objectContentType string) (versionId string, etag string, errCode s3err.ErrorCode, sseMetadata SSEResponseMetadata) {
	// Normalize object path to ensure consistency with toFilerPath behavior
	normalizedObject := removeDuplicateSlashes(object)

	// Check if .versions directory exists to determine format
	useInvertedFormat := s3a.getVersionIdFormat(bucket, normalizedObject)

	// Generate version ID using the appropriate format
	versionId = generateVersionId(useInvertedFormat)

	glog.V(2).Infof("putVersionedObject: creating version %s for %s/%s (normalized: %s, inverted=%v)", versionId, bucket, object, normalizedObject, useInvertedFormat)

	// Create the version file name
	versionFileName := s3a.getVersionFileName(versionId)

	// Upload directly to the versions directory
	// We need to construct the object path relative to the bucket
	versionObjectPath := normalizedObject + s3_constants.VersionsFolder + "/" + versionFileName
	versionFilePath := s3a.toFilerPath(bucket, versionObjectPath)
	bucketDir := s3a.bucketDir(bucket)

	body := dataReader
	if objectContentType == "" {
		body = mimeDetect(r, body)
	}

	glog.V(2).Infof("putVersionedObject: uploading %s/%s version %s to %s", bucket, object, versionId, versionFilePath)

	etag, errCode, sseMetadata = s3a.putToFiler(r, versionFilePath, body, bucket, 1)
	if errCode != s3err.ErrNone {
		glog.Errorf("putVersionedObject: failed to upload version: %v", errCode)
		return "", "", errCode, SSEResponseMetadata{}
	}

	// Get the uploaded entry to add versioning metadata
	// Use retry logic to handle filer consistency delays
	var versionEntry *filer_pb.Entry
	var err error
	maxRetries := 8
	for attempt := 1; attempt <= maxRetries; attempt++ {
		versionEntry, err = s3a.getEntry(bucketDir, versionObjectPath)
		if err == nil {
			break
		}

		if attempt < maxRetries {
			// Exponential backoff: 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms
			delay := time.Millisecond * time.Duration(10*(1<<(attempt-1)))
			time.Sleep(delay)
		}
	}

	if err != nil {
		glog.Errorf("putVersionedObject: failed to get version entry after %d attempts: %v", maxRetries, err)
		return "", "", s3err.ErrInternalError, SSEResponseMetadata{}
	}

	// Add versioning metadata to this version
	if versionEntry.Extended == nil {
		versionEntry.Extended = make(map[string][]byte)
	}
	versionEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)

	// Store ETag (unquoted) in Extended attribute
	versionEntry.Extended[s3_constants.ExtETagKey] = []byte(etag)

	// Set object owner for versioned objects
	s3a.setObjectOwnerFromRequest(r, bucket, versionEntry)

	// Extract and store object lock metadata from request headers
	if err := s3a.extractObjectLockMetadataFromRequest(r, versionEntry); err != nil {
		glog.Errorf("putVersionedObject: failed to extract object lock metadata: %v", err)
		return "", "", s3err.ErrInvalidRequest, SSEResponseMetadata{}
	}

	// Update the version entry with metadata
	err = s3a.mkFile(bucketDir, versionObjectPath, versionEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = versionEntry.Extended
		updatedEntry.Attributes = versionEntry.Attributes
		updatedEntry.Chunks = versionEntry.Chunks
	})
	if err != nil {
		glog.Errorf("putVersionedObject: failed to update version metadata: %v", err)
		return "", "", s3err.ErrInternalError, SSEResponseMetadata{}
	}

	// Update the .versions directory metadata to indicate this is the latest version
	// Pass versionEntry to cache its metadata for single-scan list efficiency
	err = s3a.updateLatestVersionInDirectory(bucket, normalizedObject, versionId, versionFileName, versionEntry)
	if err != nil {
		glog.Errorf("putVersionedObject: failed to update latest version in directory: %v", err)
		return "", "", s3err.ErrInternalError, SSEResponseMetadata{}
	}
	glog.V(2).Infof("putVersionedObject: successfully created version %s for %s/%s (normalized: %s)", versionId, bucket, object, normalizedObject)
	return versionId, etag, s3err.ErrNone, sseMetadata
}

// updateLatestVersionInDirectory updates the .versions directory metadata to indicate the latest version
// versionEntry contains the metadata (size, ETag, mtime, owner) to cache for single-scan list efficiency
func (s3a *S3ApiServer) updateLatestVersionInDirectory(bucket, object, versionId, versionFileName string, versionEntry *filer_pb.Entry) error {
	bucketDir := s3a.bucketDir(bucket)
	versionsObjectPath := object + s3_constants.VersionsFolder

	// Get the current .versions directory entry with retry logic for filer consistency
	var versionsEntry *filer_pb.Entry
	var err error
	maxRetries := 8
	for attempt := 1; attempt <= maxRetries; attempt++ {
		versionsEntry, err = s3a.getEntry(bucketDir, versionsObjectPath)
		if err == nil {
			break
		}

		if attempt < maxRetries {
			// Exponential backoff with higher base: 100ms, 200ms, 400ms, 800ms, 1600ms, 3200ms, 6400ms
			delay := time.Millisecond * time.Duration(100*(1<<(attempt-1)))
			time.Sleep(delay)
		}
	}

	if err != nil {
		glog.Errorf("updateLatestVersionInDirectory: failed to get .versions directory for %s/%s after %d attempts: %v", bucket, object, maxRetries, err)
		return fmt.Errorf("failed to get .versions directory after %d attempts: %w", maxRetries, err)
	}

	// Add or update the latest version metadata
	if versionsEntry.Extended == nil {
		versionsEntry.Extended = make(map[string][]byte)
	}
	versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey] = []byte(versionId)
	versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey] = []byte(versionFileName)

	// Cache list metadata for single-scan efficiency (avoids extra getEntry per object during list)
	setCachedListMetadata(versionsEntry, versionEntry)

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
	if err != nil {
		// Check if this is just "no encryption configured" vs a real error
		if errors.Is(err, ErrNoEncryptionConfig) {
			// No default encryption configured, return original reader
			return &BucketDefaultEncryptionResult{DataReader: dataReader}, nil
		}
		// Real error - propagate to prevent silent encryption bypass
		return nil, fmt.Errorf("failed to read bucket encryption config: %v", err)
	}
	if encryptionConfig == nil {
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
	// Use bucket parameter passed to function (not from request parsing)
	_, object := s3_constants.GetBucketAndObject(r)
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
// objectLockEnabled should be true only if the bucket has Object Lock configured
func (s3a *S3ApiServer) validateObjectLockHeaders(r *http.Request, objectLockEnabled bool) error {
	// Extract object lock headers from request
	mode := r.Header.Get(s3_constants.AmzObjectLockMode)
	retainUntilDateStr := r.Header.Get(s3_constants.AmzObjectLockRetainUntilDate)
	legalHold := r.Header.Get(s3_constants.AmzObjectLockLegalHold)

	// Check if any object lock headers are present
	hasObjectLockHeaders := mode != "" || retainUntilDateStr != "" || legalHold != ""

	// Object lock headers can only be used on buckets with Object Lock enabled
	// Per AWS S3: Object Lock can only be enabled at bucket creation, and once enabled,
	// objects can have retention/legal-hold metadata. Without Object Lock enabled,
	// these headers must be rejected.
	if hasObjectLockHeaders && !objectLockEnabled {
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

	// Check for governance bypass header - only valid for buckets with Object Lock enabled
	bypassGovernance := r.Header.Get("x-amz-bypass-governance-retention") == "true"

	// Governance bypass headers are only valid for buckets with Object Lock enabled
	if bypassGovernance && !objectLockEnabled {
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
		etag := string(etagBytes)
		if len(etag) > 0 {
			if !strings.HasPrefix(etag, "\"") {
				return "\"" + etag + "\""
			}
			return etag
		}
		// Empty stored ETag — fall through to Md5/chunk-based calculation
	}
	// Check for Md5 in Attributes (matches filer.ETag behavior)
	// Note: len(nil slice) == 0 in Go, so no need for explicit nil check
	if entry.Attributes != nil && len(entry.Attributes.Md5) > 0 {
		return fmt.Sprintf("\"%x\"", entry.Attributes.Md5)
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

// validateConditionalHeaders checks conditional headers against the provided entry
func (s3a *S3ApiServer) validateConditionalHeaders(r *http.Request, headers conditionalHeaders, entry *filer_pb.Entry, bucket, object string) s3err.ErrorCode {
	if !headers.isSet {
		return s3err.ErrNone
	}

	objectExists := entry != nil

	// For PUT requests, all specified conditions must be met.
	// The evaluation order follows AWS S3 behavior for consistency.

	// 1. Check If-Match
	if headers.ifMatch != "" {
		if !objectExists {
			return s3err.ErrPreconditionFailed
		}
		// If `ifMatch` is "*", the condition is met if the object exists.
		// Otherwise, we need to check the ETag.
		if headers.ifMatch != "*" {
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			// Use production etagMatches method
			if !s3a.etagMatches(headers.ifMatch, objectETag) {
				return s3err.ErrPreconditionFailed
			}
		}
		glog.V(3).Infof("validateConditionalHeaders: If-Match passed for object %s/%s", bucket, object)
	}

	// 2. Check If-Unmodified-Since
	if !headers.ifUnmodifiedSince.IsZero() {
		if objectExists {
			if entry.Attributes != nil {
				objectModTime := time.Unix(entry.Attributes.Mtime, 0)
				if objectModTime.After(headers.ifUnmodifiedSince) {
					glog.V(3).Infof("validateConditionalHeaders: If-Unmodified-Since failed - object modified after %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
					return s3err.ErrPreconditionFailed
				}
				glog.V(3).Infof("validateConditionalHeaders: If-Unmodified-Since passed - object not modified since %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
			}
		}
	}

	// 3. Check If-None-Match
	if headers.ifNoneMatch != "" {
		if objectExists {
			if headers.ifNoneMatch == "*" {
				glog.V(3).Infof("validateConditionalHeaders: If-None-Match=* failed - object %s/%s exists", bucket, object)
				return s3err.ErrPreconditionFailed
			}
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			// Use production etagMatches method
			if s3a.etagMatches(headers.ifNoneMatch, objectETag) {
				glog.V(3).Infof("validateConditionalHeaders: If-None-Match failed - ETag matches %s", objectETag)
				return s3err.ErrPreconditionFailed
			}
			glog.V(3).Infof("validateConditionalHeaders: If-None-Match passed - ETag %s doesn't match %s", objectETag, headers.ifNoneMatch)
		} else {
			glog.V(3).Infof("validateConditionalHeaders: If-None-Match passed - object %s/%s does not exist", bucket, object)
		}
	}

	// 4. Check If-Modified-Since
	if !headers.ifModifiedSince.IsZero() {
		if objectExists {
			if entry.Attributes != nil {
				objectModTime := time.Unix(entry.Attributes.Mtime, 0)
				if !objectModTime.After(headers.ifModifiedSince) {
					glog.V(3).Infof("validateConditionalHeaders: If-Modified-Since failed - object not modified since %s", r.Header.Get(s3_constants.IfModifiedSince))
					return s3err.ErrPreconditionFailed
				}
				glog.V(3).Infof("validateConditionalHeaders: If-Modified-Since passed - object modified after %s", r.Header.Get(s3_constants.IfModifiedSince))
			}
		}
	}

	return s3err.ErrNone
}

// checkConditionalHeadersWithGetter is a testable method that accepts a simple EntryGetter
// Uses the production getObjectETag and etagMatches methods to ensure testing of real logic
func (s3a *S3ApiServer) checkConditionalHeadersWithGetter(getter EntryGetter, r *http.Request, bucket, object string) s3err.ErrorCode {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return errCode
	}
	// Get object entry for conditional checks.
	bucketDir := "/buckets/" + bucket
	entry, entryErr := getter.getEntry(bucketDir, object)
	if entryErr != nil {
		if errors.Is(entryErr, filer_pb.ErrNotFound) {
			entry = nil
		} else {
			glog.Errorf("checkConditionalHeadersWithGetter: failed to get entry for %s/%s: %v", bucket, object, entryErr)
			return s3err.ErrInternalError
		}
	}

	return s3a.validateConditionalHeaders(r, headers, entry, bucket, object)
}

// checkConditionalHeaders is the production method that uses the S3ApiServer as EntryGetter
func (s3a *S3ApiServer) checkConditionalHeaders(r *http.Request, bucket, object string) s3err.ErrorCode {
	// Fast path: if no conditional headers are present, skip object resolution entirely.
	// This avoids expensive lookups (especially getLatestObjectVersion retries in versioned buckets)
	// for the common case where no conditions are specified.
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return errCode
	}
	if !headers.isSet {
		return s3err.ErrNone
	}

	// Use resolveObjectEntry to correctly handle versioned objects.
	// This ensures we check conditions against the LATEST version, not a null version.
	entry, err := s3a.resolveObjectEntry(bucket, object)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			entry = nil
		} else {
			glog.Errorf("checkConditionalHeaders: error resolving object entry for %s/%s: %v", bucket, object, err)
			return s3err.ErrInternalError
		}
	}
	return s3a.validateConditionalHeaders(r, headers, entry, bucket, object)
}

// validateConditionalHeadersForReads checks conditional headers for read operations against the provided entry
func (s3a *S3ApiServer) validateConditionalHeadersForReads(r *http.Request, headers conditionalHeaders, entry *filer_pb.Entry, bucket, object string) ConditionalHeaderResult {
	if !headers.isSet {
		return ConditionalHeaderResult{ErrorCode: s3err.ErrNone, Entry: entry}
	}

	objectExists := entry != nil

	// If object doesn't exist, fail for If-Match and If-Unmodified-Since
	if !objectExists {
		if headers.ifMatch != "" {
			glog.V(3).Infof("validateConditionalHeadersForReads: If-Match failed - object %s/%s does not exist", bucket, object)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed, Entry: nil}
		}
		if !headers.ifUnmodifiedSince.IsZero() {
			glog.V(3).Infof("validateConditionalHeadersForReads: If-Unmodified-Since failed - object %s/%s does not exist", bucket, object)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed, Entry: nil}
		}
		// If-None-Match and If-Modified-Since succeed when object doesn't exist
		// No entry to return since object doesn't exist
		return ConditionalHeaderResult{ErrorCode: s3err.ErrNone, Entry: nil}
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
				glog.V(3).Infof("validateConditionalHeadersForReads: If-Match failed for object %s/%s - header If-Match: [%s], object ETag: [%s]", bucket, object, headers.ifMatch, objectETag)
				return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed, Entry: entry}
			}
		}
		glog.V(3).Infof("validateConditionalHeadersForReads: If-Match passed for object %s/%s", bucket, object)
	}

	// 2. Check If-Unmodified-Since (412 Precondition Failed if fails)
	if !headers.ifUnmodifiedSince.IsZero() {
		objectModTime := time.Unix(entry.Attributes.Mtime, 0)
		if objectModTime.After(headers.ifUnmodifiedSince) {
			glog.V(3).Infof("validateConditionalHeadersForReads: If-Unmodified-Since failed - object modified after %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
			return ConditionalHeaderResult{ErrorCode: s3err.ErrPreconditionFailed, Entry: entry}
		}
		glog.V(3).Infof("validateConditionalHeadersForReads: If-Unmodified-Since passed - object not modified since %s", r.Header.Get(s3_constants.IfUnmodifiedSince))
	}

	// 3. Check If-None-Match (304 Not Modified if fails)
	if headers.ifNoneMatch != "" {
		// Use production getObjectETag method
		objectETag := s3a.getObjectETag(entry)

		if headers.ifNoneMatch == "*" {
			glog.V(3).Infof("validateConditionalHeadersForReads: If-None-Match=* failed - object %s/%s exists", bucket, object)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrNotModified, ETag: objectETag, Entry: entry}
		}
		// Use production etagMatches method
		if s3a.etagMatches(headers.ifNoneMatch, objectETag) {
			glog.V(3).Infof("validateConditionalHeadersForReads: If-None-Match failed - ETag matches %s", objectETag)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrNotModified, ETag: objectETag, Entry: entry}
		}
		glog.V(3).Infof("validateConditionalHeadersForReads: If-None-Match passed - ETag %s doesn't match %s", objectETag, headers.ifNoneMatch)
	}

	// 4. Check If-Modified-Since (304 Not Modified if fails)
	if !headers.ifModifiedSince.IsZero() {
		objectModTime := time.Unix(entry.Attributes.Mtime, 0)
		if !objectModTime.After(headers.ifModifiedSince) {
			// Use production getObjectETag method
			objectETag := s3a.getObjectETag(entry)
			glog.V(3).Infof("validateConditionalHeadersForReads: If-Modified-Since failed - object not modified since %s", r.Header.Get(s3_constants.IfModifiedSince))
			return ConditionalHeaderResult{ErrorCode: s3err.ErrNotModified, ETag: objectETag, Entry: entry}
		}
		glog.V(3).Infof("validateConditionalHeadersForReads: If-Modified-Since passed - object modified after %s", r.Header.Get(s3_constants.IfModifiedSince))
	}

	// Return success with the fetched entry for reuse
	return ConditionalHeaderResult{ErrorCode: s3err.ErrNone, Entry: entry}
}

// checkConditionalHeadersForReadsWithGetter is a testable method for read operations
// Uses the production getObjectETag and etagMatches methods to ensure testing of real logic
func (s3a *S3ApiServer) checkConditionalHeadersForReadsWithGetter(getter EntryGetter, r *http.Request, bucket, object string) ConditionalHeaderResult {
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return ConditionalHeaderResult{ErrorCode: errCode}
	}
	// Get object entry for conditional checks.
	bucketDir := "/buckets/" + bucket
	entry, entryErr := getter.getEntry(bucketDir, object)
	if entryErr != nil {
		if errors.Is(entryErr, filer_pb.ErrNotFound) {
			entry = nil
		} else {
			glog.Errorf("checkConditionalHeadersForReadsWithGetter: failed to get entry for %s/%s: %v", bucket, object, entryErr)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrInternalError}
		}
	}

	return s3a.validateConditionalHeadersForReads(r, headers, entry, bucket, object)
}

// checkConditionalHeadersForReads is the production method that uses the S3ApiServer as EntryGetter
func (s3a *S3ApiServer) checkConditionalHeadersForReads(r *http.Request, bucket, object string) ConditionalHeaderResult {
	// Fast path: if no conditional headers are present, skip object resolution entirely.
	// This avoids expensive lookups (especially getLatestObjectVersion retries in versioned buckets)
	// for the common case where no conditions are specified.
	headers, errCode := parseConditionalHeaders(r)
	if errCode != s3err.ErrNone {
		return ConditionalHeaderResult{ErrorCode: errCode}
	}
	if !headers.isSet {
		return ConditionalHeaderResult{ErrorCode: s3err.ErrNone, Entry: nil}
	}

	// Use resolveObjectEntry to correctly handle versioned objects.
	// This ensures we check conditions against the LATEST version, not a null version.
	entry, err := s3a.resolveObjectEntry(bucket, object)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			entry = nil
		} else {
			glog.Errorf("checkConditionalHeadersForReads: error resolving object entry for %s/%s: %v", bucket, object, err)
			return ConditionalHeaderResult{ErrorCode: s3err.ErrInternalError, Entry: nil}
		}
	}
	return s3a.validateConditionalHeadersForReads(r, headers, entry, bucket, object)
}

// deleteOrphanedChunks attempts to delete chunks that were uploaded but whose entry creation failed
// This prevents resource leaks and wasted storage. Errors are logged but don't prevent cleanup attempts.
func (s3a *S3ApiServer) deleteOrphanedChunks(chunks []*filer_pb.FileChunk) {
	if len(chunks) == 0 {
		return
	}

	// Extract file IDs from chunks
	var fileIds []string
	for _, chunk := range chunks {
		if chunk.GetFileIdString() != "" {
			fileIds = append(fileIds, chunk.GetFileIdString())
		}
	}

	if len(fileIds) == 0 {
		glog.Warningf("deleteOrphanedChunks: no valid file IDs found in %d chunks", len(chunks))
		return
	}

	glog.V(3).Infof("deleteOrphanedChunks: attempting to delete %d file IDs: %v", len(fileIds), fileIds)

	// Create a lookup function that queries the filer for volume locations
	// This is similar to createLookupFileIdFunction but returns the format needed by DeleteFileIdsWithLookupVolumeId
	lookupFunc := func(vids []string) (map[string]*operation.LookupResult, error) {
		results := make(map[string]*operation.LookupResult)

		err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			// Query filer for all volume IDs at once
			resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
				VolumeIds: vids,
			})
			if err != nil {
				return err
			}

			// Convert filer response to operation.LookupResult format
			for vid, locs := range resp.LocationsMap {
				result := &operation.LookupResult{
					VolumeOrFileId: vid,
				}

				for _, loc := range locs.Locations {
					result.Locations = append(result.Locations, operation.Location{
						Url:        loc.Url,
						PublicUrl:  loc.PublicUrl,
						DataCenter: loc.DataCenter,
						GrpcPort:   int(loc.GrpcPort),
					})
				}

				results[vid] = result
			}
			return nil
		})

		return results, err
	}

	// Attempt deletion using the operation package's batch delete with custom lookup
	deleteResults := operation.DeleteFileIdsWithLookupVolumeId(s3a.option.GrpcDialOption, fileIds, lookupFunc)

	// Log results - track successes and failures
	successCount := 0
	failureCount := 0
	for _, result := range deleteResults {
		if result.Error != "" {
			glog.Warningf("deleteOrphanedChunks: failed to delete chunk %s: %s (status: %d)",
				result.FileId, result.Error, result.Status)
			failureCount++
		} else {
			glog.V(4).Infof("deleteOrphanedChunks: successfully deleted chunk %s (size: %d bytes)",
				result.FileId, result.Size)
			successCount++
		}
	}

	if failureCount > 0 {
		glog.Warningf("deleteOrphanedChunks: cleanup completed with %d successes and %d failures out of %d chunks",
			successCount, failureCount, len(fileIds))
	} else {
		glog.V(3).Infof("deleteOrphanedChunks: successfully deleted all %d orphaned chunks", successCount)
	}
}

func validateStorageClass(sc string) bool {
	switch StorageClass(sc) {
	case "STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "OUTPOSTS", "GLACIER_IR", "SNOW":
		return true
	}
	return false
}

func (s3a *S3ApiServer) getStorageClassFromExtended(extended map[string][]byte) string {
	if extended != nil {
		if sc, ok := extended[s3_constants.AmzStorageClass]; ok {
			return string(sc)
		}
	}
	return "STANDARD"
}
