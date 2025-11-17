package s3api

import (
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

const (
	multipartExt     = ".part"
	multiPartMinSize = 5 * 1024 * 1024
)

type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	s3.CreateMultipartUploadOutput
}

func (s3a *S3ApiServer) createMultipartUpload(r *http.Request, input *s3.CreateMultipartUploadInput) (output *InitiateMultipartUploadResult, code s3err.ErrorCode) {

	glog.V(2).Infof("createMultipartUpload input %v", input)

	uploadIdString := s3a.generateUploadID(*input.Key)

	uploadIdString = uploadIdString + "_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// Prepare error handling outside callback scope
	var encryptionError error

	if err := s3a.mkdir(s3a.genUploadsFolder(*input.Bucket), uploadIdString, func(entry *filer_pb.Entry) {
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended[s3_constants.ExtMultipartObjectKey] = []byte(*input.Key)
		// Set object owner for multipart upload
		amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
		if amzAccountId != "" {
			entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
		}

		for k, v := range input.Metadata {
			entry.Extended[k] = []byte(*v)
		}
		if input.ContentType != nil {
			entry.Attributes.Mime = *input.ContentType
		}

		// Prepare and apply encryption configuration within directory creation
		// This ensures encryption resources are only allocated if directory creation succeeds
		encryptionConfig, prepErr := s3a.prepareMultipartEncryptionConfig(r, *input.Bucket, uploadIdString)
		if prepErr != nil {
			encryptionError = prepErr
			return // Exit callback, letting mkdir handle the error
		}
		s3a.applyMultipartEncryptionConfig(entry, encryptionConfig)

		// Extract and store object lock metadata from request headers
		// This ensures object lock settings from create_multipart_upload are preserved
		if err := s3a.extractObjectLockMetadataFromRequest(r, entry); err != nil {
			glog.Errorf("createMultipartUpload: failed to extract object lock metadata: %v", err)
			// Don't fail the upload - this matches AWS behavior for invalid metadata
		}
	}); err != nil {
		_, errorCode := handleMultipartInternalError("create multipart upload directory", err)
		return nil, errorCode
	}

	// Check for encryption configuration errors that occurred within the callback
	if encryptionError != nil {
		_, errorCode := handleMultipartInternalError("prepare encryption configuration", encryptionError)
		return nil, errorCode
	}

	output = &InitiateMultipartUploadResult{
		CreateMultipartUploadOutput: s3.CreateMultipartUploadOutput{
			Bucket:   input.Bucket,
			Key:      objectKey(input.Key),
			UploadId: aws.String(uploadIdString),
		},
	}

	return
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location *string  `xml:"Location,omitempty"`
	Bucket   *string  `xml:"Bucket,omitempty"`
	Key      *string  `xml:"Key,omitempty"`
	ETag     *string  `xml:"ETag,omitempty"`
	// VersionId is NOT included in XML body - it should only be in x-amz-version-id HTTP header

	// Store the VersionId internally for setting HTTP header, but don't marshal to XML
	VersionId *string `xml:"-"`
}

func (s3a *S3ApiServer) completeMultipartUpload(r *http.Request, input *s3.CompleteMultipartUploadInput, parts *CompleteMultipartUpload) (output *CompleteMultipartUploadResult, code s3err.ErrorCode) {

	glog.V(2).Infof("completeMultipartUpload input %v", input)
	if len(parts.Parts) == 0 {
		stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
		return nil, s3err.ErrNoSuchUpload
	}
	completedPartNumbers := []int{}
	completedPartMap := make(map[int][]string)

	maxPartNo := 1

	for _, part := range parts.Parts {
		if _, ok := completedPartMap[part.PartNumber]; !ok {
			completedPartNumbers = append(completedPartNumbers, part.PartNumber)
		}
		completedPartMap[part.PartNumber] = append(completedPartMap[part.PartNumber], part.ETag)
		maxPartNo = maxInt(maxPartNo, part.PartNumber)
	}
	sort.Ints(completedPartNumbers)

	uploadDirectory := s3a.genUploadsFolder(*input.Bucket) + "/" + *input.UploadId
	entries, _, err := s3a.list(uploadDirectory, "", "", false, 0)
	if err != nil {
		glog.Errorf("completeMultipartUpload %s %s error: %v, entries:%d", *input.Bucket, *input.UploadId, err, len(entries))
		stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
		return nil, s3err.ErrNoSuchUpload
	}

	if len(entries) == 0 {
		entryName, dirName := s3a.getEntryNameAndDir(input)
		if entry, _ := s3a.getEntry(dirName, entryName); entry != nil && entry.Extended != nil {
			if uploadId, ok := entry.Extended[s3_constants.SeaweedFSUploadId]; ok && *input.UploadId == string(uploadId) {
				return &CompleteMultipartUploadResult{
					Location: aws.String(fmt.Sprintf("http://%s%s/%s", s3a.option.Filer.ToHttpAddress(), urlEscapeObject(dirName), urlPathEscape(entryName))),
					Bucket:   input.Bucket,
					ETag:     aws.String("\"" + filer.ETagChunks(entry.GetChunks()) + "\""),
					Key:      objectKey(input.Key),
				}, s3err.ErrNone
			}
		}
		stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
		return nil, s3err.ErrNoSuchUpload
	}

	pentry, err := s3a.getEntry(s3a.genUploadsFolder(*input.Bucket), *input.UploadId)
	if err != nil {
		glog.Errorf("completeMultipartUpload %s %s error: %v", *input.Bucket, *input.UploadId, err)
		stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
		return nil, s3err.ErrNoSuchUpload
	}
	deleteEntries := []*filer_pb.Entry{}
	partEntries := make(map[int][]*filer_pb.Entry, len(entries))
	entityTooSmall := false
	entityWithTtl := false
	for _, entry := range entries {
		foundEntry := false
		glog.V(4).Infof("completeMultipartUpload part entries %s", entry.Name)
		if entry.IsDirectory || !strings.HasSuffix(entry.Name, multipartExt) {
			continue
		}
		partNumber, err := parsePartNumber(entry.Name)
		if err != nil {
			stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartNumber).Inc()
			glog.Errorf("completeMultipartUpload failed to pasre partNumber %s:%s", entry.Name, err)
			continue
		}
		completedPartsByNumber, ok := completedPartMap[partNumber]
		if !ok {
			continue
		}
		for _, partETag := range completedPartsByNumber {
			partETag = strings.Trim(partETag, `"`)
			entryETag := hex.EncodeToString(entry.Attributes.GetMd5())
			if partETag != "" && len(partETag) == 32 && entryETag != "" {
				if entryETag != partETag {
					glog.Errorf("completeMultipartUpload %s ETag mismatch chunk: %s part: %s", entry.Name, entryETag, partETag)
					stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedEtagMismatch).Inc()
					continue
				}
			} else {
				glog.Warningf("invalid complete etag %s, partEtag %s", partETag, entryETag)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedEtagInvalid).Inc()
			}
			if len(entry.Chunks) == 0 && partNumber != maxPartNo {
				glog.Warningf("completeMultipartUpload %s empty chunks", entry.Name)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartEmpty).Inc()
				continue
			}
			//there maybe multi same part, because of client retry
			partEntries[partNumber] = append(partEntries[partNumber], entry)
			foundEntry = true
		}
		if foundEntry {
			if !entityWithTtl && entry.Attributes != nil && entry.Attributes.TtlSec > 0 {
				entityWithTtl = true
			}
			if len(completedPartNumbers) > 1 && partNumber != completedPartNumbers[len(completedPartNumbers)-1] &&
				entry.Attributes.FileSize < multiPartMinSize {
				glog.Warningf("completeMultipartUpload %s part file size less 5mb", entry.Name)
				entityTooSmall = true
			}
		} else {
			deleteEntries = append(deleteEntries, entry)
		}
	}
	if entityTooSmall {
		stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompleteEntityTooSmall).Inc()
		return nil, s3err.ErrEntityTooSmall
	}
	mime := pentry.Attributes.Mime
	var finalParts []*filer_pb.FileChunk
	var offset int64
	for _, partNumber := range completedPartNumbers {
		partEntriesByNumber, ok := partEntries[partNumber]
		if !ok {
			glog.Errorf("part %d has no entry", partNumber)
			stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartNotFound).Inc()
			return nil, s3err.ErrInvalidPart
		}
		found := false
		if len(partEntriesByNumber) > 1 {
			slices.SortFunc(partEntriesByNumber, func(a, b *filer_pb.Entry) int {
				return cmp.Compare(b.Chunks[0].ModifiedTsNs, a.Chunks[0].ModifiedTsNs)
			})
		}
		for _, entry := range partEntriesByNumber {
			if found {
				deleteEntries = append(deleteEntries, entry)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartEntryMismatch).Inc()
				continue
			}

			for _, chunk := range entry.GetChunks() {
				// CRITICAL: Do NOT modify SSE metadata offsets during assembly!
				// The encrypted data was created with the offset stored in chunk.SseMetadata.
				// Changing the offset here would cause decryption to fail because CTR mode
				// uses the offset to initialize the counter. We must decrypt with the same
				// offset that was used during encryption.

				p := &filer_pb.FileChunk{
					FileId:       chunk.GetFileIdString(),
					Offset:       offset,
					Size:         chunk.Size,
					ModifiedTsNs: chunk.ModifiedTsNs,
					CipherKey:    chunk.CipherKey,
					ETag:         chunk.ETag,
					IsCompressed: chunk.IsCompressed,
					// Preserve SSE metadata UNCHANGED - do not modify the offset!
					SseType:     chunk.SseType,
					SseMetadata: chunk.SseMetadata,
				}
				finalParts = append(finalParts, p)
				offset += int64(chunk.Size)
			}
			found = true
		}
	}

	entryName, dirName := s3a.getEntryNameAndDir(input)

	// Check if versioning is configured for this bucket BEFORE creating any files
	versioningState, vErr := s3a.getVersioningState(*input.Bucket)
	if vErr == nil && versioningState == s3_constants.VersioningEnabled {
		// For versioned buckets, create a version and return the version ID
		versionId := generateVersionId()
		versionFileName := s3a.getVersionFileName(versionId)
		versionDir := dirName + "/" + entryName + s3_constants.VersionsFolder

		// Move the completed object to the versions directory
		glog.V(0).Infof("completeMultipartUpload: Creating version with %d finalParts chunks", len(finalParts))
		for i, chunk := range finalParts {
			glog.V(0).Infof("completeMultipartUpload: finalParts[%d] - SseType=%v, hasMetadata=%v", i, chunk.SseType, len(chunk.SseMetadata) > 0)
		}
		err = s3a.mkFile(versionDir, versionFileName, finalParts, func(versionEntry *filer_pb.Entry) {
			glog.V(0).Infof("completeMultipartUpload: mkFile callback - entry has %d chunks", len(versionEntry.Chunks))
			for i, chunk := range versionEntry.Chunks {
				glog.V(0).Infof("completeMultipartUpload: versionEntry.Chunks[%d] - SseType=%v, hasMetadata=%v", i, chunk.SseType, len(chunk.SseMetadata) > 0)
			}
			if versionEntry.Extended == nil {
				versionEntry.Extended = make(map[string][]byte)
			}
			versionEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
			versionEntry.Extended[s3_constants.SeaweedFSUploadId] = []byte(*input.UploadId)
			// Store parts count for x-amz-mp-parts-count header
			versionEntry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte(fmt.Sprintf("%d", len(completedPartNumbers)))

			// Set object owner for versioned multipart objects
			amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
			if amzAccountId != "" {
				versionEntry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
			}

			for k, v := range pentry.Extended {
				if k != s3_constants.ExtMultipartObjectKey {
					versionEntry.Extended[k] = v
				}
			}

			// Preserve ALL SSE metadata from the first part (if any)
			// SSE metadata is stored in individual parts, not the upload directory
			if len(completedPartNumbers) > 0 && len(partEntries[completedPartNumbers[0]]) > 0 {
				firstPartEntry := partEntries[completedPartNumbers[0]][0]
				if firstPartEntry.Extended != nil {
					// Copy ALL SSE-related headers (not just SeaweedFSSSEKMSKey)
					// This is critical for detectPrimarySSEType to work correctly
					sseKeys := []string{
						// SSE-C headers
						s3_constants.SeaweedFSSSEIV,
						s3_constants.AmzServerSideEncryptionCustomerAlgorithm,
						s3_constants.AmzServerSideEncryptionCustomerKeyMD5,
						// SSE-KMS headers
						s3_constants.SeaweedFSSSEKMSKey,
						s3_constants.AmzServerSideEncryptionAwsKmsKeyId,
						// SSE-S3 headers
						s3_constants.SeaweedFSSSES3Key,
						// Common SSE header (for SSE-KMS and SSE-S3)
						s3_constants.AmzServerSideEncryption,
					}
					for _, key := range sseKeys {
						if value, exists := firstPartEntry.Extended[key]; exists {
							versionEntry.Extended[key] = value
							glog.V(4).Infof("completeMultipartUpload: copied SSE header %s from first part (versioned)", key)
						}
					}
				}
			}
			if pentry.Attributes.Mime != "" {
				versionEntry.Attributes.Mime = pentry.Attributes.Mime
			} else if mime != "" {
				versionEntry.Attributes.Mime = mime
			}
			versionEntry.Attributes.FileSize = uint64(offset)
		})

		if err != nil {
			glog.Errorf("completeMultipartUpload: failed to create version %s: %v", versionId, err)
			return nil, s3err.ErrInternalError
		}
		glog.V(0).Infof("completeMultipartUpload: Successfully created version %s", versionId)

		// Update the .versions directory metadata to indicate this is the latest version
		err = s3a.updateLatestVersionInDirectory(*input.Bucket, *input.Key, versionId, versionFileName)
		if err != nil {
			glog.Errorf("completeMultipartUpload: failed to update latest version in directory: %v", err)
			return nil, s3err.ErrInternalError
		}

		// For versioned buckets, don't create a main object file - all content is stored in .versions directory
		// The latest version information is tracked in the .versions directory metadata

		output = &CompleteMultipartUploadResult{
			Location:  aws.String(fmt.Sprintf("http://%s%s/%s", s3a.option.Filer.ToHttpAddress(), urlEscapeObject(dirName), urlPathEscape(entryName))),
			Bucket:    input.Bucket,
			ETag:      aws.String("\"" + filer.ETagChunks(finalParts) + "\""),
			Key:       objectKey(input.Key),
			VersionId: aws.String(versionId),
		}
	} else if vErr == nil && versioningState == s3_constants.VersioningSuspended {
		// For suspended versioning, add "null" version ID metadata and return "null" version ID
		err = s3a.mkFile(dirName, entryName, finalParts, func(entry *filer_pb.Entry) {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.ExtVersionIdKey] = []byte("null")
			// Store parts count for x-amz-mp-parts-count header
			entry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte(fmt.Sprintf("%d", len(completedPartNumbers)))

			// Set object owner for suspended versioning multipart objects
			amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
			if amzAccountId != "" {
				entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
			}

			for k, v := range pentry.Extended {
				if k != s3_constants.ExtMultipartObjectKey {
					entry.Extended[k] = v
				}
			}

			// Preserve ALL SSE metadata from the first part (if any)
			// SSE metadata is stored in individual parts, not the upload directory
			if len(completedPartNumbers) > 0 && len(partEntries[completedPartNumbers[0]]) > 0 {
				firstPartEntry := partEntries[completedPartNumbers[0]][0]
				if firstPartEntry.Extended != nil {
					// Copy ALL SSE-related headers (not just SeaweedFSSSEKMSKey)
					// This is critical for detectPrimarySSEType to work correctly
					sseKeys := []string{
						// SSE-C headers
						s3_constants.SeaweedFSSSEIV,
						s3_constants.AmzServerSideEncryptionCustomerAlgorithm,
						s3_constants.AmzServerSideEncryptionCustomerKeyMD5,
						// SSE-KMS headers
						s3_constants.SeaweedFSSSEKMSKey,
						s3_constants.AmzServerSideEncryptionAwsKmsKeyId,
						// SSE-S3 headers
						s3_constants.SeaweedFSSSES3Key,
						// Common SSE header (for SSE-KMS and SSE-S3)
						s3_constants.AmzServerSideEncryption,
					}
					for _, key := range sseKeys {
						if value, exists := firstPartEntry.Extended[key]; exists {
							entry.Extended[key] = value
							glog.V(4).Infof("completeMultipartUpload: copied SSE header %s from first part (suspended versioning)", key)
						}
					}
				}
			}
			if pentry.Attributes.Mime != "" {
				entry.Attributes.Mime = pentry.Attributes.Mime
			} else if mime != "" {
				entry.Attributes.Mime = mime
			}
			entry.Attributes.FileSize = uint64(offset)
		})

		if err != nil {
			glog.Errorf("completeMultipartUpload: failed to create suspended versioning object: %v", err)
			return nil, s3err.ErrInternalError
		}

		// Note: Suspended versioning should NOT return VersionId field according to AWS S3 spec
		output = &CompleteMultipartUploadResult{
			Location: aws.String(fmt.Sprintf("http://%s%s/%s", s3a.option.Filer.ToHttpAddress(), urlEscapeObject(dirName), urlPathEscape(entryName))),
			Bucket:   input.Bucket,
			ETag:     aws.String("\"" + filer.ETagChunks(finalParts) + "\""),
			Key:      objectKey(input.Key),
			// VersionId field intentionally omitted for suspended versioning
		}
	} else {
		// For non-versioned buckets, create main object file
		err = s3a.mkFile(dirName, entryName, finalParts, func(entry *filer_pb.Entry) {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.SeaweedFSUploadId] = []byte(*input.UploadId)
			// Store parts count for x-amz-mp-parts-count header
			entry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte(fmt.Sprintf("%d", len(completedPartNumbers)))

			// Set object owner for non-versioned multipart objects
			amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
			if amzAccountId != "" {
				entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
			}

			for k, v := range pentry.Extended {
				if k != s3_constants.ExtMultipartObjectKey {
					entry.Extended[k] = v
				}
			}

			// Preserve ALL SSE metadata from the first part (if any)
			// SSE metadata is stored in individual parts, not the upload directory
			if len(completedPartNumbers) > 0 && len(partEntries[completedPartNumbers[0]]) > 0 {
				firstPartEntry := partEntries[completedPartNumbers[0]][0]
				if firstPartEntry.Extended != nil {
					// Copy ALL SSE-related headers (not just SeaweedFSSSEKMSKey)
					// This is critical for detectPrimarySSEType to work correctly
					sseKeys := []string{
						// SSE-C headers
						s3_constants.SeaweedFSSSEIV,
						s3_constants.AmzServerSideEncryptionCustomerAlgorithm,
						s3_constants.AmzServerSideEncryptionCustomerKeyMD5,
						// SSE-KMS headers
						s3_constants.SeaweedFSSSEKMSKey,
						s3_constants.AmzServerSideEncryptionAwsKmsKeyId,
						// SSE-S3 headers
						s3_constants.SeaweedFSSSES3Key,
						// Common SSE header (for SSE-KMS and SSE-S3)
						s3_constants.AmzServerSideEncryption,
					}
					for _, key := range sseKeys {
						if value, exists := firstPartEntry.Extended[key]; exists {
							entry.Extended[key] = value
							glog.V(4).Infof("completeMultipartUpload: copied SSE header %s from first part (non-versioned)", key)
						}
					}
				}
			}
			if pentry.Attributes.Mime != "" {
				entry.Attributes.Mime = pentry.Attributes.Mime
			} else if mime != "" {
				entry.Attributes.Mime = mime
			}
			entry.Attributes.FileSize = uint64(offset)
			// Set TTL-based S3 expiry (modification time)
			if entityWithTtl {
				entry.Extended[s3_constants.SeaweedFSExpiresS3] = []byte("true")
			}
		})

		if err != nil {
			glog.Errorf("completeMultipartUpload %s/%s error: %v", dirName, entryName, err)
			return nil, s3err.ErrInternalError
		}

		// For non-versioned buckets, return response without VersionId
		output = &CompleteMultipartUploadResult{
			Location: aws.String(fmt.Sprintf("http://%s%s/%s", s3a.option.Filer.ToHttpAddress(), urlEscapeObject(dirName), urlPathEscape(entryName))),
			Bucket:   input.Bucket,
			ETag:     aws.String("\"" + filer.ETagChunks(finalParts) + "\""),
			Key:      objectKey(input.Key),
		}
	}

	for _, deleteEntry := range deleteEntries {
		//delete unused part data
		if err = s3a.rm(uploadDirectory, deleteEntry.Name, true, true); err != nil {
			glog.Warningf("completeMultipartUpload cleanup %s upload %s unused %s : %v", *input.Bucket, *input.UploadId, deleteEntry.Name, err)
		}
	}
	if err = s3a.rm(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, false, true); err != nil {
		glog.V(1).Infof("completeMultipartUpload cleanup %s upload %s: %v", *input.Bucket, *input.UploadId, err)
	}

	return
}

func (s3a *S3ApiServer) getEntryNameAndDir(input *s3.CompleteMultipartUploadInput) (string, string) {
	entryName := filepath.Base(*input.Key)
	dirName := filepath.ToSlash(filepath.Dir(*input.Key))
	if dirName == "." {
		dirName = ""
	}
	if strings.HasPrefix(dirName, "/") {
		dirName = dirName[1:]
	}
	dirName = fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, *input.Bucket, dirName)

	// remove suffix '/'
	if strings.HasSuffix(dirName, "/") {
		dirName = dirName[:len(dirName)-1]
	}
	return entryName, dirName
}

func parsePartNumber(fileName string) (int, error) {
	var partNumberString string
	index := strings.Index(fileName, "_")
	if index != -1 {
		partNumberString = fileName[:index]
	} else {
		partNumberString = fileName[:len(fileName)-len(multipartExt)]
	}
	return strconv.Atoi(partNumberString)
}

func (s3a *S3ApiServer) abortMultipartUpload(input *s3.AbortMultipartUploadInput) (output *s3.AbortMultipartUploadOutput, code s3err.ErrorCode) {

	glog.V(2).Infof("abortMultipartUpload input %v", input)

	exists, err := s3a.exists(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, true)
	if err != nil {
		glog.V(1).Infof("bucket %s abort upload %s: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrNoSuchUpload
	}
	if exists {
		err = s3a.rm(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, true, true)
	}
	if err != nil {
		glog.V(1).Infof("bucket %s remove upload %s: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrInternalError
	}

	return &s3.AbortMultipartUploadOutput{}, s3err.ErrNone
}

type ListMultipartUploadsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult"`

	// copied from s3.ListMultipartUploadsOutput, the Uploads is not converting to <Upload></Upload>
	Bucket             *string               `type:"string"`
	Delimiter          *string               `type:"string"`
	EncodingType       *string               `type:"string" enum:"EncodingType"`
	IsTruncated        *bool                 `type:"boolean"`
	KeyMarker          *string               `type:"string"`
	MaxUploads         *int64                `type:"integer"`
	NextKeyMarker      *string               `type:"string"`
	NextUploadIdMarker *string               `type:"string"`
	Prefix             *string               `type:"string"`
	UploadIdMarker     *string               `type:"string"`
	Upload             []*s3.MultipartUpload `locationName:"Upload" type:"list" flattened:"true"`
}

func (s3a *S3ApiServer) listMultipartUploads(input *s3.ListMultipartUploadsInput) (output *ListMultipartUploadsResult, code s3err.ErrorCode) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html

	glog.V(2).Infof("listMultipartUploads input %v", input)

	output = &ListMultipartUploadsResult{
		Bucket:       input.Bucket,
		Delimiter:    input.Delimiter,
		EncodingType: input.EncodingType,
		KeyMarker:    input.KeyMarker,
		MaxUploads:   input.MaxUploads,
		Prefix:       input.Prefix,
		IsTruncated:  aws.Bool(false),
	}

	entries, _, err := s3a.list(s3a.genUploadsFolder(*input.Bucket), "", *input.UploadIdMarker, false, math.MaxInt32)
	if err != nil {
		glog.Errorf("listMultipartUploads %s error: %v", *input.Bucket, err)
		return
	}

	uploadsCount := int64(0)
	for _, entry := range entries {
		if entry.Extended != nil {
			key := string(entry.Extended[s3_constants.ExtMultipartObjectKey])
			if *input.KeyMarker != "" && *input.KeyMarker != key {
				continue
			}
			if *input.Prefix != "" && !strings.HasPrefix(key, *input.Prefix) {
				continue
			}
			output.Upload = append(output.Upload, &s3.MultipartUpload{
				Key:      objectKey(aws.String(key)),
				UploadId: aws.String(entry.Name),
			})
			uploadsCount += 1
		}
		if uploadsCount >= *input.MaxUploads {
			output.IsTruncated = aws.Bool(true)
			output.NextUploadIdMarker = aws.String(entry.Name)
			break
		}
	}

	return
}

type ListPartsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`

	// copied from s3.ListPartsOutput, the Parts is not converting to <Part></Part>
	Bucket               *string    `type:"string"`
	IsTruncated          *bool      `type:"boolean"`
	Key                  *string    `min:"1" type:"string"`
	MaxParts             *int64     `type:"integer"`
	NextPartNumberMarker *int64     `type:"integer"`
	PartNumberMarker     *int64     `type:"integer"`
	Part                 []*s3.Part `locationName:"Part" type:"list" flattened:"true"`
	StorageClass         *string    `type:"string" enum:"StorageClass"`
	UploadId             *string    `type:"string"`
}

func (s3a *S3ApiServer) listObjectParts(input *s3.ListPartsInput) (output *ListPartsResult, code s3err.ErrorCode) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html

	glog.V(2).Infof("listObjectParts input %v", input)

	output = &ListPartsResult{
		Bucket:           input.Bucket,
		Key:              objectKey(input.Key),
		UploadId:         input.UploadId,
		MaxParts:         input.MaxParts,         // the maximum number of parts to return.
		PartNumberMarker: input.PartNumberMarker, // the part number starts after this, exclusive
		StorageClass:     aws.String("STANDARD"),
	}

	entries, isLast, err := s3a.list(s3a.genUploadsFolder(*input.Bucket)+"/"+*input.UploadId, "", fmt.Sprintf("%04d%s", *input.PartNumberMarker, multipartExt), false, uint32(*input.MaxParts))
	if err != nil {
		glog.Errorf("listObjectParts %s %s error: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrNoSuchUpload
	}

	// Note: The upload directory is sort of a marker of the existence of an multipart upload request.
	// So can not just delete empty upload folders.

	output.IsTruncated = aws.Bool(!isLast)

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name, multipartExt) && !entry.IsDirectory {
			partNumber, err := parsePartNumber(entry.Name)
			if err != nil {
				glog.Errorf("listObjectParts %s %s parse %s: %v", *input.Bucket, *input.UploadId, entry.Name, err)
				continue
			}
			partETag := filer.ETag(entry)
			part := &s3.Part{
				PartNumber:   aws.Int64(int64(partNumber)),
				LastModified: aws.Time(time.Unix(entry.Attributes.Mtime, 0).UTC()),
				Size:         aws.Int64(int64(filer.FileSize(entry))),
				ETag:         aws.String("\"" + partETag + "\""),
			}
			output.Part = append(output.Part, part)
			glog.V(3).Infof("listObjectParts: Added part %d, size=%d, etag=%s",
				partNumber, filer.FileSize(entry), partETag)
			if !isLast {
				output.NextPartNumberMarker = aws.Int64(int64(partNumber))
			}
		}
	}

	glog.V(2).Infof("listObjectParts: Returning %d parts for uploadId=%s", len(output.Part), *input.UploadId)
	return
}

// maxInt returns the maximum of two int values
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MultipartEncryptionConfig holds pre-prepared encryption configuration to avoid error handling in callbacks
type MultipartEncryptionConfig struct {
	// SSE-KMS configuration
	IsSSEKMS          bool
	KMSKeyID          string
	BucketKeyEnabled  bool
	EncryptionContext string
	KMSBaseIVEncoded  string

	// SSE-S3 configuration
	IsSSES3          bool
	S3BaseIVEncoded  string
	S3KeyDataEncoded string
}

// prepareMultipartEncryptionConfig prepares encryption configuration with proper error handling
// This eliminates the need for criticalError variable in callback functions
// Updated to support bucket-default encryption (matches putToFiler behavior)
func (s3a *S3ApiServer) prepareMultipartEncryptionConfig(r *http.Request, bucket string, uploadIdString string) (*MultipartEncryptionConfig, error) {
	config := &MultipartEncryptionConfig{}

	// Check for explicit encryption headers first (priority over bucket defaults)
	hasExplicitSSEKMS := IsSSEKMSRequest(r)
	hasExplicitSSES3 := IsSSES3RequestInternal(r)

	// Prepare SSE-KMS configuration (explicit request headers)
	if hasExplicitSSEKMS {
		config.IsSSEKMS = true
		config.KMSKeyID = r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
		config.BucketKeyEnabled = strings.ToLower(r.Header.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled)) == "true"
		config.EncryptionContext = r.Header.Get(s3_constants.AmzServerSideEncryptionContext)

		// Generate and encode base IV with proper error handling
		baseIV := make([]byte, s3_constants.AESBlockSize)
		n, err := rand.Read(baseIV)
		if err != nil || n != len(baseIV) {
			return nil, fmt.Errorf("failed to generate secure IV for SSE-KMS multipart upload: %v (read %d/%d bytes)", err, n, len(baseIV))
		}
		config.KMSBaseIVEncoded = base64.StdEncoding.EncodeToString(baseIV)
		glog.V(4).Infof("Generated base IV %x for explicit SSE-KMS multipart upload %s", baseIV[:8], uploadIdString)
	}

	// Prepare SSE-S3 configuration (explicit request headers)
	if hasExplicitSSES3 {
		config.IsSSES3 = true

		// Generate and encode base IV with proper error handling
		baseIV := make([]byte, s3_constants.AESBlockSize)
		n, err := rand.Read(baseIV)
		if err != nil || n != len(baseIV) {
			return nil, fmt.Errorf("failed to generate secure IV for SSE-S3 multipart upload: %v (read %d/%d bytes)", err, n, len(baseIV))
		}
		config.S3BaseIVEncoded = base64.StdEncoding.EncodeToString(baseIV)
		glog.V(4).Infof("Generated base IV %x for explicit SSE-S3 multipart upload %s", baseIV[:8], uploadIdString)

		// Generate and serialize SSE-S3 key with proper error handling
		keyManager := GetSSES3KeyManager()
		sseS3Key, err := keyManager.GetOrCreateKey("")
		if err != nil {
			return nil, fmt.Errorf("failed to generate SSE-S3 key for multipart upload: %v", err)
		}

		keyData, serErr := SerializeSSES3Metadata(sseS3Key)
		if serErr != nil {
			return nil, fmt.Errorf("failed to serialize SSE-S3 metadata for multipart upload: %v", serErr)
		}

		config.S3KeyDataEncoded = base64.StdEncoding.EncodeToString(keyData)

		// Store key in manager for later retrieval
		keyManager.StoreKey(sseS3Key)
		glog.V(4).Infof("Stored SSE-S3 key %s for explicit multipart upload %s", sseS3Key.KeyID, uploadIdString)
	}

	// If no explicit encryption headers, check bucket-default encryption
	// This matches AWS S3 behavior and putToFiler() implementation
	if !hasExplicitSSEKMS && !hasExplicitSSES3 {
		encryptionConfig, err := s3a.GetBucketEncryptionConfig(bucket)
		if err == nil && encryptionConfig != nil && encryptionConfig.SseAlgorithm != "" {
			glog.V(3).Infof("prepareMultipartEncryptionConfig: applying bucket-default encryption %s for bucket %s, upload %s",
				encryptionConfig.SseAlgorithm, bucket, uploadIdString)

			switch encryptionConfig.SseAlgorithm {
			case EncryptionTypeKMS:
				// Apply SSE-KMS as bucket default
				config.IsSSEKMS = true
				config.KMSKeyID = encryptionConfig.KmsKeyId
				config.BucketKeyEnabled = encryptionConfig.BucketKeyEnabled
				// No encryption context for bucket defaults

				// Generate and encode base IV
				baseIV := make([]byte, s3_constants.AESBlockSize)
				n, readErr := rand.Read(baseIV)
				if readErr != nil || n != len(baseIV) {
					return nil, fmt.Errorf("failed to generate secure IV for bucket-default SSE-KMS multipart upload: %v (read %d/%d bytes)", readErr, n, len(baseIV))
				}
				config.KMSBaseIVEncoded = base64.StdEncoding.EncodeToString(baseIV)
				glog.V(4).Infof("Generated base IV %x for bucket-default SSE-KMS multipart upload %s", baseIV[:8], uploadIdString)

			case EncryptionTypeAES256:
				// Apply SSE-S3 (AES256) as bucket default
				config.IsSSES3 = true

				// Generate and encode base IV
				baseIV := make([]byte, s3_constants.AESBlockSize)
				n, readErr := rand.Read(baseIV)
				if readErr != nil || n != len(baseIV) {
					return nil, fmt.Errorf("failed to generate secure IV for bucket-default SSE-S3 multipart upload: %v (read %d/%d bytes)", readErr, n, len(baseIV))
				}
				config.S3BaseIVEncoded = base64.StdEncoding.EncodeToString(baseIV)
				glog.V(4).Infof("Generated base IV %x for bucket-default SSE-S3 multipart upload %s", baseIV[:8], uploadIdString)

				// Generate and serialize SSE-S3 key
				keyManager := GetSSES3KeyManager()
				sseS3Key, keyErr := keyManager.GetOrCreateKey("")
				if keyErr != nil {
					return nil, fmt.Errorf("failed to generate SSE-S3 key for bucket-default multipart upload: %v", keyErr)
				}

				keyData, serErr := SerializeSSES3Metadata(sseS3Key)
				if serErr != nil {
					return nil, fmt.Errorf("failed to serialize SSE-S3 metadata for bucket-default multipart upload: %v", serErr)
				}

				config.S3KeyDataEncoded = base64.StdEncoding.EncodeToString(keyData)

				// Store key in manager for later retrieval
				keyManager.StoreKey(sseS3Key)
				glog.V(4).Infof("Stored SSE-S3 key %s for bucket-default multipart upload %s", sseS3Key.KeyID, uploadIdString)

			default:
				glog.V(3).Infof("prepareMultipartEncryptionConfig: unsupported bucket-default encryption algorithm %s for bucket %s",
					encryptionConfig.SseAlgorithm, bucket)
			}
		}
	}

	return config, nil
}

// applyMultipartEncryptionConfig applies pre-prepared encryption configuration to filer entry
// This function is guaranteed not to fail since all error-prone operations were done during preparation
func (s3a *S3ApiServer) applyMultipartEncryptionConfig(entry *filer_pb.Entry, config *MultipartEncryptionConfig) {
	// Apply SSE-KMS configuration
	if config.IsSSEKMS {
		entry.Extended[s3_constants.SeaweedFSSSEKMSKeyID] = []byte(config.KMSKeyID)
		if config.BucketKeyEnabled {
			entry.Extended[s3_constants.SeaweedFSSSEKMSBucketKeyEnabled] = []byte("true")
		}
		if config.EncryptionContext != "" {
			entry.Extended[s3_constants.SeaweedFSSSEKMSEncryptionContext] = []byte(config.EncryptionContext)
		}
		entry.Extended[s3_constants.SeaweedFSSSEKMSBaseIV] = []byte(config.KMSBaseIVEncoded)
		glog.V(3).Infof("applyMultipartEncryptionConfig: applied SSE-KMS settings with keyID %s", config.KMSKeyID)
	}

	// Apply SSE-S3 configuration
	if config.IsSSES3 {
		entry.Extended[s3_constants.SeaweedFSSSES3Encryption] = []byte(s3_constants.SSEAlgorithmAES256)
		entry.Extended[s3_constants.SeaweedFSSSES3BaseIV] = []byte(config.S3BaseIVEncoded)
		entry.Extended[s3_constants.SeaweedFSSSES3KeyData] = []byte(config.S3KeyDataEncoded)
		glog.V(3).Infof("applyMultipartEncryptionConfig: applied SSE-S3 settings")
	}
}
