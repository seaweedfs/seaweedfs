package s3api

import (
	"cmp"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"

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

// getRequestScheme determines the URL scheme (http or https) from the request
// Checks X-Forwarded-Proto header first (for proxies), then TLS state
func getRequestScheme(r *http.Request) string {
	// Check X-Forwarded-Proto header for proxied requests
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}
	// Check if connection is TLS
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func (s3a *S3ApiServer) createMultipartUpload(r *http.Request, input *s3.CreateMultipartUploadInput) (output *InitiateMultipartUploadResult, code s3err.ErrorCode) {

	glog.V(2).Infof("createMultipartUpload input %v", input)

	uploadIdString := s3a.generateUploadID(*input.Key)

	uploadIdString = uploadIdString + "_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// Validate checksum algorithm before creating the upload directory
	checksumAlgo, checksumHeaderName, checksumErrCode := detectRequestedChecksumAlgorithm(r)
	if checksumErrCode != s3err.ErrNone {
		return nil, checksumErrCode
	}

	// Resolve and validate the requested checksum type (x-amz-checksum-type)
	// against the algorithm so CompleteMultipartUpload knows whether to produce a
	// COMPOSITE or FULL_OBJECT checksum.
	checksumType := ""
	if checksumHeaderName != "" {
		resolvedType, typeErr := resolveMultipartChecksumType(checksumAlgo, r.Header.Get(s3_constants.AmzChecksumType))
		if typeErr != nil {
			glog.Warningf("createMultipartUpload: %v", typeErr)
			return nil, s3err.ErrInvalidRequest
		}
		checksumType = resolvedType
	}

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

		// Store the requested checksum algorithm and type so CompleteMultipartUpload
		// can compute the object checksum from per-part checksums
		if checksumHeaderName != "" {
			entry.Extended[s3_constants.ExtChecksumAlgorithm] = []byte(checksumHeaderName)
			if checksumType != "" {
				entry.Extended[s3_constants.ExtChecksumType] = []byte(checksumType)
			}
		}

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

	// Checksum fields — returned as HTTP response headers, not in the XML body
	ChecksumHeaderName string `xml:"-"`
	ChecksumValue      string `xml:"-"`
	ChecksumType       string `xml:"-"`

	// VersionId is NOT included in XML body - it should only be in x-amz-version-id HTTP header

	// Store the VersionId internally for setting HTTP header, but don't marshal to XML
	VersionId *string `xml:"-"`
}

// copySSEHeadersFromFirstPart copies all SSE-related headers from the first part to the destination entry
// This is critical for detectPrimarySSEType to work correctly and ensures encryption metadata is preserved
func copySSEHeadersFromFirstPart(dst *filer_pb.Entry, firstPart *filer_pb.Entry, context string) {
	if firstPart == nil || firstPart.Extended == nil {
		return
	}

	// Copy ALL SSE-related headers (not just SeaweedFSSSEKMSKey)
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
		if value, exists := firstPart.Extended[key]; exists {
			dst.Extended[key] = value
			glog.V(4).Infof("completeMultipartUpload: copied SSE header %s from first part (%s)", key, context)
		}
	}
}

type multipartPartBoundary struct {
	PartNumber int    `json:"part"`
	StartChunk int    `json:"start"`
	EndChunk   int    `json:"end"`
	ETag       string `json:"etag"`
	// Byte offsets of the part within the object. Readers prefer these over
	// the chunk indexes above, which stop matching the entry's chunk list
	// once large completions fold it into manifest chunks.
	StartOffset int64 `json:"startOffset,omitempty"`
	EndOffset   int64 `json:"endOffset,omitempty"` // exclusive
}

type multipartSSES3Info struct {
	keyData []byte
	key     *SSES3Key
	baseIV  []byte
}

type multipartCompletionState struct {
	deleteEntries      []*filer_pb.Entry
	partEntries        map[int][]*filer_pb.Entry
	pentry             *filer_pb.Entry
	sses3Info          *multipartSSES3Info
	mime               string
	finalParts         []*filer_pb.FileChunk
	offset             int64
	partBoundaries     []multipartPartBoundary
	multipartETag      string
	entityWithTtl      bool
	checksumHeaderName string // e.g. "X-Amz-Checksum-Crc32", empty if no checksum
	checksumValue      string // multipart checksum: "base64-N" (COMPOSITE) or "base64" (FULL_OBJECT)
	checksumType       string // s3_constants.ChecksumType* (COMPOSITE or FULL_OBJECT)

	newManifestChunks       []*filer_pb.FileChunk // blobs folded for finalParts; orphans until the entry commits
	manifestsReferenced     bool                  // failed rollback left an entry holding newManifestChunks
	supersededPartManifests []*filer_pb.FileChunk // part-entry blobs replaced by flattening; deleted after commit
	metadataOnlyCleanup     bool                  // deleteEntries share chunks with the live object; keep their data
}

func completeMultipartResult(r *http.Request, input *s3.CompleteMultipartUploadInput, etag string, entry *filer_pb.Entry) *CompleteMultipartUploadResult {
	result := &CompleteMultipartUploadResult{
		Location: aws.String(fmt.Sprintf("%s://%s/%s/%s", getRequestScheme(r), r.Host, url.PathEscape(*input.Bucket), urlPathEscape(*input.Key))),
		Bucket:   input.Bucket,
		ETag:     aws.String(etag),
		Key:      objectKey(input.Key),
	}
	if entry != nil && entry.Extended != nil {
		if versionIdBytes, ok := entry.Extended[s3_constants.ExtVersionIdKey]; ok {
			versionId := string(versionIdBytes)
			if versionId != "" && versionId != "null" {
				result.VersionId = aws.String(versionId)
			}
		}
	}
	return result
}

func extractMultipartSSES3Info(entry *filer_pb.Entry) (*multipartSSES3Info, error) {
	if entry == nil || entry.Extended == nil {
		return nil, nil
	}
	if encryptionType := string(entry.Extended[s3_constants.SeaweedFSSSES3Encryption]); encryptionType != s3_constants.SSEAlgorithmAES256 {
		return nil, nil
	}

	baseIVEncoded := entry.Extended[s3_constants.SeaweedFSSSES3BaseIV]
	if len(baseIVEncoded) == 0 {
		return nil, fmt.Errorf("missing SSE-S3 multipart base IV")
	}
	baseIV, err := base64.StdEncoding.DecodeString(string(baseIVEncoded))
	if err != nil {
		return nil, fmt.Errorf("decode SSE-S3 multipart base IV: %w", err)
	}
	if len(baseIV) != s3_constants.AESBlockSize {
		return nil, fmt.Errorf("invalid SSE-S3 multipart base IV length %d", len(baseIV))
	}

	keyDataEncoded := entry.Extended[s3_constants.SeaweedFSSSES3KeyData]
	if len(keyDataEncoded) == 0 {
		return nil, fmt.Errorf("missing SSE-S3 multipart key data")
	}
	keyData, err := base64.StdEncoding.DecodeString(string(keyDataEncoded))
	if err != nil {
		return nil, fmt.Errorf("decode SSE-S3 multipart key data: %w", err)
	}

	key, err := DeserializeSSES3Metadata(keyData, GetSSES3KeyManager())
	if err != nil {
		return nil, fmt.Errorf("deserialize SSE-S3 multipart key data: %w", err)
	}

	return &multipartSSES3Info{
		keyData: keyData,
		key:     key,
		baseIV:  baseIV,
	}, nil
}

func completedMultipartChunk(chunk *filer_pb.FileChunk, offset int64, sses3Info *multipartSSES3Info) (*filer_pb.FileChunk, error) {
	finalChunk := &filer_pb.FileChunk{
		FileId:       chunk.GetFileIdString(),
		Offset:       offset,
		Size:         chunk.Size,
		ModifiedTsNs: chunk.ModifiedTsNs,
		CipherKey:    chunk.CipherKey,
		ETag:         chunk.ETag,
		IsCompressed: chunk.IsCompressed,
		SseType:      chunk.SseType,
		SseMetadata:  chunk.SseMetadata,
	}

	if sses3Info == nil {
		return finalChunk, nil
	}
	if finalChunk.GetSseType() != filer_pb.SSEType_NONE && finalChunk.GetSseType() != filer_pb.SSEType_SSE_S3 {
		return finalChunk, nil
	}
	// Trust existing per-chunk SSE-S3 metadata: if the part went through
	// putToFiler's chunk loop with a non-nil sseS3Key it carries an offset
	// derived IV that we cannot recover from the upload-entry alone. We do
	// not validate that the embedded IV equals calculateIVWithOffset(baseIV,
	// chunk.Offset); only completely missing metadata is backfilled. This
	// keeps healthy parts untouched and limits backfill to the cases that
	// caused #8908.
	if finalChunk.GetSseType() == filer_pb.SSEType_SSE_S3 && len(finalChunk.GetSseMetadata()) > 0 {
		return finalChunk, nil
	}

	// IV uses the chunk's offset within its part, with no partNumber term.
	// This mirrors the encryption side: putToFiler hardcodes partOffset=0 when
	// it calls handleSSES3MultipartEncryption for every part, so each part's
	// encrypted stream begins at IV = calculateIVWithOffset(baseIV, 0) = baseIV.
	// Each chunk within that stream is then at IV =
	// calculateIVWithOffset(baseIV, chunk.Offset_part_local). Any deviation
	// (e.g. adding (partNumber-1)*PartOffsetMultiplier) would produce IVs that
	// do not match the actual encryption and would corrupt decryption.
	chunkIV, _ := calculateIVWithOffset(sses3Info.baseIV, chunk.GetOffset())
	chunkKey := &SSES3Key{
		Key:       sses3Info.key.Key,
		KeyID:     sses3Info.key.KeyID,
		Algorithm: sses3Info.key.Algorithm,
		IV:        chunkIV,
	}
	chunkMetadata, err := SerializeSSES3Metadata(chunkKey)
	if err != nil {
		return nil, fmt.Errorf("serialize SSE-S3 chunk metadata for %s: %w", chunk.GetFileIdString(), err)
	}

	finalChunk.SseType = filer_pb.SSEType_SSE_S3
	finalChunk.SseMetadata = chunkMetadata
	return finalChunk, nil
}

// applyMultipartSSES3HeadersFromUploadEntry writes the canonical object-level
// SSE-S3 attributes (SeaweedFSSSES3Key / X-Amz-Server-Side-Encryption) onto a
// completed multipart entry when they are missing. detectPrimarySSEType uses
// the object-level X-Amz-Server-Side-Encryption header to recognize SSE-S3 on
// inline / small-object reads, and IsSSES3EncryptedInternal requires both keys
// to consider an object encrypted at all. Without this, an object whose first
// part lacked the canonical attributes (e.g. a part written through a path
// that did not set them) would be served as unencrypted on GET.
func applyMultipartSSES3HeadersFromUploadEntry(dst *filer_pb.Entry, sses3Info *multipartSSES3Info) {
	if dst == nil || sses3Info == nil {
		return
	}
	if dst.Extended == nil {
		dst.Extended = make(map[string][]byte)
	}
	if _, exists := dst.Extended[s3_constants.SeaweedFSSSES3Key]; !exists {
		dst.Extended[s3_constants.SeaweedFSSSES3Key] = sses3Info.keyData
	}
	if _, exists := dst.Extended[s3_constants.AmzServerSideEncryption]; !exists {
		dst.Extended[s3_constants.AmzServerSideEncryption] = []byte(s3_constants.SSEAlgorithmAES256)
	}
}

func (s3a *S3ApiServer) prepareMultipartCompletionState(r *http.Request, input *s3.CompleteMultipartUploadInput, uploadDirectory, entryName, dirName string, completedPartNumbers []int, completedPartMap map[int][]string, maxPartNo int) (*multipartCompletionState, *CompleteMultipartUploadResult, s3err.ErrorCode) {
	if entry, err := s3a.resolveObjectEntry(*input.Bucket, *input.Key); err == nil && entry != nil && entry.Extended != nil {
		if uploadId, ok := entry.Extended[s3_constants.SeaweedFSUploadId]; ok && *input.UploadId == string(uploadId) {
			cleanupEntries, _, cleanupErr := s3a.list(uploadDirectory, "", "", false, s3_constants.MaxS3MultipartParts+1)
			if cleanupErr != nil && !errors.Is(cleanupErr, filer_pb.ErrNotFound) {
				glog.Warningf("completeMultipartUpload: failed to list stale upload directory %s for cleanup: %v", uploadDirectory, cleanupErr)
			}
			return &multipartCompletionState{deleteEntries: cleanupEntries, metadataOnlyCleanup: true}, completeMultipartResult(r, input, getEtagFromEntry(entry), entry), s3err.ErrNone
		}
	}

	entries, _, err := s3a.list(uploadDirectory, "", "", false, s3_constants.MaxS3MultipartParts+1)
	if err != nil {
		glog.Errorf("completeMultipartUpload %s %s error: %v", *input.Bucket, *input.UploadId, err)
		if isFilerListNotFound(err) {
			stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
			return nil, nil, s3err.ErrNoSuchUpload
		}
		// a store error must not read as "upload gone": the client would drop a fully-uploaded object
		return nil, nil, s3err.ErrInternalError
	}
	if len(entries) == 0 {
		stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
		return nil, nil, s3err.ErrNoSuchUpload
	}

	pentry, err := s3a.getEntry(s3a.genUploadsFolder(*input.Bucket), *input.UploadId)
	if err != nil {
		glog.Errorf("completeMultipartUpload %s %s error: %v", *input.Bucket, *input.UploadId, err)
		if errors.Is(err, filer_pb.ErrNotFound) {
			stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedNoSuchUpload).Inc()
			return nil, nil, s3err.ErrNoSuchUpload
		}
		return nil, nil, s3err.ErrInternalError
	}

	deleteEntries := make([]*filer_pb.Entry, 0)
	partEntries := make(map[int][]*filer_pb.Entry, len(entries))
	entityTooSmall := false
	entityWithTtl := false
	for _, entry := range entries {
		foundEntry := false
		glog.V(4).Infof("completeMultipartUpload part entries %s", entry.Name)
		if entry.IsDirectory || !strings.HasSuffix(entry.Name, multipartExt) {
			continue
		}
		partNumber, parseErr := parsePartNumber(entry.Name)
		if parseErr != nil {
			stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartNumber).Inc()
			glog.Errorf("completeMultipartUpload failed to parse partNumber %s:%s", entry.Name, parseErr)
			continue
		}
		completedPartsByNumber, ok := completedPartMap[partNumber]
		if !ok {
			continue
		}
		for _, partETag := range completedPartsByNumber {
			match, invalid, normalizedPartETag, normalizedEntryETag := validateCompletePartETag(partETag, entry)
			if invalid {
				glog.Warningf("invalid complete etag %s, storedEtag %s", normalizedPartETag, normalizedEntryETag)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedEtagInvalid).Inc()
				continue
			}
			if !match {
				glog.Errorf("completeMultipartUpload %s ETag mismatch stored: %s part: %s", entry.Name, normalizedEntryETag, normalizedPartETag)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedEtagMismatch).Inc()
				continue
			}
			if len(entry.Chunks) == 0 && partNumber != maxPartNo {
				glog.Warningf("completeMultipartUpload %s empty chunks", entry.Name)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartEmpty).Inc()
				continue
			}
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
		return nil, nil, s3err.ErrEntityTooSmall
	}

	mime := ""
	if pentry.Attributes != nil {
		mime = pentry.Attributes.Mime
	}
	sses3Info, sses3Err := extractMultipartSSES3Info(pentry)
	if sses3Err != nil {
		glog.Errorf("completeMultipartUpload %s %s SSE-S3 metadata error: %v", *input.Bucket, *input.UploadId, sses3Err)
		return nil, nil, s3err.ErrInternalError
	}
	finalParts := make([]*filer_pb.FileChunk, 0)
	partBoundaries := make([]multipartPartBoundary, 0, len(completedPartNumbers))
	var supersededPartManifests []*filer_pb.FileChunk
	var offset int64

	for _, partNumber := range completedPartNumbers {
		partEntriesByNumber, ok := partEntries[partNumber]
		if !ok {
			glog.Errorf("part %d has no entry", partNumber)
			stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartNotFound).Inc()
			return nil, nil, s3err.ErrInvalidPart
		}
		found := false

		if len(partEntriesByNumber) > 1 {
			sortEntriesByLatestChunk(partEntriesByNumber)
		}
		for _, entry := range partEntriesByNumber {
			if found {
				deleteEntries = append(deleteEntries, entry)
				stats.S3HandlerCounter.WithLabelValues(stats.ErrorCompletedPartEntryMismatch).Inc()
				continue
			}

			// A part entry can itself carry manifest chunks (the filer folds an
			// oversized part, e.g. a large UploadPartCopy range). Resolve them
			// first: the rebase below shifts chunk offsets, which a manifest
			// chunk cannot express.
			removedManifests, flattenErr := s3a.flattenManifestChunks(r.Context(), entry)
			if flattenErr != nil {
				glog.Errorf("completeMultipartUpload %s %s part %d resolve manifest chunks: %v", *input.Bucket, *input.UploadId, partNumber, flattenErr)
				return nil, nil, s3err.ErrInternalError
			}
			supersededPartManifests = append(supersededPartManifests, removedManifests...)

			partStartChunk := len(finalParts)
			partStartOffset := offset
			partETag := getEtagFromEntry(entry)

			for _, chunk := range entry.GetChunks() {
				finalChunk, chunkErr := completedMultipartChunk(chunk, offset, sses3Info)
				if chunkErr != nil {
					glog.Errorf("completeMultipartUpload %s %s SSE-S3 chunk metadata error: %v", *input.Bucket, *input.UploadId, chunkErr)
					return nil, nil, s3err.ErrInternalError
				}
				finalParts = append(finalParts, finalChunk)
				offset += int64(chunk.Size)
			}

			partEndChunk := len(finalParts)
			partBoundaries = append(partBoundaries, multipartPartBoundary{
				PartNumber:  partNumber,
				StartChunk:  partStartChunk,
				EndChunk:    partEndChunk,
				ETag:        partETag,
				StartOffset: partStartOffset,
				EndOffset:   offset,
			})

			found = true
		}
	}

	// Compute the object checksum from per-part checksums if the upload was
	// initiated with a checksum algorithm (stored in the upload dir entry).
	// The checksum type (COMPOSITE or FULL_OBJECT) is resolved from the
	// x-amz-checksum-type header captured at CreateMultipartUpload
	checksumHeaderName := ""
	checksumType := ""
	checksumValue := ""
	if pentry.Extended != nil {
		if algoName, ok := pentry.Extended[s3_constants.ExtChecksumAlgorithm]; ok {
			checksumHeaderName = string(algoName)
		}
	}
	if checksumHeaderName != "" {
		algo := checksumAlgorithmFromHeaderName(checksumHeaderName)
		requestedType := ""
		if pentry.Extended != nil {
			requestedType = string(pentry.Extended[s3_constants.ExtChecksumType])
		}
		resolvedType, typeErr := resolveMultipartChecksumType(algo, requestedType)
		if typeErr != nil {
			glog.Errorf("completeMultipartUpload: %v", typeErr)
			return nil, nil, s3err.ErrInvalidRequest
		}
		checksumType = resolvedType

		var checksumErr error
		if checksumType == s3_constants.ChecksumTypeFullObject {
			checksumValue, checksumErr = computeFullObjectChecksum(checksumHeaderName, partEntries, completedPartNumbers)
		} else {
			checksumValue, checksumErr = computeCompositeChecksum(checksumHeaderName, partEntries, completedPartNumbers)
		}
		if checksumErr != nil {
			glog.Errorf("completeMultipartUpload: %s checksum computation failed: %v", checksumType, checksumErr)
			return nil, nil, s3err.ErrInvalidPart
		}
	}

	// Fold huge completions (>filer.ManifestBatch chunks) into manifest chunks
	// so the object entry stays small. Runs after the part boundaries above
	// captured their byte offsets against the flat list. finalParts holds no
	// manifest chunks at this point, so anything folded out is newly created.
	finalParts = s3a.manifestizeChunks(dirName+"/"+entryName, *input.Bucket, 0, finalParts)
	newManifestChunks, _ := filer.SeparateManifestChunks(finalParts)

	return &multipartCompletionState{
		deleteEntries:           deleteEntries,
		partEntries:             partEntries,
		pentry:                  pentry,
		sses3Info:               sses3Info,
		mime:                    mime,
		finalParts:              finalParts,
		offset:                  offset,
		partBoundaries:          partBoundaries,
		multipartETag:           calculateMultipartETag(partEntries, completedPartNumbers),
		entityWithTtl:           entityWithTtl,
		checksumHeaderName:      checksumHeaderName,
		checksumValue:           checksumValue,
		checksumType:            checksumType,
		newManifestChunks:       newManifestChunks,
		supersededPartManifests: supersededPartManifests,
	}, nil, s3err.ErrNone
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
	lastSeenPartNo := 0

	for _, part := range parts.Parts {
		if part.PartNumber < lastSeenPartNo {
			return nil, s3err.ErrInvalidPartOrder
		}
		lastSeenPartNo = part.PartNumber
		if _, ok := completedPartMap[part.PartNumber]; !ok {
			completedPartNumbers = append(completedPartNumbers, part.PartNumber)
		}
		completedPartMap[part.PartNumber] = append(completedPartMap[part.PartNumber], part.ETag)
		maxPartNo = maxInt(maxPartNo, part.PartNumber)
	}

	uploadDirectory := s3a.genUploadsFolder(*input.Bucket) + "/" + *input.UploadId
	entryName, dirName := s3a.getEntryNameAndDir(input)
	var completionState *multipartCompletionState
	// Route the completion's writes to the object's owner filer when known, off
	// the distributed lock. Idempotent replay is handled gateway-side in
	// prepareMultipartCompletionState (it returns the existing result when the
	// object already carries this UploadId), so the lock is not needed to dedupe
	// retries. With no owner yet (no ring), keep the lock as the bootstrap path.
	owner := s3a.objectWriteOwner(*input.Bucket, *input.Key)
	routeKey := s3a.objectRouteKey(*input.Bucket, *input.Key)
	completionBody := func() s3err.ErrorCode {
		var prepCode s3err.ErrorCode
		completionState, output, prepCode = s3a.prepareMultipartCompletionState(r, input, uploadDirectory, entryName, dirName, completedPartNumbers, completedPartMap, maxPartNo)
		if prepCode != s3err.ErrNone || output != nil {
			return prepCode
		}

		etagQuote := "\"" + completionState.multipartETag + "\""
		// Check if versioning is configured for this bucket BEFORE creating any files.
		versioningState, vErr := s3a.getVersioningState(*input.Bucket)
		if vErr != nil {
			glog.Errorf("completeMultipartUpload: failed to get versioning state for bucket %s: %v", *input.Bucket, vErr)
			return s3err.ErrInternalError
		}
		if versioningState == s3_constants.VersioningEnabled {
			// Use full object key (not just entryName) to ensure correct .versions directory is checked
			normalizedKey := strings.TrimPrefix(*input.Key, "/")
			useInvertedFormat := s3a.getVersionIdFormat(*input.Bucket, normalizedKey)
			versionId := generateVersionId(useInvertedFormat)
			versionFileName := s3a.getVersionFileName(versionId)
			versionDir := dirName + "/" + entryName + s3_constants.VersionsFolder

			// Capture timestamp and owner once for consistency between version entry and cache entry
			versionMtime := time.Now().Unix()
			amzAccountId := r.Header.Get(s3_constants.AmzAccountId)

			// Create the version file in the .versions directory
			if err := s3a.mkFile(versionDir, versionFileName, completionState.finalParts, func(versionEntry *filer_pb.Entry) {
				if versionEntry.Extended == nil {
					versionEntry.Extended = make(map[string][]byte)
				}
				versionEntry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
				versionEntry.Extended[s3_constants.SeaweedFSUploadId] = []byte(*input.UploadId)
				// Store parts count for x-amz-mp-parts-count header
				versionEntry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte(fmt.Sprintf("%d", len(completedPartNumbers)))
				// Store part boundaries for GetObject with PartNumber
				if partBoundariesJSON, err := json.Marshal(completionState.partBoundaries); err == nil {
					versionEntry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries] = partBoundariesJSON
				}

				// Set object owner for versioned multipart objects
				if amzAccountId != "" {
					versionEntry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
				}

				for k, v := range completionState.pentry.Extended {
					if k != s3_constants.ExtMultipartObjectKey {
						versionEntry.Extended[k] = v
					}
				}

				// Persist ETag to ensure subsequent HEAD/GET uses the same value
				versionEntry.Extended[s3_constants.ExtETagKey] = []byte(completionState.multipartETag)
				// Store the object checksum computed from per-part checksums
				if completionState.checksumHeaderName != "" && completionState.checksumValue != "" {
					versionEntry.Extended[s3_constants.ExtChecksumAlgorithm] = []byte(completionState.checksumHeaderName)
					versionEntry.Extended[s3_constants.ExtChecksumValue] = []byte(completionState.checksumValue)
					if completionState.checksumType != "" {
						versionEntry.Extended[s3_constants.ExtChecksumType] = []byte(completionState.checksumType)
					}
				}

				// Preserve ALL SSE metadata from the first part (if any)
				// SSE metadata is stored in individual parts, not the upload directory
				if len(completedPartNumbers) > 0 && len(completionState.partEntries[completedPartNumbers[0]]) > 0 {
					firstPartEntry := completionState.partEntries[completedPartNumbers[0]][0]
					copySSEHeadersFromFirstPart(versionEntry, firstPartEntry, "versioned")
				}
				applyMultipartSSES3HeadersFromUploadEntry(versionEntry, completionState.sses3Info)
				if completionState.pentry.Attributes != nil && completionState.pentry.Attributes.Mime != "" {
					versionEntry.Attributes.Mime = completionState.pentry.Attributes.Mime
				} else if completionState.mime != "" {
					versionEntry.Attributes.Mime = completionState.mime
				}
				versionEntry.Attributes.FileSize = uint64(completionState.offset)
				versionEntry.Attributes.Mtime = versionMtime
			}); err != nil {
				glog.Errorf("completeMultipartUpload: failed to create version %s: %v", versionId, err)
				return s3err.ErrInternalError
			}

			// Construct entry with metadata for caching in .versions directory
			// Reuse versionMtime to keep list vs. HEAD timestamps aligned
			// multipartETag is precomputed
			versionEntryForCache := &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{
					FileSize: uint64(completionState.offset),
					Mtime:    versionMtime,
				},
				Extended: map[string][]byte{
					s3_constants.ExtETagKey: []byte(completionState.multipartETag),
				},
			}
			if amzAccountId != "" {
				versionEntryForCache.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
			}

			// Update the .versions directory metadata to indicate this is the latest version
			// Pass entry to cache its metadata for single-scan list efficiency
			// Route the pointer flip to the owner (off the lock) via
			// RECOMPUTE_LATEST; the just-written version file is the newest.
			if owner != "" {
				if code := s3a.routedVersionedFinalize(owner, *input.Bucket, *input.Key, useInvertedFormat); code != s3err.ErrNone {
					if rollbackErr := s3a.rollbackMultipartVersion(versionDir, versionFileName); rollbackErr != nil {
						glog.Errorf("completeMultipartUpload: failed to rollback version %s for %s/%s after routed finalize error: %v", versionId, *input.Bucket, *input.Key, rollbackErr)
						completionState.manifestsReferenced = true
					}
					return code
				}
			} else if err := s3a.updateLatestVersionInDirectory(*input.Bucket, *input.Key, versionId, versionFileName, versionEntryForCache); err != nil {
				if rollbackErr := s3a.rollbackMultipartVersion(versionDir, versionFileName); rollbackErr != nil {
					glog.Errorf("completeMultipartUpload: failed to rollback version %s for %s/%s after latest pointer update error: %v", versionId, *input.Bucket, *input.Key, rollbackErr)
					completionState.manifestsReferenced = true
				}
				glog.Errorf("completeMultipartUpload: failed to update latest version in directory: %v", err)
				return s3err.ErrInternalError
			}

			// For versioned buckets, all content is stored in .versions directory
			// The latest version information is tracked in the .versions directory metadata
			output = &CompleteMultipartUploadResult{
				Location:           aws.String(fmt.Sprintf("%s://%s/%s/%s", getRequestScheme(r), r.Host, url.PathEscape(*input.Bucket), urlPathEscape(*input.Key))),
				Bucket:             input.Bucket,
				ETag:               aws.String(etagQuote),
				Key:                objectKey(input.Key),
				VersionId:          aws.String(versionId),
				ChecksumHeaderName: completionState.checksumHeaderName,
				ChecksumValue:      completionState.checksumValue,
				ChecksumType:       completionState.checksumType,
			}
			return s3err.ErrNone
		}

		if versioningState == s3_constants.VersioningSuspended {
			// For suspended versioning, add "null" version ID metadata and return "null" version ID
			if err := s3a.writeMultipartObject(owner, routeKey, dirName, entryName, completionState.finalParts, func(entry *filer_pb.Entry) {
				if entry.Extended == nil {
					entry.Extended = make(map[string][]byte)
				}
				entry.Extended[s3_constants.ExtVersionIdKey] = []byte("null")
				entry.Extended[s3_constants.SeaweedFSUploadId] = []byte(*input.UploadId)
				// Store parts count for x-amz-mp-parts-count header
				entry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte(fmt.Sprintf("%d", len(completedPartNumbers)))
				// Store part boundaries for GetObject with PartNumber
				if partBoundariesJSON, jsonErr := json.Marshal(completionState.partBoundaries); jsonErr == nil {
					entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries] = partBoundariesJSON
				}

				// Set object owner for suspended versioning multipart objects
				amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
				if amzAccountId != "" {
					entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
				}

				for k, v := range completionState.pentry.Extended {
					if k != s3_constants.ExtMultipartObjectKey {
						entry.Extended[k] = v
					}
				}

				// Preserve ALL SSE metadata from the first part (if any)
				// SSE metadata is stored in individual parts, not the upload directory
				if len(completedPartNumbers) > 0 && len(completionState.partEntries[completedPartNumbers[0]]) > 0 {
					firstPartEntry := completionState.partEntries[completedPartNumbers[0]][0]
					copySSEHeadersFromFirstPart(entry, firstPartEntry, "suspended versioning")
				}
				applyMultipartSSES3HeadersFromUploadEntry(entry, completionState.sses3Info)
				// Persist ETag to ensure subsequent HEAD/GET uses the same value
				entry.Extended[s3_constants.ExtETagKey] = []byte(completionState.multipartETag)
				// Store the object checksum computed from per-part checksums
				if completionState.checksumHeaderName != "" && completionState.checksumValue != "" {
					entry.Extended[s3_constants.ExtChecksumAlgorithm] = []byte(completionState.checksumHeaderName)
					entry.Extended[s3_constants.ExtChecksumValue] = []byte(completionState.checksumValue)
					if completionState.checksumType != "" {
						entry.Extended[s3_constants.ExtChecksumType] = []byte(completionState.checksumType)
					}
				}
				if completionState.pentry.Attributes != nil && completionState.pentry.Attributes.Mime != "" {
					entry.Attributes.Mime = completionState.pentry.Attributes.Mime
				} else if completionState.mime != "" {
					entry.Attributes.Mime = completionState.mime
				}
				entry.Attributes.FileSize = uint64(completionState.offset)
			}); err != nil {
				glog.Errorf("completeMultipartUpload: failed to create suspended versioning object: %v", err)
				return s3err.ErrInternalError
			}

			// Note: Suspended versioning should NOT return VersionId field according to AWS S3 spec
			output = &CompleteMultipartUploadResult{
				Location:           aws.String(fmt.Sprintf("%s://%s/%s/%s", getRequestScheme(r), r.Host, url.PathEscape(*input.Bucket), urlPathEscape(*input.Key))),
				Bucket:             input.Bucket,
				ETag:               aws.String(etagQuote),
				Key:                objectKey(input.Key),
				ChecksumHeaderName: completionState.checksumHeaderName,
				ChecksumValue:      completionState.checksumValue,
				ChecksumType:       completionState.checksumType,
				// VersionId field intentionally omitted for suspended versioning
			}
			return s3err.ErrNone
		}

		// For non-versioned buckets, create main object file
		if err := s3a.writeMultipartObject(owner, routeKey, dirName, entryName, completionState.finalParts, func(entry *filer_pb.Entry) {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.SeaweedFSUploadId] = []byte(*input.UploadId)
			// Store parts count for x-amz-mp-parts-count header
			entry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte(fmt.Sprintf("%d", len(completedPartNumbers)))
			// Store part boundaries for GetObject with PartNumber
			if partBoundariesJSON, err := json.Marshal(completionState.partBoundaries); err == nil {
				entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries] = partBoundariesJSON
			}

			// Set object owner for non-versioned multipart objects
			amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
			if amzAccountId != "" {
				entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(amzAccountId)
			}

			for k, v := range completionState.pentry.Extended {
				if k != s3_constants.ExtMultipartObjectKey {
					entry.Extended[k] = v
				}
			}

			// Preserve ALL SSE metadata from the first part (if any)
			// SSE metadata is stored in individual parts, not the upload directory
			if len(completedPartNumbers) > 0 && len(completionState.partEntries[completedPartNumbers[0]]) > 0 {
				firstPartEntry := completionState.partEntries[completedPartNumbers[0]][0]
				copySSEHeadersFromFirstPart(entry, firstPartEntry, "non-versioned")
			}
			applyMultipartSSES3HeadersFromUploadEntry(entry, completionState.sses3Info)
			// Persist ETag to ensure subsequent HEAD/GET uses the same value
			entry.Extended[s3_constants.ExtETagKey] = []byte(completionState.multipartETag)
			// Store the object checksum computed from per-part checksums
			if completionState.checksumHeaderName != "" && completionState.checksumValue != "" {
				entry.Extended[s3_constants.ExtChecksumAlgorithm] = []byte(completionState.checksumHeaderName)
				entry.Extended[s3_constants.ExtChecksumValue] = []byte(completionState.checksumValue)
				if completionState.checksumType != "" {
					entry.Extended[s3_constants.ExtChecksumType] = []byte(completionState.checksumType)
				}
			}
			if completionState.pentry.Attributes != nil && completionState.pentry.Attributes.Mime != "" {
				entry.Attributes.Mime = completionState.pentry.Attributes.Mime
			} else if completionState.mime != "" {
				entry.Attributes.Mime = completionState.mime
			}
			entry.Attributes.FileSize = uint64(completionState.offset)
			// Set TTL-based S3 expiry (modification time)
			if completionState.entityWithTtl {
				entry.Extended[s3_constants.SeaweedFSExpiresS3] = []byte("true")
			}
		}); err != nil {
			glog.Errorf("completeMultipartUpload %s/%s error: %v", dirName, entryName, err)
			return s3err.ErrInternalError
		}

		// For non-versioned buckets, return response without VersionId
		output = &CompleteMultipartUploadResult{
			Location:           aws.String(fmt.Sprintf("%s://%s/%s/%s", getRequestScheme(r), r.Host, url.PathEscape(*input.Bucket), urlPathEscape(*input.Key))),
			Bucket:             input.Bucket,
			ETag:               aws.String(etagQuote),
			Key:                objectKey(input.Key),
			ChecksumHeaderName: completionState.checksumHeaderName,
			ChecksumValue:      completionState.checksumValue,
			ChecksumType:       completionState.checksumType,
		}
		return s3err.ErrNone
	}
	var finalizeCode s3err.ErrorCode
	if owner != "" {
		if code := s3a.checkConditionalHeaders(r, *input.Bucket, *input.Key); code != s3err.ErrNone {
			finalizeCode = code
		} else {
			finalizeCode = completionBody()
		}
	} else {
		finalizeCode = s3a.withObjectWriteLock(*input.Bucket, *input.Key, func() s3err.ErrorCode {
			return s3a.checkConditionalHeaders(r, *input.Bucket, *input.Key)
		}, completionBody)
	}
	if finalizeCode != s3err.ErrNone {
		// this attempt's manifests are orphans unless a failed rollback left an entry holding them
		if completionState != nil && !completionState.manifestsReferenced && len(completionState.newManifestChunks) > 0 {
			s3a.deleteOrphanedChunks(completionState.newManifestChunks)
		}
		return nil, finalizeCode
	}

	if completionState != nil {
		for _, deleteEntry := range completionState.deleteEntries {
			if err := s3a.rm(uploadDirectory, deleteEntry.Name, !completionState.metadataOnlyCleanup, true); err != nil {
				glog.Warningf("completeMultipartUpload cleanup %s upload %s unused %s : %v", *input.Bucket, *input.UploadId, deleteEntry.Name, err)
			}
		}
		if err := s3a.rm(s3a.genUploadsFolder(*input.Bucket), *input.UploadId, false, true); err != nil {
			glog.V(1).Infof("completeMultipartUpload cleanup %s upload %s: %v", *input.Bucket, *input.UploadId, err)
		}
		if len(completionState.supersededPartManifests) > 0 {
			s3a.deleteOrphanedChunks(completionState.supersededPartManifests)
		}
	}

	return
}

// Metadata-only: the version file's chunks are the still-registered parts'
// chunks, which a retried completion needs.
func (s3a *S3ApiServer) rollbackMultipartVersion(versionDir, versionFileName string) error {
	return s3a.rmObject(versionDir, versionFileName, false, false)
}

func (s3a *S3ApiServer) getEntryNameAndDir(input *s3.CompleteMultipartUploadInput) (string, string) {
	entryName := path.Base(*input.Key)
	dirName := path.Dir(*input.Key)
	if dirName == "." {
		dirName = ""
	}
	dirName = strings.TrimPrefix(dirName, "/")
	dirName = fmt.Sprintf("%s/%s", s3a.bucketDir(*input.Bucket), dirName)

	// remove suffix '/'
	dirName = strings.TrimSuffix(dirName, "/")
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
		// filer_pb.Exists reports not-found as (false, nil), so an error here is
		// always a store failure; answering NoSuchUpload would leak the parts.
		glog.Errorf("bucket %s abort upload %s: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrInternalError
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
		// A missing .uploads folder normally lists as empty with no error; a
		// store that reports it as not-found still means an empty list.
		if isFilerListNotFound(err) {
			return output, s3err.ErrNone
		}
		// surface a real store error; a masked empty 200 makes a resuming client treat the upload as gone
		glog.Errorf("listMultipartUploads %s error: %v", *input.Bucket, err)
		return output, s3err.ErrInternalError
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
		// A store that reports the missing upload directory as not-found means
		// the upload is gone (completed or aborted), not a store error.
		if isFilerListNotFound(err) {
			return nil, s3err.ErrNoSuchUpload
		}
		glog.Errorf("listObjectParts %s %s error: %v", *input.Bucket, *input.UploadId, err)
		return nil, s3err.ErrInternalError
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

			partETag := getEtagFromEntry(entry)
			part := &s3.Part{
				PartNumber:   aws.Int64(int64(partNumber)),
				LastModified: aws.Time(time.Unix(entry.Attributes.Mtime, 0).UTC()),
				Size:         aws.Int64(int64(filer.FileSize(entry))),
				ETag:         aws.String(partETag),
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
		if err != nil {
			// Check if this is just "no encryption configured" vs a real error
			if !errors.Is(err, ErrNoEncryptionConfig) {
				// Real error - propagate to prevent silent encryption bypass
				return nil, fmt.Errorf("failed to read bucket encryption config for multipart upload: %v", err)
			}
			// No default encryption configured, continue without encryption
		} else if encryptionConfig != nil && encryptionConfig.SseAlgorithm != "" {
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

func sortEntriesByLatestChunk(entries []*filer_pb.Entry) {
	slices.SortFunc(entries, func(a, b *filer_pb.Entry) int {
		var aTs, bTs int64
		if len(a.Chunks) > 0 {
			aTs = a.Chunks[0].ModifiedTsNs
		}
		if len(b.Chunks) > 0 {
			bTs = b.Chunks[0].ModifiedTsNs
		}
		return cmp.Compare(bTs, aTs)
	})
}

func calculateMultipartETag(partEntries map[int][]*filer_pb.Entry, completedPartNumbers []int) string {
	var etags []byte
	for _, partNumber := range completedPartNumbers {
		entries, ok := partEntries[partNumber]
		if !ok || len(entries) == 0 {
			continue
		}
		if len(entries) > 1 {
			sortEntriesByLatestChunk(entries)
		}
		entry := entries[0]
		etag := getEtagFromEntry(entry)
		glog.V(4).Infof("calculateMultipartETag: part %d, entry %s, getEtagFromEntry result: %s", partNumber, entry.Name, etag)
		etag = strings.Trim(etag, "\"")
		if before, _, found := strings.Cut(etag, "-"); found {
			etag = before
		}
		if etagBytes, err := hex.DecodeString(etag); err == nil {
			etags = append(etags, etagBytes...)
		} else {
			glog.Warningf("calculateMultipartETag: failed to decode etag '%s' for part %d: %v", etag, partNumber, err)
		}
	}
	return fmt.Sprintf("%x-%d", md5.Sum(etags), len(completedPartNumbers))
}

func decodePartChecksum(partNumber int, entries []*filer_pb.Entry, checksumHeaderName string) ([]byte, *filer_pb.Entry, error) {
	if len(entries) == 0 {
		return nil, nil, fmt.Errorf("part %d not found", partNumber)
	}
	if len(entries) > 1 {
		sortEntriesByLatestChunk(entries)
	}
	entry := entries[0]
	if entry.Extended == nil {
		return nil, nil, fmt.Errorf("part %d missing checksum: upload initiated with %s but part was uploaded without a checksum", partNumber, checksumHeaderName)
	}
	// Validate the part's checksum algorithm matches the upload's expected algorithm
	partAlgo, ok := entry.Extended[s3_constants.ExtChecksumAlgorithm]
	if !ok || len(partAlgo) == 0 {
		return nil, nil, fmt.Errorf("part %d missing checksum: upload initiated with %s but part was uploaded without a checksum", partNumber, checksumHeaderName)
	}
	if string(partAlgo) != checksumHeaderName {
		return nil, nil, fmt.Errorf("part %d checksum algorithm mismatch: upload expects %s but part has %s", partNumber, checksumHeaderName, string(partAlgo))
	}
	partChecksumB64, ok := entry.Extended[s3_constants.ExtChecksumValue]
	if !ok || len(partChecksumB64) == 0 {
		return nil, nil, fmt.Errorf("part %d missing checksum value: upload initiated with %s but part has no checksum value", partNumber, checksumHeaderName)
	}
	raw, err := base64.StdEncoding.DecodeString(string(partChecksumB64))
	if err != nil {
		return nil, nil, fmt.Errorf("part %d has invalid checksum encoding: %w", partNumber, err)
	}
	return raw, entry, nil
}

// computeCompositeChecksum computes a composite checksum from per-part checksums.
// It concatenates the raw (decoded) per-part checksums, hashes the result with the
// same algorithm, and returns the value as "base64-N" where N is the part count.
// This follows the AWS S3 multipart checksum specification.
// Returns an error if a part is missing its checksum (the upload was initiated with
// a checksum algorithm, so all parts must have been uploaded with checksums).
func computeCompositeChecksum(checksumHeaderName string, partEntries map[int][]*filer_pb.Entry, completedPartNumbers []int) (string, error) {
	// Determine the algorithm from the header name
	algo := checksumAlgorithmFromHeaderName(checksumHeaderName)
	if algo == ChecksumAlgorithmNone {
		return "", fmt.Errorf("unknown checksum algorithm for header %q", checksumHeaderName)
	}

	// Collect raw per-part checksums
	var combined []byte
	for _, partNumber := range completedPartNumbers {
		raw, _, err := decodePartChecksum(partNumber, partEntries[partNumber], checksumHeaderName)
		if err != nil {
			return "", err
		}
		combined = append(combined, raw...)
	}

	// Hash the concatenated raw checksums
	h := getCheckSumWriter(algo)
	if h == nil {
		return "", fmt.Errorf("failed to create hash writer for %s", checksumHeaderName)
	}
	h.Write(combined)
	compositeRaw := h.Sum(nil)
	return fmt.Sprintf("%s-%d", base64.StdEncoding.EncodeToString(compositeRaw), len(completedPartNumbers)), nil
}

// computeFullObjectChecksum computes a FULL_OBJECT checksum from per-part checksums
// by combining the per-part CRCs into the CRC of the whole object. The result is the
// base64-encoded whole-object checksum with NO "-N" suffix (a full-object checksum
// is indistinguishable from a single-part upload's checksum). This is required for
// CRC64NVME, which AWS only supports as a full-object checksum.
func computeFullObjectChecksum(checksumHeaderName string, partEntries map[int][]*filer_pb.Entry, completedPartNumbers []int) (string, error) {
	algo := checksumAlgorithmFromHeaderName(checksumHeaderName)
	params, ok := crcCombineParams[algo]
	if !ok {
		return "", fmt.Errorf("full object checksum not supported for %s", checksumHeaderName)
	}

	checksumBytes := int(params.width / 8)

	var combined uint64
	for i, partNumber := range completedPartNumbers {
		raw, entry, err := decodePartChecksum(partNumber, partEntries[partNumber], checksumHeaderName)
		if err != nil {
			return "", err
		}
		if len(raw) != checksumBytes {
			return "", fmt.Errorf("part %d checksum has unexpected length %d for %s", partNumber, len(raw), checksumHeaderName)
		}

		crc := util.BytesToUint64(raw)

		if i == 0 {
			combined = crc
		} else {
			partLen := filer.FileSize(entry)
			combined = combineCRC(combined, crc, partLen, params)
		}
	}

	// Write the low checksumBytes bytes of the combined CRC big-endian. We can't
	// use util.Uint64toBytes here because it unconditionally writes 8 bytes, which
	// would panic for narrower CRCs (e.g. 4-byte CRC32/CRC32C).
	out := make([]byte, checksumBytes)
	for i := 0; i < checksumBytes; i++ {
		out[checksumBytes-1-i] = byte(combined >> (i * 8))
	}

	return base64.StdEncoding.EncodeToString(out), nil
}

// checksumAlgorithmFromHeaderName maps a canonical header name back to its algorithm.
func checksumAlgorithmFromHeaderName(headerName string) ChecksumAlgorithm {
	for _, entry := range checksumHeaders {
		if entry.name == headerName {
			return entry.alg
		}
	}
	return ChecksumAlgorithmNone
}

func checksumAlgorithmNameFromHeaderName(headerName string) string {
	for name, entry := range checksumAlgorithmMapping {
		if entry.name == headerName {
			return name
		}
	}
	return ""
}

func getEtagFromEntry(entry *filer_pb.Entry) string {
	if entry.Extended != nil {
		if etagBytes, ok := entry.Extended[s3_constants.ExtETagKey]; ok {
			etag := string(etagBytes)
			if len(etag) > 0 {
				if !strings.HasPrefix(etag, "\"") {
					return "\"" + etag + "\""
				}
				return etag
			}
			// Empty stored ETag — fall through to filer.ETag calculation
		}
	}
	// Fallback to filer.ETag which handles Attributes.Md5 consistently
	etag := filer.ETag(entry)
	entryName := entry.Name
	if entryName == "" {
		entryName = "entry"
	}
	glog.V(4).Infof("getEtagFromEntry: fallback to filer.ETag for %s: %s, chunkCount: %d", entryName, etag, len(entry.Chunks))
	return "\"" + etag + "\""
}

func validateCompletePartETag(partETag string, entry *filer_pb.Entry) (match bool, invalid bool, normalizedPartETag string, normalizedEntryETag string) {
	normalizedPartETag = strings.Trim(strings.TrimSpace(partETag), `"`)
	if normalizedPartETag == "" {
		return false, true, normalizedPartETag, ""
	}

	normalizedEntryETag = strings.Trim(getEtagFromEntry(entry), `"`)
	if normalizedEntryETag == "" {
		return false, true, normalizedPartETag, normalizedEntryETag
	}

	return normalizedPartETag == normalizedEntryETag, false, normalizedPartETag, normalizedEntryETag
}

type checksumTypeSupport struct {
	composite  bool
	fullObject bool
}

var checksumTypeSupportByAlgo = map[ChecksumAlgorithm]checksumTypeSupport{
	ChecksumAlgorithmCRC32:     {composite: true, fullObject: true},
	ChecksumAlgorithmCRC32C:    {composite: true, fullObject: true},
	ChecksumAlgorithmCRC64NVMe: {fullObject: true},
	ChecksumAlgorithmSHA1:      {composite: true}, // Technically, fullObject could be supported, but is not yet implemented
	ChecksumAlgorithmSHA256:    {composite: true}, // Technically, fullObject could be supported, but is not yet implemented
}

func resolveMultipartChecksumType(algo ChecksumAlgorithm, requested string) (string, error) {
	support, ok := checksumTypeSupportByAlgo[algo]
	if !ok {
		return "", fmt.Errorf("unsupported checksum algorithm %v", algo)
	}

	switch strings.ToUpper(strings.TrimSpace(requested)) {
	case "":
		if support.composite {
			return s3_constants.ChecksumTypeComposite, nil
		}
		return s3_constants.ChecksumTypeFullObject, nil
	case s3_constants.ChecksumTypeComposite:
		if !support.composite {
			return "", fmt.Errorf("checksum algorithm %v does not support %s checksums", algo, s3_constants.ChecksumTypeComposite)
		}
		return s3_constants.ChecksumTypeComposite, nil
	case s3_constants.ChecksumTypeFullObject:
		if !support.fullObject {
			return "", fmt.Errorf("checksum algorithm %v does not support %s checksums", algo, s3_constants.ChecksumTypeFullObject)
		}
		return s3_constants.ChecksumTypeFullObject, nil
	default:
		return "", fmt.Errorf("invalid checksum type %q", requested)
	}
}
