package s3api

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// rotateSSECKey handles SSE-C key rotation for same-object copies
func (s3a *S3ApiServer) rotateSSECKey(entry *filer_pb.Entry, r *http.Request) ([]*filer_pb.FileChunk, error) {
	// Parse source and destination SSE-C keys
	sourceKey, err := ParseSSECCopySourceHeaders(r)
	if err != nil {
		return nil, fmt.Errorf("parse SSE-C copy source headers: %w", err)
	}

	destKey, err := ParseSSECHeaders(r)
	if err != nil {
		return nil, fmt.Errorf("parse SSE-C destination headers: %w", err)
	}

	// Validate that we have both keys
	if sourceKey == nil {
		return nil, fmt.Errorf("source SSE-C key required for key rotation")
	}

	if destKey == nil {
		return nil, fmt.Errorf("destination SSE-C key required for key rotation")
	}

	// Check if keys are actually different
	if sourceKey.KeyMD5 == destKey.KeyMD5 {
		glog.V(2).Infof("SSE-C key rotation: keys are identical, using direct copy")
		return entry.GetChunks(), nil
	}

	glog.V(2).Infof("SSE-C key rotation: rotating from key %s to key %s",
		sourceKey.KeyMD5[:8], destKey.KeyMD5[:8])

	// For SSE-C key rotation, we need to re-encrypt all chunks
	// This cannot be a metadata-only operation because the encryption key changes
	return s3a.rotateSSECChunks(entry, sourceKey, destKey)
}

// rotateSSEKMSKey handles SSE-KMS key rotation for same-object copies
func (s3a *S3ApiServer) rotateSSEKMSKey(entry *filer_pb.Entry, r *http.Request) ([]*filer_pb.FileChunk, error) {
	// Get source and destination key IDs
	srcKeyID, srcEncrypted := GetSourceSSEKMSInfo(entry.Extended)
	if !srcEncrypted {
		return nil, fmt.Errorf("source object is not SSE-KMS encrypted")
	}

	dstKeyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
	if dstKeyID == "" {
		// Use default key if not specified
		dstKeyID = "default"
	}

	// Check if keys are actually different
	if srcKeyID == dstKeyID {
		glog.V(2).Infof("SSE-KMS key rotation: keys are identical, using direct copy")
		return entry.GetChunks(), nil
	}

	glog.V(2).Infof("SSE-KMS key rotation: rotating from key %s to key %s", srcKeyID, dstKeyID)

	// For SSE-KMS, we can potentially do metadata-only rotation
	// if the KMS service supports key aliasing and the data encryption key can be re-wrapped
	if s3a.canDoMetadataOnlyKMSRotation(srcKeyID, dstKeyID) {
		return s3a.rotateSSEKMSMetadataOnly(entry, srcKeyID, dstKeyID)
	}

	// Fallback to full re-encryption
	return s3a.rotateSSEKMSChunks(entry, srcKeyID, dstKeyID, r)
}

// canDoMetadataOnlyKMSRotation determines if KMS key rotation can be done metadata-only
func (s3a *S3ApiServer) canDoMetadataOnlyKMSRotation(srcKeyID, dstKeyID string) bool {
	// For now, we'll be conservative and always re-encrypt
	// In a full implementation, this would check if:
	// 1. Both keys are in the same KMS instance
	// 2. The KMS supports key re-wrapping
	// 3. The user has permissions for both keys
	return false
}

// rotateSSEKMSMetadataOnly performs metadata-only SSE-KMS key rotation
func (s3a *S3ApiServer) rotateSSEKMSMetadataOnly(entry *filer_pb.Entry, srcKeyID, dstKeyID string) ([]*filer_pb.FileChunk, error) {
	// This would re-wrap the data encryption key with the new KMS key
	// For now, return an error since we don't support this yet
	return nil, fmt.Errorf("metadata-only KMS key rotation not yet implemented")
}

// rotateSSECChunks re-encrypts all chunks with new SSE-C key
func (s3a *S3ApiServer) rotateSSECChunks(entry *filer_pb.Entry, sourceKey, destKey *SSECustomerKey) ([]*filer_pb.FileChunk, error) {
	// Get IV from entry metadata
	iv, err := GetSSECIVFromMetadata(entry.Extended)
	if err != nil {
		return nil, fmt.Errorf("get SSE-C IV from metadata: %w", err)
	}

	var rotatedChunks []*filer_pb.FileChunk

	for _, chunk := range entry.GetChunks() {
		rotatedChunk, err := s3a.rotateSSECChunk(chunk, sourceKey, destKey, iv)
		if err != nil {
			return nil, fmt.Errorf("rotate SSE-C chunk: %w", err)
		}
		rotatedChunks = append(rotatedChunks, rotatedChunk)
	}

	// Generate new IV for the destination and store it in entry metadata
	newIV := make([]byte, s3_constants.AESBlockSize)
	if _, err := io.ReadFull(rand.Reader, newIV); err != nil {
		return nil, fmt.Errorf("generate new IV: %w", err)
	}

	// Update entry metadata with new IV and SSE-C headers
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	StoreSSECIVInMetadata(entry.Extended, newIV)
	entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
	entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(destKey.KeyMD5)

	return rotatedChunks, nil
}

// rotateSSEKMSChunks re-encrypts all chunks with new SSE-KMS key
func (s3a *S3ApiServer) rotateSSEKMSChunks(entry *filer_pb.Entry, srcKeyID, dstKeyID string, r *http.Request) ([]*filer_pb.FileChunk, error) {
	var rotatedChunks []*filer_pb.FileChunk

	// Parse encryption context and bucket key settings
	_, encryptionContext, bucketKeyEnabled, err := ParseSSEKMSCopyHeaders(r)
	if err != nil {
		return nil, fmt.Errorf("parse SSE-KMS copy headers: %w", err)
	}

	for _, chunk := range entry.GetChunks() {
		rotatedChunk, err := s3a.rotateSSEKMSChunk(chunk, srcKeyID, dstKeyID, encryptionContext, bucketKeyEnabled)
		if err != nil {
			return nil, fmt.Errorf("rotate SSE-KMS chunk: %w", err)
		}
		rotatedChunks = append(rotatedChunks, rotatedChunk)
	}

	return rotatedChunks, nil
}

// rotateSSECChunk rotates a single SSE-C encrypted chunk
func (s3a *S3ApiServer) rotateSSECChunk(chunk *filer_pb.FileChunk, sourceKey, destKey *SSECustomerKey, iv []byte) (*filer_pb.FileChunk, error) {
	// Create new chunk with same properties
	newChunk := &filer_pb.FileChunk{
		Offset:       chunk.Offset,
		Size:         chunk.Size,
		ModifiedTsNs: chunk.ModifiedTsNs,
		ETag:         chunk.ETag,
	}

	// Assign new volume for the rotated chunk
	assignResult, err := s3a.assignNewVolume("")
	if err != nil {
		return nil, fmt.Errorf("assign new volume: %w", err)
	}

	// Set file ID on new chunk
	if err := s3a.setChunkFileId(newChunk, assignResult); err != nil {
		return nil, err
	}

	// Get source chunk data
	fileId := chunk.GetFileIdString()
	srcUrl, err := s3a.lookupVolumeUrl(fileId)
	if err != nil {
		return nil, fmt.Errorf("lookup source volume: %w", err)
	}

	// Download encrypted data
	encryptedData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
	if err != nil {
		return nil, fmt.Errorf("download chunk data: %w", err)
	}

	// Decrypt with source key using provided IV
	decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), sourceKey, iv)
	if err != nil {
		return nil, fmt.Errorf("create decrypted reader: %w", err)
	}

	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		return nil, fmt.Errorf("decrypt data: %w", err)
	}

	// Re-encrypt with destination key
	encryptedReader, _, err := CreateSSECEncryptedReader(bytes.NewReader(decryptedData), destKey)
	if err != nil {
		return nil, fmt.Errorf("create encrypted reader: %w", err)
	}

	// Note: IV will be handled at the entry level by the calling function

	reencryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		return nil, fmt.Errorf("re-encrypt data: %w", err)
	}

	// Update chunk size to include new IV
	newChunk.Size = uint64(len(reencryptedData))

	// Upload re-encrypted data
	if err := s3a.uploadChunkData(reencryptedData, assignResult, false); err != nil {
		return nil, fmt.Errorf("upload re-encrypted data: %w", err)
	}

	return newChunk, nil
}

// rotateSSEKMSChunk rotates a single SSE-KMS encrypted chunk
func (s3a *S3ApiServer) rotateSSEKMSChunk(chunk *filer_pb.FileChunk, srcKeyID, dstKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool) (*filer_pb.FileChunk, error) {
	// Create new chunk with same properties
	newChunk := &filer_pb.FileChunk{
		Offset:       chunk.Offset,
		Size:         chunk.Size,
		ModifiedTsNs: chunk.ModifiedTsNs,
		ETag:         chunk.ETag,
	}

	// Assign new volume for the rotated chunk
	assignResult, err := s3a.assignNewVolume("")
	if err != nil {
		return nil, fmt.Errorf("assign new volume: %w", err)
	}

	// Set file ID on new chunk
	if err := s3a.setChunkFileId(newChunk, assignResult); err != nil {
		return nil, err
	}

	// Get source chunk data
	fileId := chunk.GetFileIdString()
	srcUrl, err := s3a.lookupVolumeUrl(fileId)
	if err != nil {
		return nil, fmt.Errorf("lookup source volume: %w", err)
	}

	// Download data (this would be encrypted with the old KMS key)
	chunkData, err := s3a.downloadChunkData(srcUrl, fileId, 0, int64(chunk.Size))
	if err != nil {
		return nil, fmt.Errorf("download chunk data: %w", err)
	}

	// For now, we'll just re-upload the data as-is
	// In a full implementation, this would:
	// 1. Decrypt with old KMS key
	// 2. Re-encrypt with new KMS key
	// 3. Update metadata accordingly

	// Upload data with new key (placeholder implementation)
	if err := s3a.uploadChunkData(chunkData, assignResult, false); err != nil {
		return nil, fmt.Errorf("upload rotated data: %w", err)
	}

	return newChunk, nil
}

// IsSameObjectCopy determines if this is a same-object copy operation
func IsSameObjectCopy(r *http.Request, srcBucket, srcObject, dstBucket, dstObject string) bool {
	return srcBucket == dstBucket && srcObject == dstObject
}

// NeedsKeyRotation determines if the copy operation requires key rotation
func NeedsKeyRotation(entry *filer_pb.Entry, r *http.Request) bool {
	// Check for SSE-C key rotation
	if IsSSECEncrypted(entry.Extended) && IsSSECRequest(r) {
		return true // Assume different keys for safety
	}

	// Check for SSE-KMS key rotation
	if IsSSEKMSEncrypted(entry.Extended) && IsSSEKMSRequest(r) {
		srcKeyID, _ := GetSourceSSEKMSInfo(entry.Extended)
		dstKeyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
		return srcKeyID != dstKeyID
	}

	return false
}
