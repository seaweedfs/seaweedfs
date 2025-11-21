package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
)

// executeUnifiedCopyStrategy executes the appropriate copy strategy based on encryption state
// Returns chunks and destination metadata that should be applied to the destination entry
func (s3a *S3ApiServer) executeUnifiedCopyStrategy(entry *filer_pb.Entry, r *http.Request, dstBucket, srcObject, dstObject string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	// Detect encryption state (using entry-aware detection for multipart objects)
	srcPath := fmt.Sprintf("/%s/%s", r.Header.Get("X-Amz-Copy-Source-Bucket"), srcObject)
	dstPath := fmt.Sprintf("/%s/%s", dstBucket, dstObject)
	state := DetectEncryptionStateWithEntry(entry, r, srcPath, dstPath)

	// Debug logging for encryption state

	// Apply bucket default encryption if no explicit encryption specified
	if !state.IsTargetEncrypted() {
		bucketMetadata, err := s3a.getBucketMetadata(dstBucket)
		if err == nil && bucketMetadata != nil && bucketMetadata.Encryption != nil {
			switch bucketMetadata.Encryption.SseAlgorithm {
			case "aws:kms":
				state.DstSSEKMS = true
			case "AES256":
				state.DstSSES3 = true
			}
		}
	}

	// Determine copy strategy
	strategy, err := DetermineUnifiedCopyStrategy(state, entry.Extended, r)
	if err != nil {
		return nil, nil, err
	}

	glog.V(2).Infof("Unified copy strategy for %s → %s: %v", srcPath, dstPath, strategy)

	// Calculate optimized sizes for the strategy
	sizeCalc := CalculateOptimizedSizes(entry, r, strategy)
	glog.V(2).Infof("Size calculation: src=%d, target=%d, actual=%d, overhead=%d, preallocate=%v",
		sizeCalc.SourceSize, sizeCalc.TargetSize, sizeCalc.ActualContentSize,
		sizeCalc.EncryptionOverhead, sizeCalc.CanPreallocate)

	// Execute strategy
	switch strategy {
	case CopyStrategyDirect:
		chunks, err := s3a.copyChunks(entry, dstPath)
		return chunks, nil, err

	case CopyStrategyKeyRotation:
		return s3a.executeKeyRotation(entry, r, state)

	case CopyStrategyEncrypt:
		return s3a.executeEncryptCopy(entry, r, state, dstBucket, dstPath)

	case CopyStrategyDecrypt:
		return s3a.executeDecryptCopy(entry, r, state, dstPath)

	case CopyStrategyReencrypt:
		return s3a.executeReencryptCopy(entry, r, state, dstBucket, dstPath)

	default:
		return nil, nil, fmt.Errorf("unknown unified copy strategy: %v", strategy)
	}
}

// mapCopyErrorToS3Error maps various copy errors to appropriate S3 error codes
func (s3a *S3ApiServer) mapCopyErrorToS3Error(err error) s3err.ErrorCode {
	if err == nil {
		return s3err.ErrNone
	}

	// Check for read-only errors (quota enforcement)
	// Uses errors.Is() to properly detect wrapped errors
	if errors.Is(err, weed_server.ErrReadOnly) {
		// Bucket is read-only due to quota enforcement or other configuration
		// Return 403 Forbidden per S3 semantics (similar to MinIO's quota enforcement)
		return s3err.ErrAccessDenied
	}

	// Check for KMS errors first
	if kmsErr := MapKMSErrorToS3Error(err); kmsErr != s3err.ErrInvalidRequest {
		return kmsErr
	}

	// Check for SSE-C errors
	if ssecErr := MapSSECErrorToS3Error(err); ssecErr != s3err.ErrInvalidRequest {
		return ssecErr
	}

	// Default to internal error for unknown errors
	return s3err.ErrInternalError
}

// executeKeyRotation handles key rotation for same-object copies
func (s3a *S3ApiServer) executeKeyRotation(entry *filer_pb.Entry, r *http.Request, state *EncryptionState) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	// For key rotation, we only need to update metadata, not re-copy chunks
	// This is a significant optimization for same-object key changes

	if state.SrcSSEC && state.DstSSEC {
		// SSE-C key rotation - need to handle new key/IV, use reencrypt logic
		return s3a.executeReencryptCopy(entry, r, state, "", "")
	}

	if state.SrcSSEKMS && state.DstSSEKMS {
		// SSE-KMS key rotation - return existing chunks, metadata will be updated by caller
		return entry.GetChunks(), nil, nil
	}

	// Fallback to reencrypt if we can't do metadata-only rotation
	return s3a.executeReencryptCopy(entry, r, state, "", "")
}

// executeEncryptCopy handles plain → encrypted copies
func (s3a *S3ApiServer) executeEncryptCopy(entry *filer_pb.Entry, r *http.Request, state *EncryptionState, dstBucket, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	if state.DstSSEC {
		// Use existing SSE-C copy logic
		return s3a.copyChunksWithSSEC(entry, r)
	}

	if state.DstSSEKMS {
		// Use existing SSE-KMS copy logic - metadata is now generated internally
		chunks, dstMetadata, err := s3a.copyChunksWithSSEKMS(entry, r, dstBucket)
		return chunks, dstMetadata, err
	}

	if state.DstSSES3 {
		// Use streaming copy for SSE-S3 encryption
		chunks, err := s3a.executeStreamingReencryptCopy(entry, r, state, dstPath)
		return chunks, nil, err
	}

	return nil, nil, fmt.Errorf("unknown target encryption type")
}

// executeDecryptCopy handles encrypted → plain copies
func (s3a *S3ApiServer) executeDecryptCopy(entry *filer_pb.Entry, r *http.Request, state *EncryptionState, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	// Use unified multipart-aware decrypt copy for all encryption types
	if state.SrcSSEC || state.SrcSSEKMS {
		glog.V(2).Infof("Encrypted→Plain copy: using unified multipart decrypt copy")
		return s3a.copyMultipartCrossEncryption(entry, r, state, "", dstPath)
	}

	if state.SrcSSES3 {
		// Use streaming copy for SSE-S3 decryption
		chunks, err := s3a.executeStreamingReencryptCopy(entry, r, state, dstPath)
		return chunks, nil, err
	}

	return nil, nil, fmt.Errorf("unknown source encryption type")
}

// executeReencryptCopy handles encrypted → encrypted copies with different keys/methods
func (s3a *S3ApiServer) executeReencryptCopy(entry *filer_pb.Entry, r *http.Request, state *EncryptionState, dstBucket, dstPath string) ([]*filer_pb.FileChunk, map[string][]byte, error) {
	// Check if we should use streaming copy for better performance
	if s3a.shouldUseStreamingCopy(entry, state) {
		chunks, err := s3a.executeStreamingReencryptCopy(entry, r, state, dstPath)
		return chunks, nil, err
	}

	// Fallback to chunk-by-chunk approach for compatibility
	if state.SrcSSEC && state.DstSSEC {
		return s3a.copyChunksWithSSEC(entry, r)
	}

	if state.SrcSSEKMS && state.DstSSEKMS {
		// Use existing SSE-KMS copy logic - metadata is now generated internally
		chunks, dstMetadata, err := s3a.copyChunksWithSSEKMS(entry, r, dstBucket)
		return chunks, dstMetadata, err
	}

	if state.SrcSSEC && state.DstSSEKMS {
		// SSE-C → SSE-KMS: use unified multipart-aware cross-encryption copy
		glog.V(2).Infof("SSE-C→SSE-KMS cross-encryption copy: using unified multipart copy")
		return s3a.copyMultipartCrossEncryption(entry, r, state, dstBucket, dstPath)
	}

	if state.SrcSSEKMS && state.DstSSEC {
		// SSE-KMS → SSE-C: use unified multipart-aware cross-encryption copy
		glog.V(2).Infof("SSE-KMS→SSE-C cross-encryption copy: using unified multipart copy")
		return s3a.copyMultipartCrossEncryption(entry, r, state, dstBucket, dstPath)
	}

	// Handle SSE-S3 cross-encryption scenarios
	if state.SrcSSES3 || state.DstSSES3 {
		// Any scenario involving SSE-S3 uses streaming copy
		chunks, err := s3a.executeStreamingReencryptCopy(entry, r, state, dstPath)
		return chunks, nil, err
	}

	return nil, nil, fmt.Errorf("unsupported cross-encryption scenario")
}

// shouldUseStreamingCopy determines if streaming copy should be used
func (s3a *S3ApiServer) shouldUseStreamingCopy(entry *filer_pb.Entry, state *EncryptionState) bool {
	// Use streaming copy for large files or when beneficial
	fileSize := entry.Attributes.FileSize

	// Use streaming for files larger than 10MB
	if fileSize > 10*1024*1024 {
		return true
	}

	// Check if this is a multipart encrypted object
	isMultipartEncrypted := false
	if state.IsSourceEncrypted() {
		encryptedChunks := 0
		for _, chunk := range entry.GetChunks() {
			if chunk.GetSseType() != filer_pb.SSEType_NONE {
				encryptedChunks++
			}
		}
		isMultipartEncrypted = encryptedChunks > 1
	}

	// For multipart encrypted objects, avoid streaming copy to use per-chunk metadata approach
	if isMultipartEncrypted {
		glog.V(3).Infof("Multipart encrypted object detected, using chunk-by-chunk approach")
		return false
	}

	// Use streaming for cross-encryption scenarios (for single-part objects only)
	if state.IsSourceEncrypted() && state.IsTargetEncrypted() {
		srcType := s3a.getEncryptionTypeString(state.SrcSSEC, state.SrcSSEKMS, state.SrcSSES3)
		dstType := s3a.getEncryptionTypeString(state.DstSSEC, state.DstSSEKMS, state.DstSSES3)
		if srcType != dstType {
			return true
		}
	}

	// Use streaming for compressed files
	if isCompressedEntry(entry) {
		return true
	}

	// Use streaming for SSE-S3 scenarios (always)
	if state.SrcSSES3 || state.DstSSES3 {
		return true
	}

	return false
}

// executeStreamingReencryptCopy performs streaming re-encryption copy
func (s3a *S3ApiServer) executeStreamingReencryptCopy(entry *filer_pb.Entry, r *http.Request, state *EncryptionState, dstPath string) ([]*filer_pb.FileChunk, error) {
	// Create streaming copy manager
	streamingManager := NewStreamingCopyManager(s3a)

	// Execute streaming copy
	return streamingManager.ExecuteStreamingCopy(context.Background(), entry, r, dstPath, state)
}
