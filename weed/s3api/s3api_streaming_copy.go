package s3api

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// StreamingCopySpec defines the specification for streaming copy operations
type StreamingCopySpec struct {
	SourceReader    io.Reader
	TargetSize      int64
	EncryptionSpec  *EncryptionSpec
	CompressionSpec *CompressionSpec
	HashCalculation bool
	BufferSize      int
}

// EncryptionSpec defines encryption parameters for streaming
type EncryptionSpec struct {
	NeedsDecryption bool
	NeedsEncryption bool
	SourceKey       interface{} // SSECustomerKey or SSEKMSKey
	DestinationKey  interface{} // SSECustomerKey or SSEKMSKey
	SourceType      EncryptionType
	DestinationType EncryptionType
	SourceMetadata  map[string][]byte // Source metadata for IV extraction
	DestinationIV   []byte            // Generated IV for destination
}

// CompressionSpec defines compression parameters for streaming
type CompressionSpec struct {
	IsCompressed       bool
	CompressionType    string
	NeedsDecompression bool
	NeedsCompression   bool
}

// StreamingCopyManager handles streaming copy operations
type StreamingCopyManager struct {
	s3a        *S3ApiServer
	bufferSize int
}

// NewStreamingCopyManager creates a new streaming copy manager
func NewStreamingCopyManager(s3a *S3ApiServer) *StreamingCopyManager {
	return &StreamingCopyManager{
		s3a:        s3a,
		bufferSize: 64 * 1024, // 64KB default buffer
	}
}

// ExecuteStreamingCopy performs a streaming copy operation
func (scm *StreamingCopyManager) ExecuteStreamingCopy(ctx context.Context, entry *filer_pb.Entry, r *http.Request, dstPath string, state *EncryptionState) ([]*filer_pb.FileChunk, error) {
	// Create streaming copy specification
	spec, err := scm.createStreamingSpec(entry, r, state)
	if err != nil {
		return nil, fmt.Errorf("create streaming spec: %w", err)
	}

	// Create source reader from entry
	sourceReader, err := scm.createSourceReader(entry)
	if err != nil {
		return nil, fmt.Errorf("create source reader: %w", err)
	}
	defer sourceReader.Close()

	spec.SourceReader = sourceReader

	// Create processing pipeline
	processedReader, err := scm.createProcessingPipeline(spec)
	if err != nil {
		return nil, fmt.Errorf("create processing pipeline: %w", err)
	}

	// Stream to destination
	return scm.streamToDestination(ctx, processedReader, spec, dstPath)
}

// createStreamingSpec creates a streaming specification based on copy parameters
func (scm *StreamingCopyManager) createStreamingSpec(entry *filer_pb.Entry, r *http.Request, state *EncryptionState) (*StreamingCopySpec, error) {
	spec := &StreamingCopySpec{
		BufferSize:      scm.bufferSize,
		HashCalculation: true,
	}

	// Calculate target size
	sizeCalc := NewCopySizeCalculator(entry, r)
	spec.TargetSize = sizeCalc.CalculateTargetSize()

	// Create encryption specification
	encSpec, err := scm.createEncryptionSpec(entry, r, state)
	if err != nil {
		return nil, err
	}
	spec.EncryptionSpec = encSpec

	// Create compression specification
	spec.CompressionSpec = scm.createCompressionSpec(entry, r)

	return spec, nil
}

// createEncryptionSpec creates encryption specification for streaming
func (scm *StreamingCopyManager) createEncryptionSpec(entry *filer_pb.Entry, r *http.Request, state *EncryptionState) (*EncryptionSpec, error) {
	spec := &EncryptionSpec{
		NeedsDecryption: state.IsSourceEncrypted(),
		NeedsEncryption: state.IsTargetEncrypted(),
		SourceMetadata:  entry.Extended, // Pass source metadata for IV extraction
	}

	// Set source encryption details
	if state.SrcSSEC {
		spec.SourceType = EncryptionTypeSSEC
		sourceKey, err := ParseSSECCopySourceHeaders(r)
		if err != nil {
			return nil, fmt.Errorf("parse SSE-C copy source headers: %w", err)
		}
		spec.SourceKey = sourceKey
	} else if state.SrcSSEKMS {
		spec.SourceType = EncryptionTypeSSEKMS
		// Extract SSE-KMS key from metadata
		if keyData, exists := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]; exists {
			sseKey, err := DeserializeSSEKMSMetadata(keyData)
			if err != nil {
				return nil, fmt.Errorf("deserialize SSE-KMS metadata: %w", err)
			}
			spec.SourceKey = sseKey
		}
	} else if state.SrcSSES3 {
		spec.SourceType = EncryptionTypeSSES3
		// Extract SSE-S3 key from metadata
		if keyData, exists := entry.Extended[s3_constants.SeaweedFSSSES3Key]; exists {
			keyManager := GetSSES3KeyManager()
			sseKey, err := DeserializeSSES3Metadata(keyData, keyManager)
			if err != nil {
				return nil, fmt.Errorf("deserialize SSE-S3 metadata: %w", err)
			}
			spec.SourceKey = sseKey
		}
	}

	// Set destination encryption details
	if state.DstSSEC {
		spec.DestinationType = EncryptionTypeSSEC
		destKey, err := ParseSSECHeaders(r)
		if err != nil {
			return nil, fmt.Errorf("parse SSE-C headers: %w", err)
		}
		spec.DestinationKey = destKey
	} else if state.DstSSEKMS {
		spec.DestinationType = EncryptionTypeSSEKMS
		// Parse KMS parameters
		keyID, encryptionContext, bucketKeyEnabled, err := ParseSSEKMSCopyHeaders(r)
		if err != nil {
			return nil, fmt.Errorf("parse SSE-KMS copy headers: %w", err)
		}

		// Create SSE-KMS key for destination
		sseKey := &SSEKMSKey{
			KeyID:             keyID,
			EncryptionContext: encryptionContext,
			BucketKeyEnabled:  bucketKeyEnabled,
		}
		spec.DestinationKey = sseKey
	} else if state.DstSSES3 {
		spec.DestinationType = EncryptionTypeSSES3
		// Generate or retrieve SSE-S3 key
		keyManager := GetSSES3KeyManager()
		sseKey, err := keyManager.GetOrCreateKey("")
		if err != nil {
			return nil, fmt.Errorf("get SSE-S3 key: %w", err)
		}
		spec.DestinationKey = sseKey
	}

	return spec, nil
}

// createCompressionSpec creates compression specification for streaming
func (scm *StreamingCopyManager) createCompressionSpec(entry *filer_pb.Entry, r *http.Request) *CompressionSpec {
	return &CompressionSpec{
		IsCompressed: isCompressedEntry(entry),
		// For now, we don't change compression during copy
		NeedsDecompression: false,
		NeedsCompression:   false,
	}
}

// createSourceReader creates a reader for the source entry
func (scm *StreamingCopyManager) createSourceReader(entry *filer_pb.Entry) (io.ReadCloser, error) {
	// Create a multi-chunk reader that streams from all chunks
	return scm.s3a.createMultiChunkReader(entry)
}

// createProcessingPipeline creates a processing pipeline for the copy operation
func (scm *StreamingCopyManager) createProcessingPipeline(spec *StreamingCopySpec) (io.Reader, error) {
	reader := spec.SourceReader

	// Add decryption if needed
	if spec.EncryptionSpec.NeedsDecryption {
		decryptedReader, err := scm.createDecryptionReader(reader, spec.EncryptionSpec)
		if err != nil {
			return nil, fmt.Errorf("create decryption reader: %w", err)
		}
		reader = decryptedReader
	}

	// Add decompression if needed
	if spec.CompressionSpec.NeedsDecompression {
		decompressedReader, err := scm.createDecompressionReader(reader, spec.CompressionSpec)
		if err != nil {
			return nil, fmt.Errorf("create decompression reader: %w", err)
		}
		reader = decompressedReader
	}

	// Add compression if needed
	if spec.CompressionSpec.NeedsCompression {
		compressedReader, err := scm.createCompressionReader(reader, spec.CompressionSpec)
		if err != nil {
			return nil, fmt.Errorf("create compression reader: %w", err)
		}
		reader = compressedReader
	}

	// Add encryption if needed
	if spec.EncryptionSpec.NeedsEncryption {
		encryptedReader, err := scm.createEncryptionReader(reader, spec.EncryptionSpec)
		if err != nil {
			return nil, fmt.Errorf("create encryption reader: %w", err)
		}
		reader = encryptedReader
	}

	// Add hash calculation if needed
	if spec.HashCalculation {
		reader = scm.createHashReader(reader)
	}

	return reader, nil
}

// createDecryptionReader creates a decryption reader based on encryption type
func (scm *StreamingCopyManager) createDecryptionReader(reader io.Reader, encSpec *EncryptionSpec) (io.Reader, error) {
	switch encSpec.SourceType {
	case EncryptionTypeSSEC:
		if sourceKey, ok := encSpec.SourceKey.(*SSECustomerKey); ok {
			// Get IV from metadata
			iv, err := GetIVFromMetadata(encSpec.SourceMetadata)
			if err != nil {
				return nil, fmt.Errorf("get IV from metadata: %w", err)
			}
			return CreateSSECDecryptedReader(reader, sourceKey, iv)
		}
		return nil, fmt.Errorf("invalid SSE-C source key type")

	case EncryptionTypeSSEKMS:
		if sseKey, ok := encSpec.SourceKey.(*SSEKMSKey); ok {
			return CreateSSEKMSDecryptedReader(reader, sseKey)
		}
		return nil, fmt.Errorf("invalid SSE-KMS source key type")

	case EncryptionTypeSSES3:
		if sseKey, ok := encSpec.SourceKey.(*SSES3Key); ok {
			// Get IV from metadata
			iv, err := GetIVFromMetadata(encSpec.SourceMetadata)
			if err != nil {
				return nil, fmt.Errorf("get IV from metadata: %w", err)
			}
			return CreateSSES3DecryptedReader(reader, sseKey, iv)
		}
		return nil, fmt.Errorf("invalid SSE-S3 source key type")

	default:
		return reader, nil
	}
}

// createEncryptionReader creates an encryption reader based on encryption type
func (scm *StreamingCopyManager) createEncryptionReader(reader io.Reader, encSpec *EncryptionSpec) (io.Reader, error) {
	switch encSpec.DestinationType {
	case EncryptionTypeSSEC:
		if destKey, ok := encSpec.DestinationKey.(*SSECustomerKey); ok {
			encryptedReader, iv, err := CreateSSECEncryptedReader(reader, destKey)
			if err != nil {
				return nil, err
			}
			// Store IV in destination metadata (this would need to be handled by caller)
			encSpec.DestinationIV = iv
			return encryptedReader, nil
		}
		return nil, fmt.Errorf("invalid SSE-C destination key type")

	case EncryptionTypeSSEKMS:
		if sseKey, ok := encSpec.DestinationKey.(*SSEKMSKey); ok {
			encryptedReader, updatedKey, err := CreateSSEKMSEncryptedReaderWithBucketKey(reader, sseKey.KeyID, sseKey.EncryptionContext, sseKey.BucketKeyEnabled)
			if err != nil {
				return nil, err
			}
			// Store IV from the updated key
			encSpec.DestinationIV = updatedKey.IV
			return encryptedReader, nil
		}
		return nil, fmt.Errorf("invalid SSE-KMS destination key type")

	case EncryptionTypeSSES3:
		if sseKey, ok := encSpec.DestinationKey.(*SSES3Key); ok {
			encryptedReader, iv, err := CreateSSES3EncryptedReader(reader, sseKey)
			if err != nil {
				return nil, err
			}
			// Store IV for metadata
			encSpec.DestinationIV = iv
			return encryptedReader, nil
		}
		return nil, fmt.Errorf("invalid SSE-S3 destination key type")

	default:
		return reader, nil
	}
}

// createDecompressionReader creates a decompression reader
func (scm *StreamingCopyManager) createDecompressionReader(reader io.Reader, compSpec *CompressionSpec) (io.Reader, error) {
	if !compSpec.NeedsDecompression {
		return reader, nil
	}

	switch compSpec.CompressionType {
	case "gzip":
		// Use SeaweedFS's streaming gzip decompression
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			_, err := util.GunzipStream(pw, reader)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("gzip decompression failed: %v", err))
			}
		}()
		return pr, nil
	default:
		// Unknown compression type, return as-is
		return reader, nil
	}
}

// createCompressionReader creates a compression reader
func (scm *StreamingCopyManager) createCompressionReader(reader io.Reader, compSpec *CompressionSpec) (io.Reader, error) {
	if !compSpec.NeedsCompression {
		return reader, nil
	}

	switch compSpec.CompressionType {
	case "gzip":
		// Use SeaweedFS's streaming gzip compression
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			_, err := util.GzipStream(pw, reader)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("gzip compression failed: %v", err))
			}
		}()
		return pr, nil
	default:
		// Unknown compression type, return as-is
		return reader, nil
	}
}

// HashReader wraps an io.Reader to calculate MD5 and SHA256 hashes
type HashReader struct {
	reader     io.Reader
	md5Hash    hash.Hash
	sha256Hash hash.Hash
}

// NewHashReader creates a new hash calculating reader
func NewHashReader(reader io.Reader) *HashReader {
	return &HashReader{
		reader:     reader,
		md5Hash:    md5.New(),
		sha256Hash: sha256.New(),
	}
}

// Read implements io.Reader and calculates hashes as data flows through
func (hr *HashReader) Read(p []byte) (n int, err error) {
	n, err = hr.reader.Read(p)
	if n > 0 {
		// Update both hashes with the data read
		hr.md5Hash.Write(p[:n])
		hr.sha256Hash.Write(p[:n])
	}
	return n, err
}

// MD5Sum returns the current MD5 hash
func (hr *HashReader) MD5Sum() []byte {
	return hr.md5Hash.Sum(nil)
}

// SHA256Sum returns the current SHA256 hash
func (hr *HashReader) SHA256Sum() []byte {
	return hr.sha256Hash.Sum(nil)
}

// MD5Hex returns the MD5 hash as a hex string
func (hr *HashReader) MD5Hex() string {
	return hex.EncodeToString(hr.MD5Sum())
}

// SHA256Hex returns the SHA256 hash as a hex string
func (hr *HashReader) SHA256Hex() string {
	return hex.EncodeToString(hr.SHA256Sum())
}

// createHashReader creates a hash calculation reader
func (scm *StreamingCopyManager) createHashReader(reader io.Reader) io.Reader {
	return NewHashReader(reader)
}

// streamToDestination streams the processed data to the destination
func (scm *StreamingCopyManager) streamToDestination(ctx context.Context, reader io.Reader, spec *StreamingCopySpec, dstPath string) ([]*filer_pb.FileChunk, error) {
	// For now, we'll use the existing chunk-based approach
	// In a full implementation, this would stream directly to the destination
	// without creating intermediate chunks

	// This is a placeholder that converts back to chunk-based approach
	// A full streaming implementation would write directly to the destination
	return scm.streamToChunks(ctx, reader, spec, dstPath)
}

// streamToChunks converts streaming data back to chunks (temporary implementation)
func (scm *StreamingCopyManager) streamToChunks(ctx context.Context, reader io.Reader, spec *StreamingCopySpec, dstPath string) ([]*filer_pb.FileChunk, error) {
	// This is a simplified implementation that reads the stream and creates chunks
	// A full implementation would be more sophisticated

	var chunks []*filer_pb.FileChunk
	buffer := make([]byte, spec.BufferSize)
	offset := int64(0)

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			// Create chunk for this data
			chunk, chunkErr := scm.createChunkFromData(buffer[:n], offset, dstPath)
			if chunkErr != nil {
				return nil, fmt.Errorf("create chunk from data: %w", chunkErr)
			}
			chunks = append(chunks, chunk)
			offset += int64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read stream: %w", err)
		}
	}

	return chunks, nil
}

// createChunkFromData creates a chunk from streaming data
func (scm *StreamingCopyManager) createChunkFromData(data []byte, offset int64, dstPath string) (*filer_pb.FileChunk, error) {
	// Assign new volume
	assignResult, err := scm.s3a.assignNewVolume(dstPath)
	if err != nil {
		return nil, fmt.Errorf("assign volume: %w", err)
	}

	// Create chunk
	chunk := &filer_pb.FileChunk{
		Offset: offset,
		Size:   uint64(len(data)),
	}

	// Set file ID
	if err := scm.s3a.setChunkFileId(chunk, assignResult); err != nil {
		return nil, err
	}

	// Upload data
	if err := scm.s3a.uploadChunkData(data, assignResult); err != nil {
		return nil, fmt.Errorf("upload chunk data: %w", err)
	}

	return chunk, nil
}

// createMultiChunkReader creates a reader that streams from multiple chunks
func (s3a *S3ApiServer) createMultiChunkReader(entry *filer_pb.Entry) (io.ReadCloser, error) {
	// Create a multi-reader that combines all chunks
	var readers []io.Reader

	for _, chunk := range entry.GetChunks() {
		chunkReader, err := s3a.createChunkReader(chunk)
		if err != nil {
			return nil, fmt.Errorf("create chunk reader: %w", err)
		}
		readers = append(readers, chunkReader)
	}

	multiReader := io.MultiReader(readers...)
	return &multiReadCloser{reader: multiReader}, nil
}

// createChunkReader creates a reader for a single chunk
func (s3a *S3ApiServer) createChunkReader(chunk *filer_pb.FileChunk) (io.Reader, error) {
	// Get chunk URL
	srcUrl, err := s3a.lookupVolumeUrl(chunk.GetFileIdString())
	if err != nil {
		return nil, fmt.Errorf("lookup volume URL: %w", err)
	}

	// Create HTTP request for chunk data
	req, err := http.NewRequest("GET", srcUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}

	// Execute request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute HTTP request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP request failed: %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// multiReadCloser wraps a multi-reader with a close method
type multiReadCloser struct {
	reader io.Reader
}

func (mrc *multiReadCloser) Read(p []byte) (int, error) {
	return mrc.reader.Read(p)
}

func (mrc *multiReadCloser) Close() error {
	return nil
}
