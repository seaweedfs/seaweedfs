package repl_util

import (
	"bytes"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// MaybeDecryptReader wraps reader with SSE decryption if the entry has encrypted chunks.
// Returns the original reader unchanged if no SSE encryption is detected.
func MaybeDecryptReader(reader io.Reader, entry *filer_pb.Entry) (io.Reader, error) {
	if entry == nil {
		return reader, nil
	}

	sseType := detectSSEType(entry)
	if sseType == filer_pb.SSEType_NONE {
		return reader, nil
	}

	switch sseType {
	case filer_pb.SSEType_SSE_S3:
		return decryptSSES3(reader, entry)
	case filer_pb.SSEType_SSE_KMS:
		return decryptSSEKMS(reader, entry)
	case filer_pb.SSEType_SSE_C:
		return nil, fmt.Errorf("SSE-C encrypted object cannot be decrypted during replication (customer key not available)")
	}

	return nil, fmt.Errorf("unsupported SSE type: %v", sseType)
}

// MaybeDecryptContent decrypts inline entry content if SSE-encrypted.
// Returns the original content unchanged if no SSE encryption is detected.
func MaybeDecryptContent(content []byte, entry *filer_pb.Entry) ([]byte, error) {
	if entry == nil || len(content) == 0 {
		return content, nil
	}

	sseType := detectSSEType(entry)
	if sseType == filer_pb.SSEType_NONE {
		return content, nil
	}

	reader := bytes.NewReader(content)
	decrypted, err := MaybeDecryptReader(reader, entry)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(decrypted)
}

func detectSSEType(entry *filer_pb.Entry) filer_pb.SSEType {
	// Check chunk metadata first
	var detected filer_pb.SSEType
	for _, chunk := range entry.GetChunks() {
		if chunk.SseType != filer_pb.SSEType_NONE {
			if detected == filer_pb.SSEType_NONE {
				detected = chunk.SseType
			} else if chunk.SseType != detected {
				glog.Warningf("entry has mixed SSE types across chunks: %v and %v, using %v", detected, chunk.SseType, detected)
			}
		}
	}
	if detected != filer_pb.SSEType_NONE {
		return detected
	}

	// Fall back to extended metadata for inline objects (no chunks)
	if entry.Extended != nil {
		if len(entry.Extended[s3_constants.SeaweedFSSSES3Key]) > 0 {
			return filer_pb.SSEType_SSE_S3
		}
		if len(entry.Extended[s3_constants.SeaweedFSSSEKMSKey]) > 0 {
			return filer_pb.SSEType_SSE_KMS
		}
		if len(entry.Extended[s3_constants.SeaweedFSSSEIV]) > 0 {
			return filer_pb.SSEType_SSE_C
		}
	}
	return filer_pb.SSEType_NONE
}

func decryptSSES3(reader io.Reader, entry *filer_pb.Entry) (io.Reader, error) {
	if entry.Extended == nil {
		return nil, fmt.Errorf("SSE-S3 encrypted entry has no extended metadata")
	}

	keyData := entry.Extended[s3_constants.SeaweedFSSSES3Key]
	if len(keyData) == 0 {
		return nil, fmt.Errorf("SSE-S3 key metadata not found in entry")
	}

	keyManager := s3api.GetSSES3KeyManager()
	sseS3Key, err := s3api.DeserializeSSES3Metadata(keyData, keyManager)
	if err != nil {
		return nil, fmt.Errorf("deserialize SSE-S3 metadata: %w", err)
	}

	iv, err := s3api.GetSSES3IV(entry, sseS3Key, keyManager)
	if err != nil {
		return nil, fmt.Errorf("get SSE-S3 IV: %w", err)
	}

	return s3api.CreateSSES3DecryptedReader(reader, sseS3Key, iv)
}

func decryptSSEKMS(reader io.Reader, entry *filer_pb.Entry) (io.Reader, error) {
	if entry.Extended == nil {
		return nil, fmt.Errorf("SSE-KMS encrypted entry has no extended metadata")
	}

	kmsMetadata := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]
	if len(kmsMetadata) == 0 {
		return nil, fmt.Errorf("SSE-KMS key metadata not found in entry")
	}

	sseKMSKey, err := s3api.DeserializeSSEKMSMetadata(kmsMetadata)
	if err != nil {
		return nil, fmt.Errorf("deserialize SSE-KMS metadata: %w", err)
	}

	return s3api.CreateSSEKMSDecryptedReader(reader, sseKMSKey)
}
