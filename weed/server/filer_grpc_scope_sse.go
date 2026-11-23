package weed_server

import (
	"bytes"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/protobuf/proto"
)

var scopeSSEExtendedKeys = [...]string{
	s3_constants.AmzServerSideEncryptionCustomerAlgorithm,
	s3_constants.AmzServerSideEncryptionCustomerKeyMD5,
	s3_constants.AmzServerSideEncryptionContext,
	s3_constants.AmzServerSideEncryption,
	s3_constants.AmzServerSideEncryptionAwsKmsKeyId,
	s3_constants.AmzServerSideEncryptionBucketKeyEnabled,
	s3_constants.AmzEncryptedDataKey,
	s3_constants.AmzEncryptionContextMeta,
	s3_constants.SeaweedFSSSEKMSKey,
	s3_constants.SeaweedFSSSES3Key,
	s3_constants.SeaweedFSSSEIV,
	s3_constants.SeaweedFSSSEKMSKeyID,
	s3_constants.SeaweedFSSSEKMSEncryption,
	s3_constants.SeaweedFSSSEKMSBucketKeyEnabled,
	s3_constants.SeaweedFSSSEKMSEncryptionContext,
	s3_constants.SeaweedFSSSEKMSBaseIV,
	s3_constants.SeaweedFSSSES3Encryption,
	s3_constants.SeaweedFSSSES3BaseIV,
	s3_constants.SeaweedFSSSES3KeyData,
}

// scopeHasSSE reports whether an entry carries server-side encryption state
// that changes how its bytes are read. The allowlist mirrors the S3 read path;
// scope checks must not treat arbitrary extended attributes as encryption
// markers.
func scopeHasSSE(entry *filer_pb.Entry) bool {
	if entry == nil {
		return false
	}
	for _, chunk := range entry.GetChunks() {
		if chunk.GetSseType() != filer_pb.SSEType_NONE || len(chunk.GetSseMetadata()) > 0 {
			return true
		}
	}
	for _, key := range scopeSSEExtendedKeys {
		if _, exists := entry.GetExtended()[key]; exists {
			return true
		}
	}
	return false
}

// scopeCheckSSEState permits metadata-only replacements that resend identical
// data and SSE state. New SSE data and changes to SSE metadata need validation
// beyond a chunk assignment proof. Append always introduces its request chunks;
// it does not replace the stored inline content or extended attributes.
func scopeCheckSSEState(stored, requested *filer_pb.Entry, appending bool) error {
	storedSSE, requestedSSE := scopeHasSSE(stored), scopeHasSSE(requested)
	if appending {
		if len(requested.GetChunks()) > 0 && (storedSSE || requestedSSE) {
			return fmt.Errorf("chunks cannot be appended with S3 SSE state")
		}
		return nil
	}
	if !storedSSE && !requestedSSE {
		return nil
	}
	storedExtended := stored.GetExtended()
	requestedExtended := requested.GetExtended()
	for _, key := range scopeSSEExtendedKeys {
		storedValue, storedPresent := storedExtended[key]
		requestedValue, requestedPresent := requestedExtended[key]
		if storedPresent != requestedPresent || !bytes.Equal(storedValue, requestedValue) {
			return fmt.Errorf("S3 SSE metadata %q cannot be changed by a scoped write", key)
		}
	}
	sameChunks := scopeSameChunkData(stored.GetChunks(), requested.GetChunks())
	sameInline := bytes.Equal(stored.GetContent(), requested.GetContent())
	if sameChunks && sameInline {
		return nil
	}
	for _, chunk := range requested.GetChunks() {
		if chunk.GetSseType() != filer_pb.SSEType_NONE || len(chunk.GetSseMetadata()) > 0 {
			return fmt.Errorf("new S3 SSE chunk markers are not allowed")
		}
	}
	if storedSSE && ((!sameChunks && len(requested.GetChunks()) > 0) || !sameInline) {
		return fmt.Errorf("new data cannot be written to a stored S3 SSE entry")
	}
	return nil
}

// FileId and Fid are interchangeable serialized forms of the same descriptor.
// Compare copies so inspecting the parsed-ID cache cannot mutate a request.
func scopeSameChunkData(stored, requested []*filer_pb.FileChunk) bool {
	if len(stored) != len(requested) {
		return false
	}
	for i, chunk := range stored {
		other := requested[i]
		if chunk == nil || other == nil {
			if chunk != other {
				return false
			}
			continue
		}
		left := proto.Clone(chunk).(*filer_pb.FileChunk)
		right := proto.Clone(other).(*filer_pb.FileChunk)
		left.FileId, right.FileId = left.GetFileIdString(), right.GetFileIdString()
		left.Fid, right.Fid = nil, nil
		if !proto.Equal(left, right) {
			return false
		}
	}
	return true
}
