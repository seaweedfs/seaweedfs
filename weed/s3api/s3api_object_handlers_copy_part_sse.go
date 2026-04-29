package s3api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// errCopySourceSSEUnsupported is returned by openSourcePlaintextReader when
// the source object's SSE type is not yet implemented in the UploadPartCopy
// slow path. Callers map it to a 501 NotImplemented S3 response so clients
// can distinguish "we will not handle this shape" from "the server failed".
var errCopySourceSSEUnsupported = errors.New("UploadPartCopy source SSE type not yet supported")

// uploadEntryHasSSE reports whether the multipart upload entry was created
// with any server-side encryption configured (SSE-S3 or SSE-KMS — explicit at
// CreateMultipartUpload time or applied as bucket default). It is used to
// decide whether UploadPartCopy must re-encrypt source bytes for the
// destination, rather than copying them as raw bytes (the fast path).
func uploadEntryHasSSE(uploadEntry *filer_pb.Entry) bool {
	if uploadEntry == nil || uploadEntry.Extended == nil {
		return false
	}
	if _, ok := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSKeyID]; ok {
		return true
	}
	if v, ok := uploadEntry.Extended[s3_constants.SeaweedFSSSES3Encryption]; ok && string(v) == s3_constants.SSEAlgorithmAES256 {
		return true
	}
	return false
}

// sourceEntryHasSSE reports whether the source object's chunks are SSE
// ciphertext on disk and therefore cannot be raw-copied — they must be
// decrypted on read.
func sourceEntryHasSSE(srcEntry *filer_pb.Entry) bool {
	if srcEntry == nil {
		return false
	}
	for _, c := range srcEntry.GetChunks() {
		if c.GetSseType() != filer_pb.SSEType_NONE {
			return true
		}
	}
	if srcEntry.Extended != nil {
		if _, ok := srcEntry.Extended[s3_constants.SeaweedFSSSES3Key]; ok {
			return true
		}
		if _, ok := srcEntry.Extended[s3_constants.SeaweedFSSSEKMSKey]; ok {
			return true
		}
		if _, ok := srcEntry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; ok {
			return true
		}
	}
	return false
}

// readCloserAdapter pairs an arbitrary io.Reader with an io.Closer so callers
// can release the original underlying source even when the inner Reader (e.g.
// cipher.StreamReader, io.LimitReader) does not implement io.Closer.
type readCloserAdapter struct {
	io.Reader
	closer io.Closer
}

func (r *readCloserAdapter) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

// openSourcePlaintextReader returns a reader yielding the source object's
// plaintext bytes for [startOffset, endOffset], applying any necessary SSE
// decryption based on the source entry's metadata.
//
// Used by CopyObjectPartHandler when source or destination is SSE-encrypted:
// the fast raw-chunk-copy path leaves destination chunks SseType=NONE and
// completedMultipartChunk's NONE→SSE_S3 backfill (PR #9224) then writes
// destination-baseIV-derived metadata onto bytes that were actually encrypted
// with the source's key — producing deterministic byte corruption on GET (#8908).
//
// Returns errCopySourceSSEUnsupported when the source's SSE type is not yet
// implemented in this slow path (SSE-KMS, SSE-C). Callers should map that
// sentinel to a 501 NotImplemented S3 response rather than collapsing it to
// 500 InternalError, so clients can distinguish "we will not handle this
// shape" from "the server failed".
func (s3a *S3ApiServer) openSourcePlaintextReader(
	ctx context.Context,
	srcEntry *filer_pb.Entry,
	startOffset, endOffset int64,
) (io.ReadCloser, error) {
	if srcEntry == nil {
		return nil, fmt.Errorf("nil source entry")
	}
	if endOffset < startOffset {
		return io.NopCloser(io.LimitReader(emptyReader{}, 0)), nil
	}
	sliceLen := endOffset - startOffset + 1

	switch s3a.detectPrimarySSEType(srcEntry) {
	case s3_constants.SSETypeS3:
		return s3a.openSSES3SourcePlaintextReader(ctx, srcEntry, startOffset, sliceLen)
	case s3_constants.SSETypeKMS:
		return nil, fmt.Errorf("%w: UploadPartCopy from SSE-KMS source", errCopySourceSSEUnsupported)
	case s3_constants.SSETypeC:
		return nil, fmt.Errorf("%w: UploadPartCopy from SSE-C source", errCopySourceSSEUnsupported)
	default:
		// Unencrypted source: stream raw bytes and apply range.
		raw, err := s3a.getEncryptedStreamFromVolumes(ctx, srcEntry)
		if err != nil {
			return nil, fmt.Errorf("open unencrypted source: %w", err)
		}
		return applyRange(raw, startOffset, sliceLen)
	}
}

// openSSES3SourcePlaintextReader builds a decrypted reader for an SSE-S3
// source. It reuses buildMultipartSSES3Reader, which decrypts each chunk
// independently using its per-chunk metadata — correct for both multipart-SSE
// objects (multiple SSE-S3 chunks) and single-part SSE-S3 objects whose single
// chunk also carries per-chunk metadata after PR #9211.
//
// For older single-part SSE-S3 objects whose chunks lack per-chunk metadata,
// this falls back to the entry-level SSE-S3 key + the entry's stored IV,
// matching the read path's single-part fallback.
func (s3a *S3ApiServer) openSSES3SourcePlaintextReader(
	ctx context.Context,
	srcEntry *filer_pb.Entry,
	startOffset, sliceLen int64,
) (io.ReadCloser, error) {
	chunks := srcEntry.GetChunks()
	hasPerChunkSSE := false
	for _, c := range chunks {
		if c.GetSseType() == filer_pb.SSEType_SSE_S3 && len(c.GetSseMetadata()) > 0 {
			hasPerChunkSSE = true
			break
		}
	}

	if hasPerChunkSSE {
		sortedChunks := make([]*filer_pb.FileChunk, len(chunks))
		copy(sortedChunks, chunks)
		sort.Slice(sortedChunks, func(i, j int) bool {
			return sortedChunks[i].GetOffset() < sortedChunks[j].GetOffset()
		})
		decReader, err := buildMultipartSSES3Reader(
			sortedChunks,
			GetSSES3KeyManager(),
			func(c *filer_pb.FileChunk) (io.ReadCloser, error) {
				return s3a.createEncryptedChunkReader(ctx, c)
			},
		)
		if err != nil {
			return nil, fmt.Errorf("build SSE-S3 source reader: %w", err)
		}
		// buildMultipartSSES3Reader returns a *lazyMultipartChunkReader whose
		// Close() releases the live chunk body. Use it as the closer.
		var closer io.Closer
		if rc, ok := decReader.(io.Closer); ok {
			closer = rc
		}
		return applyRange(&readCloserAdapter{Reader: decReader, closer: closer}, startOffset, sliceLen)
	}

	// Legacy single-part fallback: entry-level SeaweedFSSSES3Key + entry IV.
	keyData, ok := srcEntry.Extended[s3_constants.SeaweedFSSSES3Key]
	if !ok || len(keyData) == 0 {
		return nil, fmt.Errorf("SSE-S3 source has no per-chunk metadata and no entry-level SSE-S3 key")
	}
	keyManager := GetSSES3KeyManager()
	sseS3Key, err := DeserializeSSES3Metadata(keyData, keyManager)
	if err != nil {
		return nil, fmt.Errorf("deserialize entry-level SSE-S3 key: %w", err)
	}
	iv, err := GetSSES3IV(srcEntry, sseS3Key, keyManager)
	if err != nil {
		return nil, fmt.Errorf("get SSE-S3 IV: %w", err)
	}
	encStream, err := s3a.getEncryptedStreamFromVolumes(ctx, srcEntry)
	if err != nil {
		return nil, fmt.Errorf("open ciphertext source: %w", err)
	}
	dec, err := CreateSSES3DecryptedReader(encStream, sseS3Key, iv)
	if err != nil {
		encStream.Close()
		return nil, fmt.Errorf("create SSE-S3 decrypted reader: %w", err)
	}
	rc, ok := dec.(io.ReadCloser)
	if !ok {
		rc = &readCloserAdapter{Reader: dec, closer: encStream}
	}
	return applyRange(rc, startOffset, sliceLen)
}

// applyRange skips startOffset bytes from src and limits the result to
// sliceLen bytes. The returned ReadCloser closes the underlying source.
func applyRange(src io.ReadCloser, startOffset, sliceLen int64) (io.ReadCloser, error) {
	if startOffset > 0 {
		if _, err := io.CopyN(io.Discard, src, startOffset); err != nil {
			src.Close()
			return nil, fmt.Errorf("skip to range start %d: %w", startOffset, err)
		}
	}
	if sliceLen <= 0 {
		return &readCloserAdapter{Reader: io.LimitReader(src, 0), closer: src}, nil
	}
	return &readCloserAdapter{Reader: io.LimitReader(src, sliceLen), closer: src}, nil
}

// emptyReader yields no bytes. Used for empty-range UploadPartCopy.
type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

// applyDestSSEHeadersToCopyRequest stages the destination's SSE setup on the
// (cloned) request so that putToFiler's existing handleAllSSEEncryption picks
// it up. The upload-entry markers (laid down at CreateMultipartUpload) bind
// every part of the upload to the same key+baseIV, matching PutObjectPart.
func (s3a *S3ApiServer) applyDestSSEHeadersToCopyRequest(
	r *http.Request, uploadEntry *filer_pb.Entry, uploadID string,
) error {
	if uploadEntry == nil || uploadEntry.Extended == nil {
		return nil
	}

	if keyIDBytes, hasKMS := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSKeyID]; hasKMS {
		// Mirror the SSE-KMS branch of PutObjectPartHandler: stage
		// X-Amz-Server-Side-Encryption=aws:kms plus the key ID, encryption
		// context, bucket-key flag and base IV onto the request.
		keyID := string(keyIDBytes)

		bucketKeyEnabled := false
		if v, ok := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSBucketKeyEnabled]; ok && string(v) == "true" {
			bucketKeyEnabled = true
		}

		var encryptionContext map[string]string
		if cb, ok := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSEncryptionContext]; ok {
			if err := json.Unmarshal(cb, &encryptionContext); err != nil {
				glog.Errorf("UploadPartCopy: failed to parse SSE-KMS context for upload %s: %v", uploadID, err)
				encryptionContext = nil
			}
		}
		if len(encryptionContext) == 0 {
			// Bucket and object are populated on the cloned request; reuse
			// the same builder PutObjectPartHandler does.
			bucket, object := s3_constants.GetBucketAndObject(r)
			encryptionContext = BuildEncryptionContext(bucket, object, bucketKeyEnabled)
		}

		var baseIV []byte
		if ivBytes, ok := uploadEntry.Extended[s3_constants.SeaweedFSSSEKMSBaseIV]; ok {
			decoded, decErr := base64.StdEncoding.DecodeString(string(ivBytes))
			if decErr != nil || len(decoded) != s3_constants.AESBlockSize {
				return fmt.Errorf("invalid SSE-KMS base IV on upload %s", uploadID)
			}
			baseIV = decoded
		} else {
			return fmt.Errorf("no SSE-KMS base IV on upload %s", uploadID)
		}

		r.Header.Set(s3_constants.AmzServerSideEncryption, "aws:kms")
		r.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, keyID)
		if bucketKeyEnabled {
			r.Header.Set(s3_constants.AmzServerSideEncryptionBucketKeyEnabled, "true")
		}
		if len(encryptionContext) > 0 {
			if cj, err := json.Marshal(encryptionContext); err == nil {
				r.Header.Set(s3_constants.AmzServerSideEncryptionContext, base64.StdEncoding.EncodeToString(cj))
			}
		}
		r.Header.Set(s3_constants.SeaweedFSSSEKMSBaseIVHeader, base64.StdEncoding.EncodeToString(baseIV))
		return nil
	}

	// SSE-S3 path: reuse the existing PutObjectPart helper unchanged. It is
	// pure header manipulation on r and does not touch S3ApiServer state.
	return s3a.handleSSES3MultipartHeaders(r, uploadEntry, uploadID)
}

// fakeContentRequest builds a minimal request representing "PUT this body" for
// the multipart-part write path used by UploadPartCopy. It clones the original
// request's headers (so things like AmzAccountId carry over) and clears the
// copy-only headers; SSE setup is added later by applyDestSSEHeadersToCopyRequest.
func fakeContentRequest(orig *http.Request, body io.ReadCloser, contentLength int64) *http.Request {
	cloned := orig.Clone(orig.Context())
	cloned.Body = body
	cloned.ContentLength = contentLength
	if cloned.Header == nil {
		cloned.Header = http.Header{}
	} else {
		cloned.Header = cloned.Header.Clone()
	}
	cloned.Header.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	cloned.Header.Del("X-Amz-Copy-Source")
	cloned.Header.Del("X-Amz-Copy-Source-Range")
	cloned.Header.Del("X-Amz-Metadata-Directive")
	cloned.Header.Del("X-Amz-Tagging-Directive")
	// Content-Md5 cannot be reproduced from the source plaintext without
	// streaming it once first; clear it so putToFiler doesn't validate.
	cloned.Header.Del("Content-Md5")
	return cloned
}

// copyObjectPartViaReencryption implements the slow path of UploadPartCopy when
// either the source object is SSE-encrypted or the destination multipart upload
// is configured for SSE encryption. It:
//
//  1. Opens a plaintext reader of the source range (decrypting if needed).
//  2. Stages the destination's SSE-S3 / SSE-KMS multipart headers on a cloned
//     request so handleAllSSEEncryption (called from putToFiler) routes the
//     body through the matching multipart-encryption helper.
//  3. Calls putToFiler with the plaintext reader, which encrypts using the
//     destination upload session's key+baseIV (consistent with PutObjectPart),
//     auto-chunks, and writes the part entry with proper per-chunk SSE metadata.
//
// Without this path, copyChunksForRange's raw byte copy leaves destination
// chunks SseType=NONE; completedMultipartChunk then "backfills" SSE-S3 metadata
// with destination-baseIV-derived IVs, but the bytes on disk were encrypted
// with the source's key — yielding deterministic byte corruption on GET (#8908).
func (s3a *S3ApiServer) copyObjectPartViaReencryption(
	r *http.Request,
	srcEntry *filer_pb.Entry,
	startOffset, endOffset int64,
	dstBucket, uploadID string,
	partID int,
	uploadEntry *filer_pb.Entry,
) (etag string, errCode s3err.ErrorCode) {
	if endOffset < startOffset {
		return s3a.writeEmptyCopyPart(dstBucket, uploadID, partID)
	}
	sliceLen := endOffset - startOffset + 1

	srcReader, err := s3a.openSourcePlaintextReader(r.Context(), srcEntry, startOffset, endOffset)
	if err != nil {
		glog.Errorf("UploadPartCopy: open source plaintext reader: %v", err)
		// Distinguish "we will not handle this shape" (501) from "the server
		// failed" (500). SSE-KMS / SSE-C source support in this slow path is
		// staged work; the explicit error lets clients see it as a feature
		// gap rather than a server fault.
		if errors.Is(err, errCopySourceSSEUnsupported) {
			return "", s3err.ErrNotImplemented
		}
		return "", s3err.ErrInternalError
	}
	defer srcReader.Close()

	cloned := fakeContentRequest(r, srcReader, sliceLen)
	if err := s3a.applyDestSSEHeadersToCopyRequest(cloned, uploadEntry, uploadID); err != nil {
		glog.Errorf("UploadPartCopy: apply destination SSE headers: %v", err)
		return "", s3err.ErrInternalError
	}

	filePath := s3a.genPartUploadPath(dstBucket, uploadID, partID)
	tag, code, _ := s3a.putToFiler(cloned, filePath, srcReader, dstBucket, "", partID, nil)
	if code != s3err.ErrNone {
		return "", code
	}
	return tag, s3err.ErrNone
}

// writeEmptyCopyPart writes a 0-byte part entry for an empty UploadPartCopy
// range, mirroring the legacy fast path's handling of endOffset < startOffset.
func (s3a *S3ApiServer) writeEmptyCopyPart(dstBucket, uploadID string, partID int) (string, s3err.ErrorCode) {
	uploadDir := s3a.genUploadsFolder(dstBucket) + "/" + uploadID
	partName := fmt.Sprintf("%04d_%s.part", partID, "copy")
	if exists, _ := s3a.exists(uploadDir, partName, false); exists {
		if err := s3a.rm(uploadDir, partName, false, false); err != nil {
			return "", s3err.ErrInternalError
		}
	}
	if err := s3a.mkFile(uploadDir, partName, nil, func(e *filer_pb.Entry) {
		if e.Attributes == nil {
			e.Attributes = &filer_pb.FuseAttributes{}
		}
		e.Attributes.FileSize = 0
	}); err != nil {
		return "", s3err.ErrInternalError
	}
	const emptyMD5Hex = "d41d8cd98f00b204e9800998ecf8427e"
	return emptyMD5Hex, s3err.ErrNone
}
