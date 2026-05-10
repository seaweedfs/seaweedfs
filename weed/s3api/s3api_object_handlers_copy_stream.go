package s3api

import (
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// canStreamCopyChunk reports whether the chunk-copy fast path can use the
// io.Pipe streaming variant. The streaming path bypasses both the s3-side
// download buffer and the s3-side multipart-encode buffer, holding only the
// io.Pipe internal buffer (~32 KiB) per copy in flight. It does not work
// when bytes need to be transformed in transit:
//
//   - SSE-C / SSE-KMS / SSE-S3: bytes need to be re-encrypted with a
//     different key on the destination, so they have to land in memory.
//   - Per-chunk CipherKey (per-chunk encryption from the source): the
//     source bytes need to be decrypted before being re-uploaded.
//
// In both cases the caller falls back to the buffered copySingleChunk path,
// which already handles those transformations correctly. The streaming
// path is still safe for IsCompressed chunks — gzip is end-to-end and we
// just forward the compressed bytes with the Content-Encoding header.
func canStreamCopyChunk(chunk *filer_pb.FileChunk) bool {
	if len(chunk.CipherKey) > 0 {
		return false
	}
	if chunk.GetSseType() != filer_pb.SSEType_NONE {
		return false
	}
	return true
}

// streamCopyChunkRange copies size bytes starting at offset from srcUrl to
// the destination volume identified by assignResult, using io.Pipe to avoid
// buffering the chunk in memory. The destination volume server expects the
// same multipart wire format that operation.upload_content writes — see
// upload_content.go for the canonical version; we mirror its part headers
// here so the volume's needle.parseUpload accepts the request.
func (s3a *S3ApiServer) streamCopyChunkRange(
	ctx context.Context,
	srcUrl, srcFileId string,
	offset, size int64,
	assignResult *filer_pb.AssignVolumeResponse,
	isCompressed bool,
) error {
	dstUrl := fmt.Sprintf("http://%s/%s", assignResult.Location.Url, assignResult.FileId)
	dstJwt := security.EncodedJwt(assignResult.Auth)
	srcJwt := filer.JwtForVolumeServer(srcFileId)

	// io.Pipe gives us a synchronous handoff: the producer goroutine
	// builds the multipart body, the consumer (HTTP transport) reads it.
	// Writes block until reads consume them, so the in-flight footprint
	// is the pipe's internal hand-off buffer — not the chunk size.
	pipeReader, pipeWriter := io.Pipe()
	mw := multipart.NewWriter(pipeWriter)
	contentType := mw.FormDataContentType()

	go func() {
		// CloseWithError on the writer end propagates the failure to the
		// HTTP transport reading from pipeReader, which then aborts the
		// POST. Plain Close (with err==nil) sends EOF normally.
		var producerErr error
		defer func() {
			pipeWriter.CloseWithError(producerErr)
		}()

		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", mime.FormatMediaType("form-data",
			map[string]string{"name": "file", "filename": ""}))
		h.Set("Idempotency-Key", dstUrl)
		if isCompressed {
			h.Set("Content-Encoding", "gzip")
		}

		fw, err := mw.CreatePart(h)
		if err != nil {
			producerErr = fmt.Errorf("multipart create part: %w", err)
			return
		}

		// ReadUrlAsStream reads in 256 KiB ticks and hands each tick to
		// the callback. Forwarding directly into fw means each tick is
		// flushed through the multipart writer into the pipe, where the
		// HTTP transport picks it up — no per-chunk buffering on either
		// side of the pipe.
		var writeErr error
		shouldRetry, readErr := util_http.ReadUrlAsStream(ctx, srcUrl, srcJwt, nil, false, false, offset, int(size), func(data []byte) {
			if writeErr != nil {
				return
			}
			if _, err := fw.Write(data); err != nil {
				writeErr = err
			}
		})
		if writeErr != nil {
			producerErr = fmt.Errorf("stream write: %w", writeErr)
			return
		}
		if readErr != nil {
			if shouldRetry {
				glog.V(2).Infof("stream copy %s offset=%d size=%d: retryable error: %v",
					srcUrl, offset, size, readErr)
			}
			producerErr = fmt.Errorf("stream read: %w", readErr)
			return
		}

		if err := mw.Close(); err != nil {
			producerErr = fmt.Errorf("multipart close: %w", err)
			return
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dstUrl, pipeReader)
	if err != nil {
		// Drain the pipe so the producer goroutine doesn't leak waiting
		// on a never-read writer.
		pipeReader.CloseWithError(err)
		return fmt.Errorf("create POST request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	if dstJwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(dstJwt))
	}

	resp, err := util_http.GetGlobalHttpClient().Do(req)
	if err != nil {
		// Closing the reader unblocks the producer if it was still mid-write.
		pipeReader.CloseWithError(err)
		return fmt.Errorf("POST: %w", err)
	}
	defer util_http.CloseResponse(resp)

	if resp.StatusCode >= 400 {
		// Drain the body so the connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("POST %s: %s", dstUrl, resp.Status)
	}
	// Drain the body even on success — http.Client's keepalive depends on it.
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
