package s3api

import (
	"context"
	"fmt"
	"io"
	"math"
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
// just forward the wire bytes (with the destination Content-Encoding
// header set to whatever the source actually returned).
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
//
// isFullChunk is the load-bearing distinction:
//
//   - false (partial range): a Range header is sent to the source volume,
//     which serves the requested byte range. For compressed chunks the
//     volume internally decompresses to satisfy the range, then returns
//     raw bytes (no Content-Encoding); we forward those raw bytes.
//   - true (full chunk): we ask the source volume to send compressed
//     bytes via Accept-Encoding: gzip, bypass Go's auto-decompression on
//     resp.Body, and forward whatever wire bytes arrived to the
//     destination. When the chunk is stored gzipped that means we never
//     decompress on either the source-volume or s3-server side; the
//     destination receives gzipped bytes and stores them as-is.
//
// For the Harbor-style assemble repro (where each UploadPartCopy range
// matches a source chunk boundary), the full-chunk path eliminates the
// "bytes.growSlice from util.DecompressData" cost that dominates the
// volume-side heap profile after the s3-side fixes.
//
// The destination's Content-Encoding header is set from the source's
// response Content-Encoding only — never from a caller-supplied hint.
// Trusting a hint that disagrees with the wire bytes (e.g. caller says
// "compressed" but the volume returned raw bytes for a partial range)
// would label raw bytes as gzip and corrupt reads on the destination.
func (s3a *S3ApiServer) streamCopyChunkRange(
	ctx context.Context,
	srcUrl, srcFileId string,
	offset, size int64,
	isFullChunk bool,
	assignResult *filer_pb.AssignVolumeResponse,
) error {
	// Range header takes int64, but mirror the int32 bound from
	// downloadChunkData so the two paths reject the same inputs.
	if size > int64(math.MaxInt32) {
		return fmt.Errorf("chunk size %d exceeds maximum int32 size", size)
	}
	// Child context so a terminal error here unblocks both legs
	// immediately. Without this, a failed POST closes pipeReader
	// (which only fails the producer's writes), but the source GET's  //codespell:ignore
	// read loop would keep draining srcResp.Body in the background
	// until EOF — wasting source-volume bandwidth and CPU on a copy
	// that's already failed. Cancelling streamCtx tears down both the
	// source GET and the destination POST when this function returns.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	dstUrl := fmt.Sprintf("http://%s/%s", assignResult.Location.Url, assignResult.FileId)
	dstJwt := security.EncodedJwt(assignResult.Auth)
	srcJwt := filer.JwtForVolumeServer(srcFileId)

	// Source GET: built directly (not through ReadUrlAsStream) because that
	// helper auto-decompresses gzipped responses, which would defeat the
	// whole-chunk pass-through optimization. We want the wire bytes.
	srcReq, err := http.NewRequestWithContext(streamCtx, http.MethodGet, srcUrl, nil)
	if err != nil {
		return fmt.Errorf("create source GET: %w", err)
	}
	if srcJwt != "" {
		srcReq.Header.Set("Authorization", security.BearerPrefix+srcJwt)
	}
	if isFullChunk {
		// Manually setting Accept-Encoding tells Go's http.Transport that
		// the user wants raw bytes back — Go's automatic decompression is
		// only applied when it adds the header itself. The server may or
		// may not actually gzip; the response Content-Encoding header is
		// authoritative.
		srcReq.Header.Set("Accept-Encoding", "gzip")
	} else {
		srcReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	}

	srcResp, err := util_http.GetGlobalHttpClient().Do(srcReq)
	if err != nil {
		return fmt.Errorf("source GET: %w", err)
	}
	// CloseResponse drains and closes resp.Body for keepalive.
	defer util_http.CloseResponse(srcResp)

	if srcResp.StatusCode >= 400 {
		return fmt.Errorf("source GET %s: %s", srcUrl, srcResp.Status)
	}

	// What's actually on the wire — trust this over caller hints. For range
	// fetches the volume always returns raw bytes (no Content-Encoding); for
	// full-chunk fetches it depends on whether the chunk is stored gzipped.
	bodyIsGzipped := srcResp.Header.Get("Content-Encoding") == "gzip"

	// io.Pipe gives us a synchronous handoff: the producer goroutine builds
	// the multipart body, the consumer (HTTP transport) reads it. Writes
	// block until reads consume them, so the in-flight footprint is the
	// pipe's hand-off buffer — not the chunk size.
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
		if bodyIsGzipped {
			h.Set("Content-Encoding", "gzip")
		}

		fw, err := mw.CreatePart(h)
		if err != nil {
			producerErr = fmt.Errorf("multipart create part: %w", err)
			return
		}

		if _, err := io.Copy(fw, srcResp.Body); err != nil {
			if shouldLogStreamError(err) {
				glog.V(2).Infof("stream copy %s offset=%d size=%d: %v",
					srcUrl, offset, size, err)
			}
			producerErr = fmt.Errorf("stream copy: %w", err)
			return
		}

		if err := mw.Close(); err != nil {
			producerErr = fmt.Errorf("multipart close: %w", err)
			return
		}
	}()

	req, err := http.NewRequestWithContext(streamCtx, http.MethodPost, dstUrl, pipeReader)
	if err != nil {
		pipeReader.CloseWithError(err)
		return fmt.Errorf("create POST request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	if dstJwt != "" {
		req.Header.Set("Authorization", security.BearerPrefix+string(dstJwt))
	}

	resp, err := util_http.GetGlobalHttpClient().Do(req)
	if err != nil {
		// Closing the reader unblocks the producer if it was still mid-write;
		// the deferred cancel above also stops any in-flight source read.
		pipeReader.CloseWithError(err)
		return fmt.Errorf("POST: %w", err)
	}
	defer util_http.CloseResponse(resp)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("POST %s: %s", dstUrl, resp.Status)
	}
	return nil
}

func shouldLogStreamError(err error) bool {
	// io.ErrClosedPipe shows up when the HTTP transport closed pipeReader
	// after the POST failed; the underlying error is reported via the POST
	// path, no need to log twice.
	return err != nil && err != io.ErrClosedPipe
}
