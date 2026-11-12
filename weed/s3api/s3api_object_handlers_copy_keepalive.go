package s3api

import (
	"encoding/xml"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

// A server-side copy moves every byte through a volume before it can answer,
// which on a slow disk takes longer than a client's low-speed timeout allows
// for a response that sends nothing. Like AWS S3, a copy still running after
// copyKeepAliveDelay commits to 200, sends the XML declaration, and then a
// space every copyKeepAliveInterval until it ends; the outcome follows as the
// result document or as an <Error> document in that same 200 response, which
// the AWS SDKs read as the error it names.
//
// The numbers answer the strictest client we serve: the AWS SDK for C++ (under
// pyarrow) sets libcurl's CURLOPT_LOW_SPEED_LIMIT to 1 byte/s over
// CURLOPT_LOW_SPEED_TIME = its request timeout, ~3 s by default. So the
// declaration (39 bytes) goes out after 1 s, well inside the first window, and
// then one byte a second keeps every later window at >= 1 byte/s. A copy that
// ends within 1 s answers exactly as it always did. Variables so tests can
// shorten them.
var (
	copyKeepAliveDelay    = 1 * time.Second
	copyKeepAliveInterval = 1 * time.Second
)

// copyKeepAlive owns the ResponseWriter from start until halt returns: the
// handler must not touch w (headers included) in between, and afterwards
// answers only through writeSuccess or writeError.
type copyKeepAlive struct {
	w       http.ResponseWriter
	r       *http.Request
	headers func(h http.Header) // headers known before the copy, sent on the slow path too
	started bool                // the 200 and the declaration went out; set by run, read after done
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

// startCopyKeepAlive arms the keep-alive once a copy is authorized and
// validated. Disabled, it never writes and the answer is exactly today's.
func startCopyKeepAlive(w http.ResponseWriter, r *http.Request, enabled bool, headers func(h http.Header)) *copyKeepAlive {
	// One request id for the header and a late <Error>; a private copy of the
	// request, because the copy paths may still set headers on theirs.
	r, _ = request_id.Ensure(r)
	r = r.Clone(r.Context())
	k := &copyKeepAlive{w: w, r: r, headers: headers, stop: make(chan struct{}), done: make(chan struct{})}
	if !enabled {
		close(k.done)
		return k
	}
	go k.run()
	return k
}

func (k *copyKeepAlive) run() {
	defer close(k.done)
	timer := time.NewTimer(copyKeepAliveDelay)
	defer timer.Stop()
	select {
	case <-k.stop:
		return
	case <-k.r.Context().Done():
		return
	case <-timer.C:
	}
	if k.headers != nil {
		k.headers(k.w.Header())
	}
	s3err.StartStreamingResponse(k.w, k.r, http.StatusOK, s3err.MimeXML)
	k.started = true
	if !k.write([]byte(xml.Header)) {
		return
	}
	ticker := time.NewTicker(copyKeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-k.stop:
			return
		case <-k.r.Context().Done():
			return
		case <-ticker.C:
			if !k.write([]byte(" ")) {
				return
			}
		}
	}
}

// copySourceVersionHeader sets x-amz-copy-source-version-id, which a copy
// knows before it moves any data.
func copySourceVersionHeader(versionId string) func(h http.Header) {
	return func(h http.Header) {
		if versionId != "" {
			h.Set("x-amz-copy-source-version-id", versionId)
		}
	}
}

// partCopyAnswersSSEHeaders reports whether an UploadPartCopy answers SSE
// headers it can know only once the part is written: any SSE recorded on the
// upload (SSE-KMS, SSE-S3, or SSE-C's algorithm and key MD5), or SSE headers
// on the request, its copy-source key included.
func partCopyAnswersSSEHeaders(r *http.Request, uploadEntry *filer_pb.Entry) bool {
	if uploadEntryHasSSE(uploadEntry) || IsSSECEncrypted(uploadEntry.GetExtended()) {
		return true
	}
	return r.Header.Get(s3_constants.AmzServerSideEncryption) != "" ||
		r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" ||
		r.Header.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm) != ""
}

// Test seams around the data-moving steps of each copy (a slow volume or
// remote fill, a failing one); production never reassigns them.
var (
	copyObjectData    = (*S3ApiServer).executeUnifiedCopyStrategy
	copyPartData      = (*S3ApiServer).copyChunksForRange
	copyPartReencrypt = (*S3ApiServer).copyObjectPartViaReencryption
	copyRemoteFill    = (*S3ApiServer).cacheRemoteObjectForCopy
)

// write sends p and flushes it; false once the client is gone.
func (k *copyKeepAlive) write(p []byte) bool {
	if _, err := k.w.Write(p); err != nil {
		glog.V(1).Infof("copy keep-alive %s: %v", k.r.URL.Path, err)
		return false
	}
	if err := http.NewResponseController(k.w).Flush(); err != nil {
		glog.V(1).Infof("copy keep-alive flush %s: %v", k.r.URL.Path, err)
		return false
	}
	return true
}

// halt stops the keep-alive and waits for it, so the caller owns the writer
// again; it reports whether the response has started. Safe to call twice.
func (k *copyKeepAlive) halt() bool {
	k.once.Do(func() { close(k.stop) })
	<-k.done
	return k.started
}

// writeSuccess answers response. setHeaders runs only while the status line
// is still unsent: headers known only after the copy cannot reach a response
// already started (its body carries the ETag).
func (k *copyKeepAlive) writeSuccess(setHeaders func(), response interface{}) {
	if !k.halt() {
		if k.headers != nil {
			k.headers(k.w.Header())
		}
		if setHeaders != nil {
			setHeaders()
		}
		writeSuccessResponseXML(k.w, k.r, response)
		return
	}
	k.write(s3err.EncodeXMLBody(response))
	s3err.PostLog(k.r, http.StatusOK, s3err.ErrNone)
}

// writeError answers errorCode: with its own status while nothing is sent,
// else as the <Error> document closing the 200 already sent.
func (k *copyKeepAlive) writeError(errorCode s3err.ErrorCode) {
	k.writeErrorHeaders(errorCode, nil)
}

// writeErrorHeaders is writeError with headers (e.g. Retry-After) that only
// an unstarted response can still carry.
func (k *copyKeepAlive) writeErrorHeaders(errorCode s3err.ErrorCode, setHeaders func(h http.Header)) {
	if !k.halt() {
		if setHeaders != nil {
			setHeaders(k.w.Header())
		}
		s3err.WriteErrorResponse(k.w, k.r, errorCode)
		return
	}
	k.write(s3err.EncodeXMLBody(s3err.ErrorResponseFor(k.r, errorCode)))
	s3err.PostLog(k.r, s3err.GetAPIError(errorCode).HTTPStatusCode, errorCode)
}
