package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

/*

If volume server is started with a separated public port, the public port will
be more "secure".

Public port currently only supports reads.

Later writes on public port can have one of the 3
security settings:
1. not secured
2. secured by white list
3. secured by JWT(Json Web Token)

*/

// checkDownloadLimit handles download concurrency limiting with timeout and proxy fallback.
//
// Returns:
//   - true:  Request should proceed with normal processing (limit not exceeded,
//     or successfully waited for available capacity)
//   - false: Request was already handled by this function (proxied to replica,
//     timed out with 429 response, cancelled with 499 response, or
//     failed with error response). Caller should NOT continue processing.
//
// Control Flow:
//   - No limit configured → return true (proceed normally)
//   - Within limit → return true (proceed normally)
//   - Over limit + has replicas + readMode not local → proxy to replica, return false (already handled)
//   - Over limit otherwise → wait with timeout:
//   - Timeout → send 429 response, return false (already handled)
//   - Cancelled → send 499 response, return false (already handled)
//   - Capacity available → return true (proceed normally)
func (vs *VolumeServer) checkDownloadLimit(w http.ResponseWriter, r *http.Request) bool {
	inFlightDownloadSize := atomic.LoadInt64(&vs.inFlightDownloadDataSize)
	stats.VolumeServerInFlightDownloadSize.Set(float64(inFlightDownloadSize))

	if vs.concurrentDownloadLimit == 0 || inFlightDownloadSize <= vs.concurrentDownloadLimit {
		return true // no limit configured or within limit - proceed normally
	}

	stats.VolumeServerHandlerCounter.WithLabelValues(stats.DownloadLimitCond).Inc()
	glog.V(4).Infof("request %s wait because inflight download data %d > %d",
		r.URL.Path, inFlightDownloadSize, vs.concurrentDownloadLimit)

	// Try to proxy to replica if available
	if vs.tryProxyToReplica(w, r) {
		return false // handled by proxy
	}

	// Wait with timeout
	return vs.waitForDownloadSlot(w, r)
}

// tryProxyToReplica attempts to proxy the request to a replica server if the volume has replication.
// Returns:
//   - true:  Request was handled (either proxied successfully or failed with error response)
//   - false: No proxy available (readMode is local, volume has no replicas, or request already proxied)
func (vs *VolumeServer) tryProxyToReplica(w http.ResponseWriter, r *http.Request) bool {
	if vs.ReadMode == "local" || r.URL.Query().Get(reqIsProxied) == "true" {
		return false
	}

	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(1).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return true // handled (with error)
	}

	volume := vs.store.GetVolume(volumeId)
	if volume != nil && volume.ReplicaPlacement != nil && volume.ReplicaPlacement.HasReplication() {
		vs.proxyReqToTargetServer(w, r)
		return true // handled by proxy
	}
	return false // no proxy available
}

// waitForDownloadSlot waits for available download capacity with timeout.
//
// This function implements a blocking wait mechanism with timeout for download capacity.
// It continuously checks if download capacity becomes available and handles timeout
// and cancellation scenarios appropriately.
//
// Returns:
//   - true:  Download capacity became available, request should proceed
//   - false: Request failed (timeout or cancellation), error response already sent
//
// HTTP Status Codes:
//   - 429 Too Many Requests: Wait timeout exceeded
//   - 499 Client Closed Request: Request cancelled by client
func (vs *VolumeServer) waitForDownloadSlot(w http.ResponseWriter, r *http.Request) bool {
	ctx, cancel := context.WithTimeout(r.Context(), vs.inflightDownloadDataTimeout)
	defer cancel()

	var inFlightDownloadSize int64
	vs.inFlightDownloadDataLimitCond.L.Lock()
	err := util.WaitUntil(ctx, vs.inFlightDownloadDataLimitCond, 0, func() bool {
		inFlightDownloadSize = atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		stats.VolumeServerInFlightDownloadSize.Set(float64(inFlightDownloadSize))
		return inFlightDownloadSize <= vs.concurrentDownloadLimit
	})
	vs.inFlightDownloadDataLimitCond.L.Unlock()
	if err == nil {
		return true
	}
	if r.Context().Err() != nil {
		glog.V(1).Infof("request %s cancelled from %s: %v", r.URL.Path, r.RemoteAddr, r.Context().Err())
		w.WriteHeader(util.HttpStatusCancelled)
		return false
	}
	err = fmt.Errorf("request %s because inflight download data %d > %d, and wait timeout",
		r.URL.Path, inFlightDownloadSize, vs.concurrentDownloadLimit)
	glog.V(1).Infof("too many requests: %v", err)
	writeJsonError(w, r, http.StatusTooManyRequests, err)
	return false
}

// volumeClientUploadWaitCap bounds how long a client write (anything except a
// volume server's own primary-to-replica forward) waits for upload budget,
// whatever -inflightUploadDataTimeout says.
//
// Invariant: it stays below operation.uploadBodyMinWait (10s), so the volume
// server answers 503 before the uploading client's body deadline cuts the
// request off as a stall.
const volumeClientUploadWaitCap = 5 * time.Second

// uploadRejectCloseThreshold is the body size above which a rejected HTTP/1
// upload is answered with Connection: close. Smaller bodies are cheaper to
// drain than a new connection is to dial, so they keep the pooled connection.
const uploadRejectCloseThreshold = 256 << 10

// uploadCharge returns the bytes an upload reserves against
// concurrentUploadLimit. A known length is charged as-is. An unknown length
// (chunked transfer) counts as one chunk, fileSizeLimitBytes, but at most half
// the budget (and at least 1 byte): charged the whole limit, a chunked upload
// would only be admitted on an idle server; charged half, it is admitted while
// at most half the budget is in use. Counting the bytes it actually sends is
// left to a later phase.
func (vs *VolumeServer) uploadCharge(r *http.Request) int64 {
	unknownCharge := vs.fileSizeLimitBytes
	if vs.concurrentUploadLimit > 0 {
		unknownCharge = min(unknownCharge, max(vs.concurrentUploadLimit/2, 1))
	}
	return util.CountedUploadSize(r.ContentLength, unknownCharge)
}

// clientUploadWait is how long a client write waits for upload budget:
// -inflightUploadDataTimeout, at most volumeClientUploadWaitCap (also when
// the timeout is 0).
func (vs *VolumeServer) clientUploadWait() time.Duration {
	if vs.inflightUploadDataTimeout > 0 && vs.inflightUploadDataTimeout < volumeClientUploadWaitCap {
		return vs.inflightUploadDataTimeout
	}
	return volumeClientUploadWaitCap
}

// waitForUploadSlot reserves charged bytes of upload budget for r.
//
// Returns:
//   - true:  charged bytes were added to inFlightUploadDataSize; the caller
//     owns them and must give them back with releaseUploadSlot(charged).
//   - false: nothing was reserved and the response was already written.
//
// Admission:
//   - concurrentUploadLimit == 0: counted without a lock, never refused.
//   - Primary-to-replica forwards (operation.IsReplicaForward): counted under
//     the lock, never refused, so a write the primary already accepted always
//     reaches its replicas.
//   - Everything else, including a gateway's ?type=replicate&src=client
//     fan-out: waits up to clientUploadWait until util.UploadAdmits, then
//     reserves before the lock is released.
//
// HTTP Status Codes:
//   - 503 Service Unavailable with Retry-After: no budget within the wait.
//     The body is never read; an HTTP/1 connection carrying a large or
//     unknown-length body is closed rather than drained.
//   - 499 Client Closed Request: the client went away while waiting.
func (vs *VolumeServer) waitForUploadSlot(w http.ResponseWriter, r *http.Request, charged int64) bool {
	if vs.concurrentUploadLimit == 0 {
		inFlight := atomic.AddInt64(&vs.inFlightUploadDataSize, charged)
		stats.VolumeServerInFlightUploadSize.Set(float64(inFlight))
		return true
	}

	limit := vs.concurrentUploadLimit
	var inFlight int64
	var err error
	vs.inFlightUploadDataLimitCond.L.Lock()
	if !operation.IsReplicaForward(r.URL.Query()) {
		admits := func() bool {
			inFlight = atomic.LoadInt64(&vs.inFlightUploadDataSize)
			return util.UploadAdmits(inFlight, charged, limit)
		}
		if !admits() {
			glog.V(4).Infof("wait because inflight upload data %d + %d > %d", inFlight, charged, limit)
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.UploadLimitCond).Inc()
			err = util.WaitUntil(r.Context(), vs.inFlightUploadDataLimitCond, vs.clientUploadWait(), admits)
		}
	}
	if err == nil {
		inFlight = atomic.AddInt64(&vs.inFlightUploadDataSize, charged)
	}
	vs.inFlightUploadDataLimitCond.L.Unlock()
	stats.VolumeServerInFlightUploadSize.Set(float64(inFlight))

	if err == nil {
		return true
	}

	// The body of a rejected upload is never read.
	r.Body = http.NoBody
	if r.Context().Err() != nil {
		glog.V(1).Infof("request cancelled from %s: %v", r.RemoteAddr, r.Context().Err())
		writeJsonError(w, r, util.HttpStatusCancelled, r.Context().Err())
		return false
	}
	w.Header().Set("Retry-After", "1")
	if r.ProtoMajor == 1 && (r.ContentLength < 0 || r.ContentLength > uploadRejectCloseThreshold) {
		w.Header().Set("Connection", "close")
	}
	err = fmt.Errorf("reject because inflight upload data %d + %d > %d, and wait timeout",
		inFlight, charged, limit)
	glog.V(1).Infof("too many requests: %v", err)
	writeJsonError(w, r, http.StatusServiceUnavailable, err)
	return false
}

// releaseUploadSlot gives back charged bytes reserved by waitForUploadSlot
// and wakes the uploads waiting for budget.
func (vs *VolumeServer) releaseUploadSlot(charged int64) {
	if vs.concurrentUploadLimit == 0 {
		inFlight := atomic.AddInt64(&vs.inFlightUploadDataSize, -charged)
		stats.VolumeServerInFlightUploadSize.Set(float64(inFlight))
		return
	}
	vs.inFlightUploadDataLimitCond.L.Lock()
	inFlight := atomic.AddInt64(&vs.inFlightUploadDataSize, -charged)
	vs.inFlightUploadDataLimitCond.Broadcast()
	vs.inFlightUploadDataLimitCond.L.Unlock()
	stats.VolumeServerInFlightUploadSize.Set(float64(inFlight))
}

// handleGetRequest processes GET/HEAD requests with download limiting.
//
// This function orchestrates the complete GET/HEAD request handling workflow:
// 1. Records read request statistics
// 2. Applies download concurrency limits with proxy fallback
// 3. Delegates to GetOrHeadHandler for actual file serving (if limits allow)
//
// The download limiting logic may handle the request completely (via proxy,
// timeout, or error), in which case normal file serving is skipped.
func (vs *VolumeServer) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	stats.ReadRequest()
	if vs.checkDownloadLimit(w, r) {
		vs.GetOrHeadHandler(w, r)
	}
}

// handleUploadRequest processes PUT/POST requests with upload limiting.
//
// It reserves the request's charge (see uploadCharge) with
// waitForUploadSlot, hands the request to PostHandler, and releases the
// charge when PostHandler returns. A request that gets no budget is answered
// by waitForUploadSlot (503 on timeout, 499 on cancellation).
func (vs *VolumeServer) handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	charged := vs.uploadCharge(r)
	if !vs.waitForUploadSlot(w, r, charged) {
		return
	}
	defer vs.releaseUploadSlot(charged)

	// processes uploads
	stats.WriteRequest()
	vs.guard.WhiteList(vs.PostHandler)(w, r)
}

func (vs *VolumeServer) privateStoreHandler(w http.ResponseWriter, r *http.Request) {
	inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(r.Method)
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	start := time.Now()
	requestMethod := r.Method
	defer func(start time.Time, method *string, statusRecorder *stats.StatusRecorder) {
		stats.VolumeServerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.VolumeServerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(start, &requestMethod, statusRecorder)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		vs.handleGetRequest(w, r)
	case http.MethodDelete:
		stats.DeleteRequest()
		vs.guard.WhiteList(vs.DeleteHandler)(w, r)
	case http.MethodPut, http.MethodPost:
		vs.handleUploadRequest(w, r)
	case http.MethodOptions:
		stats.ReadRequest()
		w.Header().Add("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
		w.Header().Add("Access-Control-Allow-Headers", "*")
	default:
		requestMethod = "INVALID"
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("unsupported method %s", r.Method))
	}
}

func getContentLength(r *http.Request) int64 {
	contentLength := r.Header.Get("Content-Length")
	if contentLength != "" {
		length, err := strconv.ParseInt(contentLength, 10, 64)
		if err != nil {
			return 0
		}
		return length
	}
	return 0
}

func (vs *VolumeServer) publicReadOnlyHandler(w http.ResponseWriter, r *http.Request) {
	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	start := time.Now()
	requestMethod := r.Method
	defer func(start time.Time, method *string, statusRecorder *stats.StatusRecorder) {
		stats.VolumeServerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.VolumeServerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(start, &requestMethod, statusRecorder)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		vs.handleGetRequest(w, r)
	case http.MethodOptions:
		stats.ReadRequest()
		w.Header().Add("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Add("Access-Control-Allow-Headers", "*")
	}
}

func (vs *VolumeServer) maybeCheckJwtAuthorization(r *http.Request, vid, fid string, isWrite bool) bool {
	signingKey, previousSigningKey := vs.guard.VerificationKeys(isWrite)
	return vs.checkJwtAuthorizationWithKey(r, vid, fid, signingKey, previousSigningKey)
}

// checkJwtAuthorizationWithKey authorizes against caller-snapshotted signing
// keys, so a write handler can classify an expired-JWT refusal against the exact
// keys it authorized under rather than re-reading ones that may have rotated
// between the two reads.
func (vs *VolumeServer) checkJwtAuthorizationWithKey(r *http.Request, vid, fid string, signingKey, previousSigningKey security.SigningKey) bool {

	if len(signingKey) == 0 {
		return true
	}

	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	token, err := security.DecodeJwt(signingKey, tokenStr, &security.SeaweedFileIdClaims{}, previousSigningKey)
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", r.RemoteAddr, err)
		return false
	}
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", r.RemoteAddr, tokenStr)
		return false
	}

	if sc, ok := token.Claims.(*security.SeaweedFileIdClaims); ok {
		if sepIndex := strings.LastIndex(fid, "_"); sepIndex > 0 {
			fid = fid[:sepIndex]
		}
		expectedFid := vid + "," + fid
		if sc.Fid != expectedFid {
			glog.V(1).Infof("jwt fid mismatch from %s: token has %q, request has %q", r.RemoteAddr, sc.Fid, expectedFid)
			return false
		}
		return true
	}
	glog.V(1).Infof("unexpected jwt from %s: %v", r.RemoteAddr, tokenStr)
	return false
}
