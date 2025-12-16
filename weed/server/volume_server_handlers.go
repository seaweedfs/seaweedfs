package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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
//   - Over limit + has replicas → proxy to replica, return false (already handled)
//   - Over limit + no replicas → wait with timeout:
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
//   - false: No proxy available (volume has no replicas or request already proxied)
func (vs *VolumeServer) tryProxyToReplica(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Query().Get(reqIsProxied) == "true" {
		return false // already proxied
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
	timerDownload := time.NewTimer(vs.inflightDownloadDataTimeout)
	defer timerDownload.Stop()

	inFlightDownloadSize := atomic.LoadInt64(&vs.inFlightDownloadDataSize)
	for inFlightDownloadSize > vs.concurrentDownloadLimit {
		switch util.WaitWithTimeout(r.Context(), vs.inFlightDownloadDataLimitCond, timerDownload) {
		case http.StatusTooManyRequests:
			err := fmt.Errorf("request %s because inflight download data %d > %d, and wait timeout",
				r.URL.Path, inFlightDownloadSize, vs.concurrentDownloadLimit)
			glog.V(1).Infof("too many requests: %v", err)
			writeJsonError(w, r, http.StatusTooManyRequests, err)
			return false
		case util.HttpStatusCancelled:
			glog.V(1).Infof("request %s cancelled from %s: %v", r.URL.Path, r.RemoteAddr, r.Context().Err())
			w.WriteHeader(util.HttpStatusCancelled)
			return false
		}
		inFlightDownloadSize = atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		stats.VolumeServerInFlightDownloadSize.Set(float64(inFlightDownloadSize))
	}
	return true
}

// checkUploadLimit handles upload concurrency limiting with timeout.
//
// This function implements upload throttling to prevent overwhelming the volume server
// with too many concurrent uploads. It excludes replication traffic from limits.
//
// Returns:
//   - true:  Request should proceed with upload processing (no limit, within limit,
//     or successfully waited for capacity)
//   - false: Request failed (timeout or cancellation), error response already sent
//
// Special Handling:
//   - Replication requests (type=replicate) bypass upload limits
//   - No upload limit configured (concurrentUploadLimit=0) allows all uploads
func (vs *VolumeServer) checkUploadLimit(w http.ResponseWriter, r *http.Request) bool {
	// exclude the replication from the concurrentUploadLimitMB
	if vs.concurrentUploadLimit == 0 || r.URL.Query().Get("type") == "replicate" {
		return true
	}

	inFlightUploadDataSize := atomic.LoadInt64(&vs.inFlightUploadDataSize)
	stats.VolumeServerInFlightUploadSize.Set(float64(inFlightUploadDataSize))

	if inFlightUploadDataSize <= vs.concurrentUploadLimit {
		return true
	}

	return vs.waitForUploadSlot(w, r)
}

// waitForUploadSlot waits for available upload capacity with timeout.
//
// Returns:
//   - true:  Upload capacity became available, request should proceed
//   - false: Request failed (timeout or cancellation), error response already sent
//
// HTTP Status Codes:
//   - 429 Too Many Requests: Wait timeout exceeded
//   - 499 Client Closed Request: Request cancelled by client
func (vs *VolumeServer) waitForUploadSlot(w http.ResponseWriter, r *http.Request) bool {
	var timerUpload *time.Timer
	inFlightUploadDataSize := atomic.LoadInt64(&vs.inFlightUploadDataSize)

	for inFlightUploadDataSize > vs.concurrentUploadLimit {
		if timerUpload == nil {
			timerUpload = time.NewTimer(vs.inflightUploadDataTimeout)
			defer timerUpload.Stop()
		}

		glog.V(4).Infof("wait because inflight upload data %d > %d", inFlightUploadDataSize, vs.concurrentUploadLimit)
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.UploadLimitCond).Inc()

		switch util.WaitWithTimeout(r.Context(), vs.inFlightUploadDataLimitCond, timerUpload) {
		case http.StatusTooManyRequests:
			err := fmt.Errorf("reject because inflight upload data %d > %d, and wait timeout",
				inFlightUploadDataSize, vs.concurrentUploadLimit)
			glog.V(1).Infof("too many requests: %v", err)
			writeJsonError(w, r, http.StatusTooManyRequests, err)
			return false
		case util.HttpStatusCancelled:
			glog.V(1).Infof("request cancelled from %s: %v", r.RemoteAddr, r.Context().Err())
			writeJsonError(w, r, util.HttpStatusCancelled, r.Context().Err())
			return false
		}

		inFlightUploadDataSize = atomic.LoadInt64(&vs.inFlightUploadDataSize)
		stats.VolumeServerInFlightUploadSize.Set(float64(inFlightUploadDataSize))
	}
	return true
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
// This function manages the complete upload request workflow:
// 1. Extracts content length from request headers
// 2. Applies upload concurrency limits with timeout handling
// 3. Tracks in-flight upload data size for monitoring
// 4. Delegates to PostHandler for actual file processing
// 5. Ensures proper cleanup of in-flight counters
//
// The upload limiting logic may reject the request with appropriate HTTP
// status codes (429 for timeout, 499 for cancellation).
func (vs *VolumeServer) handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	contentLength := getContentLength(r)

	if !vs.checkUploadLimit(w, r) {
		return
	}

	atomic.AddInt64(&vs.inFlightUploadDataSize, contentLength)
	defer func() {
		atomic.AddInt64(&vs.inFlightUploadDataSize, -contentLength)
		if vs.concurrentUploadLimit != 0 {
			vs.inFlightUploadDataLimitCond.Broadcast()
		}
	}()

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

	var signingKey security.SigningKey

	if isWrite {
		if len(vs.guard.SigningKey) == 0 {
			return true
		} else {
			signingKey = vs.guard.SigningKey
		}
	} else {
		if len(vs.guard.ReadSigningKey) == 0 {
			return true
		} else {
			signingKey = vs.guard.ReadSigningKey
		}
	}

	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	token, err := security.DecodeJwt(signingKey, tokenStr, &security.SeaweedFileIdClaims{})
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
