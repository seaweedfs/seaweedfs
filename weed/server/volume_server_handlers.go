package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

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

func (vs *VolumeServer) privateStoreHandler(w http.ResponseWriter, r *http.Request) {
	inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(r.Method)
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
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
		stats.ReadRequest()
		vs.inFlightDownloadDataLimitCond.L.Lock()
		inFlightDownloadSize := atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		for vs.concurrentDownloadLimit != 0 && inFlightDownloadSize > vs.concurrentDownloadLimit {
			select {
			case <-r.Context().Done():
				glog.V(4).Infof("request cancelled from %s: %v", r.RemoteAddr, r.Context().Err())
				w.WriteHeader(util.HttpStatusCancelled)
				vs.inFlightDownloadDataLimitCond.L.Unlock()
				return
			default:
				glog.V(4).Infof("wait because inflight download data %d > %d", inFlightDownloadSize, vs.concurrentDownloadLimit)
				vs.inFlightDownloadDataLimitCond.Wait()
			}
			inFlightDownloadSize = atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		}
		vs.inFlightDownloadDataLimitCond.L.Unlock()
		vs.GetOrHeadHandler(w, r)
	case http.MethodDelete:
		stats.DeleteRequest()
		vs.guard.WhiteList(vs.DeleteHandler)(w, r)
	case http.MethodPut, http.MethodPost:
		contentLength := getContentLength(r)
		// exclude the replication from the concurrentUploadLimitMB
		if r.URL.Query().Get("type") != "replicate" && vs.concurrentUploadLimit != 0 {
			startTime := time.Now()
			vs.inFlightUploadDataLimitCond.L.Lock()
			inFlightUploadDataSize := atomic.LoadInt64(&vs.inFlightUploadDataSize)
			for inFlightUploadDataSize > vs.concurrentUploadLimit {
				//wait timeout check
				if startTime.Add(vs.inflightUploadDataTimeout).Before(time.Now()) {
					vs.inFlightUploadDataLimitCond.L.Unlock()
					err := fmt.Errorf("reject because inflight upload data %d > %d, and wait timeout", inFlightUploadDataSize, vs.concurrentUploadLimit)
					glog.V(1).Infof("too many requests: %v", err)
					writeJsonError(w, r, http.StatusTooManyRequests, err)
					return
				}
				glog.V(4).Infof("wait because inflight upload data %d > %d", inFlightUploadDataSize, vs.concurrentUploadLimit)
				vs.inFlightUploadDataLimitCond.Wait()
				inFlightUploadDataSize = atomic.LoadInt64(&vs.inFlightUploadDataSize)
			}
			vs.inFlightUploadDataLimitCond.L.Unlock()
		}
		atomic.AddInt64(&vs.inFlightUploadDataSize, contentLength)
		defer func() {
			atomic.AddInt64(&vs.inFlightUploadDataSize, -contentLength)
			if vs.concurrentUploadLimit != 0 {
				vs.inFlightUploadDataLimitCond.Signal()
			}
		}()

		// processes uploads
		stats.WriteRequest()
		vs.guard.WhiteList(vs.PostHandler)(w, r)

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
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
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
		stats.ReadRequest()
		vs.inFlightDownloadDataLimitCond.L.Lock()
		inFlightDownloadSize := atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		for vs.concurrentDownloadLimit != 0 && inFlightDownloadSize > vs.concurrentDownloadLimit {
			glog.V(4).Infof("wait because inflight download data %d > %d", inFlightDownloadSize, vs.concurrentDownloadLimit)
			vs.inFlightDownloadDataLimitCond.Wait()
			inFlightDownloadSize = atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		}
		vs.inFlightDownloadDataLimitCond.L.Unlock()
		vs.GetOrHeadHandler(w, r)
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
		return sc.Fid == vid+","+fid
	}
	glog.V(1).Infof("unexpected jwt from %s: %v", r.RemoteAddr, tokenStr)
	return false
}
