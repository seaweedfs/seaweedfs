package weed_server

import (
	"errors"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chrislusf/seaweedfs/weed/stats"
)

func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {

	start := time.Now()

	if r.Method == "OPTIONS" {
		stats.FilerRequestCounter.WithLabelValues("options").Inc()
		OptionsHandler(w, r, false)
		stats.FilerRequestHistogram.WithLabelValues("options").Observe(time.Since(start).Seconds())
		return
	}

	isReadHttpCall := r.Method == "GET" || r.Method == "HEAD"
	if !fs.maybeCheckJwtAuthorization(r, !isReadHttpCall) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// proxy to volume servers
	var fileId string
	if strings.HasPrefix(r.RequestURI, "/?proxyChunkId=") {
		fileId = r.RequestURI[len("/?proxyChunkId="):]
	}
	if fileId != "" {
		stats.FilerRequestCounter.WithLabelValues("proxy").Inc()
		fs.proxyToVolumeServer(w, r, fileId)
		stats.FilerRequestHistogram.WithLabelValues("proxy").Observe(time.Since(start).Seconds())
		return
	}

	w.Header().Set("Server", "SeaweedFS Filer "+util.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	switch r.Method {
	case "GET":
		stats.FilerRequestCounter.WithLabelValues("get").Inc()
		fs.GetOrHeadHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds())
	case "HEAD":
		stats.FilerRequestCounter.WithLabelValues("head").Inc()
		fs.GetOrHeadHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("head").Observe(time.Since(start).Seconds())
	case "DELETE":
		stats.FilerRequestCounter.WithLabelValues("delete").Inc()
		if _, ok := r.URL.Query()["tagging"]; ok {
			fs.DeleteTaggingHandler(w, r)
		} else {
			fs.DeleteHandler(w, r)
		}
		stats.FilerRequestHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	case "POST", "PUT":

		// wait until in flight data is less than the limit
		contentLength := getContentLength(r)
		fs.inFlightDataLimitCond.L.Lock()
		for fs.option.ConcurrentUploadLimit != 0 && atomic.LoadInt64(&fs.inFlightDataSize) > fs.option.ConcurrentUploadLimit {
			glog.V(4).Infof("wait because inflight data %d > %d", fs.inFlightDataSize, fs.option.ConcurrentUploadLimit)
			fs.inFlightDataLimitCond.Wait()
		}
		fs.inFlightDataLimitCond.L.Unlock()
		atomic.AddInt64(&fs.inFlightDataSize, contentLength)
		defer func() {
			atomic.AddInt64(&fs.inFlightDataSize, -contentLength)
			fs.inFlightDataLimitCond.Signal()
		}()

		if r.Method == "PUT" {
			stats.FilerRequestCounter.WithLabelValues("put").Inc()
			if _, ok := r.URL.Query()["tagging"]; ok {
				fs.PutTaggingHandler(w, r)
			} else {
				fs.PostHandler(w, r, contentLength)
			}
			stats.FilerRequestHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds())
		} else { // method == "POST"
			stats.FilerRequestCounter.WithLabelValues("post").Inc()
			fs.PostHandler(w, r, contentLength)
			stats.FilerRequestHistogram.WithLabelValues("post").Observe(time.Since(start).Seconds())
		}
	}
}

func (fs *FilerServer) readonlyFilerHandler(w http.ResponseWriter, r *http.Request) {

	start := time.Now()

	// We handle OPTIONS first because it never should be authenticated
	if r.Method == "OPTIONS" {
		stats.FilerRequestCounter.WithLabelValues("options").Inc()
		OptionsHandler(w, r, true)
		stats.FilerRequestHistogram.WithLabelValues("options").Observe(time.Since(start).Seconds())
		return
	}

	if !fs.maybeCheckJwtAuthorization(r, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	w.Header().Set("Server", "SeaweedFS Filer "+util.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	switch r.Method {
	case "GET":
		stats.FilerRequestCounter.WithLabelValues("get").Inc()
		fs.GetOrHeadHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds())
	case "HEAD":
		stats.FilerRequestCounter.WithLabelValues("head").Inc()
		fs.GetOrHeadHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("head").Observe(time.Since(start).Seconds())
	}
}

func OptionsHandler(w http.ResponseWriter, r *http.Request, isReadOnly bool) {
	if isReadOnly {
		w.Header().Add("Access-Control-Allow-Methods", "GET, OPTIONS")
	} else {
		w.Header().Add("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
	}
	w.Header().Add("Access-Control-Allow-Headers", "*")
}

// maybeCheckJwtAuthorization returns true if access should be granted, false if it should be denied
func (fs *FilerServer) maybeCheckJwtAuthorization(r *http.Request, isWrite bool) bool {

	var signingKey security.SigningKey

	if isWrite {
		if len(fs.filerGuard.SigningKey) == 0 {
			return true
		} else {
			signingKey = fs.filerGuard.SigningKey
		}
	} else {
		if len(fs.filerGuard.ReadSigningKey) == 0 {
			return true
		} else {
			signingKey = fs.filerGuard.ReadSigningKey
		}
	}

	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	token, err := security.DecodeJwt(signingKey, tokenStr, &security.SeaweedFilerClaims{})
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", r.RemoteAddr, err)
		return false
	}
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", r.RemoteAddr, tokenStr)
		return false
	} else {
		return true
	}
}
