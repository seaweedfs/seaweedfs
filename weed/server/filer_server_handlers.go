package weed_server

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	inFlightGauge := stats.FilerInFlightRequestsGauge.WithLabelValues(r.Method)
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder
	origin := r.Header.Get("Origin")
	if origin != "" {
		if fs.option.AllowedOrigins == nil || len(fs.option.AllowedOrigins) == 0 || fs.option.AllowedOrigins[0] == "*" {
			origin = "*"
		} else {
			originFound := false
			for _, allowedOrigin := range fs.option.AllowedOrigins {
				if origin == allowedOrigin {
					originFound = true
				}
			}
			if !originFound {
				writeJsonError(w, r, http.StatusForbidden, errors.New("origin not allowed"))
				return
			}
		}

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Expose-Headers", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
	}

	if r.Method == http.MethodOptions {
		OptionsHandler(w, r, false)
		return
	}

	// proxy to volume servers
	var fileId string
	if r.URL.Path == "/" {
		fileId = r.URL.Query().Get("proxyChunkId")
	}
	if fileId != "" {
		fs.proxyToVolumeServer(w, r, fileId)
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkProxy).Inc()
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkProxy).Observe(time.Since(start).Seconds())
		return
	}
	requestMethod := r.Method
	defer func(method *string) {
		stats.FilerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.FilerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(&requestMethod)

	isReadHttpCall := r.Method == http.MethodGet || r.Method == http.MethodHead
	if !fs.maybeCheckJwtAuthorization(r, !isReadHttpCall) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		fs.GetOrHeadHandler(w, r)
	case http.MethodDelete:
		if _, ok := r.URL.Query()["tagging"]; ok {
			fs.DeleteTaggingHandler(w, r)
		} else {
			fs.DeleteHandler(w, r)
		}
	case http.MethodPost, http.MethodPut:
		// wait until in flight data is less than the limit
		contentLength := getContentLength(r)
		fs.inFlightDataLimitCond.L.Lock()
		inFlightDataSize := atomic.LoadInt64(&fs.inFlightDataSize)
		inFlightUploads := atomic.LoadInt64(&fs.inFlightUploads)

		// Wait if either data size limit or file count limit is exceeded
		for (fs.option.ConcurrentUploadLimit != 0 && inFlightDataSize > fs.option.ConcurrentUploadLimit) || (fs.option.ConcurrentFileUploadLimit != 0 && inFlightUploads >= fs.option.ConcurrentFileUploadLimit) {
			if fs.option.ConcurrentUploadLimit != 0 && inFlightDataSize > fs.option.ConcurrentUploadLimit {
				glog.V(4).Infof("wait because inflight data %d > %d", inFlightDataSize, fs.option.ConcurrentUploadLimit)
			}
			if fs.option.ConcurrentFileUploadLimit != 0 && inFlightUploads >= fs.option.ConcurrentFileUploadLimit {
				glog.V(4).Infof("wait because inflight uploads %d >= %d", inFlightUploads, fs.option.ConcurrentFileUploadLimit)
			}
			fs.inFlightDataLimitCond.Wait()
			inFlightDataSize = atomic.LoadInt64(&fs.inFlightDataSize)
			inFlightUploads = atomic.LoadInt64(&fs.inFlightUploads)
		}
		fs.inFlightDataLimitCond.L.Unlock()

		// Increment counters
		newUploads := atomic.AddInt64(&fs.inFlightUploads, 1)
		newSize := atomic.AddInt64(&fs.inFlightDataSize, contentLength)
		// Update metrics
		stats.FilerInFlightUploadCountGauge.Set(float64(newUploads))
		stats.FilerInFlightUploadBytesGauge.Set(float64(newSize))
		defer func() {
			// Decrement counters
			newUploads := atomic.AddInt64(&fs.inFlightUploads, -1)
			newSize := atomic.AddInt64(&fs.inFlightDataSize, -contentLength)
			// Update metrics
			stats.FilerInFlightUploadCountGauge.Set(float64(newUploads))
			stats.FilerInFlightUploadBytesGauge.Set(float64(newSize))
			fs.inFlightDataLimitCond.Signal()
		}()

		if r.Method == http.MethodPut {
			if _, ok := r.URL.Query()["tagging"]; ok {
				fs.PutTaggingHandler(w, r)
			} else {
				fs.PostHandler(w, r, contentLength)
			}
		} else { // method == "POST"
			fs.PostHandler(w, r, contentLength)
		}
	default:
		requestMethod = "INVALID"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (fs *FilerServer) readonlyFilerHandler(w http.ResponseWriter, r *http.Request) {

	start := time.Now()
	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder

	glog.V(4).Infof("Request: %s %s", r.Method, r.URL.Path)

	origin := r.Header.Get("Origin")
	if origin != "" {
		if fs.option.AllowedOrigins == nil || len(fs.option.AllowedOrigins) == 0 || fs.option.AllowedOrigins[0] == "*" {
			origin = "*"
		} else {
			originFound := false
			for _, allowedOrigin := range fs.option.AllowedOrigins {
				if origin == allowedOrigin {
					originFound = true
				}
			}
			if !originFound {
				writeJsonError(w, r, http.StatusForbidden, errors.New("origin not allowed"))
				return
			}
		}

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "OPTIONS, GET, HEAD")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	requestMethod := r.Method
	defer func(method *string) {
		stats.FilerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.FilerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(&requestMethod)
	// We handle OPTIONS first because it never should be authenticated
	if r.Method == http.MethodOptions {
		OptionsHandler(w, r, true)
		return
	}

	if !fs.maybeCheckJwtAuthorization(r, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		fs.GetOrHeadHandler(w, r)
	default:
		requestMethod = "INVALID"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func OptionsHandler(w http.ResponseWriter, r *http.Request, isReadOnly bool) {
	if isReadOnly {
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	} else {
		w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Expose-Headers", "*")
	}
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

// maybeCheckJwtAuthorization returns true if access should be granted, false if it should be denied
func (fs *FilerServer) maybeCheckJwtAuthorization(r *http.Request, isWrite bool) bool {

	if !isWrite && r.URL.Path == "/" {
		return true
	}

	return fs.checkJwtAuthorization(r, isWrite, func() ([]string, error) { return jwtScopedRequestPaths(r), nil })
}

// checkJwtAuthorization verifies the request carries a valid filer JWT for the
// requested access level and, for prefix-restricted tokens, that every resolved
// path falls within AllowedPrefixes. resolveScopedPaths is invoked only for a
// prefix-restricted token, so callers that read extra state to name their target
// (the TUS handler reads the session's stored path) pay for it only when it is
// consulted; a resolution error denies the request.
func (fs *FilerServer) checkJwtAuthorization(r *http.Request, isWrite bool, resolveScopedPaths func() ([]string, error)) bool {

	var signingKey security.SigningKey

	if isWrite {
		signingKey = fs.filerGuard.SigningKey()
		if len(signingKey) == 0 {
			return true
		}
	} else {
		signingKey = fs.filerGuard.ReadSigningKey()
		if len(signingKey) == 0 {
			return true
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
	}

	claims, ok := token.Claims.(*security.SeaweedFilerClaims)
	if !ok {
		glog.V(1).Infof("jwt claims not of type *SeaweedFilerClaims from %s", r.RemoteAddr)
		return false
	}

	if len(claims.AllowedPrefixes) > 0 {
		scopedPaths, err := resolveScopedPaths()
		if err != nil {
			glog.V(1).Infof("jwt scope resolution failed from %s: %v", r.RemoteAddr, err)
			return false
		}
		for _, p := range scopedPaths {
			if !anyComponentPrefixMatches(claims.AllowedPrefixes, p) {
				glog.V(1).Infof("jwt path not allowed from %s: %v", r.RemoteAddr, p)
				return false
			}
		}
	}
	if len(claims.AllowedMethods) > 0 {
		hasMethod := false
		for _, method := range claims.AllowedMethods {
			if method == r.Method {
				hasMethod = true
				break
			}
		}
		if !hasMethod {
			glog.V(1).Infof("jwt method not allowed from %s: %v", r.RemoteAddr, r.Method)
			return false
		}
	}

	return true
}

// jwtScopedRequestPaths returns every filer path a request touches that must be
// covered by the JWT AllowedPrefixes: the write target (r.URL.Path) plus any
// copy/move source named by the cp.from / mv.from query parameters.
func jwtScopedRequestPaths(r *http.Request) []string {
	paths := []string{r.URL.Path}
	if query := r.URL.Query(); query.Has("cp.from") || query.Has("mv.from") {
		if from := query.Get("cp.from"); from != "" {
			paths = append(paths, from)
		}
		if from := query.Get("mv.from"); from != "" {
			paths = append(paths, from)
		}
	}
	return paths
}

// anyComponentPrefixMatches reports whether p is within any of the prefixes.
func anyComponentPrefixMatches(prefixes []string, p string) bool {
	for _, prefix := range prefixes {
		if pathHasComponentPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// pathHasComponentPrefix reports whether reqPath is contained within the
// directory subtree denoted by prefix, treating both as "/"-separated
// path components. Both inputs are normalised with path.Clean to neutralise
// "." and ".." segments and collapse duplicate slashes. A prefix of "/"
// matches any path.
func pathHasComponentPrefix(reqPath, prefix string) bool {
	if prefix == "" {
		return false
	}
	cleanedPath := path.Clean(reqPath)
	if cleanedPath == "." {
		cleanedPath = "/"
	}
	cleanedPrefix := path.Clean(prefix)
	if cleanedPrefix == "." {
		cleanedPrefix = "/"
	}
	if cleanedPrefix == "/" {
		return true
	}
	if cleanedPath == cleanedPrefix {
		return true
	}
	return strings.HasPrefix(cleanedPath, cleanedPrefix+"/")
}

func (fs *FilerServer) filerHealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS "+version.VERSION)
	if _, err := fs.filer.Store.FindEntry(context.Background(), filer.TopicsDir); err != nil && err != filer_pb.ErrNotFound {
		glog.Warningf("filerHealthzHandler FindEntry: %+v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
