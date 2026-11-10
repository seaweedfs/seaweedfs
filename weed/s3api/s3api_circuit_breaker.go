package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type CircuitBreaker struct {
	sync.RWMutex
	Enabled     bool
	counters    map[string]*int64
	limitations map[string]int64
	s3a         *S3ApiServer

	// Interceptor, if set, wraps the per-route handler ahead of ALL
	// circuit-breaker logic (upload limiting and the breaker checks) and runs
	// even when the breaker is disabled. It is a general per-request
	// interceptor seam: an implementation may reject the request (write its own
	// response and not call next) or wrap next to observe/shape it. Nil by
	// default, so it is a no-op unless explicitly set.
	Interceptor func(next http.HandlerFunc, action string) http.HandlerFunc
}

func NewCircuitBreaker(option *S3ApiServerOption) *CircuitBreaker {
	cb := &CircuitBreaker{
		counters:    make(map[string]*int64),
		limitations: make(map[string]int64),
	}

	// Use WithOneOfGrpcFilerClients to support multiple filers with failover
	err := pb.WithOneOfGrpcFilerClients(false, option.Filers, option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(context.Background(), client, s3_constants.CircuitBreakerConfigDir, s3_constants.CircuitBreakerConfigFile)
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read S3 circuit breaker config: %w", err)
		}
		return cb.LoadS3ApiConfigurationFromBytes(content)
	})

	if err != nil {
		glog.Warningf("S3 circuit breaker disabled; failed to load config from any filer: %v", err)
	}

	return cb
}

func (cb *CircuitBreaker) LoadS3ApiConfigurationFromBytes(content []byte) error {
	cbCfg := &s3_pb.S3CircuitBreakerConfig{}
	if err := filer.ParseS3ConfigurationFromBytes(content, cbCfg); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return fmt.Errorf("unmarshal error: %w", err)
	}
	if err := cb.loadCircuitBreakerConfig(cbCfg); err != nil {
		return err
	}
	return nil
}

func (cb *CircuitBreaker) loadCircuitBreakerConfig(cfg *s3_pb.S3CircuitBreakerConfig) error {

	//global
	globalEnabled := false
	globalOptions := cfg.Global
	limitations := make(map[string]int64)
	if globalOptions != nil && globalOptions.Enabled && len(globalOptions.Actions) > 0 {
		globalEnabled = globalOptions.Enabled
		for action, limit := range globalOptions.Actions {
			limitations[action] = limit
		}
	}
	cb.Enabled = globalEnabled

	//buckets
	for bucket, cbOptions := range cfg.Buckets {
		if cbOptions.Enabled {
			for action, limit := range cbOptions.Actions {
				limitations[s3_constants.Concat(bucket, action)] = limit
			}
		}
	}

	cb.limitations = limitations
	return nil
}

func (cb *CircuitBreaker) Limit(f func(w http.ResponseWriter, r *http.Request), action string) (http.HandlerFunc, Action) {
	inner := func(w http.ResponseWriter, r *http.Request) {
		// Apply upload limiting for write actions if configured
		if cb.s3a != nil && (action == s3_constants.ACTION_WRITE) &&
			(cb.s3a.option.ConcurrentUploadLimit != 0 || cb.s3a.option.ConcurrentFileUploadLimit != 0) {

			// Get content length, default to 0 if not provided
			contentLength := r.ContentLength
			if contentLength < 0 {
				contentLength = 0
			}

			if !cb.waitForUploadSlot(w, r, contentLength) {
				return
			}
			defer func() {
				cb.s3a.inFlightDataLimitCond.L.Lock()
				defer cb.s3a.inFlightDataLimitCond.L.Unlock()
				// Decrement counters
				newUploads := atomic.AddInt64(&cb.s3a.inFlightUploads, -1)
				newSize := atomic.AddInt64(&cb.s3a.inFlightDataSize, -contentLength)
				// Update metrics
				stats.S3InFlightUploadCountGauge.Set(float64(newUploads))
				stats.S3InFlightUploadBytesGauge.Set(float64(newSize))
				cb.s3a.inFlightDataLimitCond.Broadcast()
			}()
		}

		// Apply circuit breaker logic
		if !cb.Enabled {
			f(w, r)
			return
		}

		vars := mux.Vars(r)
		bucket := vars["bucket"]

		rollback, errCode := cb.limit(r, bucket, action)
		defer func() {
			for _, rf := range rollback {
				rf()
			}
		}()

		if errCode == s3err.ErrNone {
			f(w, r)
			return
		}
		s3err.WriteErrorResponse(w, r, errCode)
	}

	// The interceptor is consulted per request rather than captured here, so it
	// can be installed after the routes are registered (e.g. once the server and
	// its dependencies are constructed) and so a nil CircuitBreaker is never
	// dereferenced at registration time. When unset this is just a nil check.
	// It runs outermost: before upload limiting and the breaker checks, and
	// regardless of cb.Enabled.
	return func(w http.ResponseWriter, r *http.Request) {
		if cb.Interceptor != nil {
			cb.Interceptor(inner, action)(w, r)
			return
		}
		inner(w, r)
	}, Action(action)
}

func (cb *CircuitBreaker) waitForUploadSlot(w http.ResponseWriter, r *http.Request, contentLength int64) bool {
	s3a := cb.s3a
	s3a.inFlightDataLimitCond.L.Lock()
	err := util.WaitUntil(r.Context(), s3a.inFlightDataLimitCond, s3a.option.InflightUploadDataTimeout, func() bool {
		inFlightDataSize := atomic.LoadInt64(&s3a.inFlightDataSize)
		if s3a.option.ConcurrentUploadLimit != 0 && inFlightDataSize > s3a.option.ConcurrentUploadLimit {
			glog.V(4).Infof("wait because inflight data %d > %d", inFlightDataSize, s3a.option.ConcurrentUploadLimit)
			return false
		}
		inFlightUploads := atomic.LoadInt64(&s3a.inFlightUploads)
		if s3a.option.ConcurrentFileUploadLimit != 0 && inFlightUploads >= s3a.option.ConcurrentFileUploadLimit {
			glog.V(4).Infof("wait because inflight uploads %d >= %d", inFlightUploads, s3a.option.ConcurrentFileUploadLimit)
			return false
		}
		return true
	})
	if err == nil {
		// Increment counters
		newUploads := atomic.AddInt64(&s3a.inFlightUploads, 1)
		newSize := atomic.AddInt64(&s3a.inFlightDataSize, contentLength)
		// Update metrics
		stats.S3InFlightUploadCountGauge.Set(float64(newUploads))
		stats.S3InFlightUploadBytesGauge.Set(float64(newSize))
	}
	s3a.inFlightDataLimitCond.L.Unlock()

	if err == nil {
		return true
	}
	// Reject without reading the body: draining it would send an
	// Expect: 100-continue client the go-ahead, and closing an HTTP/1
	// connection stops net/http from reading it before the reply.
	if r.ProtoMajor == 1 {
		w.Header().Set("Connection", "close")
	}
	rejected := r.WithContext(r.Context())
	rejected.Body = http.NoBody
	if r.Context().Err() != nil {
		glog.V(1).Infof("upload from %s cancelled while waiting for an upload slot: %v", r.RemoteAddr, err)
		s3err.WriteErrorResponse(w, rejected, s3err.ErrClientDisconnected)
		return false
	}
	glog.V(1).Infof("reject upload from %s: no upload slot within %v", r.RemoteAddr, s3a.option.InflightUploadDataTimeout)
	s3err.WriteErrorResponse(w, rejected, s3err.ErrTooManyRequest)
	return false
}

func (cb *CircuitBreaker) limit(r *http.Request, bucket string, action string) (rollback []func(), errCode s3err.ErrorCode) {

	//bucket simultaneous request count
	bucketCountRollBack, errCode := cb.loadCounterAndCompare(s3_constants.Concat(bucket, action, s3_constants.LimitTypeCount), 1, s3err.ErrTooManyRequest)
	if bucketCountRollBack != nil {
		rollback = append(rollback, bucketCountRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}

	//bucket simultaneous request content bytes
	bucketContentLengthRollBack, errCode := cb.loadCounterAndCompare(s3_constants.Concat(bucket, action, s3_constants.LimitTypeBytes), r.ContentLength, s3err.ErrRequestBytesExceed)
	if bucketContentLengthRollBack != nil {
		rollback = append(rollback, bucketContentLengthRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}

	//global simultaneous request count
	globalCountRollBack, errCode := cb.loadCounterAndCompare(s3_constants.Concat(action, s3_constants.LimitTypeCount), 1, s3err.ErrTooManyRequest)
	if globalCountRollBack != nil {
		rollback = append(rollback, globalCountRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}

	//global simultaneous request content bytes
	globalContentLengthRollBack, errCode := cb.loadCounterAndCompare(s3_constants.Concat(action, s3_constants.LimitTypeBytes), r.ContentLength, s3err.ErrRequestBytesExceed)
	if globalContentLengthRollBack != nil {
		rollback = append(rollback, globalContentLengthRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}
	return
}

func (cb *CircuitBreaker) loadCounterAndCompare(key string, inc int64, errCode s3err.ErrorCode) (f func(), e s3err.ErrorCode) {
	e = s3err.ErrNone
	if max, ok := cb.limitations[key]; ok {
		cb.RLock()
		counter, exists := cb.counters[key]
		cb.RUnlock()

		if !exists {
			cb.Lock()
			counter, exists = cb.counters[key]
			if !exists {
				var newCounter int64
				counter = &newCounter
				cb.counters[key] = counter
			}
			cb.Unlock()
		}
		current := atomic.LoadInt64(counter)
		if current+inc > max {
			e = errCode
			return
		} else {
			current := atomic.AddInt64(counter, inc)
			f = func() {
				atomic.AddInt64(counter, -inc)
			}
			if current > max {
				e = errCode
				return
			}
		}
	}
	return
}

func (s3a *S3ApiServer) Limit(handler http.HandlerFunc, action string) http.HandlerFunc {
	if s3a.cb == nil {
		return handler
	}
	limited, _ := s3a.cb.Limit(handler, action)
	return limited
}
