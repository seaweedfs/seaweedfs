package s3api

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"net/http"
	"sync"
	"sync/atomic"
)

type CircuitBreaker struct {
	sync.RWMutex
	Enabled     bool
	counters    map[string]*int64
	limitations map[string]int64
}

func NewCircuitBreaker(option *S3ApiServerOption) *CircuitBreaker {
	cb := &CircuitBreaker{
		counters:    make(map[string]*int64),
		limitations: make(map[string]int64),
	}

	err := pb.WithFilerClient(false, 0, option.Filer, option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(client, s3_constants.CircuitBreakerConfigDir, s3_constants.CircuitBreakerConfigFile)
		if errors.Is(err, filer_pb.ErrNotFound) {
			glog.Infof("s3 circuit breaker not configured")
			return nil
		}
		if err != nil {
			return fmt.Errorf("read S3 circuit breaker config: %v", err)
		}
		return cb.LoadS3ApiConfigurationFromBytes(content)
	})

	if err != nil {
		glog.Infof("s3 circuit breaker not configured correctly: %v", err)
	}

	return cb
}

func (cb *CircuitBreaker) LoadS3ApiConfigurationFromBytes(content []byte) error {
	cbCfg := &s3_pb.S3CircuitBreakerConfig{}
	if err := filer.ParseS3ConfigurationFromBytes(content, cbCfg); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return fmt.Errorf("unmarshal error: %v", err)
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
	return func(w http.ResponseWriter, r *http.Request) {
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
	}, Action(action)
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
