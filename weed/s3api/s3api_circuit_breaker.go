package s3api

import (
	"errors"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/s3_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"strings"
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

	err := pb.WithFilerClient(false, option.Filer, option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(client, s3_constants.CircuitBreakerConfigDir, s3_constants.CircuitBreakerConfigFile)
		if err != nil {
			return fmt.Errorf("read S3 circuit breaker config: %v", err)
		}
		return cb.LoadS3ApiConfigurationFromBytes(content)
	})

	if err != nil {
		glog.Infof("s3 circuit breaker not configured: %v", err)
	}

	return cb
}

func (cb *CircuitBreaker) LoadS3ApiConfigurationFromBytes(content []byte) error {
	glog.V(1).Infof("load s3 circuit breaker config: %s", string(content))
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
	globalEnabled := false
	globalOptions := cfg.Global
	limitations := make(map[string]int64)
	if globalOptions != nil && globalOptions.Enabled {
		globalEnabled = true

		//global
		for action, limit := range globalOptions.Actions {
			value, err := parseLimitValueByType(action, limit)
			if err != nil || value < 0 {
				glog.Warningf("invalid limit config: %v", err)
			}
			limitations[action] = value
		}

		//buckets
		for bucket, cbOptions := range cfg.Buckets {
			if cbOptions != nil && cbOptions.Enabled {
				for action, limit := range cbOptions.Actions {
					value, err := parseLimitValueByType(action, limit)
					if err != nil || value < 0 {
						glog.Warningf("invalid limit config: %v", err)
					}
					limitations[s3_constants.Concat(bucket, action)] = value
				}
			}
		}
	}
	cb.Enabled = globalEnabled
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

		action, rollback, errCode := cb.limit(r, bucket, action)
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
		stats.S3RequestLimitCounter.WithLabelValues(action).Inc()
	}, Action(action)
}

func (cb *CircuitBreaker) limit(r *http.Request, bucket string, action string) (limitAction string, rollback []func(), errCode s3err.ErrorCode) {

	//bucket simultaneous request count
	limitAction = s3_constants.Concat(bucket, action, s3_constants.LimitTypeCount)
	bucketCountRollBack, errCode := cb.loadCounterAndCompare(limitAction, 1, s3err.ErrTooManyRequest)
	if bucketCountRollBack != nil {
		rollback = append(rollback, bucketCountRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}

	//bucket simultaneous request content bytes
	limitAction = s3_constants.Concat(bucket, action, s3_constants.LimitTypeMB)
	bucketContentLengthRollBack, errCode := cb.loadCounterAndCompare(limitAction, r.ContentLength, s3err.ErrRequestBytesExceed)
	if bucketContentLengthRollBack != nil {
		rollback = append(rollback, bucketContentLengthRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}

	//global simultaneous request count
	limitAction = s3_constants.Concat(action, s3_constants.LimitTypeCount)
	globalCountRollBack, errCode := cb.loadCounterAndCompare(limitAction, 1, s3err.ErrTooManyRequest)
	if globalCountRollBack != nil {
		rollback = append(rollback, globalCountRollBack)
	}
	if errCode != s3err.ErrNone {
		return
	}

	//global simultaneous request content bytes
	limitAction = s3_constants.Concat(action, s3_constants.LimitTypeMB)
	globalContentLengthRollBack, errCode := cb.loadCounterAndCompare(limitAction, r.ContentLength, s3err.ErrRequestBytesExceed)
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

func parseLimitValueByType(action string, valueStr string) (int64, error) {
	if strings.HasSuffix(action, s3_constants.LimitTypeCount) {
		v, err := strconv.Atoi(valueStr)
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	} else if strings.HasSuffix(action, s3_constants.LimitTypeMB) {
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return 0, err
		}
		return int64(v * 1024 * 1024), nil
	} else {
		return 0, errors.New("unknown action with limit type: " + action)
	}
}
