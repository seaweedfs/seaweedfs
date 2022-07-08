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
	"sync/atomic"
)

type TrafficLimits map[string]*BucketTrafficLimit

func (tl TrafficLimits) doLimit(r *http.Request, action string, rollbackFns []func()) (bool, []func()) {
	//Count
	countLimitAction := s3_constants.Concat(action, s3_constants.LimitTypeCount)
	if countTraffic, exists := tl[countLimitAction]; exists {
		reject, rollbackFn := countTraffic.loadAndCompare(1)
		if rollbackFn != nil {
			rollbackFns = append(rollbackFns, rollbackFn)
		}
		if reject {
			stats.S3RequestLimitCounter.WithLabelValues(countTraffic.name, countLimitAction, s3_constants.LimitTypeCount)
			return true, rollbackFns
		}
	}

	//Bytes
	bytesLimitAction := s3_constants.Concat(action, s3_constants.LimitTypeMB)
	if bytesTraffic, exists := tl[bytesLimitAction]; exists {
		reject, rollbackFn := bytesTraffic.loadAndCompare(r.ContentLength)
		if rollbackFn != nil {
			rollbackFns = append(rollbackFns, rollbackFn)
		}
		if reject {
			stats.S3RequestLimitCounter.WithLabelValues(bytesTraffic.name, action, s3_constants.LimitTypeMB)
			return true, rollbackFns
		}
	}

	return false, rollbackFns
}

type BucketTrafficLimit struct {
	name  string
	max   int64
	count int64
}

func (btl *BucketTrafficLimit) loadAndCompare(delta int64) (bool, func()) {
	if atomic.LoadInt64(&btl.count)+delta > btl.max {
		return true, nil
	}

	current := atomic.AddInt64(&btl.count, delta)
	rollbackFn := func() {
		atomic.AddInt64(&btl.count, -delta)
	}
	if current > btl.max {
		return true, rollbackFn
	}

	return false, rollbackFn
}

type CircuitBreaker struct {
	Enabled             bool
	globalTrafficLimits TrafficLimits
	bucketTrafficLimits map[string]TrafficLimits
}

func NewCircuitBreaker(option *S3ApiServerOption) *CircuitBreaker {
	cb := &CircuitBreaker{
		globalTrafficLimits: TrafficLimits{},
		bucketTrafficLimits: map[string]TrafficLimits{},
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

	globalTrafficLimits := TrafficLimits{}
	bucketTrafficLimits := make(map[string]TrafficLimits)

	if globalOptions != nil && globalOptions.Enabled {
		globalEnabled = true

		//global
		for action, limit := range globalOptions.Actions {
			value, err := parseLimitValueByType(action, limit)
			if err != nil || value < 0 {
				glog.Warningf("invalid limit config: %v", err)
			}
			globalTrafficLimits[action] = &BucketTrafficLimit{name: "", max: value}
		}

		//buckets
		for bucket, cbOptions := range cfg.Buckets {
			if cbOptions != nil && cbOptions.Enabled {
				trafficLimits := TrafficLimits{}
				for action, limit := range cbOptions.Actions {
					value, err := parseLimitValueByType(action, limit)
					if err != nil || value < 0 {
						glog.Warningf("invalid limit config: %v", err)
					}
					trafficLimits[action] = &BucketTrafficLimit{name: bucket, max: value}
				}
				bucketTrafficLimits[bucket] = trafficLimits
			}
		}
	}

	cb.Enabled = globalEnabled
	cb.globalTrafficLimits = globalTrafficLimits
	cb.bucketTrafficLimits = bucketTrafficLimits
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

func (cb *CircuitBreaker) limit(r *http.Request, bucket string, action string) ([]func(), s3err.ErrorCode) {
	var rollbackFns []func()
	var reject bool

	//bucket
	if trafficLimits, exists := cb.bucketTrafficLimits[bucket]; exists {
		if reject, rollbackFns = trafficLimits.doLimit(r, action, rollbackFns); reject {
			return rollbackFns, s3err.ErrTooManyRequest
		}
	}

	//global
	if cb.globalTrafficLimits != nil {
		if reject, rollbackFns = cb.globalTrafficLimits.doLimit(r, action, rollbackFns); reject {
			return rollbackFns, s3err.ErrTooManyRequest
		}
	}

	return rollbackFns, s3err.ErrNone
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
