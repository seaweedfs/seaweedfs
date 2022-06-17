package s3api

import (
	"github.com/chrislusf/seaweedfs/weed/pb/s3_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"go.uber.org/atomic"
	"net/http"
	"sync"
	"testing"
)

type TestLimitCase struct {
	actionName       string
	limitType        string
	bucketLimitValue int64
	globalLimitValue int64

	routineCount int
	reqBytes     int64

	successCount int64
}

var (
	bucket         = "/test"
	action         = s3_constants.ACTION_READ
	TestLimitCases = []*TestLimitCase{
		{action, s3_constants.LimitTypeCount, 5, 5, 6, 1024, 5},
		{action, s3_constants.LimitTypeCount, 6, 6, 6, 1024, 6},
		{action, s3_constants.LimitTypeCount, 5, 6, 6, 1024, 5},
		{action, s3_constants.LimitTypeBytes, 1024, 1024, 6, 200, 5},
		{action, s3_constants.LimitTypeBytes, 1200, 1200, 6, 200, 6},
		{action, s3_constants.LimitTypeBytes, 11990, 11990, 60, 200, 59},
		{action, s3_constants.LimitTypeBytes, 11790, 11990, 60, 200, 58},
	}
)

func TestLimit(t *testing.T) {
	for _, tc := range TestLimitCases {
		circuitBreakerConfig := &s3_pb.S3CircuitBreakerConfig{
			Global: &s3_pb.S3CircuitBreakerOptions{
				Enabled: true,
				Actions: map[string]int64{
					s3_constants.Concat(tc.actionName, tc.limitType): tc.globalLimitValue,
					s3_constants.Concat(tc.actionName, tc.limitType): tc.globalLimitValue,
				},
			},
			Buckets: map[string]*s3_pb.S3CircuitBreakerOptions{
				bucket: {
					Enabled: true,
					Actions: map[string]int64{
						s3_constants.Concat(tc.actionName, tc.limitType): tc.bucketLimitValue,
					},
				},
			},
		}
		circuitBreaker := &CircuitBreaker{
			counters:    make(map[string]*atomic.Int64),
			limitations: make(map[string]int64),
		}
		err := circuitBreaker.loadCircuitBreakerConfig(circuitBreakerConfig)
		if err != nil {
			t.Fatal(err)
		}

		successCount := doLimit(circuitBreaker, tc.routineCount, &http.Request{ContentLength: tc.reqBytes})
		if successCount != tc.successCount {
			t.Errorf("successCount not equal, expect=%d, actual=%d", tc.successCount, successCount)
		}
	}
}

func doLimit(circuitBreaker *CircuitBreaker, routineCount int, r *http.Request) int64 {
	var successCounter atomic.Int64
	resultCh := make(chan []func(), routineCount)
	var wg sync.WaitGroup
	for i := 0; i < routineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rollbackFn, errCode := circuitBreaker.limit(r, bucket, action)
			if errCode == s3err.ErrNone {
				successCounter.Inc()
			}
			resultCh <- rollbackFn
		}()
	}
	wg.Wait()
	close(resultCh)
	for fns := range resultCh {
		for _, fn := range fns {
			fn()
		}
	}
	return successCounter.Load()
}
