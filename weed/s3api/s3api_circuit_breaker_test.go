package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

type TestLimitCase struct {
	actionName string

	limitType        string
	bucketLimitValue int64
	globalLimitValue int64

	routineCount int
	successCount int64
}

var (
	bucket         = "/test"
	action         = s3_constants.ACTION_WRITE
	fileSize int64 = 200

	TestLimitCases = []*TestLimitCase{

		//bucket-LimitTypeCount
		{action, s3_constants.LimitTypeCount, 5, 6, 60, 5},
		{action, s3_constants.LimitTypeCount, 0, 6, 6, 0},

		//global-LimitTypeCount
		{action, s3_constants.LimitTypeCount, 6, 5, 6, 5},
		{action, s3_constants.LimitTypeCount, 6, 0, 6, 0},

		//bucket-LimitTypeBytes
		{action, s3_constants.LimitTypeBytes, 1000, 1020, 6, 5},
		{action, s3_constants.LimitTypeBytes, 0, 1020, 6, 0},

		//global-LimitTypeBytes
		{action, s3_constants.LimitTypeBytes, 1020, 1000, 6, 5},
		{action, s3_constants.LimitTypeBytes, 1020, 0, 6, 0},
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
			counters:    make(map[string]*int64),
			limitations: make(map[string]int64),
		}
		err := circuitBreaker.loadCircuitBreakerConfig(circuitBreakerConfig)
		if err != nil {
			t.Fatal(err)
		}

		successCount := doLimit(circuitBreaker, tc.routineCount, &http.Request{ContentLength: fileSize}, tc.actionName)
		if successCount != tc.successCount {
			t.Errorf("successCount not equal, expect=%d, actual=%d, case: %v", tc.successCount, successCount, tc)
		}
	}
}

func doLimit(circuitBreaker *CircuitBreaker, routineCount int, r *http.Request, action string) int64 {
	var successCounter int64
	resultCh := make(chan []func(), routineCount)
	var wg sync.WaitGroup
	for i := 0; i < routineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rollbackFn, errCode := circuitBreaker.limit(r, bucket, action)
			if errCode == s3err.ErrNone {
				atomic.AddInt64(&successCounter, 1)
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
	return successCounter
}
