package s3api

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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

// TestLimitInterceptor verifies the optional request interceptor: it is a no-op
// when nil, runs ahead of the (disabled) breaker logic, and can either reject a
// request or pass it through to the wrapped handler.
func TestLimitInterceptor(t *testing.T) {
	readAction := s3_constants.ACTION_READ
	newCB := func() *CircuitBreaker {
		// Enabled defaults to false and s3a is nil, so without an interceptor
		// Limit's handler falls straight through to the wrapped handler.
		return &CircuitBreaker{counters: make(map[string]*int64), limitations: make(map[string]int64)}
	}

	// 1. nil interceptor must not change behavior: the handler still runs.
	t.Run("nil interceptor is a no-op", func(t *testing.T) {
		cb := newCB()
		called := false
		h, _ := cb.Limit(func(w http.ResponseWriter, r *http.Request) { called = true }, readAction)
		h(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/object", nil))
		if !called {
			t.Fatal("handler should run when no interceptor is set")
		}
	})

	// 2. a rejecting interceptor runs first and prevents the handler, even
	//    though the breaker itself is disabled.
	t.Run("interceptor can reject", func(t *testing.T) {
		cb := newCB()
		var order []string
		cb.Interceptor = func(next http.HandlerFunc, action string) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				order = append(order, "interceptor")
				s3err.WriteErrorResponse(w, r, s3err.ErrTooManyRequest) // reject; do not call next
			}
		}
		handlerRan := false
		h, _ := cb.Limit(func(w http.ResponseWriter, r *http.Request) {
			handlerRan = true
			order = append(order, "handler")
		}, readAction)
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/bucket/object", nil))
		if handlerRan {
			t.Fatal("a rejecting interceptor must prevent the handler from running")
		}
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 from ErrTooManyRequest, got %d", rec.Code)
		}
		if len(order) != 1 || order[0] != "interceptor" {
			t.Fatalf("interceptor should run alone, got %v", order)
		}
	})

	// 3. a pass-through interceptor runs before the handler and sees the action.
	t.Run("interceptor can pass through", func(t *testing.T) {
		cb := newCB()
		var order []string
		var seenAction string
		cb.Interceptor = func(next http.HandlerFunc, action string) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				seenAction = action
				order = append(order, "interceptor")
				next(w, r)
			}
		}
		h, _ := cb.Limit(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "handler")
		}, readAction)
		h(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/object", nil))
		if seenAction != readAction {
			t.Fatalf("interceptor should receive the route action, got %q", seenAction)
		}
		if len(order) != 2 || order[0] != "interceptor" || order[1] != "handler" {
			t.Fatalf("interceptor must run before the handler, got %v", order)
		}
	})

	// 4. installed AFTER Limit() returns: still takes effect, because the
	//    interceptor is consulted per request rather than captured at
	//    registration time (the handlers are built during router registration,
	//    before dependencies that need the running server exist).
	t.Run("interceptor installed after registration takes effect", func(t *testing.T) {
		cb := newCB()
		h, _ := cb.Limit(func(w http.ResponseWriter, r *http.Request) {}, readAction) // built while nil
		ran := false
		cb.Interceptor = func(next http.HandlerFunc, action string) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				ran = true
				next(w, r)
			}
		}
		h(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/object", nil))
		if !ran {
			t.Fatal("interceptor set after Limit() must still run (request-time evaluation)")
		}
	})
}
