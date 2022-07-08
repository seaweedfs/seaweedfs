package s3api

import (
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

type TestLimitCase struct {
	routineCount int
	successCount int64
	reqBucket    string
	reqAction    string
	config       string
}

var (
	fileSize int64 = 200_000

	TestLimitCases = []*TestLimitCase{
		//global enabled
		{10, 5, "x", "Write",
			`{
				"global": {
					"enabled": true,
					"actions": {
						"Read:Count": "5",
						"Write:Count": "5"
					}
				}
			}`,
		},

		//global disabled
		{10, 10, "x", "Write",
			`{
				"global": {
					"actions": {
						"Read:Count": "5",
						"Write:Count": "5"
					}
				}
			}`,
		},

		//buckets limit disabled due to global disabled though buckets config enabled
		{10, 2, "x", "Write",
			`{
				"global": {
					"enabled": true,
					"actions": {
						"Read:Count": "5",
						"Write:Count": "5"
					}
				},
				"buckets": {
					"x": {
						"enabled": true,
						"actions": {
							"Read:Count": "2",
							"Write:Count": "2"
						}
					}
				}
			}`,
		},

		{10, 5, "x", "Write",
			`{
				"global": {
					"enabled": true,
					"actions": {
						"Read:Count": "5",
						"Write:Count": "5"
					}
				}
			}`,
		},

		{10, 2, "x", "Write",
			`{
				"global": {
					"enabled": true,
					"actions": {
						"Read:Count": "5",
						"Write:Count": "5"
					}
				},
				"buckets": {
					"x": {
						"enabled": true,
						"actions": {
							"Read:Count": "2",
							"Write:Count": "2"
						}
					}
				}
			}`,
		},

		{10, 5, "y", "Write",
			`{
				"global": {
					"enabled": true,
					"actions": {
						"Read:Count": "5",
						"Write:Count": "5"
					}
				},
				"buckets": {
					"x": {
						"enabled": true,
						"actions": {
							"Read:Count": "2",
							"Write:Count": "2"
						}
					}
				}
			}`,
		},

		{10, 6, "y", "Read",
			`{
			  "global": {
				"enabled": true,
				"actions": {
				  "Read:MB": "1.2",
				  "Write:Count": "50"
				}
			  },
			  "buckets": {
				"y": {
				  "enabled": true
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:MB": "0.6",
					"Write:MB": "0.6"
				  }
				}
			  }
			}`,
		},

		{10, 3, "z", "Write",
			`{
			  "global": {
				"enabled": true,
				"actions": {
				  "Read:MB": "1.2",
				  "Write:Count": "50"
				}
			  },
			  "buckets": {
				"y": {
				  "enabled": true
				},
				"z": {
				  "enabled": true,
				  "actions": {
					"Read:MB": "0.6",
					"Write:MB": "0.6"
				  }
				}
			  }
			}`,
		},

		{10, 10, "y", "Write",
			`{

			}`,
		},
	}
)

func TestLimit(t *testing.T) {
	circuitBreaker := &CircuitBreaker{
		globalTrafficLimits: TrafficLimits{},
		bucketTrafficLimits: map[string]TrafficLimits{},
	}

	for _, tc := range TestLimitCases {
		err := circuitBreaker.LoadS3ApiConfigurationFromBytes([]byte(tc.config))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 2; i++ {
			successCount := doLimit(circuitBreaker, tc)
			if successCount != tc.successCount {
				t.Fatalf("successCount not equal, expect=%d, actual=%d, case: %v", tc.successCount, successCount, tc)
			}
		}
	}
}

func doLimit(circuitBreaker *CircuitBreaker, tc *TestLimitCase) int64 {
	var successCounter int64
	resultCh := make(chan []func(), tc.routineCount)
	var wg sync.WaitGroup
	for i := 0; i < tc.routineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var rollbackFn []func()
			errCode := s3err.ErrNone
			if circuitBreaker.Enabled {
				rollbackFn, errCode = circuitBreaker.limit(&http.Request{ContentLength: fileSize}, tc.reqBucket, tc.reqAction)
			}
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
