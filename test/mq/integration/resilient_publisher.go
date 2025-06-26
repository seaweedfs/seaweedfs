package integration

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResilientPublisher wraps TopicPublisher with enhanced error handling
type ResilientPublisher struct {
	publisher *pub_client.TopicPublisher
	config    *PublisherTestConfig
	suite     *IntegrationTestSuite

	// Error tracking
	connectionErrors  int64
	applicationErrors int64
	retryAttempts     int64
	totalPublishes    int64

	// Retry configuration
	maxRetries    int
	baseDelay     time.Duration
	maxDelay      time.Duration
	backoffFactor float64

	// Circuit breaker
	circuitOpen     bool
	circuitOpenTime time.Time
	circuitTimeout  time.Duration

	mu sync.RWMutex
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxRetries     int
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	BackoffFactor  float64
	CircuitTimeout time.Duration
}

// DefaultRetryConfig returns sensible defaults for retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:     5,
		BaseDelay:      10 * time.Millisecond,
		MaxDelay:       5 * time.Second,
		BackoffFactor:  2.0,
		CircuitTimeout: 30 * time.Second,
	}
}

// NewResilientPublisher creates a new resilient publisher
func (its *IntegrationTestSuite) CreateResilientPublisher(config *PublisherTestConfig, retryConfig *RetryConfig) (*ResilientPublisher, error) {
	if retryConfig == nil {
		retryConfig = DefaultRetryConfig()
	}

	publisher, err := its.CreatePublisher(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create base publisher: %v", err)
	}

	return &ResilientPublisher{
		publisher:      publisher,
		config:         config,
		suite:          its,
		maxRetries:     retryConfig.MaxRetries,
		baseDelay:      retryConfig.BaseDelay,
		maxDelay:       retryConfig.MaxDelay,
		backoffFactor:  retryConfig.BackoffFactor,
		circuitTimeout: retryConfig.CircuitTimeout,
	}, nil
}

// PublishWithRetry publishes a message with retry logic and error handling
func (rp *ResilientPublisher) PublishWithRetry(key, value []byte) error {
	atomic.AddInt64(&rp.totalPublishes, 1)

	// Check circuit breaker
	if rp.isCircuitOpen() {
		atomic.AddInt64(&rp.applicationErrors, 1)
		return fmt.Errorf("circuit breaker is open")
	}

	var lastErr error
	for attempt := 0; attempt <= rp.maxRetries; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&rp.retryAttempts, 1)
			delay := rp.calculateDelay(attempt)
			glog.V(1).Infof("Retrying publish after %v (attempt %d/%d)", delay, attempt, rp.maxRetries)
			time.Sleep(delay)
		}

		err := rp.publisher.Publish(key, value)
		if err == nil {
			// Success - reset circuit breaker if it was open
			rp.resetCircuitBreaker()
			return nil
		}

		lastErr = err

		// Classify error type
		if rp.isConnectionError(err) {
			atomic.AddInt64(&rp.connectionErrors, 1)
			glog.V(1).Infof("Connection error on attempt %d: %v", attempt+1, err)

			// For connection errors, try to recreate the publisher
			if attempt < rp.maxRetries {
				if recreateErr := rp.recreatePublisher(); recreateErr != nil {
					glog.Warningf("Failed to recreate publisher: %v", recreateErr)
				}
			}
			continue
		} else {
			// Application error - don't retry
			atomic.AddInt64(&rp.applicationErrors, 1)
			glog.Warningf("Application error (not retrying): %v", err)
			break
		}
	}

	// All retries exhausted or non-retryable error
	rp.openCircuitBreaker()
	return fmt.Errorf("publish failed after %d attempts, last error: %v", rp.maxRetries+1, lastErr)
}

// PublishRecord publishes a record with retry logic
func (rp *ResilientPublisher) PublishRecord(key []byte, record *schema_pb.RecordValue) error {
	atomic.AddInt64(&rp.totalPublishes, 1)

	if rp.isCircuitOpen() {
		atomic.AddInt64(&rp.applicationErrors, 1)
		return fmt.Errorf("circuit breaker is open")
	}

	var lastErr error
	for attempt := 0; attempt <= rp.maxRetries; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&rp.retryAttempts, 1)
			delay := rp.calculateDelay(attempt)
			time.Sleep(delay)
		}

		err := rp.publisher.PublishRecord(key, record)
		if err == nil {
			rp.resetCircuitBreaker()
			return nil
		}

		lastErr = err

		if rp.isConnectionError(err) {
			atomic.AddInt64(&rp.connectionErrors, 1)
			if attempt < rp.maxRetries {
				if recreateErr := rp.recreatePublisher(); recreateErr != nil {
					glog.Warningf("Failed to recreate publisher: %v", recreateErr)
				}
			}
			continue
		} else {
			atomic.AddInt64(&rp.applicationErrors, 1)
			break
		}
	}

	rp.openCircuitBreaker()
	return fmt.Errorf("publish record failed after %d attempts, last error: %v", rp.maxRetries+1, lastErr)
}

// recreatePublisher attempts to recreate the underlying publisher
func (rp *ResilientPublisher) recreatePublisher() error {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Shutdown old publisher
	if rp.publisher != nil {
		rp.publisher.Shutdown()
	}

	// Create new publisher
	newPublisher, err := rp.suite.CreatePublisher(rp.config)
	if err != nil {
		return fmt.Errorf("failed to recreate publisher: %v", err)
	}

	rp.publisher = newPublisher
	glog.V(1).Infof("Successfully recreated publisher")
	return nil
}

// isConnectionError determines if an error is a connection-level error that should be retried
func (rp *ResilientPublisher) isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Check for gRPC status codes
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown:
			return true
		}
	}

	// Check for common connection error strings
	connectionErrorPatterns := []string{
		"EOF",
		"error reading server preface",
		"connection refused",
		"connection reset",
		"broken pipe",
		"network is unreachable",
		"no route to host",
		"transport is closing",
		"connection error",
		"dial tcp",
		"context deadline exceeded",
	}

	for _, pattern := range connectionErrorPatterns {
		if contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// calculateDelay calculates exponential backoff delay
func (rp *ResilientPublisher) calculateDelay(attempt int) time.Duration {
	delay := float64(rp.baseDelay) * math.Pow(rp.backoffFactor, float64(attempt-1))
	if delay > float64(rp.maxDelay) {
		delay = float64(rp.maxDelay)
	}
	return time.Duration(delay)
}

// Circuit breaker methods
func (rp *ResilientPublisher) isCircuitOpen() bool {
	rp.mu.RLock()
	defer rp.mu.RUnlock()

	if !rp.circuitOpen {
		return false
	}

	// Check if circuit should be reset
	if time.Since(rp.circuitOpenTime) > rp.circuitTimeout {
		return false
	}

	return true
}

func (rp *ResilientPublisher) openCircuitBreaker() {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.circuitOpen = true
	rp.circuitOpenTime = time.Now()
	glog.Warningf("Circuit breaker opened due to repeated failures")
}

func (rp *ResilientPublisher) resetCircuitBreaker() {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if rp.circuitOpen {
		rp.circuitOpen = false
		glog.V(1).Infof("Circuit breaker reset")
	}
}

// GetErrorStats returns error statistics
func (rp *ResilientPublisher) GetErrorStats() ErrorStats {
	return ErrorStats{
		ConnectionErrors:  atomic.LoadInt64(&rp.connectionErrors),
		ApplicationErrors: atomic.LoadInt64(&rp.applicationErrors),
		RetryAttempts:     atomic.LoadInt64(&rp.retryAttempts),
		TotalPublishes:    atomic.LoadInt64(&rp.totalPublishes),
		CircuitOpen:       rp.isCircuitOpen(),
	}
}

// ErrorStats holds error statistics
type ErrorStats struct {
	ConnectionErrors  int64
	ApplicationErrors int64
	RetryAttempts     int64
	TotalPublishes    int64
	CircuitOpen       bool
}

// Shutdown gracefully shuts down the resilient publisher
func (rp *ResilientPublisher) Shutdown() error {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if rp.publisher != nil {
		return rp.publisher.Shutdown()
	}
	return nil
}
