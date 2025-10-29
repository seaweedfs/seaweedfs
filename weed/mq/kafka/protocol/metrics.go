package protocol

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks basic request/error/latency statistics for Kafka protocol operations
type Metrics struct {
	// Request counters by API key
	requestCounts map[uint16]*int64
	errorCounts   map[uint16]*int64

	// Latency tracking
	latencySum   map[uint16]*int64 // Total latency in microseconds
	latencyCount map[uint16]*int64 // Number of requests for average calculation

	// Connection metrics
	activeConnections int64
	totalConnections  int64

	// Mutex for map operations
	mu sync.RWMutex

	// Start time for uptime calculation
	startTime time.Time
}

// APIMetrics represents metrics for a specific API
type APIMetrics struct {
	APIKey       uint16  `json:"api_key"`
	APIName      string  `json:"api_name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}

// ConnectionMetrics represents connection-related metrics
type ConnectionMetrics struct {
	ActiveConnections int64     `json:"active_connections"`
	TotalConnections  int64     `json:"total_connections"`
	UptimeSeconds     int64     `json:"uptime_seconds"`
	StartTime         time.Time `json:"start_time"`
}

// MetricsSnapshot represents a complete metrics snapshot
type MetricsSnapshot struct {
	APIs        []APIMetrics      `json:"apis"`
	Connections ConnectionMetrics `json:"connections"`
	Timestamp   time.Time         `json:"timestamp"`
}

// NewMetrics creates a new metrics tracker
func NewMetrics() *Metrics {
	return &Metrics{
		requestCounts: make(map[uint16]*int64),
		errorCounts:   make(map[uint16]*int64),
		latencySum:    make(map[uint16]*int64),
		latencyCount:  make(map[uint16]*int64),
		startTime:     time.Now(),
	}
}

// RecordRequest records a successful request with latency
func (m *Metrics) RecordRequest(apiKey uint16, latency time.Duration) {
	m.ensureCounters(apiKey)

	atomic.AddInt64(m.requestCounts[apiKey], 1)
	atomic.AddInt64(m.latencySum[apiKey], latency.Microseconds())
	atomic.AddInt64(m.latencyCount[apiKey], 1)
}

// RecordError records an error for a specific API
func (m *Metrics) RecordError(apiKey uint16, latency time.Duration) {
	m.ensureCounters(apiKey)

	atomic.AddInt64(m.requestCounts[apiKey], 1)
	atomic.AddInt64(m.errorCounts[apiKey], 1)
	atomic.AddInt64(m.latencySum[apiKey], latency.Microseconds())
	atomic.AddInt64(m.latencyCount[apiKey], 1)
}

// RecordConnection records a new connection
func (m *Metrics) RecordConnection() {
	atomic.AddInt64(&m.activeConnections, 1)
	atomic.AddInt64(&m.totalConnections, 1)
}

// RecordDisconnection records a connection closure
func (m *Metrics) RecordDisconnection() {
	atomic.AddInt64(&m.activeConnections, -1)
}

// GetSnapshot returns a complete metrics snapshot
func (m *Metrics) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	apis := make([]APIMetrics, 0, len(m.requestCounts))

	for apiKey, requestCount := range m.requestCounts {
		requests := atomic.LoadInt64(requestCount)
		errors := atomic.LoadInt64(m.errorCounts[apiKey])
		latencySum := atomic.LoadInt64(m.latencySum[apiKey])
		latencyCount := atomic.LoadInt64(m.latencyCount[apiKey])

		var avgLatencyMs float64
		if latencyCount > 0 {
			avgLatencyMs = float64(latencySum) / float64(latencyCount) / 1000.0 // Convert to milliseconds
		}

		apis = append(apis, APIMetrics{
			APIKey:       apiKey,
			APIName:      getAPIName(APIKey(apiKey)),
			RequestCount: requests,
			ErrorCount:   errors,
			AvgLatencyMs: avgLatencyMs,
		})
	}

	return MetricsSnapshot{
		APIs: apis,
		Connections: ConnectionMetrics{
			ActiveConnections: atomic.LoadInt64(&m.activeConnections),
			TotalConnections:  atomic.LoadInt64(&m.totalConnections),
			UptimeSeconds:     int64(time.Since(m.startTime).Seconds()),
			StartTime:         m.startTime,
		},
		Timestamp: time.Now(),
	}
}

// GetAPIMetrics returns metrics for a specific API
func (m *Metrics) GetAPIMetrics(apiKey uint16) APIMetrics {
	m.ensureCounters(apiKey)

	requests := atomic.LoadInt64(m.requestCounts[apiKey])
	errors := atomic.LoadInt64(m.errorCounts[apiKey])
	latencySum := atomic.LoadInt64(m.latencySum[apiKey])
	latencyCount := atomic.LoadInt64(m.latencyCount[apiKey])

	var avgLatencyMs float64
	if latencyCount > 0 {
		avgLatencyMs = float64(latencySum) / float64(latencyCount) / 1000.0
	}

	return APIMetrics{
		APIKey:       apiKey,
		APIName:      getAPIName(APIKey(apiKey)),
		RequestCount: requests,
		ErrorCount:   errors,
		AvgLatencyMs: avgLatencyMs,
	}
}

// GetConnectionMetrics returns connection-related metrics
func (m *Metrics) GetConnectionMetrics() ConnectionMetrics {
	return ConnectionMetrics{
		ActiveConnections: atomic.LoadInt64(&m.activeConnections),
		TotalConnections:  atomic.LoadInt64(&m.totalConnections),
		UptimeSeconds:     int64(time.Since(m.startTime).Seconds()),
		StartTime:         m.startTime,
	}
}

// Reset resets all metrics (useful for testing)
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for apiKey := range m.requestCounts {
		atomic.StoreInt64(m.requestCounts[apiKey], 0)
		atomic.StoreInt64(m.errorCounts[apiKey], 0)
		atomic.StoreInt64(m.latencySum[apiKey], 0)
		atomic.StoreInt64(m.latencyCount[apiKey], 0)
	}

	atomic.StoreInt64(&m.activeConnections, 0)
	atomic.StoreInt64(&m.totalConnections, 0)
	m.startTime = time.Now()
}

// ensureCounters ensures that counters exist for the given API key
func (m *Metrics) ensureCounters(apiKey uint16) {
	m.mu.RLock()
	if _, exists := m.requestCounts[apiKey]; exists {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if _, exists := m.requestCounts[apiKey]; exists {
		return
	}

	m.requestCounts[apiKey] = new(int64)
	m.errorCounts[apiKey] = new(int64)
	m.latencySum[apiKey] = new(int64)
	m.latencyCount[apiKey] = new(int64)
}

// Global metrics instance
var globalMetrics = NewMetrics()

// GetGlobalMetrics returns the global metrics instance
func GetGlobalMetrics() *Metrics {
	return globalMetrics
}

// RecordRequestMetrics is a convenience function to record request metrics globally
func RecordRequestMetrics(apiKey uint16, latency time.Duration) {
	globalMetrics.RecordRequest(apiKey, latency)
}

// RecordErrorMetrics is a convenience function to record error metrics globally
func RecordErrorMetrics(apiKey uint16, latency time.Duration) {
	globalMetrics.RecordError(apiKey, latency)
}

// RecordConnectionMetrics is a convenience function to record connection metrics globally
func RecordConnectionMetrics() {
	globalMetrics.RecordConnection()
}

// RecordDisconnectionMetrics is a convenience function to record disconnection metrics globally
func RecordDisconnectionMetrics() {
	globalMetrics.RecordDisconnection()
}
