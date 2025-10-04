package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector handles metrics collection for the load test
type Collector struct {
	// Atomic counters for thread-safe operations
	messagesProduced int64
	messagesConsumed int64
	bytesProduced    int64
	bytesConsumed    int64
	producerErrors   int64
	consumerErrors   int64

	// Latency tracking
	latencies    []time.Duration
	latencyMutex sync.RWMutex

	// Consumer lag tracking
	consumerLag      map[string]int64
	consumerLagMutex sync.RWMutex

	// Test timing
	startTime time.Time

	// Prometheus metrics
	prometheusMetrics *PrometheusMetrics
}

// PrometheusMetrics holds all Prometheus metric definitions
type PrometheusMetrics struct {
	MessagesProducedTotal prometheus.Counter
	MessagesConsumedTotal prometheus.Counter
	BytesProducedTotal    prometheus.Counter
	BytesConsumedTotal    prometheus.Counter
	ProducerErrorsTotal   prometheus.Counter
	ConsumerErrorsTotal   prometheus.Counter

	MessageLatencyHistogram prometheus.Histogram
	ProducerThroughput      prometheus.Gauge
	ConsumerThroughput      prometheus.Gauge
	ConsumerLagGauge        *prometheus.GaugeVec

	ActiveProducers prometheus.Gauge
	ActiveConsumers prometheus.Gauge
}

// NewCollector creates a new metrics collector
func NewCollector() *Collector {
	return &Collector{
		startTime:   time.Now(),
		consumerLag: make(map[string]int64),
		prometheusMetrics: &PrometheusMetrics{
			MessagesProducedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_loadtest_messages_produced_total",
				Help: "Total number of messages produced",
			}),
			MessagesConsumedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_loadtest_messages_consumed_total",
				Help: "Total number of messages consumed",
			}),
			BytesProducedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_loadtest_bytes_produced_total",
				Help: "Total bytes produced",
			}),
			BytesConsumedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_loadtest_bytes_consumed_total",
				Help: "Total bytes consumed",
			}),
			ProducerErrorsTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_loadtest_producer_errors_total",
				Help: "Total number of producer errors",
			}),
			ConsumerErrorsTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_loadtest_consumer_errors_total",
				Help: "Total number of consumer errors",
			}),
			MessageLatencyHistogram: promauto.NewHistogram(prometheus.HistogramOpts{
				Name:    "kafka_loadtest_message_latency_seconds",
				Help:    "Message end-to-end latency in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~32s
			}),
			ProducerThroughput: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "kafka_loadtest_producer_throughput_msgs_per_sec",
				Help: "Current producer throughput in messages per second",
			}),
			ConsumerThroughput: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "kafka_loadtest_consumer_throughput_msgs_per_sec",
				Help: "Current consumer throughput in messages per second",
			}),
			ConsumerLagGauge: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "kafka_loadtest_consumer_lag_messages",
				Help: "Consumer lag in messages",
			}, []string{"consumer_group", "topic", "partition"}),
			ActiveProducers: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "kafka_loadtest_active_producers",
				Help: "Number of active producers",
			}),
			ActiveConsumers: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "kafka_loadtest_active_consumers",
				Help: "Number of active consumers",
			}),
		},
	}
}

// RecordProducedMessage records a successfully produced message
func (c *Collector) RecordProducedMessage(size int, latency time.Duration) {
	atomic.AddInt64(&c.messagesProduced, 1)
	atomic.AddInt64(&c.bytesProduced, int64(size))

	c.prometheusMetrics.MessagesProducedTotal.Inc()
	c.prometheusMetrics.BytesProducedTotal.Add(float64(size))
	c.prometheusMetrics.MessageLatencyHistogram.Observe(latency.Seconds())

	// Store latency for percentile calculations
	c.latencyMutex.Lock()
	c.latencies = append(c.latencies, latency)
	// Keep only recent latencies to avoid memory bloat
	if len(c.latencies) > 100000 {
		c.latencies = c.latencies[50000:]
	}
	c.latencyMutex.Unlock()
}

// RecordConsumedMessage records a successfully consumed message
func (c *Collector) RecordConsumedMessage(size int) {
	atomic.AddInt64(&c.messagesConsumed, 1)
	atomic.AddInt64(&c.bytesConsumed, int64(size))

	c.prometheusMetrics.MessagesConsumedTotal.Inc()
	c.prometheusMetrics.BytesConsumedTotal.Add(float64(size))
}

// RecordProducerError records a producer error
func (c *Collector) RecordProducerError() {
	atomic.AddInt64(&c.producerErrors, 1)
	c.prometheusMetrics.ProducerErrorsTotal.Inc()
}

// RecordConsumerError records a consumer error
func (c *Collector) RecordConsumerError() {
	atomic.AddInt64(&c.consumerErrors, 1)
	c.prometheusMetrics.ConsumerErrorsTotal.Inc()
}

// UpdateConsumerLag updates consumer lag metrics
func (c *Collector) UpdateConsumerLag(consumerGroup, topic string, partition int32, lag int64) {
	key := fmt.Sprintf("%s-%s-%d", consumerGroup, topic, partition)

	c.consumerLagMutex.Lock()
	c.consumerLag[key] = lag
	c.consumerLagMutex.Unlock()

	c.prometheusMetrics.ConsumerLagGauge.WithLabelValues(
		consumerGroup, topic, fmt.Sprintf("%d", partition),
	).Set(float64(lag))
}

// UpdateThroughput updates throughput gauges
func (c *Collector) UpdateThroughput(producerRate, consumerRate float64) {
	c.prometheusMetrics.ProducerThroughput.Set(producerRate)
	c.prometheusMetrics.ConsumerThroughput.Set(consumerRate)
}

// UpdateActiveClients updates active client counts
func (c *Collector) UpdateActiveClients(producers, consumers int) {
	c.prometheusMetrics.ActiveProducers.Set(float64(producers))
	c.prometheusMetrics.ActiveConsumers.Set(float64(consumers))
}

// GetStats returns current statistics
func (c *Collector) GetStats() Stats {
	produced := atomic.LoadInt64(&c.messagesProduced)
	consumed := atomic.LoadInt64(&c.messagesConsumed)
	bytesProduced := atomic.LoadInt64(&c.bytesProduced)
	bytesConsumed := atomic.LoadInt64(&c.bytesConsumed)
	producerErrors := atomic.LoadInt64(&c.producerErrors)
	consumerErrors := atomic.LoadInt64(&c.consumerErrors)

	duration := time.Since(c.startTime)

	// Calculate throughput
	producerThroughput := float64(produced) / duration.Seconds()
	consumerThroughput := float64(consumed) / duration.Seconds()

	// Calculate latency percentiles
	var latencyPercentiles map[float64]time.Duration
	c.latencyMutex.RLock()
	if len(c.latencies) > 0 {
		latencyPercentiles = c.calculatePercentiles(c.latencies)
	}
	c.latencyMutex.RUnlock()

	// Get consumer lag summary
	c.consumerLagMutex.RLock()
	totalLag := int64(0)
	maxLag := int64(0)
	for _, lag := range c.consumerLag {
		totalLag += lag
		if lag > maxLag {
			maxLag = lag
		}
	}
	avgLag := float64(0)
	if len(c.consumerLag) > 0 {
		avgLag = float64(totalLag) / float64(len(c.consumerLag))
	}
	c.consumerLagMutex.RUnlock()

	return Stats{
		Duration:           duration,
		MessagesProduced:   produced,
		MessagesConsumed:   consumed,
		BytesProduced:      bytesProduced,
		BytesConsumed:      bytesConsumed,
		ProducerErrors:     producerErrors,
		ConsumerErrors:     consumerErrors,
		ProducerThroughput: producerThroughput,
		ConsumerThroughput: consumerThroughput,
		LatencyPercentiles: latencyPercentiles,
		TotalConsumerLag:   totalLag,
		MaxConsumerLag:     maxLag,
		AvgConsumerLag:     avgLag,
	}
}

// PrintSummary prints a summary of the test statistics
func (c *Collector) PrintSummary() {
	stats := c.GetStats()

	fmt.Printf("\n=== Load Test Summary ===\n")
	fmt.Printf("Test Duration: %v\n", stats.Duration)
	fmt.Printf("\nMessages:\n")
	fmt.Printf("  Produced: %d (%.2f MB)\n", stats.MessagesProduced, float64(stats.BytesProduced)/1024/1024)
	fmt.Printf("  Consumed: %d (%.2f MB)\n", stats.MessagesConsumed, float64(stats.BytesConsumed)/1024/1024)
	fmt.Printf("  Producer Errors: %d\n", stats.ProducerErrors)
	fmt.Printf("  Consumer Errors: %d\n", stats.ConsumerErrors)

	fmt.Printf("\nThroughput:\n")
	fmt.Printf("  Producer: %.2f msgs/sec\n", stats.ProducerThroughput)
	fmt.Printf("  Consumer: %.2f msgs/sec\n", stats.ConsumerThroughput)

	if stats.LatencyPercentiles != nil {
		fmt.Printf("\nLatency Percentiles:\n")
		percentiles := []float64{50, 90, 95, 99, 99.9}
		for _, p := range percentiles {
			if latency, exists := stats.LatencyPercentiles[p]; exists {
				fmt.Printf("  p%.1f: %v\n", p, latency)
			}
		}
	}

	fmt.Printf("\nConsumer Lag:\n")
	fmt.Printf("  Total: %d messages\n", stats.TotalConsumerLag)
	fmt.Printf("  Max: %d messages\n", stats.MaxConsumerLag)
	fmt.Printf("  Average: %.2f messages\n", stats.AvgConsumerLag)
	fmt.Printf("=========================\n")
}

// WriteStats writes statistics to a writer (for HTTP endpoint)
func (c *Collector) WriteStats(w io.Writer) {
	stats := c.GetStats()

	fmt.Fprintf(w, "# Load Test Statistics\n")
	fmt.Fprintf(w, "duration_seconds %v\n", stats.Duration.Seconds())
	fmt.Fprintf(w, "messages_produced %d\n", stats.MessagesProduced)
	fmt.Fprintf(w, "messages_consumed %d\n", stats.MessagesConsumed)
	fmt.Fprintf(w, "bytes_produced %d\n", stats.BytesProduced)
	fmt.Fprintf(w, "bytes_consumed %d\n", stats.BytesConsumed)
	fmt.Fprintf(w, "producer_errors %d\n", stats.ProducerErrors)
	fmt.Fprintf(w, "consumer_errors %d\n", stats.ConsumerErrors)
	fmt.Fprintf(w, "producer_throughput_msgs_per_sec %f\n", stats.ProducerThroughput)
	fmt.Fprintf(w, "consumer_throughput_msgs_per_sec %f\n", stats.ConsumerThroughput)
	fmt.Fprintf(w, "total_consumer_lag %d\n", stats.TotalConsumerLag)
	fmt.Fprintf(w, "max_consumer_lag %d\n", stats.MaxConsumerLag)
	fmt.Fprintf(w, "avg_consumer_lag %f\n", stats.AvgConsumerLag)

	if stats.LatencyPercentiles != nil {
		for percentile, latency := range stats.LatencyPercentiles {
			fmt.Fprintf(w, "latency_p%g_seconds %f\n", percentile, latency.Seconds())
		}
	}
}

// calculatePercentiles calculates latency percentiles
func (c *Collector) calculatePercentiles(latencies []time.Duration) map[float64]time.Duration {
	if len(latencies) == 0 {
		return nil
	}

	// Make a copy and sort
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	percentiles := map[float64]time.Duration{
		50:   calculatePercentile(sorted, 50),
		90:   calculatePercentile(sorted, 90),
		95:   calculatePercentile(sorted, 95),
		99:   calculatePercentile(sorted, 99),
		99.9: calculatePercentile(sorted, 99.9),
	}

	return percentiles
}

// calculatePercentile calculates a specific percentile from sorted data
func calculatePercentile(sorted []time.Duration, percentile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := percentile / 100.0 * float64(len(sorted)-1)
	if index == float64(int(index)) {
		return sorted[int(index)]
	}

	lower := sorted[int(index)]
	upper := sorted[int(index)+1]
	weight := index - float64(int(index))

	return time.Duration(float64(lower) + weight*float64(upper-lower))
}

// Stats represents the current test statistics
type Stats struct {
	Duration           time.Duration
	MessagesProduced   int64
	MessagesConsumed   int64
	BytesProduced      int64
	BytesConsumed      int64
	ProducerErrors     int64
	ConsumerErrors     int64
	ProducerThroughput float64
	ConsumerThroughput float64
	LatencyPercentiles map[float64]time.Duration
	TotalConsumerLag   int64
	MaxConsumerLag     int64
	AvgConsumerLag     float64
}
