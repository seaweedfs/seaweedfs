package storage

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

type PrometheusStorage struct {
	// Prometheus metrics
	totalClusters     prometheus.Gauge
	activeClusters    prometheus.Gauge
	volumeServerCount *prometheus.GaugeVec
	totalDiskBytes    *prometheus.GaugeVec
	totalVolumeCount  *prometheus.GaugeVec
	filerCount        *prometheus.GaugeVec
	brokerCount       *prometheus.GaugeVec
	clusterInfo       *prometheus.GaugeVec
	telemetryReceived prometheus.Counter

	// In-memory storage for API endpoints (if needed)
	mu        sync.RWMutex
	instances map[string]*telemetryData
	stats     map[string]interface{}
}

// telemetryData is an internal struct that includes the received timestamp
type telemetryData struct {
	*proto.TelemetryData
	ReceivedAt time.Time `json:"received_at"`
}

func NewPrometheusStorage() *PrometheusStorage {
	return &PrometheusStorage{
		totalClusters: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_total_clusters",
			Help: "Total number of unique SeaweedFS clusters (last 30 days)",
		}),
		activeClusters: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_active_clusters",
			Help: "Number of active SeaweedFS clusters (last 7 days)",
		}),
		volumeServerCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_volume_servers",
			Help: "Number of volume servers per cluster",
		}, []string{"cluster_id", "version", "os"}),
		totalDiskBytes: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_disk_bytes",
			Help: "Total disk usage in bytes per cluster",
		}, []string{"cluster_id", "version", "os"}),
		totalVolumeCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_volume_count",
			Help: "Total number of volumes per cluster",
		}, []string{"cluster_id", "version", "os"}),
		filerCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_filer_count",
			Help: "Number of filer servers per cluster",
		}, []string{"cluster_id", "version", "os"}),
		brokerCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_broker_count",
			Help: "Number of broker servers per cluster",
		}, []string{"cluster_id", "version", "os"}),
		clusterInfo: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seaweedfs_telemetry_cluster_info",
			Help: "Cluster information (always 1, labels contain metadata)",
		}, []string{"cluster_id", "version", "os"}),
		telemetryReceived: promauto.NewCounter(prometheus.CounterOpts{
			Name: "seaweedfs_telemetry_reports_received_total",
			Help: "Total number of telemetry reports received",
		}),
		instances: make(map[string]*telemetryData),
		stats:     make(map[string]interface{}),
	}
}

func (s *PrometheusStorage) StoreTelemetry(data *proto.TelemetryData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update Prometheus metrics
	labels := prometheus.Labels{
		"cluster_id": data.ClusterId,
		"version":    data.Version,
		"os":         data.Os,
	}

	s.volumeServerCount.With(labels).Set(float64(data.VolumeServerCount))
	s.totalDiskBytes.With(labels).Set(float64(data.TotalDiskBytes))
	s.totalVolumeCount.With(labels).Set(float64(data.TotalVolumeCount))
	s.filerCount.With(labels).Set(float64(data.FilerCount))
	s.brokerCount.With(labels).Set(float64(data.BrokerCount))

	infoLabels := prometheus.Labels{
		"cluster_id": data.ClusterId,
		"version":    data.Version,
		"os":         data.Os,
	}
	s.clusterInfo.With(infoLabels).Set(1)

	s.telemetryReceived.Inc()

	// Store in memory for API endpoints
	s.instances[data.ClusterId] = &telemetryData{
		TelemetryData: data,
		ReceivedAt:    time.Now().UTC(),
	}

	// Update aggregated stats
	s.updateStats()

	return nil
}

func (s *PrometheusStorage) GetStats() (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return cached stats
	result := make(map[string]interface{})
	for k, v := range s.stats {
		result[k] = v
	}
	return result, nil
}

func (s *PrometheusStorage) GetInstances(limit int) ([]*telemetryData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var instances []*telemetryData
	count := 0
	for _, instance := range s.instances {
		if count >= limit {
			break
		}
		instances = append(instances, instance)
		count++
	}

	return instances, nil
}

func (s *PrometheusStorage) GetMetrics(days int) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return current metrics from in-memory storage
	// Historical data should be queried from Prometheus directly
	cutoff := time.Now().AddDate(0, 0, -days)

	var volumeServers []map[string]interface{}
	var diskUsage []map[string]interface{}

	for _, instance := range s.instances {
		if instance.ReceivedAt.After(cutoff) {
			volumeServers = append(volumeServers, map[string]interface{}{
				"date":  instance.ReceivedAt.Format("2006-01-02"),
				"value": instance.TelemetryData.VolumeServerCount,
			})
			diskUsage = append(diskUsage, map[string]interface{}{
				"date":  instance.ReceivedAt.Format("2006-01-02"),
				"value": instance.TelemetryData.TotalDiskBytes,
			})
		}
	}

	return map[string]interface{}{
		"volume_servers": volumeServers,
		"disk_usage":     diskUsage,
	}, nil
}

func (s *PrometheusStorage) updateStats() {
	now := time.Now()
	last7Days := now.AddDate(0, 0, -7)
	last30Days := now.AddDate(0, 0, -30)

	totalInstances := 0
	activeInstances := 0
	versions := make(map[string]int)
	osDistribution := make(map[string]int)

	for _, instance := range s.instances {
		if instance.ReceivedAt.After(last30Days) {
			totalInstances++
		}
		if instance.ReceivedAt.After(last7Days) {
			activeInstances++
			versions[instance.TelemetryData.Version]++
			osDistribution[instance.TelemetryData.Os]++
		}
	}

	// Update Prometheus gauges
	s.totalClusters.Set(float64(totalInstances))
	s.activeClusters.Set(float64(activeInstances))

	// Update cached stats for API
	s.stats = map[string]interface{}{
		"total_instances":  totalInstances,
		"active_instances": activeInstances,
		"versions":         versions,
		"os_distribution":  osDistribution,
	}
}

// CleanupOldInstances removes instances older than the specified duration
func (s *PrometheusStorage) CleanupOldInstances(maxAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for instanceID, instance := range s.instances {
		if instance.ReceivedAt.Before(cutoff) {
			delete(s.instances, instanceID)

			// Remove from Prometheus metrics
			labels := prometheus.Labels{
				"cluster_id": instance.TelemetryData.ClusterId,
				"version":    instance.TelemetryData.Version,
				"os":         instance.TelemetryData.Os,
			}
			s.volumeServerCount.Delete(labels)
			s.totalDiskBytes.Delete(labels)
			s.totalVolumeCount.Delete(labels)
			s.filerCount.Delete(labels)
			s.brokerCount.Delete(labels)
		}
	}

	s.updateStats()
}
