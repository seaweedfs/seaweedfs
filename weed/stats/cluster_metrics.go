package stats

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

const (
	PushGatewayURL = "http://metrics.seaweedfs.com:9091"
)

var (
	ClusterMetrics = prometheus.NewRegistry()

	ClusterId = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "id",
			Help:      "Unique cluster identifier",
		})

	ClusterVolumeCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "volume_count",
			Help:      "Total number of volumes in the cluster",
		})

	ClusterTotalCapacity = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "total_capacity_bytes",
			Help:      "Total storage capacity in bytes",
		})

	ClusterUsedCapacity = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "used_capacity_bytes",
			Help:      "Used storage capacity in bytes",
		})

	ClusterVersion = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "version",
			Help:      "Cluster version",
		})

	ClusterVolumeServerCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "volume_server_count",
			Help:      "Number of volume servers in the cluster",
		})

	ClusterStorageDistribution = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "storage_distribution_bytes",
			Help:      "Storage distribution by data center and rack",
		}, []string{"datacenter", "rack"})

	ClusterUsedStorageDistribution = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "cluster",
			Name:      "used_storage_distribution_bytes",
			Help:      "Used storage distribution by data center and rack",
		}, []string{"datacenter", "rack"})

	pusher *push.Pusher
)

func init() {
	ClusterMetrics.MustRegister(ClusterId)
	ClusterMetrics.MustRegister(ClusterVolumeCount)
	ClusterMetrics.MustRegister(ClusterTotalCapacity)
	ClusterMetrics.MustRegister(ClusterUsedCapacity)
	ClusterMetrics.MustRegister(ClusterVersion)
	ClusterMetrics.MustRegister(ClusterVolumeServerCount)
	ClusterMetrics.MustRegister(ClusterStorageDistribution)
	ClusterMetrics.MustRegister(ClusterUsedStorageDistribution)

	// Initialize the push gateway client
	pusher = push.New(PushGatewayURL, "seaweedfs_cluster").Gatherer(ClusterMetrics)
}

var (
	clusterIdOnce sync.Once
	clusterId     string
)

// GenerateClusterId creates a unique cluster ID based on the first volume server's address
func GenerateClusterId(topologyInfo *master_pb.TopologyInfo) string {
	clusterIdOnce.Do(func() {
		if len(topologyInfo.DataCenterInfos) > 0 {
			dc := topologyInfo.DataCenterInfos[0]
			if len(dc.RackInfos) > 0 {
				rack := dc.RackInfos[0]
				if len(rack.DataNodeInfos) > 0 {
					node := rack.DataNodeInfos[0]
					hash := sha256.Sum256([]byte(node.Id))
					clusterId = hex.EncodeToString(hash[:8])
				}
			}
		}
		if clusterId == "" {
			clusterId = "unknown"
		}
	})
	return clusterId
}

// UpdateClusterMetrics updates all cluster-related metrics
func UpdateClusterMetrics(topologyInfo *master_pb.TopologyInfo, volumeSizeLimitMB uint32) {
	// Set cluster ID
	GenerateClusterId(topologyInfo) // Generate but don't store the ID

	// Set version
	ClusterVersion.Set(0) // Reset to 0 since we're using a string version

	// Calculate total metrics
	var totalVolumeCount uint64
	var totalCapacity uint64
	var totalUsedCapacity uint64
	var volumeServerCount uint64

	// Reset all distribution metrics
	ClusterStorageDistribution.Reset()
	ClusterUsedStorageDistribution.Reset()

	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			var rackVolumeCount uint64
			var rackCapacity uint64
			var rackUsedCapacity uint64

			for _, node := range rack.DataNodeInfos {
				volumeServerCount++
				for _, diskInfo := range node.DiskInfos {
					rackCapacity += uint64(diskInfo.MaxVolumeCount) * uint64(volumeSizeLimitMB) * 1024 * 1024
					for _, volumeInfo := range diskInfo.VolumeInfos {
						rackVolumeCount++
						rackUsedCapacity += volumeInfo.Size
					}
				}
			}

			// Update distribution metrics
			ClusterStorageDistribution.WithLabelValues(dc.Id, rack.Id).Set(float64(rackCapacity))
			ClusterUsedStorageDistribution.WithLabelValues(dc.Id, rack.Id).Set(float64(rackUsedCapacity))

			// Update total metrics
			totalVolumeCount += rackVolumeCount
			totalCapacity += rackCapacity
			totalUsedCapacity += rackUsedCapacity
		}
	}

	// Update total metrics
	ClusterVolumeCount.Set(float64(totalVolumeCount))
	ClusterTotalCapacity.Set(float64(totalCapacity))
	ClusterUsedCapacity.Set(float64(totalUsedCapacity))
	ClusterVolumeServerCount.Set(float64(volumeServerCount))
}

// PushMetrics sends the current metrics to the push gateway
func PushMetrics(clusterId string, enabled bool) error {
	if !enabled || pusher == nil {
		return nil
	}
	return pusher.Push()
}
