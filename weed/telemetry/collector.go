package telemetry

import (
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

type Collector struct {
	client     *Client
	topo       *topology.Topology
	features   []string
	deployment string
	version    string
	os         string
}

// NewCollector creates a new telemetry collector
func NewCollector(client *Client, topo *topology.Topology) *Collector {
	return &Collector{
		client:     client,
		topo:       topo,
		features:   []string{},
		deployment: "unknown",
		version:    "unknown",
		os:         "unknown",
	}
}

// SetFeatures sets the list of enabled features
func (c *Collector) SetFeatures(features []string) {
	c.features = features
}

// SetDeployment sets the deployment type (standalone, cluster, etc.)
func (c *Collector) SetDeployment(deployment string) {
	c.deployment = deployment
}

// SetVersion sets the SeaweedFS version
func (c *Collector) SetVersion(version string) {
	c.version = version
}

// SetOS sets the operating system information
func (c *Collector) SetOS(os string) {
	c.os = os
}

// CollectAndSendAsync collects telemetry data and sends it asynchronously
func (c *Collector) CollectAndSendAsync() {
	if !c.client.IsEnabled() {
		return
	}

	go func() {
		data := c.collectData()
		c.client.SendTelemetryAsync(data)
	}()
}

// StartPeriodicCollection starts sending telemetry data periodically
func (c *Collector) StartPeriodicCollection(interval time.Duration) {
	if !c.client.IsEnabled() {
		glog.V(1).Infof("Telemetry is disabled, skipping periodic collection")
		return
	}

	glog.V(0).Infof("Starting telemetry collection every %v", interval)

	// Send initial telemetry after a short delay
	go func() {
		time.Sleep(30 * time.Second) // Wait for cluster to stabilize
		c.CollectAndSendAsync()
	}()

	// Start periodic collection
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			c.CollectAndSendAsync()
		}
	}()
}

// collectData gathers telemetry data from the topology
func (c *Collector) collectData() *proto.TelemetryData {
	data := &proto.TelemetryData{
		Version:    c.version,
		Os:         c.os,
		Features:   c.features,
		Deployment: c.deployment,
		Timestamp:  time.Now().Unix(),
	}

	if c.topo != nil {
		// Collect volume server count
		data.VolumeServerCount = int32(c.countVolumeServers())

		// Collect total disk usage and volume count
		diskBytes, volumeCount := c.collectVolumeStats()
		data.TotalDiskBytes = diskBytes
		data.TotalVolumeCount = int32(volumeCount)
	}

	return data
}

// countVolumeServers counts the number of active volume servers
func (c *Collector) countVolumeServers() int {
	count := 0
	for _, dcNode := range c.topo.Children() {
		dc := dcNode.(*topology.DataCenter)
		for _, rackNode := range dc.Children() {
			rack := rackNode.(*topology.Rack)
			for range rack.Children() {
				count++
			}
		}
	}
	return count
}

// collectVolumeStats collects total disk usage and volume count
func (c *Collector) collectVolumeStats() (uint64, int) {
	var totalDiskBytes uint64
	var totalVolumeCount int

	for _, dcNode := range c.topo.Children() {
		dc := dcNode.(*topology.DataCenter)
		for _, rackNode := range dc.Children() {
			rack := rackNode.(*topology.Rack)
			for _, dnNode := range rack.Children() {
				dn := dnNode.(*topology.DataNode)
				volumes := dn.GetVolumes()
				for _, volumeInfo := range volumes {
					totalVolumeCount++
					totalDiskBytes += volumeInfo.Size
				}
			}
		}
	}

	return totalDiskBytes, totalVolumeCount
}

// DetermineDeployment determines the deployment type based on configuration
func DetermineDeployment(isMasterEnabled, isVolumeEnabled bool, peerCount int) string {
	if isMasterEnabled && isVolumeEnabled {
		if peerCount > 1 {
			return "cluster"
		}
		return "standalone"
	}
	if isMasterEnabled {
		return "master-only"
	}
	if isVolumeEnabled {
		return "volume-only"
	}
	return "unknown"
}
