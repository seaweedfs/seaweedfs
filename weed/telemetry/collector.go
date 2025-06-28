package telemetry

import (
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

type Collector struct {
	client       *Client
	topo         *topology.Topology
	cluster      *cluster.Cluster
	masterServer interface{} // Will be set to *weed_server.MasterServer to access client tracking
	features     []string
	deployment   string
	version      string
	os           string
}

// NewCollector creates a new telemetry collector
func NewCollector(client *Client, topo *topology.Topology, cluster *cluster.Cluster) *Collector {
	return &Collector{
		client:       client,
		topo:         topo,
		cluster:      cluster,
		masterServer: nil,
		features:     []string{},
		deployment:   "unknown",
		version:      "unknown",
		os:           "unknown",
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

// SetMasterServer sets a reference to the master server for client tracking
func (c *Collector) SetMasterServer(masterServer interface{}) {
	c.masterServer = masterServer
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

	if c.cluster != nil {
		// Collect filer and broker counts
		data.FilerCount = int32(c.countFilers())
		data.BrokerCount = int32(c.countBrokers())
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

// countFilers counts the number of active filer servers across all groups
func (c *Collector) countFilers() int {
	// Count all filer-type nodes in the cluster
	// This includes both pure filer servers and S3 servers (which register as filers)
	count := 0
	for _, groupName := range c.getAllFilerGroups() {
		nodes := c.cluster.ListClusterNode(cluster.FilerGroupName(groupName), cluster.FilerType)
		count += len(nodes)
	}
	return count
}

// countBrokers counts the number of active broker servers
func (c *Collector) countBrokers() int {
	// Count brokers across all broker groups
	count := 0
	for _, groupName := range c.getAllBrokerGroups() {
		nodes := c.cluster.ListClusterNode(cluster.FilerGroupName(groupName), cluster.BrokerType)
		count += len(nodes)
	}
	return count
}

// getAllFilerGroups returns all filer group names
func (c *Collector) getAllFilerGroups() []string {
	// For simplicity, we check the default group
	// In a more sophisticated implementation, we could enumerate all groups
	return []string{""}
}

// getAllBrokerGroups returns all broker group names
func (c *Collector) getAllBrokerGroups() []string {
	// For simplicity, we check the default group
	// In a more sophisticated implementation, we could enumerate all groups
	return []string{""}
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
