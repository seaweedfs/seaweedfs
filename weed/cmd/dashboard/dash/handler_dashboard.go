package dash

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type DashboardData struct {
	Username      string         `json:"username"`
	ClusterStatus string         `json:"cluster_status"`
	TotalVolumes  int            `json:"total_volumes"`
	TotalFiles    int64          `json:"total_files"`
	TotalSize     int64          `json:"total_size"`
	MasterNodes   []MasterNode   `json:"master_nodes"`
	VolumeServers []VolumeServer `json:"volume_servers"`
	DataCenters   []DataCenter   `json:"datacenters"`
	LastUpdated   time.Time      `json:"last_updated"`
	SystemHealth  string         `json:"system_health"`
}

// ShowDashboardPage displays the main dashboard
func (s *DashboardServer) ShowDashboardPage(c *gin.Context) {
	username := c.GetString("username")

	// Get cluster topology
	topology, err := s.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster topology: " + err.Error()})
		return
	}

	// Get master nodes status
	masterNodes := s.getMasterNodesStatus()

	// Prepare dashboard data
	dashboardData := DashboardData{
		Username:      username,
		ClusterStatus: s.determineClusterStatus(topology, masterNodes),
		TotalVolumes:  topology.TotalVolumes,
		TotalFiles:    topology.TotalFiles,
		TotalSize:     topology.TotalSize,
		MasterNodes:   masterNodes,
		VolumeServers: topology.VolumeServers,
		DataCenters:   topology.DataCenters,
		LastUpdated:   topology.UpdatedAt,
		SystemHealth:  s.determineSystemHealth(topology, masterNodes),
	}

	// For now, return JSON instead of template rendering
	c.JSON(http.StatusOK, dashboardData)
}

// ShowOverviewPage displays cluster overview
func (s *DashboardServer) ShowOverviewPage(c *gin.Context) {
	topology, err := s.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, topology)
}

// getMasterNodesStatus checks status of all master nodes
func (s *DashboardServer) getMasterNodesStatus() []MasterNode {
	var masterNodes []MasterNode

	for _, masterAddr := range s.masterAddresses {
		var isLeader bool
		var status string

		// Try to get leader info from this master
		err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
			resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return err
			}
			isLeader = resp.IsLeader
			return nil
		})

		if err != nil {
			status = "unreachable"
		} else {
			status = "active"
		}

		masterNodes = append(masterNodes, MasterNode{
			Address:  string(masterAddr),
			IsLeader: isLeader,
			Status:   status,
		})
	}

	return masterNodes
}

// determineClusterStatus analyzes cluster health
func (s *DashboardServer) determineClusterStatus(topology *ClusterTopology, masters []MasterNode) string {
	// Check if we have an active leader
	hasActiveLeader := false
	for _, master := range masters {
		if master.IsLeader && master.Status == "active" {
			hasActiveLeader = true
			break
		}
	}

	if !hasActiveLeader {
		return "critical"
	}

	// Check volume server health
	activeServers := 0
	for _, vs := range topology.VolumeServers {
		if vs.Status == "active" {
			activeServers++
		}
	}

	if activeServers == 0 {
		return "critical"
	} else if activeServers < len(topology.VolumeServers) {
		return "warning"
	}

	return "healthy"
}

// determineSystemHealth provides overall system health assessment
func (s *DashboardServer) determineSystemHealth(topology *ClusterTopology, masters []MasterNode) string {
	// Simple health calculation based on active components
	totalComponents := len(masters) + len(topology.VolumeServers)
	activeComponents := 0

	for _, master := range masters {
		if master.Status == "active" {
			activeComponents++
		}
	}

	for _, vs := range topology.VolumeServers {
		if vs.Status == "active" {
			activeComponents++
		}
	}

	if totalComponents == 0 {
		return "unknown"
	}

	healthPercent := float64(activeComponents) / float64(totalComponents) * 100

	if healthPercent >= 95 {
		return "excellent"
	} else if healthPercent >= 80 {
		return "good"
	} else if healthPercent >= 60 {
		return "fair"
	} else {
		return "poor"
	}
}
