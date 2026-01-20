package dash

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type AdminData struct {
	Username          string              `json:"username"`
	TotalVolumes      int                 `json:"total_volumes"`
	TotalFiles        int64               `json:"total_files"`
	TotalSize         int64               `json:"total_size"`
	VolumeSizeLimitMB uint64              `json:"volume_size_limit_mb"`
	MasterNodes       []MasterNode        `json:"master_nodes"`
	VolumeServers     []VolumeServer      `json:"volume_servers"`
	FilerNodes        []FilerNode         `json:"filer_nodes"`
	MessageBrokers    []MessageBrokerNode `json:"message_brokers"`
	DataCenters       []DataCenter        `json:"datacenters"`
	LastUpdated       time.Time           `json:"last_updated"`
	IsMasterHealthy   bool                `json:"is_master_healthy"`

	// EC shard totals for dashboard
	TotalEcVolumes int `json:"total_ec_volumes"` // Total number of EC volumes across all servers
	TotalEcShards  int `json:"total_ec_shards"`  // Total number of EC shards across all servers
}

// Object Store Users management structures - DEPRECATED/REMOVED
// Use IAMUser and related structs in iam_client.go

type FilerNode struct {
	Address     string    `json:"address"`
	DataCenter  string    `json:"datacenter"`
	Rack        string    `json:"rack"`
	LastUpdated time.Time `json:"last_updated"`
}

type MessageBrokerNode struct {
	Address     string    `json:"address"`
	DataCenter  string    `json:"datacenter"`
	Rack        string    `json:"rack"`
	LastUpdated time.Time `json:"last_updated"`
}

// GetAdminData retrieves admin data as a struct (for reuse by both JSON and HTML handlers)
func (s *AdminServer) GetAdminData(username string) (AdminData, error) {
	if username == "" {
		username = "admin"
	}

	// Get cluster topology
	topology, err := s.GetClusterTopology()
	if err != nil {
		glog.Errorf("Failed to get cluster topology: %v", err)
		return AdminData{}, err
	}

	// Get volume servers data with EC shard information
	volumeServersData, err := s.GetClusterVolumeServers()
	if err != nil {
		glog.Errorf("Failed to get cluster volume servers: %v", err)
		return AdminData{}, err
	}
	// Sort the servers so they show up in consistent order after each reload
	sort.Slice(volumeServersData.VolumeServers, func(i, j int) bool {
		s1Name := volumeServersData.VolumeServers[i].GetDisplayAddress()
		s2Name := volumeServersData.VolumeServers[j].GetDisplayAddress()

		return s1Name < s2Name
	})

	// Get master nodes status
	masterNodes := s.getMasterNodesStatus()
	
	// Determine overall master health
	isMasterHealthy := false
	for _, m := range masterNodes {
		if m.IsHealthy {
			isMasterHealthy = true
			break
		}
	}

	// Get filer nodes status
	filerNodes := s.getFilerNodesStatus()

	// Get message broker nodes status
	messageBrokers := s.getMessageBrokerNodesStatus()

	// Get volume size limit from master configuration
	var volumeSizeLimitMB uint64 = 30000 // Default to 30GB
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		volumeSizeLimitMB = uint64(resp.VolumeSizeLimitMB)
		return nil
	})
	if err != nil {
		glog.Warningf("Failed to get volume size limit from master: %v", err)
		// Keep default value on error
	}

	// Calculate EC shard totals
	var totalEcVolumes, totalEcShards int
	ecVolumeSet := make(map[uint32]bool) // To avoid counting the same EC volume multiple times

	for _, vs := range volumeServersData.VolumeServers {
		totalEcShards += vs.EcShards
		// Count unique EC volumes across all servers
		for _, ecInfo := range vs.EcShardDetails {
			ecVolumeSet[ecInfo.VolumeID] = true
		}
	}
	totalEcVolumes = len(ecVolumeSet)

	// Prepare admin data
	adminData := AdminData{
		Username:          username,
		TotalVolumes:      topology.TotalVolumes,
		TotalFiles:        topology.TotalFiles,
		TotalSize:         topology.TotalSize,
		VolumeSizeLimitMB: volumeSizeLimitMB,
		MasterNodes:       masterNodes,
		VolumeServers:     volumeServersData.VolumeServers,
		FilerNodes:        filerNodes,
		MessageBrokers:    messageBrokers,
		DataCenters:       topology.DataCenters,
		LastUpdated:       topology.UpdatedAt,
		IsMasterHealthy:   isMasterHealthy,
		TotalEcVolumes:    totalEcVolumes,
		TotalEcShards:     totalEcShards,
	}

	return adminData, nil
}

// ShowAdmin displays the main admin page (now uses GetAdminData)
func (s *AdminServer) ShowAdmin(c *gin.Context) {
	username := c.GetString("username")

	adminData, err := s.GetAdminData(username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get admin data: " + err.Error()})
		return
	}

	// Return JSON for API calls
	c.JSON(http.StatusOK, adminData)
}

// ShowOverview displays cluster overview
func (s *AdminServer) ShowOverview(c *gin.Context) {
	topology, err := s.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, topology)
}

// getMasterNodesStatus checks status of all master nodes
func (s *AdminServer) getMasterNodesStatus() []MasterNode {
	var masterNodes []MasterNode

	// Since we have a single master address, create one entry
	var isLeader bool = true // Assume leader since it's the only master we know about
	var isHealthy bool = false

	// Try to get leader info from this master
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		_, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		// If we can talk to it, it is healthy
		isHealthy = true
		// For now, assume this master is the leader since we can connect to it
		isLeader = true
		return nil
	})

	if err != nil {
		isLeader = false
		isHealthy = false
	}

	currentMaster := s.masterClient.GetMaster(context.Background())
	if currentMaster != "" {
		masterNodes = append(masterNodes, MasterNode{
			Address:   string(currentMaster),
			IsLeader:  isLeader,
			IsHealthy: isHealthy,
		})
	} else if len(masterNodes) == 0 {
		// If we couldn't get master from client loop, try to get from configured masters
		// This handles the case where initial connection failed completely
		// But in AdminServer we initialized with specific masters, we can't easily retrieve the "configured" string here 
		// without modifying the struct, but we can return an empty list or the unhealthy state if we know it.
		// However, returning "disconnected" master node is useful for UI.
		// Let's rely on what we have. If currentMaster is empty, it means KeepConnectedToMaster hasn't established yet 
		// or all failed.
	}

	return masterNodes
}

// getFilerNodesStatus checks status of all filer nodes using master's ListClusterNodes
func (s *AdminServer) getFilerNodesStatus() []FilerNode {
	var filerNodes []FilerNode

	// Get filer nodes from master using ListClusterNodes
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})
		if err != nil {
			return err
		}

		// Process each filer node
		for _, node := range resp.ClusterNodes {
			filerNodes = append(filerNodes, FilerNode{
				Address:     node.Address,
				DataCenter:  node.DataCenter,
				Rack:        node.Rack,
				LastUpdated: time.Now(),
			})
		}

		return nil
	})

	if err != nil {
		currentMaster := s.masterClient.GetMaster(context.Background())
		glog.Errorf("Failed to get filer nodes from master %s: %v", currentMaster, err)
		// Return empty list if we can't get filer info from master
		return []FilerNode{}
	}

	return filerNodes
}

// getMessageBrokerNodesStatus checks status of all message broker nodes using master's ListClusterNodes
func (s *AdminServer) getMessageBrokerNodesStatus() []MessageBrokerNode {
	var messageBrokers []MessageBrokerNode

	// Get message broker nodes from master using ListClusterNodes
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
		})
		if err != nil {
			return err
		}

		// Process each message broker node
		for _, node := range resp.ClusterNodes {
			messageBrokers = append(messageBrokers, MessageBrokerNode{
				Address:     node.Address,
				DataCenter:  node.DataCenter,
				Rack:        node.Rack,
				LastUpdated: time.Now(),
			})
		}

		return nil
	})

	if err != nil {
		currentMaster := s.masterClient.GetMaster(context.Background())
		glog.Errorf("Failed to get message broker nodes from master %s: %v", currentMaster, err)
		// Return empty list if we can't get broker info from master
		return []MessageBrokerNode{}
	}

	return messageBrokers
}
