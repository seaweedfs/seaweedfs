package dash

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type AdminData struct {
	Username      string         `json:"username"`
	TotalVolumes  int            `json:"total_volumes"`
	TotalFiles    int64          `json:"total_files"`
	TotalSize     int64          `json:"total_size"`
	MasterNodes   []MasterNode   `json:"master_nodes"`
	VolumeServers []VolumeServer `json:"volume_servers"`
	FilerNodes    []FilerNode    `json:"filer_nodes"`
	DataCenters   []DataCenter   `json:"datacenters"`
	LastUpdated   time.Time      `json:"last_updated"`
}

// Object Store Users management structures
type ObjectStoreUser struct {
	Username    string   `json:"username"`
	Email       string   `json:"email"`
	AccessKey   string   `json:"access_key"`
	SecretKey   string   `json:"secret_key"`
	Permissions []string `json:"permissions"`
}

type ObjectStoreUsersData struct {
	Username    string            `json:"username"`
	Users       []ObjectStoreUser `json:"users"`
	TotalUsers  int               `json:"total_users"`
	LastUpdated time.Time         `json:"last_updated"`
}

// User management request structures
type CreateUserRequest struct {
	Username    string   `json:"username" binding:"required"`
	Email       string   `json:"email"`
	Actions     []string `json:"actions"`
	GenerateKey bool     `json:"generate_key"`
}

type UpdateUserRequest struct {
	Email   string   `json:"email"`
	Actions []string `json:"actions"`
}

type UpdateUserPoliciesRequest struct {
	Actions []string `json:"actions" binding:"required"`
}

type AccessKeyInfo struct {
	AccessKey string    `json:"access_key"`
	SecretKey string    `json:"secret_key"`
	CreatedAt time.Time `json:"created_at"`
}

type UserDetails struct {
	Username   string          `json:"username"`
	Email      string          `json:"email"`
	Actions    []string        `json:"actions"`
	AccessKeys []AccessKeyInfo `json:"access_keys"`
}

type FilerNode struct {
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

	// Get master nodes status
	masterNodes := s.getMasterNodesStatus()

	// Get filer nodes status
	filerNodes := s.getFilerNodesStatus()

	// Prepare admin data
	adminData := AdminData{
		Username:      username,
		TotalVolumes:  topology.TotalVolumes,
		TotalFiles:    topology.TotalFiles,
		TotalSize:     topology.TotalSize,
		MasterNodes:   masterNodes,
		VolumeServers: topology.VolumeServers,
		FilerNodes:    filerNodes,
		DataCenters:   topology.DataCenters,
		LastUpdated:   topology.UpdatedAt,
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

	// Try to get leader info from this master
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		_, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		// For now, assume this master is the leader since we can connect to it
		isLeader = true
		return nil
	})

	if err != nil {
		isLeader = false
	}

	masterNodes = append(masterNodes, MasterNode{
		Address:  s.masterAddress,
		IsLeader: isLeader,
	})

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
		glog.Errorf("Failed to get filer nodes from master %s: %v", s.masterAddress, err)
		// Return empty list if we can't get filer info from master
		return []FilerNode{}
	}

	return filerNodes
}
