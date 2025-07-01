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
	ClusterStatus string         `json:"cluster_status"`
	TotalVolumes  int            `json:"total_volumes"`
	TotalFiles    int64          `json:"total_files"`
	TotalSize     int64          `json:"total_size"`
	MasterNodes   []MasterNode   `json:"master_nodes"`
	VolumeServers []VolumeServer `json:"volume_servers"`
	FilerNodes    []FilerNode    `json:"filer_nodes"`
	DataCenters   []DataCenter   `json:"datacenters"`
	LastUpdated   time.Time      `json:"last_updated"`
	SystemHealth  string         `json:"system_health"`
}

// S3 Bucket management data structures for templates
type S3BucketsData struct {
	Username     string     `json:"username"`
	Buckets      []S3Bucket `json:"buckets"`
	TotalBuckets int        `json:"total_buckets"`
	TotalSize    int64      `json:"total_size"`
	LastUpdated  time.Time  `json:"last_updated"`
}

type CreateBucketRequest struct {
	Name   string `json:"name" binding:"required"`
	Region string `json:"region"`
}

// Object Store Users management structures
type ObjectStoreUser struct {
	Username    string    `json:"username"`
	Email       string    `json:"email"`
	AccessKey   string    `json:"access_key"`
	SecretKey   string    `json:"secret_key"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	LastLogin   time.Time `json:"last_login"`
	Permissions []string  `json:"permissions"`
}

type ObjectStoreUsersData struct {
	Username    string            `json:"username"`
	Users       []ObjectStoreUser `json:"users"`
	TotalUsers  int               `json:"total_users"`
	LastUpdated time.Time         `json:"last_updated"`
}

type FilerNode struct {
	Address     string    `json:"address"`
	DataCenter  string    `json:"datacenter"`
	Rack        string    `json:"rack"`
	Status      string    `json:"status"`
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
		ClusterStatus: s.determineClusterStatus(topology, masterNodes),
		TotalVolumes:  topology.TotalVolumes,
		TotalFiles:    topology.TotalFiles,
		TotalSize:     topology.TotalSize,
		MasterNodes:   masterNodes,
		VolumeServers: topology.VolumeServers,
		FilerNodes:    filerNodes,
		DataCenters:   topology.DataCenters,
		LastUpdated:   topology.UpdatedAt,
		SystemHealth:  s.determineSystemHealth(topology, masterNodes),
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

// S3 Bucket Management Handlers

// ShowS3Buckets displays the S3 buckets management page
func (s *AdminServer) ShowS3Buckets(c *gin.Context) {
	username := c.GetString("username")

	buckets, err := s.GetS3Buckets()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get S3 buckets: " + err.Error()})
		return
	}

	// Calculate totals
	var totalSize int64
	for _, bucket := range buckets {
		totalSize += bucket.Size
	}

	data := S3BucketsData{
		Username:     username,
		Buckets:      buckets,
		TotalBuckets: len(buckets),
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
	}

	c.JSON(http.StatusOK, data)
}

// ShowBucketDetails displays detailed information about a specific bucket
func (s *AdminServer) ShowBucketDetails(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name is required"})
		return
	}

	details, err := s.GetBucketDetails(bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get bucket details: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, details)
}

// CreateBucket creates a new S3 bucket
func (s *AdminServer) CreateBucket(c *gin.Context) {
	var req CreateBucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate bucket name (basic validation)
	if len(req.Name) < 3 || len(req.Name) > 63 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name must be between 3 and 63 characters"})
		return
	}

	err := s.CreateS3Bucket(req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create bucket: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Bucket created successfully",
		"bucket":  req.Name,
	})
}

// DeleteBucket deletes an S3 bucket
func (s *AdminServer) DeleteBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name is required"})
		return
	}

	err := s.DeleteS3Bucket(bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete bucket: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Bucket deleted successfully",
		"bucket":  bucketName,
	})
}

// ListBucketsAPI returns buckets as JSON API
func (s *AdminServer) ListBucketsAPI(c *gin.Context) {
	buckets, err := s.GetS3Buckets()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"buckets": buckets,
		"count":   len(buckets),
	})
}

// getMasterNodesStatus checks status of all master nodes
func (s *AdminServer) getMasterNodesStatus() []MasterNode {
	var masterNodes []MasterNode

	// Since we have a single master address, create one entry
	var isLeader bool = true // Assume leader since it's the only master we know about
	var status string

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
		status = "unreachable"
		isLeader = false
	} else {
		status = "active"
	}

	masterNodes = append(masterNodes, MasterNode{
		Address:  s.masterAddress,
		IsLeader: isLeader,
		Status:   status,
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
				Status:      "active", // If it's in the cluster list, it's considered active
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

// determineClusterStatus analyzes cluster health
func (s *AdminServer) determineClusterStatus(topology *ClusterTopology, masters []MasterNode) string {
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
func (s *AdminServer) determineSystemHealth(topology *ClusterTopology, masters []MasterNode) string {
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
