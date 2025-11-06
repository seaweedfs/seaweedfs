package handlers

import (
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// ClusterHandlers contains all the HTTP handlers for cluster management
type ClusterHandlers struct {
	adminServer *dash.AdminServer
}

// NewClusterHandlers creates a new instance of ClusterHandlers
func NewClusterHandlers(adminServer *dash.AdminServer) *ClusterHandlers {
	return &ClusterHandlers{
		adminServer: adminServer,
	}
}

// ShowClusterVolumeServers renders the cluster volume servers page
func (h *ClusterHandlers) ShowClusterVolumeServers(c *gin.Context) {
	// Get cluster volume servers data
	volumeServersData, err := h.adminServer.GetClusterVolumeServers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster volume servers: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	volumeServersData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	volumeServersComponent := app.ClusterVolumeServers(*volumeServersData)
	layoutComponent := layout.Layout(c, volumeServersComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowClusterVolumes renders the cluster volumes page
func (h *ClusterHandlers) ShowClusterVolumes(c *gin.Context) {
	// Get pagination and sorting parameters from query string
	page := 1
	if p := c.Query("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	pageSize := 100
	if ps := c.Query("pageSize"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil && parsed > 0 && parsed <= 1000 {
			pageSize = parsed
		}
	}

	sortBy := c.DefaultQuery("sortBy", "id")
	sortOrder := c.DefaultQuery("sortOrder", "asc")
	collection := c.Query("collection") // Optional collection filter

	// Get cluster volumes data
	volumesData, err := h.adminServer.GetClusterVolumes(page, pageSize, sortBy, sortOrder, collection)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster volumes: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	volumesData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	volumesComponent := app.ClusterVolumes(*volumesData)
	layoutComponent := layout.Layout(c, volumesComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowVolumeDetails renders the volume details page
func (h *ClusterHandlers) ShowVolumeDetails(c *gin.Context) {
	volumeIDStr := c.Param("id")
	server := c.Param("server")

	if volumeIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Volume ID is required"})
		return
	}

	if server == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Server is required"})
		return
	}

	volumeID, err := strconv.Atoi(volumeIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid volume ID"})
		return
	}

	// Get volume details
	volumeDetails, err := h.adminServer.GetVolumeDetails(volumeID, server)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get volume details: " + err.Error()})
		return
	}

	// Render HTML template
	c.Header("Content-Type", "text/html")
	volumeDetailsComponent := app.VolumeDetails(*volumeDetails)
	layoutComponent := layout.Layout(c, volumeDetailsComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowClusterCollections renders the cluster collections page
func (h *ClusterHandlers) ShowClusterCollections(c *gin.Context) {
	// Get cluster collections data
	collectionsData, err := h.adminServer.GetClusterCollections()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster collections: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	collectionsData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	collectionsComponent := app.ClusterCollections(*collectionsData)
	layoutComponent := layout.Layout(c, collectionsComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowCollectionDetails renders the collection detail page
func (h *ClusterHandlers) ShowCollectionDetails(c *gin.Context) {
	collectionName := c.Param("name")
	if collectionName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Collection name is required"})
		return
	}

	// Parse query parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "25"))
	sortBy := c.DefaultQuery("sort_by", "volume_id")
	sortOrder := c.DefaultQuery("sort_order", "asc")

	// Get collection details data (volumes and EC volumes)
	collectionDetailsData, err := h.adminServer.GetCollectionDetails(collectionName, page, pageSize, sortBy, sortOrder)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get collection details: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	collectionDetailsData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	collectionDetailsComponent := app.CollectionDetails(*collectionDetailsData)
	layoutComponent := layout.Layout(c, collectionDetailsComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowClusterEcShards handles the cluster EC shards page (individual shards view)
func (h *ClusterHandlers) ShowClusterEcShards(c *gin.Context) {
	// Parse query parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "100"))
	sortBy := c.DefaultQuery("sort_by", "volume_id")
	sortOrder := c.DefaultQuery("sort_order", "asc")
	collection := c.DefaultQuery("collection", "")

	// Get data from admin server
	data, err := h.adminServer.GetClusterEcVolumes(page, pageSize, sortBy, sortOrder, collection)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	data.Username = username

	// Render template
	c.Header("Content-Type", "text/html")
	ecVolumesComponent := app.ClusterEcVolumes(*data)
	layoutComponent := layout.Layout(c, ecVolumesComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
}

// ShowEcVolumeDetails renders the EC volume details page
func (h *ClusterHandlers) ShowEcVolumeDetails(c *gin.Context) {
	volumeIDStr := c.Param("id")

	if volumeIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Volume ID is required"})
		return
	}

	volumeID, err := strconv.Atoi(volumeIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid volume ID"})
		return
	}

	// Check that volumeID is within uint32 range
	if volumeID < 0 || uint64(volumeID) > math.MaxUint32 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Volume ID out of range"})
		return
	}

	// Parse sorting parameters
	sortBy := c.DefaultQuery("sort_by", "shard_id")
	sortOrder := c.DefaultQuery("sort_order", "asc")

	// Get EC volume details
	ecVolumeDetails, err := h.adminServer.GetEcVolumeDetails(uint32(volumeID), sortBy, sortOrder)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get EC volume details: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	ecVolumeDetails.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	ecVolumeDetailsComponent := app.EcVolumeDetails(*ecVolumeDetails)
	layoutComponent := layout.Layout(c, ecVolumeDetailsComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowClusterMasters renders the cluster masters page
func (h *ClusterHandlers) ShowClusterMasters(c *gin.Context) {
	// Get cluster masters data
	mastersData, err := h.adminServer.GetClusterMasters()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster masters: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	mastersData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	mastersComponent := app.ClusterMasters(*mastersData)
	layoutComponent := layout.Layout(c, mastersComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowClusterFilers renders the cluster filers page
func (h *ClusterHandlers) ShowClusterFilers(c *gin.Context) {
	// Get cluster filers data
	filersData, err := h.adminServer.GetClusterFilers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster filers: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	filersData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	filersComponent := app.ClusterFilers(*filersData)
	layoutComponent := layout.Layout(c, filersComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowClusterBrokers renders the cluster message brokers page
func (h *ClusterHandlers) ShowClusterBrokers(c *gin.Context) {
	// Get cluster brokers data
	brokersData, err := h.adminServer.GetClusterBrokers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get cluster brokers: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	brokersData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	brokersComponent := app.ClusterBrokers(*brokersData)
	layoutComponent := layout.Layout(c, brokersComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// GetClusterTopology returns the cluster topology as JSON
func (h *ClusterHandlers) GetClusterTopology(c *gin.Context) {
	topology, err := h.adminServer.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, topology)
}

// GetMasters returns master node information
func (h *ClusterHandlers) GetMasters(c *gin.Context) {
	// Simple master info
	c.JSON(http.StatusOK, gin.H{"masters": []gin.H{{"address": "localhost:9333"}}})
}

// GetVolumeServers returns volume server information
func (h *ClusterHandlers) GetVolumeServers(c *gin.Context) {
	topology, err := h.adminServer.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"volume_servers": topology.VolumeServers})
}

// VacuumVolume handles volume vacuum requests via API
func (h *ClusterHandlers) VacuumVolume(c *gin.Context) {
	volumeIDStr := c.Param("id")
	server := c.Param("server")

	if volumeIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Volume ID is required"})
		return
	}

	volumeID, err := strconv.Atoi(volumeIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid volume ID"})
		return
	}

	// Perform vacuum operation
	err = h.adminServer.VacuumVolume(volumeID, server)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to vacuum volume: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   "Volume vacuum started successfully",
		"volume_id": volumeID,
		"server":    server,
	})
}
