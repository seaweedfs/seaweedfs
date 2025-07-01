package handlers

import (
	"net/http"

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
	// Get cluster volumes data
	volumesData, err := h.adminServer.GetClusterVolumes()
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
	c.JSON(http.StatusOK, gin.H{"masters": []gin.H{{"address": "localhost:9333", "status": "active"}}})
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
