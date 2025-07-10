package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// MessageQueueHandlers contains all the HTTP handlers for message queue management
type MessageQueueHandlers struct {
	adminServer *dash.AdminServer
}

// NewMessageQueueHandlers creates a new instance of MessageQueueHandlers
func NewMessageQueueHandlers(adminServer *dash.AdminServer) *MessageQueueHandlers {
	return &MessageQueueHandlers{
		adminServer: adminServer,
	}
}

// ShowBrokers renders the message queue brokers page
func (h *MessageQueueHandlers) ShowBrokers(c *gin.Context) {
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

// ShowTopics renders the message queue topics page
func (h *MessageQueueHandlers) ShowTopics(c *gin.Context) {
	// Get topics data
	topicsData, err := h.adminServer.GetTopics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get topics: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	topicsData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	topicsComponent := app.Topics(*topicsData)
	layoutComponent := layout.Layout(c, topicsComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowSubscribers renders the message queue subscribers page
func (h *MessageQueueHandlers) ShowSubscribers(c *gin.Context) {
	// Get subscribers data
	subscribersData, err := h.adminServer.GetSubscribers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get subscribers: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	subscribersData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	subscribersComponent := app.Subscribers(*subscribersData)
	layoutComponent := layout.Layout(c, subscribersComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowTopicDetails renders the topic details page
func (h *MessageQueueHandlers) ShowTopicDetails(c *gin.Context) {
	// Get topic parameters from URL
	namespace := c.Param("namespace")
	topicName := c.Param("topic")

	if namespace == "" || topicName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing namespace or topic name"})
		return
	}

	// Get topic details data
	topicDetailsData, err := h.adminServer.GetTopicDetails(namespace, topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get topic details: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	topicDetailsData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	topicDetailsComponent := app.TopicDetails(*topicDetailsData)
	layoutComponent := layout.Layout(c, topicDetailsComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// GetTopicDetailsAPI returns topic details as JSON for AJAX calls
func (h *MessageQueueHandlers) GetTopicDetailsAPI(c *gin.Context) {
	// Get topic parameters from URL
	namespace := c.Param("namespace")
	topicName := c.Param("topic")

	if namespace == "" || topicName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing namespace or topic name"})
		return
	}

	// Get topic details data
	topicDetailsData, err := h.adminServer.GetTopicDetails(namespace, topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get topic details: " + err.Error()})
		return
	}

	// Return JSON data
	c.JSON(http.StatusOK, topicDetailsData)
}

// CreateTopicAPI creates a new topic with retention configuration
func (h *MessageQueueHandlers) CreateTopicAPI(c *gin.Context) {
	var req struct {
		Namespace      string `json:"namespace" binding:"required"`
		Name           string `json:"name" binding:"required"`
		PartitionCount int32  `json:"partition_count" binding:"required"`
		Retention      struct {
			Enabled          bool  `json:"enabled"`
			RetentionSeconds int64 `json:"retention_seconds"`
		} `json:"retention"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate inputs
	if req.PartitionCount < 1 || req.PartitionCount > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Partition count must be between 1 and 100"})
		return
	}

	if req.Retention.Enabled && req.Retention.RetentionSeconds <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Retention seconds must be positive when retention is enabled"})
		return
	}

	// Create the topic via admin server
	err := h.adminServer.CreateTopicWithRetention(req.Namespace, req.Name, req.PartitionCount, req.Retention.Enabled, req.Retention.RetentionSeconds)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create topic: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Topic created successfully",
		"topic":   fmt.Sprintf("%s.%s", req.Namespace, req.Name),
	})
}

type UpdateTopicRetentionRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Retention struct {
		Enabled          bool  `json:"enabled"`
		RetentionSeconds int64 `json:"retention_seconds"`
	} `json:"retention"`
}

func (h *MessageQueueHandlers) UpdateTopicRetentionAPI(c *gin.Context) {
	var request UpdateTopicRetentionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate required fields
	if request.Namespace == "" || request.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace and name are required"})
		return
	}

	// Update the topic retention
	err := h.adminServer.UpdateTopicRetention(request.Namespace, request.Name, request.Retention.Enabled, request.Retention.RetentionSeconds)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Topic retention updated successfully",
		"topic":   request.Namespace + "." + request.Name,
	})
}
