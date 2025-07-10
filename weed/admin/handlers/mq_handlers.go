package handlers

import (
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
