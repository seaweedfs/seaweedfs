package handlers

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
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
func (h *MessageQueueHandlers) ShowBrokers(w http.ResponseWriter, r *http.Request) {
	// Get cluster brokers data
	brokersData, err := h.adminServer.GetClusterBrokers()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster brokers: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	brokersData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	brokersComponent := app.ClusterBrokers(*brokersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, brokersComponent)
	err = layoutComponent.Render(r.Context(), w)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowTopics renders the message queue topics page
func (h *MessageQueueHandlers) ShowTopics(w http.ResponseWriter, r *http.Request) {
	// Get topics data
	topicsData, err := h.adminServer.GetTopics()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get topics: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	topicsData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	topicsComponent := app.Topics(*topicsData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, topicsComponent)
	err = layoutComponent.Render(r.Context(), w)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowSubscribers renders the message queue subscribers page
func (h *MessageQueueHandlers) ShowSubscribers(w http.ResponseWriter, r *http.Request) {
	// Get subscribers data
	subscribersData, err := h.adminServer.GetSubscribers()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get subscribers: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	subscribersData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	subscribersComponent := app.Subscribers(*subscribersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, subscribersComponent)
	err = layoutComponent.Render(r.Context(), w)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowTopicDetails renders the topic details page
func (h *MessageQueueHandlers) ShowTopicDetails(w http.ResponseWriter, r *http.Request) {
	// Get topic parameters from URL
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	topicName := vars["topic"]

	if namespace == "" || topicName == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing namespace or topic name")
		return
	}

	// Get topic details data
	topicDetailsData, err := h.adminServer.GetTopicDetails(namespace, topicName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get topic details: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	topicDetailsData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	topicDetailsComponent := app.TopicDetails(*topicDetailsData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, topicDetailsComponent)
	err = layoutComponent.Render(r.Context(), w)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// GetTopicDetailsAPI returns topic details as JSON for AJAX calls
func (h *MessageQueueHandlers) GetTopicDetailsAPI(w http.ResponseWriter, r *http.Request) {
	// Get topic parameters from URL
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	topicName := vars["topic"]

	if namespace == "" || topicName == "" {
		writeJSONError(w, http.StatusBadRequest, "Missing namespace or topic name")
		return
	}

	// Get topic details data
	topicDetailsData, err := h.adminServer.GetTopicDetails(namespace, topicName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get topic details: "+err.Error())
		return
	}

	// Return JSON data
	writeJSON(w, http.StatusOK, topicDetailsData)
}

// CreateTopicAPI creates a new topic with retention configuration
func (h *MessageQueueHandlers) CreateTopicAPI(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace      string `json:"namespace" binding:"required"`
		Name           string `json:"name" binding:"required"`
		PartitionCount int32  `json:"partition_count" binding:"required"`
		Retention      struct {
			Enabled          bool  `json:"enabled"`
			RetentionSeconds int64 `json:"retention_seconds"`
		} `json:"retention"`
	}

	if err := decodeJSONBody(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Validate inputs
	if req.PartitionCount < 1 || req.PartitionCount > 100 {
		writeJSONError(w, http.StatusBadRequest, "Partition count must be between 1 and 100")
		return
	}

	if req.Retention.Enabled && req.Retention.RetentionSeconds <= 0 {
		writeJSONError(w, http.StatusBadRequest, "Retention seconds must be positive when retention is enabled")
		return
	}

	// Create the topic via admin server
	err := h.adminServer.CreateTopicWithRetention(req.Namespace, req.Name, req.PartitionCount, req.Retention.Enabled, req.Retention.RetentionSeconds)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to create topic: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
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

func (h *MessageQueueHandlers) UpdateTopicRetentionAPI(w http.ResponseWriter, r *http.Request) {
	var request UpdateTopicRetentionRequest
	if err := decodeJSONBody(r, &request); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Validate required fields
	if request.Namespace == "" || request.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "namespace and name are required")
		return
	}

	// Update the topic retention
	err := h.adminServer.UpdateTopicRetention(request.Namespace, request.Name, request.Retention.Enabled, request.Retention.RetentionSeconds)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Topic retention updated successfully",
		"topic":   request.Namespace + "." + request.Name,
	})
}
