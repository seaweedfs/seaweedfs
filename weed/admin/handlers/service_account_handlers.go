package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ServiceAccountHandlers contains HTTP handlers for service account management
type ServiceAccountHandlers struct {
	adminServer *dash.AdminServer
}

// NewServiceAccountHandlers creates a new instance of ServiceAccountHandlers
func NewServiceAccountHandlers(adminServer *dash.AdminServer) *ServiceAccountHandlers {
	return &ServiceAccountHandlers{
		adminServer: adminServer,
	}
}

// ShowServiceAccounts renders the service accounts management page
func (h *ServiceAccountHandlers) ShowServiceAccounts(c *gin.Context) {
	data := h.getServiceAccountsData(c)

	c.Header("Content-Type", "text/html")
	component := app.ServiceAccounts(data)
	layoutComponent := layout.Layout(c, component)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// GetServiceAccounts returns the list of service accounts as JSON
func (h *ServiceAccountHandlers) GetServiceAccounts(c *gin.Context) {
	parentUser := c.Query("parent_user")

	accounts, err := h.adminServer.GetServiceAccounts(parentUser)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get service accounts: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"service_accounts": accounts})
}

// CreateServiceAccount handles service account creation
func (h *ServiceAccountHandlers) CreateServiceAccount(c *gin.Context) {
	var req dash.CreateServiceAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if req.ParentUser == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ParentUser is required"})
		return
	}

	sa, err := h.adminServer.CreateServiceAccount(req)
	if err != nil {
		glog.Errorf("Failed to create service account for user %s: %v", req.ParentUser, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create service account: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":         "Service account created successfully",
		"service_account": sa,
	})
}

// GetServiceAccountDetails returns detailed information about a service account
func (h *ServiceAccountHandlers) GetServiceAccountDetails(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Service account ID is required"})
		return
	}

	sa, err := h.adminServer.GetServiceAccountDetails(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Service account not found: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, sa)
}

// UpdateServiceAccount handles service account updates
func (h *ServiceAccountHandlers) UpdateServiceAccount(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Service account ID is required"})
		return
	}

	var req dash.UpdateServiceAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	sa, err := h.adminServer.UpdateServiceAccount(id, req)
	if err != nil {
		glog.Errorf("Failed to update service account %s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update service account: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":         "Service account updated successfully",
		"service_account": sa,
	})
}

// DeleteServiceAccount handles service account deletion
func (h *ServiceAccountHandlers) DeleteServiceAccount(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Service account ID is required"})
		return
	}

	err := h.adminServer.DeleteServiceAccount(id)
	if err != nil {
		glog.Errorf("Failed to delete service account %s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete service account: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Service account deleted successfully",
	})
}

// getServiceAccountsData retrieves service accounts data for the template
func (h *ServiceAccountHandlers) getServiceAccountsData(c *gin.Context) dash.ServiceAccountsData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get all service accounts
	accounts, err := h.adminServer.GetServiceAccounts("")
	if err != nil {
		glog.Errorf("Failed to get service accounts: %v", err)
		return dash.ServiceAccountsData{
			Username:        username,
			ServiceAccounts: []dash.ServiceAccount{},
			TotalAccounts:   0,
			LastUpdated:     time.Now(),
		}
	}

	// Count active accounts
	activeCount := 0
	for _, sa := range accounts {
		if sa.Status == "Active" {
			activeCount++
		}
	}

	return dash.ServiceAccountsData{
		Username:        username,
		ServiceAccounts: accounts,
		TotalAccounts:   len(accounts),
		ActiveAccounts:  activeCount,
		LastUpdated:     time.Now(),
	}
}
