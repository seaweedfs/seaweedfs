package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// UserHandlers contains all the HTTP handlers for user management
type UserHandlers struct {
	adminServer *dash.AdminServer
}

// NewUserHandlers creates a new instance of UserHandlers
func NewUserHandlers(adminServer *dash.AdminServer) *UserHandlers {
	return &UserHandlers{
		adminServer: adminServer,
	}
}

// ShowObjectStoreUsers renders the object store users management page
func (h *UserHandlers) ShowObjectStoreUsers(c *gin.Context) {
	// Get object store users data from the server
	usersData := h.getObjectStoreUsersData(c)

	// Render HTML template
	// Add cache-control headers to prevent browser caching of inline JavaScript
	c.Header("Content-Type", "text/html")
	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Header("Pragma", "no-cache")
	c.Header("Expires", "0")
	c.Header("ETag", fmt.Sprintf("\"%d\"", time.Now().Unix()))
	usersComponent := app.ObjectStoreUsers(usersData)
	layoutComponent := layout.Layout(c, usersComponent)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// GetUsers returns the list of users as JSON
func (h *UserHandlers) GetUsers(c *gin.Context) {
	users, err := h.adminServer.GetObjectStoreUsers(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get users: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"users": users})
}

// CreateUser handles user creation
func (h *UserHandlers) CreateUser(c *gin.Context) {
	var req dash.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate required fields
	if req.Username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	user, err := h.adminServer.CreateObjectStoreUser(req)
	if err != nil {
		glog.Errorf("Failed to create user %s: %v", req.Username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "User created successfully",
		"user":    user,
	})
}

// UpdateUser handles user updates
func (h *UserHandlers) UpdateUser(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	var req dash.UpdateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	user, err := h.adminServer.UpdateObjectStoreUser(username, req)
	if err != nil {
		glog.Errorf("Failed to update user %s: %v", username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update user: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "User updated successfully",
		"user":    user,
	})
}

// DeleteUser handles user deletion
func (h *UserHandlers) DeleteUser(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	err := h.adminServer.DeleteObjectStoreUser(username)
	if err != nil {
		glog.Errorf("Failed to delete user %s: %v", username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete user: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "User deleted successfully",
	})
}

// GetUserDetails returns detailed information about a specific user
func (h *UserHandlers) GetUserDetails(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	user, err := h.adminServer.GetObjectStoreUserDetails(username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, user)
}

// CreateAccessKey creates a new access key for a user
func (h *UserHandlers) CreateAccessKey(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	accessKey, err := h.adminServer.CreateAccessKey(username)
	if err != nil {
		glog.Errorf("Failed to create access key for user %s: %v", username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create access key: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":    "Access key created successfully",
		"access_key": accessKey,
	})
}

// DeleteAccessKey deletes an access key for a user
func (h *UserHandlers) DeleteAccessKey(c *gin.Context) {
	username := c.Param("username")
	accessKeyId := c.Param("accessKeyId")

	if username == "" || accessKeyId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username and access key ID are required"})
		return
	}

	err := h.adminServer.DeleteAccessKey(username, accessKeyId)
	if err != nil {
		glog.Errorf("Failed to delete access key %s for user %s: %v", accessKeyId, username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete access key: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Access key deleted successfully",
	})
}

// UpdateAccessKeyStatus updates the status of an access key for a user
func (h *UserHandlers) UpdateAccessKeyStatus(c *gin.Context) {
	username := c.Param("username")
	accessKeyId := c.Param("accessKeyId")

	if username == "" || accessKeyId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username and access key ID are required"})
		return
	}

	var req dash.UpdateAccessKeyStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate status
	if req.Status != "Active" && req.Status != "Inactive" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Status must be 'Active' or 'Inactive'"})
		return
	}

	err := h.adminServer.UpdateAccessKeyStatus(username, accessKeyId, req.Status)
	if err != nil {
		glog.Errorf("Failed to update access key status %s for user %s: %v", accessKeyId, username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update access key status: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Access key updated successfully",
	})
}

// GetUserPolicies returns the policies for a user
func (h *UserHandlers) GetUserPolicies(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	policies, err := h.adminServer.GetUserPolicies(username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user policies: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policies": policies})
}

// UpdateUserPolicies updates the policies for a user
func (h *UserHandlers) UpdateUserPolicies(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	var req dash.UpdateUserPoliciesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	err := h.adminServer.UpdateUserPolicies(username, req.Actions)
	if err != nil {
		glog.Errorf("Failed to update policies for user %s: %v", username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update user policies: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "User policies updated successfully",
	})
}

// getObjectStoreUsersData retrieves object store users data from the server
func (h *UserHandlers) getObjectStoreUsersData(c *gin.Context) dash.ObjectStoreUsersData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get object store users
	users, err := h.adminServer.GetObjectStoreUsers(c.Request.Context())
	if err != nil {
		glog.Errorf("Failed to get object store users: %v", err)
		// Return empty data on error
		return dash.ObjectStoreUsersData{
			Username:    username,
			Users:       []dash.ObjectStoreUser{},
			TotalUsers:  0,
			LastUpdated: time.Now(),
		}
	}

	return dash.ObjectStoreUsersData{
		Username:    username,
		Users:       users,
		TotalUsers:  len(users),
		LastUpdated: time.Now(),
	}
}
