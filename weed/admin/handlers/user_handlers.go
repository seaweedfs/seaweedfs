package handlers

import (

	"net/http"
	"regexp"
	"slices"
	"strings"
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
	// Get users data from the server
	usersData := h.getUsersData(c)

	// Render HTML template
	c.Header("Content-Type", "text/html")
	usersComponent := app.IAMUsers(usersData)
	layoutComponent := layout.Layout(c, usersComponent)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// GetUsers returns the list of users as JSON
func (h *UserHandlers) GetUsers(c *gin.Context) {
	users, err := h.adminServer.IamClient.ListUsers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get users: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"users": users})
}

// CreateUser handles user creation
func (h *UserHandlers) CreateUser(c *gin.Context) {
	var req dash.IAMCreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate required fields
	if req.UserName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	if len(req.UserName) < 2 || len(req.UserName) > 64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username must be between 2 and 64 characters"})
		return
	}

	if matched, _ := regexp.MatchString(`^[a-zA-Z0-9+=,.@_-]+$`, req.UserName); !matched {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username contains invalid characters. Allowed: a-z, A-Z, 0-9, and +=,.@_-"})
		return
	}

	err := h.adminServer.IamClient.CreateUser(req.UserName)
	if err != nil {
		glog.Errorf("Failed to create user %s: %v", req.UserName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user: " + err.Error()})
		return
	}

	// Auto-create an access key for the new user
	accessKey, err := h.adminServer.IamClient.CreateAccessKey(req.UserName)
	if err != nil {
		glog.Errorf("Failed to create access key for new user %s: %v", req.UserName, err)
		// We still created the user, so return partial success but warn about key
		c.JSON(http.StatusCreated, gin.H{
			"message": "User created successfully, but failed to generate access key: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":    "User and access key created successfully",
		"access_key": accessKey,
	})
}

// DeleteUser handles user deletion
func (h *UserHandlers) DeleteUser(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	err := h.adminServer.IamClient.DeleteUser(username)
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

	user, err := h.adminServer.IamClient.GetUser(username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found: " + err.Error()})
		return
	}
	
	// Also get access keys
	keys, err := h.adminServer.IamClient.ListAccessKeys(username)
	if err != nil {
		glog.Errorf("Failed to list access keys for user %s: %v", username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list access keys: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user": user,
		"accessKeys": keys,
	})
}

// CreateAccessKey creates a new access key for a user
func (h *UserHandlers) CreateAccessKey(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	accessKey, err := h.adminServer.IamClient.CreateAccessKey(username)
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

	err := h.adminServer.IamClient.DeleteAccessKey(username, accessKeyId)
	if err != nil {
		glog.Errorf("Failed to delete access key %s for user %s: %v", accessKeyId, username, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete access key: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Access key deleted successfully",
	})
}

// GetAccessKeys returns the access keys for a user
func (h *UserHandlers) GetAccessKeys(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username is required"})
		return
	}

	keys, err := h.adminServer.IamClient.ListAccessKeys(username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get access keys: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"access_keys": keys})
}


// getUsersData retrieves users data from the server
func (h *UserHandlers) getUsersData(c *gin.Context) dash.UsersData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get users
	users, err := h.adminServer.IamClient.ListUsers()
	if err != nil {
		glog.Errorf("Failed to get users: %v", err)
		// Return empty data on error
		return dash.UsersData{
			Username:    username,
			Users:       []dash.IAMUser{},
			TotalUsers:  0,
			LastUpdated: time.Now(),
		}
	}

	// Sort users alphabetically
	slices.SortFunc(users, func(a, b dash.IAMUser) int {
		return strings.Compare(a.UserName, b.UserName)
	})

	return dash.UsersData{
		Username:    username,
		Users:       users,
		TotalUsers:  len(users),
		LastUpdated: time.Now(),
	}
}

// Deprecated: UpdateUser is not supported in simple view or IAM API same way, usually just adding/removing policies/keys
func (h *UserHandlers) UpdateUser(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Update user not implemented yet"})
}

// Deprecated: GetUserPolicies / UpdateUserPolicies - use PolicyHandlers for attachment
func (h *UserHandlers) GetUserPolicies(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Use policy management"})
}

func (h *UserHandlers) UpdateUserPolicies(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "Use policy management"})
}

// UpdateAccessKeyStatusRequest is the request structure for updating access key status
type UpdateAccessKeyStatusRequest struct {
	Status string `json:"status" binding:"required"`
}

// UpdateAccessKeyStatus updates the status of an access key
func (h *UserHandlers) UpdateAccessKeyStatus(c *gin.Context) {
	username := c.Param("username")
	accessKeyId := c.Param("accessKeyId")
	
	if username == "" || accessKeyId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username and Access Key ID are required"})
		return
	}

	var req UpdateAccessKeyStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if req.Status != "Active" && req.Status != "Inactive" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Status must be either 'Active' or 'Inactive'"})
		return
	}

	err := h.adminServer.IamClient.UpdateAccessKey(username, accessKeyId, req.Status)
	if err != nil {
		glog.Errorf("Failed to update access key status for user %s key %s: %v", username, accessKeyId, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update access key status: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Access key status updated successfully",
		"status":  req.Status,
	})
}

