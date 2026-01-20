package handlers

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type GroupHandlers struct {
	adminServer *dash.AdminServer
}

func NewGroupHandlers(adminServer *dash.AdminServer) *GroupHandlers {
	return &GroupHandlers{
		adminServer: adminServer,
	}
}

// ShowGroups renders the Groups management page
func (h *GroupHandlers) ShowGroups(c *gin.Context) {
	groupsData := h.getGroupsData(c)

	c.Header("Content-Type", "text/html")
	groupsComponent := app.Groups(groupsData)
	layoutComponent := layout.Layout(c, groupsComponent)
	layoutComponent.Render(c.Request.Context(), c.Writer)
}

func (h *GroupHandlers) getGroupsData(c *gin.Context) dash.GroupsData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	groups, err := h.adminServer.IamClient.ListGroups()
	if err != nil {
		glog.Warningf("Failed to list groups via IAM API: %v", err)
		return dash.GroupsData{
			Username:    username,
			Groups:      []dash.IAMGroup{},
			TotalGroups: 0,
			LastUpdated: time.Now(),
		}
	}
	
	slices.SortFunc(groups, func(a, b dash.IAMGroup) int {
		return strings.Compare(a.GroupName, b.GroupName)
	})

	return dash.GroupsData{
		Username:    username,
		Groups:      groups,
		TotalGroups: len(groups),
		LastUpdated: time.Now(),
	}
}

// API Handlers

// GetGroups returns all groups in JSON format
func (h *GroupHandlers) GetGroups(c *gin.Context) {
	groups, err := h.adminServer.IamClient.ListGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"groups": groups})
}

// GetGroup returns a specific group details
func (h *GroupHandlers) GetGroup(c *gin.Context) {
	groupName := c.Param("name")
	if groupName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Group name is required"})
		return
	}
	
	group, err := h.adminServer.IamClient.GetGroup(groupName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, group)
}

// CreateGroup creates a new group
func (h *GroupHandlers) CreateGroup(c *gin.Context) {
	var req dash.CreateGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	if err := h.adminServer.IamClient.CreateGroup(req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create group: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"success": true})
}

// DeleteGroup deletes a group
func (h *GroupHandlers) DeleteGroup(c *gin.Context) {
	groupName := c.Param("name")
	if groupName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Group name is required"})
		return
	}

	if err := h.adminServer.IamClient.DeleteGroup(groupName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete group: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// AddUserToGroup adds a user to a group
func (h *GroupHandlers) AddUserToGroup(c *gin.Context) {
	groupName := c.Param("name")
	userName := c.Param("user")
	
	if err := h.adminServer.IamClient.AddUserToGroup(groupName, userName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// RemoveUserFromGroup removes a user from a group
func (h *GroupHandlers) RemoveUserFromGroup(c *gin.Context) {
	groupName := c.Param("name")
	userName := c.Param("user")
	
	if err := h.adminServer.IamClient.RemoveUserFromGroup(groupName, userName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// AttachPolicy attaches a policy to the group
func (h *GroupHandlers) AttachPolicy(c *gin.Context) {
	groupName := c.Param("name")
	var req struct {
		PolicyArn string `json:"policyArn"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.adminServer.IamClient.AttachGroupPolicy(groupName, req.PolicyArn); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// DetachPolicy detaches a policy from the group
func (h *GroupHandlers) DetachPolicy(c *gin.Context) {
	groupName := c.Param("name")
	policyArn := c.Query("policyArn")
	
	if err := h.adminServer.IamClient.DetachGroupPolicy(groupName, policyArn); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}
