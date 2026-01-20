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

type RoleHandlers struct {
	adminServer *dash.AdminServer
}

func NewRoleHandlers(adminServer *dash.AdminServer) *RoleHandlers {
	return &RoleHandlers{
		adminServer: adminServer,
	}
}

// ShowRoles renders the Roles management page
func (h *RoleHandlers) ShowRoles(c *gin.Context) {
	rolesData := h.getRolesData(c)

	c.Header("Content-Type", "text/html")
	rolesComponent := app.Roles(rolesData)
	layoutComponent := layout.Layout(c, rolesComponent)
	layoutComponent.Render(c.Request.Context(), c.Writer)
}

func (h *RoleHandlers) getRolesData(c *gin.Context) dash.RolesData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	roles, err := h.adminServer.IamClient.ListRoles()
	if err != nil {
		glog.Warningf("Failed to list roles via IAM API: %v", err)
		return dash.RolesData{
			Username:    username,
			Roles:       []dash.IAMRole{},
			TotalRoles:  0,
			LastUpdated: time.Now(),
		}
	}

	slices.SortFunc(roles, func(a, b dash.IAMRole) int {
		return strings.Compare(a.RoleName, b.RoleName)
	})

	return dash.RolesData{
		Username:    username,
		Roles:       roles,
		TotalRoles:  len(roles),
		LastUpdated: time.Now(),
	}
}

// API Handlers

// GetRoles returns all roles in JSON format
func (h *RoleHandlers) GetRoles(c *gin.Context) {
	roles, err := h.adminServer.IamClient.ListRoles()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"roles": roles})
}

// CreateRole creates a new role
func (h *RoleHandlers) CreateRole(c *gin.Context) {
	var req dash.CreateRoleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	if err := h.adminServer.IamClient.CreateRole(req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create role: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// DeleteRole deletes a role
func (h *RoleHandlers) DeleteRole(c *gin.Context) {
	roleName := c.Param("name")
	if roleName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Role name is required"})
		return
	}

	if err := h.adminServer.IamClient.DeleteRole(roleName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete role: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// UpdateRole updates an existing role
func (h *RoleHandlers) UpdateRole(c *gin.Context) {
	roleName := c.Param("name")
	if roleName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Role name is required"})
		return
	}

	var req dash.CreateRoleRequest // Reuse CreateRoleRequest as it has all needed fields
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	// 1. Update Description and MaxSessionDuration (if provided)
	// IAM UpdateRole only updates Description (and MaxSessionDuration)
	if err := h.adminServer.IamClient.UpdateRole(roleName, req.Description, req.MaxSessionDuration); err != nil {
		glog.Warningf("Failed to update role description for %s: %v", roleName, err)
		// Continue to try updating other parts
	}

	// 2. Update Trust Policy
	if req.TrustPolicyJSON != "" {
		if err := h.adminServer.IamClient.UpdateAssumeRolePolicy(roleName, req.TrustPolicyJSON); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update trust policy: " + err.Error()})
			return
		}
	}

	// 3. Update Attached Policies
	// First list existing attached policies
	existingPolicies, err := h.adminServer.IamClient.ListAttachedRolePolicies(roleName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list attached policies: " + err.Error()})
		return
	}

	existingPolicySet := make(map[string]bool)
	for _, p := range existingPolicies {
		existingPolicySet[p] = true
	}

	newPolicySet := make(map[string]bool)
	for _, p := range req.AttachedPolicies {
		// Ensure ARN format
		arn := p
		if len(arn) < 3 || arn[:3] != "arn" {
			arn = "arn:aws:iam:::policy/" + p
		}
		newPolicySet[arn] = true
	}

	// Detach policies that are no longer present
	for p := range existingPolicySet {
		if !newPolicySet[p] {
			if err := h.adminServer.IamClient.DetachRolePolicy(roleName, p); err != nil {
				glog.Errorf("Failed to detach policy %s from role %s: %v", p, roleName, err)
			}
		}
	}

	// Attach new policies
	for p := range newPolicySet {
		if !existingPolicySet[p] {
			if err := h.adminServer.IamClient.AttachRolePolicy(roleName, p); err != nil {
				glog.Errorf("Failed to attach policy %s to role %s: %v", p, roleName, err)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}
