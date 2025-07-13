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
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// PolicyHandlers contains all the HTTP handlers for policy management
type PolicyHandlers struct {
	adminServer *dash.AdminServer
}

// NewPolicyHandlers creates a new instance of PolicyHandlers
func NewPolicyHandlers(adminServer *dash.AdminServer) *PolicyHandlers {
	return &PolicyHandlers{
		adminServer: adminServer,
	}
}

// ShowPolicies renders the policies management page
func (h *PolicyHandlers) ShowPolicies(c *gin.Context) {
	// Get policies data from the server
	policiesData := h.getPoliciesData(c)

	// Render HTML template
	c.Header("Content-Type", "text/html")
	policiesComponent := app.Policies(policiesData)
	layoutComponent := layout.Layout(c, policiesComponent)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// GetPolicies returns the list of policies as JSON
func (h *PolicyHandlers) GetPolicies(c *gin.Context) {
	policies, err := h.adminServer.GetPolicies()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get policies: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"policies": policies})
}

// CreatePolicy handles policy creation
func (h *PolicyHandlers) CreatePolicy(c *gin.Context) {
	var req dash.CreatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate policy name
	if req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy name is required"})
		return
	}

	// Check if policy already exists
	existingPolicy, err := h.adminServer.GetPolicy(req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check existing policy: " + err.Error()})
		return
	}
	if existingPolicy != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "Policy with this name already exists"})
		return
	}

	// Create the policy
	err = h.adminServer.CreatePolicy(req.Name, req.Document)
	if err != nil {
		glog.Errorf("Failed to create policy %s: %v", req.Name, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create policy: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"message": "Policy created successfully",
		"policy":  req.Name,
	})
}

// GetPolicy returns a specific policy
func (h *PolicyHandlers) GetPolicy(c *gin.Context) {
	policyName := c.Param("name")
	if policyName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy name is required"})
		return
	}

	policy, err := h.adminServer.GetPolicy(policyName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get policy: " + err.Error()})
		return
	}

	if policy == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Policy not found"})
		return
	}

	c.JSON(http.StatusOK, policy)
}

// UpdatePolicy handles policy updates
func (h *PolicyHandlers) UpdatePolicy(c *gin.Context) {
	policyName := c.Param("name")
	if policyName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy name is required"})
		return
	}

	var req dash.UpdatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Check if policy exists
	existingPolicy, err := h.adminServer.GetPolicy(policyName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check existing policy: " + err.Error()})
		return
	}
	if existingPolicy == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Policy not found"})
		return
	}

	// Update the policy
	err = h.adminServer.UpdatePolicy(policyName, req.Document)
	if err != nil {
		glog.Errorf("Failed to update policy %s: %v", policyName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update policy: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Policy updated successfully",
		"policy":  policyName,
	})
}

// DeletePolicy handles policy deletion
func (h *PolicyHandlers) DeletePolicy(c *gin.Context) {
	policyName := c.Param("name")
	if policyName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy name is required"})
		return
	}

	// Check if policy exists
	existingPolicy, err := h.adminServer.GetPolicy(policyName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check existing policy: " + err.Error()})
		return
	}
	if existingPolicy == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Policy not found"})
		return
	}

	// Delete the policy
	err = h.adminServer.DeletePolicy(policyName)
	if err != nil {
		glog.Errorf("Failed to delete policy %s: %v", policyName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete policy: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Policy deleted successfully",
		"policy":  policyName,
	})
}

// ValidatePolicy validates a policy document without saving it
func (h *PolicyHandlers) ValidatePolicy(c *gin.Context) {
	var req struct {
		Document policy_engine.PolicyDocument `json:"document" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Basic validation
	if req.Document.Version == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy version is required"})
		return
	}

	if len(req.Document.Statement) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy must have at least one statement"})
		return
	}

	// Validate each statement
	for i, statement := range req.Document.Statement {
		if statement.Effect != "Allow" && statement.Effect != "Deny" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Statement %d: Effect must be 'Allow' or 'Deny'", i+1),
			})
			return
		}

		if len(statement.Action.Strings()) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Statement %d: Action is required", i+1),
			})
			return
		}

		if len(statement.Resource.Strings()) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Statement %d: Resource is required", i+1),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"valid":   true,
		"message": "Policy document is valid",
	})
}

// getPoliciesData retrieves policies data from the server
func (h *PolicyHandlers) getPoliciesData(c *gin.Context) dash.PoliciesData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get policies
	policies, err := h.adminServer.GetPolicies()
	if err != nil {
		glog.Errorf("Failed to get policies: %v", err)
		// Return empty data on error
		return dash.PoliciesData{
			Username:      username,
			Policies:      []dash.IAMPolicy{},
			TotalPolicies: 0,
			LastUpdated:   time.Now(),
		}
	}

	// Ensure policies is never nil
	if policies == nil {
		policies = []dash.IAMPolicy{}
	}

	return dash.PoliciesData{
		Username:      username,
		Policies:      policies,
		TotalPolicies: len(policies),
		LastUpdated:   time.Now(),
	}
}
