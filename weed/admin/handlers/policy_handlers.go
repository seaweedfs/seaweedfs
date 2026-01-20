package handlers

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
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
	policies, err := h.adminServer.IamClient.ListPolicies()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get policies: " + err.Error()})
		return
	}
	
	if policies == nil {
		policies = []dash.ClientIAMPolicy{}
	}
	
	c.JSON(http.StatusOK, gin.H{"policies": policies})
}

// CreatePolicy handles policy creation
func (h *PolicyHandlers) CreatePolicy(c *gin.Context) {
	var req dash.ClientCreatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate policy name
	if req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy name is required"})
		return
	}

	// Create the policy via IAM Client
	err := h.adminServer.IamClient.CreatePolicy(req)
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

	// Construct ARN (assuming default path)
	policyArn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)

	policy, err := h.adminServer.IamClient.GetPolicy(policyArn)
	if err != nil {
		glog.Errorf("Failed to get policy %s: %v", policyName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get policy: " + err.Error()})
		return
	}

	if policy == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Policy not found"})
		return
	}

	// The frontend expects the JSON structure of dash.ClientIAMPolicy
	c.JSON(http.StatusOK, policy)
}

// UpdatePolicy handles policy updates
func (h *PolicyHandlers) UpdatePolicy(c *gin.Context) {
	policyName := c.Param("name")
	if policyName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy name is required"})
		return
	}

	var req dash.ClientUpdatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Update the policy via IAM Client (using simulated overwrite)
	err := h.adminServer.IamClient.UpdatePolicy(policyName, req.Document)
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

	// Construct ARN
	policyArn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)

	// Delete the policy via IAM Client
	err := h.adminServer.IamClient.DeletePolicy(policyArn)
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

	// Get policies via IAM Client
	policies, err := h.adminServer.IamClient.ListPolicies()
	if err != nil {
		glog.Errorf("Failed to get policies: %v", err)
		// Return empty data on error
		return dash.PoliciesData{
			Username:      username,
			Policies:      []dash.ClientIAMPolicy{},
			TotalPolicies: 0,
			LastUpdated:   time.Now(),
		}
	}

	// Ensure policies is never nil
	if policies == nil {
		policies = []dash.ClientIAMPolicy{}
	}

	slices.SortFunc(policies, func(a, b dash.ClientIAMPolicy) int {
		return strings.Compare(a.PolicyName, b.PolicyName)
	})

	return dash.PoliciesData{
		Username:      username,
		Policies:      policies,
		TotalPolicies: len(policies),
		LastUpdated:   time.Now(),
	}
}
