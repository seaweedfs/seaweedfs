package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
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
func (h *PolicyHandlers) ShowPolicies(w http.ResponseWriter, r *http.Request) {
	// Get policies data from the server
	policiesData := h.getPoliciesData(r)

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	policiesComponent := app.Policies(policiesData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, policiesComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// GetPolicies returns the list of policies as JSON
func (h *PolicyHandlers) GetPolicies(w http.ResponseWriter, r *http.Request) {
	policies, err := h.adminServer.GetPolicies()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get policies: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"policies": policies})
}

// CreatePolicy handles policy creation
func (h *PolicyHandlers) CreatePolicy(w http.ResponseWriter, r *http.Request) {
	var req dash.CreatePolicyRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Validate policy name
	if req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "Policy name is required")
		return
	}

	// Check if policy already exists
	existingPolicy, err := h.adminServer.GetPolicy(req.Name)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to check existing policy: "+err.Error())
		return
	}
	if existingPolicy != nil {
		writeJSONError(w, http.StatusConflict, "Policy with this name already exists")
		return
	}

	// Create the policy
	err = h.adminServer.CreatePolicy(req.Name, req.Document)
	if err != nil {
		glog.Errorf("Failed to create policy %s: %v", req.Name, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to create policy: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true,
		"message": "Policy created successfully",
		"policy":  req.Name,
	})
}

// GetPolicy returns a specific policy
func (h *PolicyHandlers) GetPolicy(w http.ResponseWriter, r *http.Request) {
	policyName := mux.Vars(r)["name"]
	if policyName == "" {
		writeJSONError(w, http.StatusBadRequest, "Policy name is required")
		return
	}

	policy, err := h.adminServer.GetPolicy(policyName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get policy: "+err.Error())
		return
	}

	if policy == nil {
		writeJSONError(w, http.StatusNotFound, "Policy not found")
		return
	}

	writeJSON(w, http.StatusOK, policy)
}

// UpdatePolicy handles policy updates
func (h *PolicyHandlers) UpdatePolicy(w http.ResponseWriter, r *http.Request) {
	policyName := mux.Vars(r)["name"]
	if policyName == "" {
		writeJSONError(w, http.StatusBadRequest, "Policy name is required")
		return
	}

	var req dash.UpdatePolicyRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Check if policy exists
	existingPolicy, err := h.adminServer.GetPolicy(policyName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to check existing policy: "+err.Error())
		return
	}
	if existingPolicy == nil {
		writeJSONError(w, http.StatusNotFound, "Policy not found")
		return
	}

	// Update the policy
	err = h.adminServer.UpdatePolicy(policyName, req.Document)
	if err != nil {
		glog.Errorf("Failed to update policy %s: %v", policyName, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to update policy: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Policy updated successfully",
		"policy":  policyName,
	})
}

// DeletePolicy handles policy deletion
func (h *PolicyHandlers) DeletePolicy(w http.ResponseWriter, r *http.Request) {
	policyName := mux.Vars(r)["name"]
	if policyName == "" {
		writeJSONError(w, http.StatusBadRequest, "Policy name is required")
		return
	}

	// Check if policy exists
	existingPolicy, err := h.adminServer.GetPolicy(policyName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to check existing policy: "+err.Error())
		return
	}
	if existingPolicy == nil {
		writeJSONError(w, http.StatusNotFound, "Policy not found")
		return
	}

	// Delete the policy
	err = h.adminServer.DeletePolicy(policyName)
	if err != nil {
		glog.Errorf("Failed to delete policy %s: %v", policyName, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to delete policy: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Policy deleted successfully",
		"policy":  policyName,
	})
}

// ValidatePolicy validates a policy document without saving it
func (h *PolicyHandlers) ValidatePolicy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Document policy_engine.PolicyDocument `json:"document"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Basic validation
	if req.Document.Version == "" {
		writeJSONError(w, http.StatusBadRequest, "Policy version is required")
		return
	}

	if len(req.Document.Statement) == 0 {
		writeJSONError(w, http.StatusBadRequest, "Policy must have at least one statement")
		return
	}

	// Validate each statement
	for i, statement := range req.Document.Statement {
		if statement.Effect != "Allow" && statement.Effect != "Deny" {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Statement %d: Effect must be 'Allow' or 'Deny'", i+1))
			return
		}

		if len(statement.Action.Strings()) == 0 {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Statement %d: Action is required", i+1))
			return
		}

		if len(statement.Resource.Strings()) == 0 {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Statement %d: Resource is required", i+1))
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid":   true,
		"message": "Policy document is valid",
	})
}

// getPoliciesData retrieves policies data from the server
func (h *PolicyHandlers) getPoliciesData(r *http.Request) dash.PoliciesData {
	username := dash.UsernameFromContext(r.Context())
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
