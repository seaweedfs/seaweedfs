package handlers

import (
	"bytes"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/mux"
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
func (h *ServiceAccountHandlers) ShowServiceAccounts(w http.ResponseWriter, r *http.Request) {
	data := h.getServiceAccountsData(r)

	// Render to buffer first to avoid partial writes on error
	var buf bytes.Buffer
	component := app.ServiceAccounts(data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)
	err := layoutComponent.Render(r.Context(), &buf)
	if err != nil {
		glog.Errorf("Failed to render service accounts template: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Only write to response if render succeeded
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write(buf.Bytes())
}

// GetServiceAccounts returns the list of service accounts as JSON
func (h *ServiceAccountHandlers) GetServiceAccounts(w http.ResponseWriter, r *http.Request) {
	parentUser := r.URL.Query().Get("parent_user")

	accounts, err := h.adminServer.GetServiceAccounts(r.Context(), parentUser)
	if err != nil {
		glog.Errorf("Failed to get service accounts: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to get service accounts")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"service_accounts": accounts})
}

// CreateServiceAccount handles service account creation
func (h *ServiceAccountHandlers) CreateServiceAccount(w http.ResponseWriter, r *http.Request) {
	var req dash.CreateServiceAccountRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	if req.ParentUser == "" {
		writeJSONError(w, http.StatusBadRequest, "ParentUser is required")
		return
	}

	sa, err := h.adminServer.CreateServiceAccount(r.Context(), req)
	if err != nil {
		glog.Errorf("Failed to create service account for user %s: %v", req.ParentUser, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to create service account")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"message":         "Service account created successfully",
		"service_account": sa,
	})
}

// GetServiceAccountDetails returns detailed information about a service account
func (h *ServiceAccountHandlers) GetServiceAccountDetails(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Service account ID is required")
		return
	}

	sa, err := h.adminServer.GetServiceAccountDetails(r.Context(), id)
	if err != nil {
		// Distinguish not-found errors from internal errors
		if errors.Is(err, dash.ErrServiceAccountNotFound) {
			writeJSONError(w, http.StatusNotFound, "Service account not found: "+err.Error())
		} else {
			glog.Errorf("Failed to get service account details for %s: %v", id, err)
			writeJSONError(w, http.StatusInternalServerError, "Failed to get service account details")
		}
		return
	}

	writeJSON(w, http.StatusOK, sa)
}

// UpdateServiceAccount handles service account updates
func (h *ServiceAccountHandlers) UpdateServiceAccount(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Service account ID is required")
		return
	}

	var req dash.UpdateServiceAccountRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	sa, err := h.adminServer.UpdateServiceAccount(r.Context(), id, req)
	if err != nil {
		// Distinguish not-found errors from internal errors
		if errors.Is(err, dash.ErrServiceAccountNotFound) {
			writeJSONError(w, http.StatusNotFound, "Service account not found")
		} else {
			glog.Errorf("Failed to update service account %s: %v", id, err)
			writeJSONError(w, http.StatusInternalServerError, "Failed to update service account")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":         "Service account updated successfully",
		"service_account": sa,
	})
}

// DeleteServiceAccount handles service account deletion
func (h *ServiceAccountHandlers) DeleteServiceAccount(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "Service account ID is required")
		return
	}

	err := h.adminServer.DeleteServiceAccount(r.Context(), id)
	if err != nil {
		// Distinguish not-found errors from internal errors
		if errors.Is(err, dash.ErrServiceAccountNotFound) {
			writeJSONError(w, http.StatusNotFound, "Service account not found")
		} else {
			glog.Errorf("Failed to delete service account %s: %v", id, err)
			writeJSONError(w, http.StatusInternalServerError, "Failed to delete service account")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Service account deleted successfully",
	})
}

// getServiceAccountsData retrieves service accounts data for the template
func (h *ServiceAccountHandlers) getServiceAccountsData(r *http.Request) dash.ServiceAccountsData {
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}

	// Get all service accounts
	accounts, err := h.adminServer.GetServiceAccounts(r.Context(), "")
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
		if sa.Status == dash.StatusActive {
			activeCount++
		}
	}

	// Get available users for dropdown
	var availableUsers []string
	users, err := h.adminServer.GetObjectStoreUsers(r.Context())
	if err != nil {
		glog.Errorf("Failed to get users for dropdown: %v", err)
	} else {
		for _, user := range users {
			availableUsers = append(availableUsers, user.Username)
		}
	}

	return dash.ServiceAccountsData{
		Username:        username,
		ServiceAccounts: accounts,
		TotalAccounts:   len(accounts),
		ActiveAccounts:  activeCount,
		AvailableUsers:  availableUsers,
		LastUpdated:     time.Now(),
	}
}
