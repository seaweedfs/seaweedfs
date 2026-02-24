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
func (h *UserHandlers) ShowObjectStoreUsers(w http.ResponseWriter, r *http.Request) {
	// Get object store users data from the server
	usersData := h.getObjectStoreUsersData(r)

	// Render HTML template
	// Add cache-control headers to prevent browser caching of inline JavaScript
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", time.Now().Unix()))
	usersComponent := app.ObjectStoreUsers(usersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, usersComponent)
	err := layoutComponent.Render(r.Context(), w)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// GetUsers returns the list of users as JSON
func (h *UserHandlers) GetUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.adminServer.GetObjectStoreUsers(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get users: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": users})
}

// CreateUser handles user creation
func (h *UserHandlers) CreateUser(w http.ResponseWriter, r *http.Request) {
	var req dash.CreateUserRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Validate required fields
	if req.Username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	user, err := h.adminServer.CreateObjectStoreUser(req)
	if err != nil {
		glog.Errorf("Failed to create user %s: %v", req.Username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to create user: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"message": "User created successfully",
		"user":    user,
	})
}

// UpdateUser handles user updates
func (h *UserHandlers) UpdateUser(w http.ResponseWriter, r *http.Request) {
	username := mux.Vars(r)["username"]
	if username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	var req dash.UpdateUserRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	user, err := h.adminServer.UpdateObjectStoreUser(username, req)
	if err != nil {
		glog.Errorf("Failed to update user %s: %v", username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to update user: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "User updated successfully",
		"user":    user,
	})
}

// DeleteUser handles user deletion
func (h *UserHandlers) DeleteUser(w http.ResponseWriter, r *http.Request) {
	username := mux.Vars(r)["username"]
	if username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	err := h.adminServer.DeleteObjectStoreUser(username)
	if err != nil {
		glog.Errorf("Failed to delete user %s: %v", username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to delete user: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "User deleted successfully",
	})
}

// GetUserDetails returns detailed information about a specific user
func (h *UserHandlers) GetUserDetails(w http.ResponseWriter, r *http.Request) {
	username := mux.Vars(r)["username"]
	if username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	user, err := h.adminServer.GetObjectStoreUserDetails(username)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "User not found: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, user)
}

// CreateAccessKey creates a new access key for a user
func (h *UserHandlers) CreateAccessKey(w http.ResponseWriter, r *http.Request) {
	username := mux.Vars(r)["username"]
	if username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	accessKey, err := h.adminServer.CreateAccessKey(username)
	if err != nil {
		glog.Errorf("Failed to create access key for user %s: %v", username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to create access key: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"message":    "Access key created successfully",
		"access_key": accessKey,
	})
}

// DeleteAccessKey deletes an access key for a user
func (h *UserHandlers) DeleteAccessKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]
	accessKeyId := vars["accessKeyId"]

	if username == "" || accessKeyId == "" {
		writeJSONError(w, http.StatusBadRequest, "Username and access key ID are required")
		return
	}

	err := h.adminServer.DeleteAccessKey(username, accessKeyId)
	if err != nil {
		glog.Errorf("Failed to delete access key %s for user %s: %v", accessKeyId, username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to delete access key: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Access key deleted successfully",
	})
}

// UpdateAccessKeyStatus updates the status of an access key for a user
func (h *UserHandlers) UpdateAccessKeyStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]
	accessKeyId := vars["accessKeyId"]

	if username == "" || accessKeyId == "" {
		writeJSONError(w, http.StatusBadRequest, "Username and access key ID are required")
		return
	}

	var req dash.UpdateAccessKeyStatusRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Validate status
	if req.Status != dash.AccessKeyStatusActive && req.Status != dash.AccessKeyStatusInactive {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("Status must be '%s' or '%s'", dash.AccessKeyStatusActive, dash.AccessKeyStatusInactive))
		return
	}

	err := h.adminServer.UpdateAccessKeyStatus(username, accessKeyId, req.Status)
	if err != nil {
		glog.Errorf("Failed to update access key status %s for user %s: %v", accessKeyId, username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to update access key status: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Access key updated successfully",
	})
}

// GetUserPolicies returns the policies for a user
func (h *UserHandlers) GetUserPolicies(w http.ResponseWriter, r *http.Request) {
	username := mux.Vars(r)["username"]
	if username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	policies, err := h.adminServer.GetUserPolicies(username)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get user policies: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"policies": policies})
}

// UpdateUserPolicies updates the policies for a user
func (h *UserHandlers) UpdateUserPolicies(w http.ResponseWriter, r *http.Request) {
	username := mux.Vars(r)["username"]
	if username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}

	var req dash.UpdateUserPoliciesRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	err := h.adminServer.UpdateUserPolicies(username, req.Actions)
	if err != nil {
		glog.Errorf("Failed to update policies for user %s: %v", username, err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to update user policies: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "User policies updated successfully",
	})
}

// getObjectStoreUsersData retrieves object store users data from the server
func (h *UserHandlers) getObjectStoreUsersData(r *http.Request) dash.ObjectStoreUsersData {
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}

	// Get object store users
	users, err := h.adminServer.GetObjectStoreUsers(r.Context())
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
