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
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func groupErrorToHTTPStatus(err error) int {
	if errors.Is(err, credential.ErrGroupNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, credential.ErrGroupAlreadyExists) {
		return http.StatusConflict
	}
	if errors.Is(err, credential.ErrUserNotInGroup) {
		return http.StatusBadRequest
	}
	if errors.Is(err, credential.ErrPolicyNotAttached) {
		return http.StatusBadRequest
	}
	if errors.Is(err, credential.ErrUserNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, credential.ErrPolicyNotFound) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

type GroupHandlers struct {
	adminServer *dash.AdminServer
}

func NewGroupHandlers(adminServer *dash.AdminServer) *GroupHandlers {
	return &GroupHandlers{adminServer: adminServer}
}

func (h *GroupHandlers) ShowGroups(w http.ResponseWriter, r *http.Request) {
	data := h.getGroupsPageData(r)

	var buf bytes.Buffer
	component := app.Groups(data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)
	if err := layoutComponent.Render(r.Context(), &buf); err != nil {
		glog.Errorf("Failed to render groups template: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write(buf.Bytes())
}

func (h *GroupHandlers) GetGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := h.adminServer.GetGroups(r.Context())
	if err != nil {
		glog.Errorf("Failed to get groups: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "Failed to get groups")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"groups": groups})
}

func (h *GroupHandlers) CreateGroup(w http.ResponseWriter, r *http.Request) {
	var req dash.CreateGroupRequest
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "Group name is required")
		return
	}
	group, err := h.adminServer.CreateGroup(r.Context(), req.Name)
	if err != nil {
		glog.Errorf("Failed to create group: %v", err)
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to create group: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (h *GroupHandlers) GetGroupDetails(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	group, err := h.adminServer.GetGroupDetails(r.Context(), name)
	if err != nil {
		glog.Errorf("Failed to get group details: %v", err)
		writeJSONError(w, groupErrorToHTTPStatus(err), "Group not found")
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (h *GroupHandlers) DeleteGroup(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if err := h.adminServer.DeleteGroup(r.Context(), name); err != nil {
		glog.Errorf("Failed to delete group: %v", err)
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to delete group: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Group deleted successfully"})
}

func (h *GroupHandlers) GetGroupMembers(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	group, err := h.adminServer.GetGroupDetails(r.Context(), name)
	if err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to get group: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"members": group.Members})
}

func (h *GroupHandlers) AddGroupMember(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	var req struct {
		Username string `json:"username"`
	}
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if req.Username == "" {
		writeJSONError(w, http.StatusBadRequest, "Username is required")
		return
	}
	if err := h.adminServer.AddGroupMember(r.Context(), name, req.Username); err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to add member: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Member added successfully"})
}

func (h *GroupHandlers) RemoveGroupMember(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	username := mux.Vars(r)["username"]
	if err := h.adminServer.RemoveGroupMember(r.Context(), name, username); err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to remove member: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Member removed successfully"})
}

func (h *GroupHandlers) GetGroupPolicies(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	group, err := h.adminServer.GetGroupDetails(r.Context(), name)
	if err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to get group: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"policies": group.PolicyNames})
}

func (h *GroupHandlers) AttachGroupPolicy(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	var req struct {
		PolicyName string `json:"policy_name"`
	}
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if req.PolicyName == "" {
		writeJSONError(w, http.StatusBadRequest, "Policy name is required")
		return
	}
	if err := h.adminServer.AttachGroupPolicy(r.Context(), name, req.PolicyName); err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to attach policy: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Policy attached successfully"})
}

func (h *GroupHandlers) DetachGroupPolicy(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	policyName := mux.Vars(r)["policyName"]
	if err := h.adminServer.DetachGroupPolicy(r.Context(), name, policyName); err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to detach policy: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Policy detached successfully"})
}

func (h *GroupHandlers) SetGroupStatus(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	if req.Enabled == nil {
		writeJSONError(w, http.StatusBadRequest, "enabled field is required")
		return
	}
	if err := h.adminServer.SetGroupStatus(r.Context(), name, *req.Enabled); err != nil {
		writeJSONError(w, groupErrorToHTTPStatus(err), "Failed to update group status: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Group status updated"})
}

func (h *GroupHandlers) getGroupsPageData(r *http.Request) dash.GroupsPageData {
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}

	groups, err := h.adminServer.GetGroups(r.Context())
	if err != nil {
		glog.Errorf("Failed to get groups: %v", err)
		return dash.GroupsPageData{
			Username:    username,
			Groups:      []dash.GroupData{},
			LastUpdated: time.Now(),
		}
	}

	activeCount := 0
	for _, g := range groups {
		if g.Status == "enabled" {
			activeCount++
		}
	}

	// Get available users for dropdown
	var availableUsers []string
	users, err := h.adminServer.GetObjectStoreUsers(r.Context())
	if err == nil {
		for _, user := range users {
			availableUsers = append(availableUsers, user.Username)
		}
	}

	// Get available policies for dropdown
	var availablePolicies []string
	policies, err := h.adminServer.GetPolicies()
	if err == nil {
		for _, p := range policies {
			availablePolicies = append(availablePolicies, p.Name)
		}
	}

	return dash.GroupsPageData{
		Username:          username,
		Groups:            groups,
		TotalGroups:       len(groups),
		ActiveGroups:      activeCount,
		AvailableUsers:    availableUsers,
		AvailablePolicies: availablePolicies,
		LastUpdated:       time.Now(),
	}
}
