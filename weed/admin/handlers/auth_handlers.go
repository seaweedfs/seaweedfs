package handlers

import (
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// AuthHandlers contains authentication-related HTTP handlers
type AuthHandlers struct {
	adminServer  *dash.AdminServer
	sessionStore sessions.Store
}

// NewAuthHandlers creates a new instance of AuthHandlers
func NewAuthHandlers(adminServer *dash.AdminServer, store sessions.Store) *AuthHandlers {
	return &AuthHandlers{
		adminServer:  adminServer,
		sessionStore: store,
	}
}

// ShowLogin displays the login page
func (a *AuthHandlers) ShowLogin(w http.ResponseWriter, r *http.Request) {
	session, err := a.sessionStore.Get(r, dash.SessionName())
	if err == nil {
		if authenticated, _ := session.Values["authenticated"].(bool); authenticated {
			http.Redirect(w, r, "/admin", http.StatusSeeOther)
			return
		}
	} else {
		glog.V(1).Infof("Failed to load session for login page: %v", err)
	}

	errorMessage := r.URL.Query().Get("error")

	// Render login template
	w.Header().Set("Content-Type", "text/html")
	loginComponent := layout.LoginForm("SeaweedFS Admin", errorMessage)
	if err := loginComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render login template: "+err.Error())
		return
	}
}

// HandleLogin handles login form submission
func (a *AuthHandlers) HandleLogin(adminUser, adminPassword, readOnlyUser, readOnlyPassword string) http.HandlerFunc {
	return a.adminServer.HandleLogin(a.sessionStore, adminUser, adminPassword, readOnlyUser, readOnlyPassword)
}

// HandleLogout handles user logout
func (a *AuthHandlers) HandleLogout(w http.ResponseWriter, r *http.Request) {
	a.adminServer.HandleLogout(a.sessionStore, w, r)
}
