package handlers

import (
	"net/http"
	"time"

	"github.com/gorilla/sessions"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// AuthHandlers contains authentication-related HTTP handlers
type AuthHandlers struct {
	adminServer  *dash.AdminServer
	sessionStore sessions.Store
	authConfig   AuthConfig
}

// NewAuthHandlers creates a new instance of AuthHandlers
func NewAuthHandlers(adminServer *dash.AdminServer, store sessions.Store, authConfig AuthConfig) *AuthHandlers {
	return &AuthHandlers{
		adminServer:  adminServer,
		sessionStore: store,
		authConfig:   authConfig,
	}
}

// ShowLogin displays the login page
func (a *AuthHandlers) ShowLogin(w http.ResponseWriter, r *http.Request) {
	session, err := a.sessionStore.Get(r, dash.SessionName())
	var csrfToken string
	if err == nil {
		if authenticated, _ := session.Values["authenticated"].(bool); authenticated {
			http.Redirect(w, r, "/admin", http.StatusSeeOther)
			return
		}
	} else {
		glog.V(1).Infof("Failed to load session for login page: %v", err)
	}

	if session != nil {
		token, tokenErr := dash.EnsureSessionCSRFToken(session, r, w)
		if tokenErr != nil {
			glog.V(1).Infof("Failed to ensure CSRF token for login page: %v", tokenErr)
		} else {
			csrfToken = token
		}
	}

	errorMessage := r.URL.Query().Get("error")

	// Render login template
	w.Header().Set("Content-Type", "text/html")
	loginComponent := layout.LoginForm(
		"SeaweedFS Admin",
		errorMessage,
		csrfToken,
		a.authConfig.LocalAuthEnabled(),
		a.authConfig.OIDCAuthEnabled(),
	)
	if err := loginComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render login template: "+err.Error())
		return
	}
}

// HandleLogin handles login form submission
func (a *AuthHandlers) HandleLogin() http.HandlerFunc {
	if !a.authConfig.LocalAuthEnabled() {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/login?error=Local username/password login is disabled", http.StatusSeeOther)
		}
	}

	return a.adminServer.HandleLogin(
		a.sessionStore,
		a.authConfig.AdminUser,
		a.authConfig.AdminPassword,
		a.authConfig.ReadOnlyUser,
		a.authConfig.ReadOnlyPassword,
	)
}

// HandleOIDCLogin starts the OIDC authorization code flow.
func (a *AuthHandlers) HandleOIDCLogin(w http.ResponseWriter, r *http.Request) {
	if !a.authConfig.OIDCAuthEnabled() {
		http.Redirect(w, r, "/login?error=OIDC login is disabled", http.StatusSeeOther)
		return
	}

	session, err := a.sessionStore.Get(r, dash.SessionName())
	if err != nil {
		http.Redirect(w, r, "/login?error=Unable to initialize session", http.StatusSeeOther)
		return
	}

	loginURL, err := a.authConfig.OIDCAuth.BeginLogin(session, r, w)
	if err != nil {
		glog.Errorf("Failed to start OIDC login flow: %v", err)
		http.Redirect(w, r, "/login?error=Unable to start OIDC login", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, loginURL, http.StatusSeeOther)
}

// HandleOIDCCallback handles the OIDC authorization code callback.
func (a *AuthHandlers) HandleOIDCCallback(w http.ResponseWriter, r *http.Request) {
	if !a.authConfig.OIDCAuthEnabled() {
		http.Redirect(w, r, "/login?error=OIDC login is disabled", http.StatusSeeOther)
		return
	}

	session, err := a.sessionStore.Get(r, dash.SessionName())
	if err != nil {
		http.Redirect(w, r, "/login?error=Unable to initialize session", http.StatusSeeOther)
		return
	}

	result, err := a.authConfig.OIDCAuth.CompleteLogin(session, r, w)
	if err != nil {
		glog.Warningf("OIDC callback failed: %v", err)
		http.Redirect(w, r, "/login?error=OIDC login failed", http.StatusSeeOther)
		return
	}

	for key := range session.Values {
		delete(session.Values, key)
	}

	session.Values["authenticated"] = true
	session.Values["username"] = result.Username
	session.Values["role"] = result.Role
	session.Values["auth_provider"] = "oidc"

	csrfToken, err := dash.GenerateSessionToken()
	if err != nil {
		glog.Errorf("Failed to create session CSRF token for OIDC user %s: %v", result.Username, err)
		http.Redirect(w, r, "/login?error=Unable to initialize session", http.StatusSeeOther)
		return
	}
	session.Values[dash.SessionCSRFTokenKey()] = csrfToken

	// OIDC sessions must not outlive the source token.
	if result.TokenExpiration != nil {
		session.Options = cloneSessionOptions(session.Options)
		maxAgeFromToken := int(time.Until(*result.TokenExpiration).Seconds())
		if maxAgeFromToken < 1 {
			http.Redirect(w, r, "/login?error=OIDC token has expired", http.StatusSeeOther)
			return
		}
		if session.Options == nil {
			session.Options = &sessions.Options{Path: "/", HttpOnly: true}
		}
		if session.Options.MaxAge <= 0 || maxAgeFromToken < session.Options.MaxAge {
			session.Options.MaxAge = maxAgeFromToken
		}
	}

	if err := session.Save(r, w); err != nil {
		glog.Errorf("Failed to save OIDC session for user %s: %v", result.Username, err)
		http.Redirect(w, r, "/login?error=Unable to save session", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}

// HandleLogout handles user logout
func (a *AuthHandlers) HandleLogout(w http.ResponseWriter, r *http.Request) {
	a.adminServer.HandleLogout(a.sessionStore, w, r)
}

func cloneSessionOptions(options *sessions.Options) *sessions.Options {
	if options == nil {
		return nil
	}
	cloned := *options
	return &cloned
}
