package dash

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

const sessionName = "admin-session"

// SessionName returns the cookie session name used by the admin UI.
func SessionName() string {
	return sessionName
}

// RequireAuth checks if user is authenticated.
func RequireAuth(store sessions.Store) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session, err := store.Get(r, sessionName)
			if err != nil {
				http.Redirect(w, r, "/login?error=Unable to initialize session", http.StatusTemporaryRedirect)
				return
			}

			authenticated, _ := session.Values["authenticated"].(bool)
			username, _ := session.Values["username"].(string)
			role, _ := session.Values["role"].(string)
			if !authenticated || username == "" {
				http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
				return
			}

			csrfToken, err := getOrCreateSessionCSRFToken(session, r, w)
			if err != nil {
				http.Redirect(w, r, "/login?error=Unable to initialize session", http.StatusTemporaryRedirect)
				return
			}

			ctx := WithAuthContext(r.Context(), username, role, csrfToken)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireAuthAPI checks if user is authenticated for API endpoints.
// Returns JSON error instead of redirecting to login page.
func RequireAuthAPI(store sessions.Store) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session, err := store.Get(r, sessionName)
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{
					"error":   "Authentication required",
					"message": "Please log in to access this endpoint",
				})
				return
			}

			authenticated, _ := session.Values["authenticated"].(bool)
			username, _ := session.Values["username"].(string)
			role, _ := session.Values["role"].(string)
			if !authenticated || username == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]string{
					"error":   "Authentication required",
					"message": "Please log in to access this endpoint",
				})
				return
			}

			csrfToken, err := getOrCreateSessionCSRFToken(session, r, w)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error":   "Failed to initialize session",
					"message": "Unable to initialize CSRF token",
				})
				return
			}

			ctx := WithAuthContext(r.Context(), username, role, csrfToken)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireWriteAccess checks if user has admin role (write access).
// Returns JSON error for API endpoints, redirects for HTML endpoints.
func RequireWriteAccess() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role := RoleFromContext(r.Context())
			if role == "" {
				role = "admin" // Default for backward compatibility
			}

			if role != "admin" {
				// Check if this is an API request (path starts with /api) or HTML request.
				if strings.HasPrefix(r.URL.Path, "/api") {
					writeJSON(w, http.StatusForbidden, map[string]string{
						"error":   "Insufficient permissions",
						"message": "This operation requires admin access. Read-only users can only view data.",
					})
				} else {
					http.Redirect(w, r, "/admin?error=Insufficient permissions", http.StatusSeeOther)
				}
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
