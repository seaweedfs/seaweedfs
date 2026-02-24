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

type sessionValidationErrorKind int

const (
	sessionValidationErrorKindUnauthenticated sessionValidationErrorKind = iota
	sessionValidationErrorKindSessionInit
)

type sessionValidationError struct {
	kind sessionValidationErrorKind
	err  error
}

func (e *sessionValidationError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return "session validation failed"
}

func (e *sessionValidationError) Unwrap() error {
	return e.err
}

func validateSession(store sessions.Store, w http.ResponseWriter, r *http.Request) (string, string, string, error) {
	session, err := store.Get(r, sessionName)
	if err != nil {
		return "", "", "", &sessionValidationError{kind: sessionValidationErrorKindSessionInit, err: err}
	}

	authenticated, _ := session.Values["authenticated"].(bool)
	username, _ := session.Values["username"].(string)
	role, _ := session.Values["role"].(string)
	if !authenticated || username == "" {
		return "", "", "", &sessionValidationError{kind: sessionValidationErrorKindUnauthenticated}
	}

	csrfToken, err := getOrCreateSessionCSRFToken(session, r, w)
	if err != nil {
		return "", "", "", &sessionValidationError{kind: sessionValidationErrorKindSessionInit, err: err}
	}

	return username, role, csrfToken, nil
}

// RequireAuth checks if user is authenticated.
func RequireAuth(store sessions.Store) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, role, csrfToken, err := validateSession(store, w, r)
			if err != nil {
				if verr, ok := err.(*sessionValidationError); ok && verr.kind == sessionValidationErrorKindUnauthenticated {
					http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
				} else {
					http.Redirect(w, r, "/login?error=Unable to initialize session", http.StatusTemporaryRedirect)
				}
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
			username, role, csrfToken, err := validateSession(store, w, r)
			if err != nil {
				if verr, ok := err.(*sessionValidationError); ok && verr.kind == sessionValidationErrorKindUnauthenticated {
					writeJSON(w, http.StatusUnauthorized, map[string]string{
						"error":   "Authentication required",
						"message": "Please log in to access this endpoint",
					})
				} else {
					writeJSON(w, http.StatusInternalServerError, map[string]string{
						"error":   "Failed to initialize session",
						"message": "Unable to initialize session",
					})
				}
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
