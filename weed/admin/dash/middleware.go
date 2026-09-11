package dash

import (
	"crypto/subtle"
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

// bearerTokenFromRequest extracts a bearer token from the Authorization header.
// The scheme name is matched case-insensitively per RFC 7235. The token itself
// is compared exactly (not case-folded).
// Returns ("", false) when no bearer token is present.
func bearerTokenFromRequest(r *http.Request) (string, bool) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", false
	}
	parts := strings.Fields(auth)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	token := parts[1]
	if token == "" {
		return "", false
	}
	return token, true
}

// validateBearerToken checks the Authorization header for a valid bearer token.
// When apiKey is empty, token auth is disabled and the function returns false
// (caller should fall back to session validation). When a token is present and
// matches apiKey, the request is authenticated as admin with write access.
func validateBearerToken(apiKey string, r *http.Request) bool {
	if apiKey == "" {
		return false
	}
	token, ok := bearerTokenFromRequest(r)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) == 1
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
				prefix := URLPrefixFromContext(r.Context())
				if verr, ok := err.(*sessionValidationError); ok && verr.kind == sessionValidationErrorKindUnauthenticated {
					http.Redirect(w, r, prefix+"/login", http.StatusTemporaryRedirect)
				} else {
					http.Redirect(w, r, prefix+"/login?error=Unable to initialize session", http.StatusTemporaryRedirect)
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
// When apiKey is non-empty, a matching Bearer token in the Authorization header
// authenticates the request as admin with write access, bypassing session auth.
func RequireAuthAPI(store sessions.Store, apiKey string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bearer token auth: alternative to session-based auth for API clients.
			if validateBearerToken(apiKey, r) {
				ctx := WithAuthContext(r.Context(), "api-token", "admin", "")
				ctx = WithAuthMethod(ctx, AuthMethodBearer)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

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
			ctx = WithAuthMethod(ctx, AuthMethodSession)
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
					http.Redirect(w, r, URLPrefixFromContext(r.Context())+"/admin?error=Insufficient permissions", http.StatusSeeOther)
				}
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
