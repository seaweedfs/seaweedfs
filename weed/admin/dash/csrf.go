package dash

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/gorilla/sessions"
)

const sessionCSRFTokenKey = "csrf_token"

func generateCSRFToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(tokenBytes), nil
}

func getOrCreateSessionCSRFToken(session *sessions.Session, r *http.Request, w http.ResponseWriter) (string, error) {
	if existing, ok := session.Values[sessionCSRFTokenKey].(string); ok && existing != "" {
		return existing, nil
	}
	token, err := generateCSRFToken()
	if err != nil {
		return "", err
	}
	session.Values[sessionCSRFTokenKey] = token
	if err := session.Save(r, w); err != nil {
		return "", err
	}
	return token, nil
}

func requireSessionCSRFToken(w http.ResponseWriter, r *http.Request) bool {
	expectedToken := CSRFTokenFromContext(r.Context())
	username := UsernameFromContext(r.Context())
	if expectedToken == "" {
		// Admin UI can run without auth; in that mode CSRF token checks are not applicable.
		if username == "" {
			return true
		}
		writeJSONError(w, http.StatusForbidden, "missing CSRF session token")
		return false
	}

	providedToken, err := getProvidedCSRFToken(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
		return false
	}
	if providedToken == "" || subtle.ConstantTimeCompare([]byte(expectedToken), []byte(providedToken)) != 1 {
		writeJSONError(w, http.StatusForbidden, "invalid CSRF token")
		return false
	}
	return true
}

func getProvidedCSRFToken(r *http.Request) (string, error) {
	providedToken := r.Header.Get("X-CSRF-Token")
	if providedToken != "" {
		return providedToken, nil
	}
	if err := r.ParseForm(); err != nil {
		return "", err
	}
	return r.FormValue("csrf_token"), nil
}

func EnsureSessionCSRFToken(session *sessions.Session, r *http.Request, w http.ResponseWriter) (string, error) {
	if session == nil {
		return "", fmt.Errorf("session is nil")
	}
	return getOrCreateSessionCSRFToken(session, r, w)
}

func ValidateSessionCSRFToken(session *sessions.Session, r *http.Request) error {
	if session == nil {
		return fmt.Errorf("session is nil")
	}
	expectedToken, _ := session.Values[sessionCSRFTokenKey].(string)
	providedToken, err := getProvidedCSRFToken(r)
	if err != nil {
		return fmt.Errorf("failed to read CSRF token: %w", err)
	}
	if expectedToken == "" {
		return fmt.Errorf("missing session CSRF token")
	}
	if providedToken == "" || subtle.ConstantTimeCompare([]byte(expectedToken), []byte(providedToken)) != 1 {
		return fmt.Errorf("invalid CSRF token")
	}
	return nil
}
