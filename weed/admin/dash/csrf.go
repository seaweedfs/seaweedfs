package dash

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

const sessionCSRFTokenKey = "csrf_token"

func generateCSRFToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(tokenBytes), nil
}

func getOrCreateSessionCSRFToken(session sessions.Session) (string, error) {
	if existing, ok := session.Get(sessionCSRFTokenKey).(string); ok && existing != "" {
		return existing, nil
	}
	token, err := generateCSRFToken()
	if err != nil {
		return "", err
	}
	session.Set(sessionCSRFTokenKey, token)
	if err := session.Save(); err != nil {
		return "", err
	}
	return token, nil
}

func requireSessionCSRFToken(c *gin.Context) bool {
	session := sessions.Default(c)
	if session.Get("authenticated") != true {
		// Admin UI can run without auth; in that mode CSRF token checks are not applicable.
		return true
	}

	expectedToken, ok := session.Get(sessionCSRFTokenKey).(string)
	if !ok || expectedToken == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "missing CSRF session token"})
		return false
	}

	providedToken := c.GetHeader("X-CSRF-Token")
	if providedToken == "" {
		providedToken = c.PostForm("csrf_token")
	}
	if providedToken == "" || subtle.ConstantTimeCompare([]byte(expectedToken), []byte(providedToken)) != 1 {
		c.JSON(http.StatusForbidden, gin.H{"error": "invalid CSRF token"})
		return false
	}
	return true
}
