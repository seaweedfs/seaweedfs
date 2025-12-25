package dash

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ShowLogin displays the login page
func (s *AdminServer) ShowLogin(c *gin.Context) {
	// If authentication is not required, redirect to admin
	session := sessions.Default(c)
	if session.Get("authenticated") == true {
		c.Redirect(http.StatusSeeOther, "/admin")
		return
	}

	// For now, return a simple login form as JSON
	c.HTML(http.StatusOK, "login.html", gin.H{
		"title": "SeaweedFS Admin Login",
		"error": c.Query("error"),
	})
}

// HandleLogin handles login form submission
func (s *AdminServer) HandleLogin(adminUser, adminPassword, readOnlyUser, readOnlyPassword string) gin.HandlerFunc {
	return func(c *gin.Context) {
		loginUsername := c.PostForm("username")
		loginPassword := c.PostForm("password")

		var role string
		var authenticated bool

		// Check admin credentials
		if adminPassword != "" && loginUsername == adminUser && subtle.ConstantTimeCompare([]byte(loginPassword), []byte(adminPassword)) == 1 {
			role = "admin"
			authenticated = true
		} else if readOnlyPassword != "" && loginUsername == readOnlyUser && subtle.ConstantTimeCompare([]byte(loginPassword), []byte(readOnlyPassword)) == 1 {
			// Check read-only credentials
			role = "readonly"
			authenticated = true
		}

		if authenticated {
			session := sessions.Default(c)
			// Clear any existing invalid session data before setting new values
			session.Clear()
			session.Set("authenticated", true)
			session.Set("username", loginUsername)
			session.Set("role", role)
			if err := session.Save(); err != nil {
				// Log the detailed error server-side for diagnostics
				glog.Errorf("Failed to save session for user %s: %v", loginUsername, err)
				c.Redirect(http.StatusSeeOther, "/login?error=Unable to create session. Please try again or contact administrator.")
				return
			}

			c.Redirect(http.StatusSeeOther, "/admin")
			return
		}

		// Authentication failed
		c.Redirect(http.StatusSeeOther, "/login?error=Invalid credentials")
	}
}

// HandleLogout handles user logout
func (s *AdminServer) HandleLogout(c *gin.Context) {
	session := sessions.Default(c)
	session.Clear()
	if err := session.Save(); err != nil {
		glog.Warningf("Failed to save session during logout: %v", err)
	}
	c.Redirect(http.StatusSeeOther, "/login")
}
