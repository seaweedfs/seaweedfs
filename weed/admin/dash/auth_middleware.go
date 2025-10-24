package dash

import (
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
func (s *AdminServer) HandleLogin(username, password string) gin.HandlerFunc {
	return func(c *gin.Context) {
		loginUsername := c.PostForm("username")
		loginPassword := c.PostForm("password")

		if loginUsername == username && loginPassword == password {
			session := sessions.Default(c)
			// Clear any existing invalid session data before setting new values
			session.Clear()
			session.Set("authenticated", true)
			session.Set("username", loginUsername)
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
	session.Save()
	c.Redirect(http.StatusSeeOther, "/login")
}
