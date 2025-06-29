package dash

import (
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/cmd/dashboard/view/layout"
)

// ShowLoginForm displays the login page
func (s *DashboardServer) ShowLoginForm(c *gin.Context) {
	// If no password is set, redirect to dashboard
	if s.adminPassword == "" {
		c.Redirect(http.StatusTemporaryRedirect, "/dashboard")
		return
	}

	// Check if already authenticated
	session := sessions.Default(c)
	if session.Get("authenticated") == true {
		c.Redirect(http.StatusTemporaryRedirect, "/dashboard")
		return
	}

	layout.LoginForm(c, "SeaweedFS Dashboard", "").Render(c.Request.Context(), c.Writer)
}

// ProcessLogin handles login form submission
func (s *DashboardServer) ProcessLogin(c *gin.Context) {
	if s.adminPassword == "" {
		c.Redirect(http.StatusTemporaryRedirect, "/dashboard")
		return
	}

	username := c.PostForm("username")
	password := c.PostForm("password")

	if username == s.adminUser && password == s.adminPassword {
		session := sessions.Default(c)
		session.Set("authenticated", true)
		session.Set("username", username)
		session.Save()

		c.Redirect(http.StatusTemporaryRedirect, "/dashboard")
		return
	}

	// Authentication failed
	layout.LoginForm(c, "SeaweedFS Dashboard", "Invalid username or password").Render(c.Request.Context(), c.Writer)
}

// Logout handles user logout
func (s *DashboardServer) Logout(c *gin.Context) {
	session := sessions.Default(c)
	session.Clear()
	session.Save()
	c.Redirect(http.StatusTemporaryRedirect, "/login")
}

// HealthCheck returns system health status
func (s *DashboardServer) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
		"master": len(s.masterAddresses),
		"filer":  s.filerAddress,
	})
}
