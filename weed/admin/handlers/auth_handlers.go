package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// AuthHandlers contains authentication-related HTTP handlers
type AuthHandlers struct {
	adminServer *dash.AdminServer
}

// NewAuthHandlers creates a new instance of AuthHandlers
func NewAuthHandlers(adminServer *dash.AdminServer) *AuthHandlers {
	return &AuthHandlers{
		adminServer: adminServer,
	}
}

// ShowLogin displays the login page
func (a *AuthHandlers) ShowLogin(c *gin.Context) {
	errorMessage := c.Query("error")

	// Render login template
	c.Header("Content-Type", "text/html")
	loginComponent := layout.LoginForm(c, "SeaweedFS Admin", errorMessage)
	err := loginComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render login template: " + err.Error()})
		return
	}
}

// HandleLogin handles login form submission
func (a *AuthHandlers) HandleLogin(adminUser, adminPassword, readOnlyUser, readOnlyPassword string) gin.HandlerFunc {
	return a.adminServer.HandleLogin(adminUser, adminPassword, readOnlyUser, readOnlyPassword)
}

// HandleLogout handles user logout
func (a *AuthHandlers) HandleLogout(c *gin.Context) {
	a.adminServer.HandleLogout(c)
}
