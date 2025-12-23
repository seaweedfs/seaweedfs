package dash

import (
	"net/http"
	"strings"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

// RequireAuth checks if user is authenticated
func RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		authenticated := session.Get("authenticated")
		username := session.Get("username")
		role := session.Get("role")

		if authenticated != true || username == nil {
			c.Redirect(http.StatusTemporaryRedirect, "/login")
			c.Abort()
			return
		}

		// Set username and role in context for use in handlers
		c.Set("username", username)
		if role != nil {
			c.Set("role", role)
		} else {
			// Default to admin for backward compatibility
			c.Set("role", "admin")
		}
		c.Next()
	}
}

// RequireAuthAPI checks if user is authenticated for API endpoints
// Returns JSON error instead of redirecting to login page
func RequireAuthAPI() gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		authenticated := session.Get("authenticated")
		username := session.Get("username")
		role := session.Get("role")

		if authenticated != true || username == nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":   "Authentication required",
				"message": "Please log in to access this endpoint",
			})
			c.Abort()
			return
		}

		// Set username and role in context for use in handlers
		c.Set("username", username)
		if role != nil {
			c.Set("role", role)
		} else {
			// Default to admin for backward compatibility
			c.Set("role", "admin")
		}
		c.Next()
	}
}

// RequireWriteAccess checks if user has admin role (write access)
// Returns JSON error for API endpoints, redirects for HTML endpoints
func RequireWriteAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		role, exists := c.Get("role")
		if !exists {
			role = "admin" // Default for backward compatibility
		}

		roleStr, ok := role.(string)
		if !ok || roleStr != "admin" {
			// Check if this is an API request (path starts with /api) or HTML request
			path := c.Request.URL.Path
			if strings.HasPrefix(path, "/api") {
				c.JSON(http.StatusForbidden, gin.H{
					"error":   "Insufficient permissions",
					"message": "This operation requires admin access. Read-only users can only view data.",
				})
			} else {
				c.Redirect(http.StatusSeeOther, "/admin?error=Insufficient permissions")
			}
			c.Abort()
			return
		}

		c.Next()
	}
}
