package dash

import (
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

// RequireAuth checks if user is authenticated
func RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		authenticated := session.Get("authenticated")
		username := session.Get("username")

		if authenticated != true || username == nil {
			c.Redirect(http.StatusTemporaryRedirect, "/login")
			c.Abort()
			return
		}

		// Set username in context for use in handlers
		c.Set("username", username)
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

		if authenticated != true || username == nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error":   "Authentication required",
				"message": "Please log in to access this endpoint",
			})
			c.Abort()
			return
		}

		// Set username in context for use in handlers
		c.Set("username", username)
		c.Next()
	}
}
