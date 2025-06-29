package dash

import (
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

// AuthMiddleware checks if user is authenticated
func (s *DashboardServer) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// If no password is set, allow all requests
		if s.adminPassword == "" {
			c.Set("username", s.adminUser)
			c.Next()
			return
		}

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
