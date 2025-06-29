package dash

import (
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

// ShowLogin displays the login page
func (s *AdminServer) ShowLogin(c *gin.Context) {
	// If authentication is not required, redirect to admin
	session := sessions.Default(c)
	if session.Get("authenticated") == true {
		c.Redirect(http.StatusTemporaryRedirect, "/admin")
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
			session.Set("authenticated", true)
			session.Set("username", loginUsername)
			session.Save()

			c.Redirect(http.StatusTemporaryRedirect, "/admin")
			return
		}

		// Authentication failed
		c.Redirect(http.StatusTemporaryRedirect, "/login?error=Invalid credentials")
	}
}

// HandleLogout handles user logout
func (s *AdminServer) HandleLogout(c *gin.Context) {
	session := sessions.Default(c)
	session.Clear()
	session.Save()
	c.Redirect(http.StatusTemporaryRedirect, "/login")
}

// Additional methods for admin functionality
func (s *AdminServer) GetClusterTopologyHandler(c *gin.Context) {
	topology, err := s.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, topology)
}

func (s *AdminServer) GetMasters(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"masters": []string{s.masterAddress}})
}

func (s *AdminServer) GetVolumeServers(c *gin.Context) {
	topology, err := s.GetClusterTopology()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"volume_servers": topology.VolumeServers})
}

func (s *AdminServer) AssignVolume(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Volume assignment not yet implemented"})
}

func (s *AdminServer) ListVolumes(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Volume listing not yet implemented"})
}

func (s *AdminServer) CreateVolume(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Volume creation not yet implemented"})
}

func (s *AdminServer) DeleteVolume(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Volume deletion not yet implemented"})
}

func (s *AdminServer) ReplicateVolume(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Volume replication not yet implemented"})
}

func (s *AdminServer) BrowseFiles(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "File browsing not yet implemented"})
}

func (s *AdminServer) UploadFile(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "File upload not yet implemented"})
}

func (s *AdminServer) DeleteFile(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "File deletion not yet implemented"})
}

func (s *AdminServer) ShowMetrics(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Metrics display not yet implemented"})
}

func (s *AdminServer) GetMetricsData(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Metrics data not yet implemented"})
}

func (s *AdminServer) TriggerGC(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Garbage collection not yet implemented"})
}

func (s *AdminServer) CompactVolumes(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Volume compaction not yet implemented"})
}

func (s *AdminServer) GetMaintenanceStatus(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "Maintenance status not yet implemented"})
}
