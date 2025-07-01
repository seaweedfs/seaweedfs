package handlers

import (
	"context"
	"net/http"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type FileBrowserHandlers struct {
	adminServer *dash.AdminServer
}

func NewFileBrowserHandlers(adminServer *dash.AdminServer) *FileBrowserHandlers {
	return &FileBrowserHandlers{
		adminServer: adminServer,
	}
}

// ShowFileBrowser renders the file browser page
func (h *FileBrowserHandlers) ShowFileBrowser(c *gin.Context) {
	// Get path from query parameter, default to root
	path := c.DefaultQuery("path", "/")

	// Get file browser data
	browserData, err := h.adminServer.GetFileBrowser(path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get file browser data: " + err.Error()})
		return
	}

	// Set username
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	browserData.Username = username

	// Render HTML template
	c.Header("Content-Type", "text/html")
	browserComponent := app.FileBrowser(*browserData)
	layoutComponent := layout.Layout(c, browserComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// DeleteFile handles file deletion API requests
func (h *FileBrowserHandlers) DeleteFile(c *gin.Context) {
	var request struct {
		Path string `json:"path" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Delete file via filer
	err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:            filepath.Dir(request.Path),
			Name:                 filepath.Base(request.Path),
			IsDeleteData:         true,
			IsRecursive:          true,
			IgnoreRecursiveError: false,
		})
		return err
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete file: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "File deleted successfully"})
}
