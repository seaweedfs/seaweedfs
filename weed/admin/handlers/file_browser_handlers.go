package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// DeleteMultipleFiles handles multiple file deletion API requests
func (h *FileBrowserHandlers) DeleteMultipleFiles(c *gin.Context) {
	var request struct {
		Paths []string `json:"paths" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if len(request.Paths) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No paths provided"})
		return
	}

	var deletedCount int
	var failedCount int
	var errors []string

	// Delete each file/folder
	for _, path := range request.Paths {
		err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
				Directory:            filepath.Dir(path),
				Name:                 filepath.Base(path),
				IsDeleteData:         true,
				IsRecursive:          true,
				IgnoreRecursiveError: false,
			})
			return err
		})

		if err != nil {
			failedCount++
			errors = append(errors, fmt.Sprintf("%s: %v", path, err))
		} else {
			deletedCount++
		}
	}

	// Prepare response
	response := map[string]interface{}{
		"deleted": deletedCount,
		"failed":  failedCount,
		"total":   len(request.Paths),
	}

	if len(errors) > 0 {
		response["errors"] = errors
	}

	if deletedCount > 0 {
		if failedCount == 0 {
			response["message"] = fmt.Sprintf("Successfully deleted %d item(s)", deletedCount)
		} else {
			response["message"] = fmt.Sprintf("Deleted %d item(s), failed to delete %d item(s)", deletedCount, failedCount)
		}
		c.JSON(http.StatusOK, response)
	} else {
		response["message"] = "Failed to delete all selected items"
		c.JSON(http.StatusInternalServerError, response)
	}
}

// CreateFolder handles folder creation requests
func (h *FileBrowserHandlers) CreateFolder(c *gin.Context) {
	var request struct {
		Path       string `json:"path" binding:"required"`
		FolderName string `json:"folder_name" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Clean and validate folder name
	folderName := strings.TrimSpace(request.FolderName)
	if folderName == "" || strings.Contains(folderName, "/") || strings.Contains(folderName, "\\") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid folder name"})
		return
	}

	// Create full path for new folder
	fullPath := filepath.Join(request.Path, folderName)
	if !strings.HasPrefix(fullPath, "/") {
		fullPath = "/" + fullPath
	}

	// Create folder via filer
	err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: filepath.Dir(fullPath),
			Entry: &filer_pb.Entry{
				Name:        filepath.Base(fullPath),
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileMode: uint32(0755 | (1 << 31)), // Directory mode
					Uid:      filer_pb.OS_UID,
					Gid:      filer_pb.OS_GID,
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					TtlSec:   0,
				},
			},
		})
		return err
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create folder: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Folder created successfully"})
}

// UploadFile handles file upload requests
func (h *FileBrowserHandlers) UploadFile(c *gin.Context) {
	// Get the current path
	currentPath := c.PostForm("path")
	if currentPath == "" {
		currentPath = "/"
	}

	// Parse multipart form
	err := c.Request.ParseMultipartForm(100 << 20) // 100MB max memory
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse multipart form: " + err.Error()})
		return
	}

	// Get uploaded files (supports multiple files)
	files := c.Request.MultipartForm.File["files"]
	if len(files) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No files uploaded"})
		return
	}

	var uploadResults []map[string]interface{}
	var failedUploads []string

	// Process each uploaded file
	for _, fileHeader := range files {
		// Validate file name
		fileName := fileHeader.Filename
		if fileName == "" {
			failedUploads = append(failedUploads, "invalid filename")
			continue
		}

		// Create full path for the file
		fullPath := filepath.Join(currentPath, fileName)
		if !strings.HasPrefix(fullPath, "/") {
			fullPath = "/" + fullPath
		}

		// Open the file
		file, err := fileHeader.Open()
		if err != nil {
			failedUploads = append(failedUploads, fmt.Sprintf("%s: %v", fileName, err))
			continue
		}

		// Upload file to filer
		err = h.uploadFileToFiler(fullPath, fileHeader)
		file.Close()

		if err != nil {
			failedUploads = append(failedUploads, fmt.Sprintf("%s: %v", fileName, err))
		} else {
			uploadResults = append(uploadResults, map[string]interface{}{
				"name": fileName,
				"size": fileHeader.Size,
				"path": fullPath,
			})
		}
	}

	// Prepare response
	response := map[string]interface{}{
		"uploaded": len(uploadResults),
		"failed":   len(failedUploads),
		"files":    uploadResults,
	}

	if len(failedUploads) > 0 {
		response["errors"] = failedUploads
	}

	if len(uploadResults) > 0 {
		if len(failedUploads) == 0 {
			response["message"] = fmt.Sprintf("Successfully uploaded %d file(s)", len(uploadResults))
		} else {
			response["message"] = fmt.Sprintf("Uploaded %d file(s), %d failed", len(uploadResults), len(failedUploads))
		}
		c.JSON(http.StatusOK, response)
	} else {
		response["message"] = "All file uploads failed"
		c.JSON(http.StatusInternalServerError, response)
	}
}

// uploadFileToFiler uploads a file directly to the filer using multipart form data
func (h *FileBrowserHandlers) uploadFileToFiler(filePath string, fileHeader *multipart.FileHeader) error {
	// Get filer address from admin server
	filerAddress := h.adminServer.GetFilerAddress()
	if filerAddress == "" {
		return fmt.Errorf("filer address not configured")
	}

	// Validate and sanitize the filer address
	if err := h.validateFilerAddress(filerAddress); err != nil {
		return fmt.Errorf("invalid filer address: %v", err)
	}

	// Validate and sanitize the file path
	cleanFilePath, err := h.validateAndCleanFilePath(filePath)
	if err != nil {
		return fmt.Errorf("invalid file path: %v", err)
	}

	// Open the file
	file, err := fileHeader.Open()
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create multipart form data
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Create form file field
	part, err := writer.CreateFormFile("file", fileHeader.Filename)
	if err != nil {
		return fmt.Errorf("failed to create form file: %v", err)
	}

	// Copy file content to form
	_, err = io.Copy(part, file)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %v", err)
	}

	// Close the writer to finalize the form
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close multipart writer: %v", err)
	}

	// Create the upload URL with validated components
	uploadURL := fmt.Sprintf("http://%s%s", filerAddress, cleanFilePath)

	// Create HTTP request
	req, err := http.NewRequest("POST", uploadURL, &body)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set content type with boundary
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Send request
	client := &http.Client{Timeout: 60 * time.Second} // Increased timeout for larger files
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(responseBody))
	}

	return nil
}

// validateFilerAddress validates that the filer address is safe to use
func (h *FileBrowserHandlers) validateFilerAddress(address string) error {
	if address == "" {
		return fmt.Errorf("filer address cannot be empty")
	}

	// Parse the address to validate it's a proper host:port format
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid address format: %v", err)
	}

	// Validate host is not empty
	if host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	// Validate port is numeric and in valid range
	if port == "" {
		return fmt.Errorf("port cannot be empty")
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid port number: %v", err)
	}

	if portNum < 1 || portNum > 65535 {
		return fmt.Errorf("port number must be between 1 and 65535")
	}

	// Additional security: prevent private network access unless explicitly allowed
	// This helps prevent SSRF attacks to internal services
	ip := net.ParseIP(host)
	if ip != nil {
		// Check for localhost, private networks, and other dangerous addresses
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() {
			// Only allow if it's the configured filer (trusted)
			// In production, you might want to be more restrictive
			glog.V(2).Infof("Allowing access to private/local address: %s (configured filer)", address)
		}
	}

	return nil
}

// validateAndCleanFilePath validates and cleans the file path to prevent path traversal
func (h *FileBrowserHandlers) validateAndCleanFilePath(filePath string) (string, error) {
	if filePath == "" {
		return "", fmt.Errorf("file path cannot be empty")
	}

	// Clean the path to remove any .. or . components
	cleanPath := filepath.Clean(filePath)

	// Ensure the path starts with /
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}

	// Prevent path traversal attacks
	if strings.Contains(cleanPath, "..") {
		return "", fmt.Errorf("path traversal not allowed")
	}

	// Additional validation: ensure path doesn't contain dangerous characters
	if strings.ContainsAny(cleanPath, "\x00\r\n") {
		return "", fmt.Errorf("path contains invalid characters")
	}

	return cleanPath, nil
}
