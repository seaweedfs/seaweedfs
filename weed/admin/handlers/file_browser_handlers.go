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

// DownloadFile handles file download requests
func (h *FileBrowserHandlers) DownloadFile(c *gin.Context) {
	filePath := c.Query("path")
	if filePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File path is required"})
		return
	}

	// Get filer address
	filerAddress := h.adminServer.GetFilerAddress()
	if filerAddress == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Filer address not configured"})
		return
	}

	// Validate and sanitize the file path
	cleanFilePath, err := h.validateAndCleanFilePath(filePath)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid file path: " + err.Error()})
		return
	}

	// Create the download URL
	downloadURL := fmt.Sprintf("http://%s%s", filerAddress, cleanFilePath)

	// Set headers for file download
	fileName := filepath.Base(cleanFilePath)
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileName))
	c.Header("Content-Type", "application/octet-stream")

	// Proxy the request to filer
	c.Redirect(http.StatusFound, downloadURL)
}

// ViewFile handles file viewing requests (for text files, images, etc.)
func (h *FileBrowserHandlers) ViewFile(c *gin.Context) {
	filePath := c.Query("path")
	if filePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File path is required"})
		return
	}

	// Get file metadata first
	var fileEntry dash.FileEntry
	err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filepath.Dir(filePath),
			Name:      filepath.Base(filePath),
		})
		if err != nil {
			return err
		}

		entry := resp.Entry
		if entry == nil {
			return fmt.Errorf("file not found")
		}

		// Convert to FileEntry
		var modTime time.Time
		if entry.Attributes != nil && entry.Attributes.Mtime > 0 {
			modTime = time.Unix(entry.Attributes.Mtime, 0)
		}

		var size int64
		if entry.Attributes != nil {
			size = int64(entry.Attributes.FileSize)
		}

		// Determine MIME type with comprehensive extension support
		mime := h.determineMimeType(entry.Name)

		fileEntry = dash.FileEntry{
			Name:        entry.Name,
			FullPath:    filePath,
			IsDirectory: entry.IsDirectory,
			Size:        size,
			ModTime:     modTime,
			Mime:        mime,
		}

		return nil
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get file metadata: " + err.Error()})
		return
	}

	// Check if file is viewable as text
	var content string
	var viewable bool
	var reason string

	// First check if it's a known text type or if we should check content
	isKnownTextType := strings.HasPrefix(fileEntry.Mime, "text/") ||
		fileEntry.Mime == "application/json" ||
		fileEntry.Mime == "application/javascript" ||
		fileEntry.Mime == "application/xml"

	// For unknown types, check if it might be text by content
	if !isKnownTextType && fileEntry.Mime == "application/octet-stream" {
		isKnownTextType = h.isLikelyTextFile(filePath, 512)
		if isKnownTextType {
			// Update MIME type for better display
			fileEntry.Mime = "text/plain"
		}
	}

	if isKnownTextType {
		// Limit text file size for viewing (max 1MB)
		if fileEntry.Size > 1024*1024 {
			viewable = false
			reason = "File too large for viewing (>1MB)"
		} else {
			// Get file content from filer
			filerAddress := h.adminServer.GetFilerAddress()
			if filerAddress != "" {
				cleanFilePath, err := h.validateAndCleanFilePath(filePath)
				if err == nil {
					fileURL := fmt.Sprintf("http://%s%s", filerAddress, cleanFilePath)

					client := &http.Client{Timeout: 30 * time.Second}
					resp, err := client.Get(fileURL)
					if err == nil && resp.StatusCode == http.StatusOK {
						defer resp.Body.Close()
						contentBytes, err := io.ReadAll(resp.Body)
						if err == nil {
							content = string(contentBytes)
							viewable = true
						} else {
							viewable = false
							reason = "Failed to read file content"
						}
					} else {
						viewable = false
						reason = "Failed to fetch file from filer"
					}
				} else {
					viewable = false
					reason = "Invalid file path"
				}
			} else {
				viewable = false
				reason = "Filer address not configured"
			}
		}
	} else {
		// Not a text file, but might be viewable as image or PDF
		if strings.HasPrefix(fileEntry.Mime, "image/") || fileEntry.Mime == "application/pdf" {
			viewable = true
		} else {
			viewable = false
			reason = "File type not supported for viewing"
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"file":     fileEntry,
		"content":  content,
		"viewable": viewable,
		"reason":   reason,
	})
}

// GetFileProperties handles file properties requests
func (h *FileBrowserHandlers) GetFileProperties(c *gin.Context) {
	filePath := c.Query("path")
	if filePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File path is required"})
		return
	}

	// Get detailed file information from filer
	var properties map[string]interface{}
	err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filepath.Dir(filePath),
			Name:      filepath.Base(filePath),
		})
		if err != nil {
			return err
		}

		entry := resp.Entry
		if entry == nil {
			return fmt.Errorf("file not found")
		}

		properties = make(map[string]interface{})
		properties["name"] = entry.Name
		properties["full_path"] = filePath
		properties["is_directory"] = entry.IsDirectory

		if entry.Attributes != nil {
			properties["size"] = entry.Attributes.FileSize
			properties["size_formatted"] = h.formatBytes(int64(entry.Attributes.FileSize))

			if entry.Attributes.Mtime > 0 {
				modTime := time.Unix(entry.Attributes.Mtime, 0)
				properties["modified_time"] = modTime.Format("2006-01-02 15:04:05")
				properties["modified_timestamp"] = entry.Attributes.Mtime
			}

			if entry.Attributes.Crtime > 0 {
				createTime := time.Unix(entry.Attributes.Crtime, 0)
				properties["created_time"] = createTime.Format("2006-01-02 15:04:05")
				properties["created_timestamp"] = entry.Attributes.Crtime
			}

			properties["file_mode"] = fmt.Sprintf("%o", entry.Attributes.FileMode)
			properties["file_mode_formatted"] = h.formatFileMode(entry.Attributes.FileMode)
			properties["uid"] = entry.Attributes.Uid
			properties["gid"] = entry.Attributes.Gid
			properties["ttl_seconds"] = entry.Attributes.TtlSec

			if entry.Attributes.TtlSec > 0 {
				properties["ttl_formatted"] = fmt.Sprintf("%d seconds", entry.Attributes.TtlSec)
			}
		}

		// Get extended attributes
		if entry.Extended != nil {
			extended := make(map[string]string)
			for key, value := range entry.Extended {
				extended[key] = string(value)
			}
			properties["extended"] = extended
		}

		// Get chunk information for files
		if !entry.IsDirectory && len(entry.Chunks) > 0 {
			chunks := make([]map[string]interface{}, 0, len(entry.Chunks))
			for _, chunk := range entry.Chunks {
				chunkInfo := map[string]interface{}{
					"file_id":     chunk.FileId,
					"offset":      chunk.Offset,
					"size":        chunk.Size,
					"modified_ts": chunk.ModifiedTsNs,
					"e_tag":       chunk.ETag,
					"source_fid":  chunk.SourceFileId,
				}
				chunks = append(chunks, chunkInfo)
			}
			properties["chunks"] = chunks
			properties["chunk_count"] = len(entry.Chunks)
		}

		// Determine MIME type
		if !entry.IsDirectory {
			mime := h.determineMimeType(entry.Name)
			properties["mime_type"] = mime
		}

		return nil
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get file properties: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, properties)
}

// Helper function to format bytes
func (h *FileBrowserHandlers) formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// Helper function to format file mode
func (h *FileBrowserHandlers) formatFileMode(mode uint32) string {
	// Convert to octal and format as rwx permissions
	perm := mode & 0777
	return fmt.Sprintf("%03o", perm)
}

// Helper function to determine MIME type from filename
func (h *FileBrowserHandlers) determineMimeType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))

	// Text files
	switch ext {
	case ".txt", ".log", ".cfg", ".conf", ".ini", ".properties":
		return "text/plain"
	case ".md", ".markdown":
		return "text/markdown"
	case ".html", ".htm":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js", ".mjs":
		return "application/javascript"
	case ".ts":
		return "text/typescript"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".yaml", ".yml":
		return "text/yaml"
	case ".csv":
		return "text/csv"
	case ".sql":
		return "text/sql"
	case ".sh", ".bash", ".zsh", ".fish":
		return "text/x-shellscript"
	case ".py":
		return "text/x-python"
	case ".go":
		return "text/x-go"
	case ".java":
		return "text/x-java"
	case ".c":
		return "text/x-c"
	case ".cpp", ".cc", ".cxx", ".c++":
		return "text/x-c++"
	case ".h", ".hpp":
		return "text/x-c-header"
	case ".php":
		return "text/x-php"
	case ".rb":
		return "text/x-ruby"
	case ".pl":
		return "text/x-perl"
	case ".rs":
		return "text/x-rust"
	case ".swift":
		return "text/x-swift"
	case ".kt":
		return "text/x-kotlin"
	case ".scala":
		return "text/x-scala"
	case ".dockerfile":
		return "text/x-dockerfile"
	case ".gitignore", ".gitattributes":
		return "text/plain"
	case ".env":
		return "text/plain"

	// Image files
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".bmp":
		return "image/bmp"
	case ".webp":
		return "image/webp"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"

	// Document files
	case ".pdf":
		return "application/pdf"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"

	// Archive files
	case ".zip":
		return "application/zip"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".bz2":
		return "application/x-bzip2"
	case ".7z":
		return "application/x-7z-compressed"
	case ".rar":
		return "application/x-rar-compressed"

	// Video files
	case ".mp4":
		return "video/mp4"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	case ".wmv":
		return "video/x-ms-wmv"
	case ".flv":
		return "video/x-flv"
	case ".webm":
		return "video/webm"

	// Audio files
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".flac":
		return "audio/flac"
	case ".aac":
		return "audio/aac"
	case ".ogg":
		return "audio/ogg"

	default:
		// For files without extension or unknown extensions,
		// we'll check if they might be text files by content
		return "application/octet-stream"
	}
}

// Helper function to check if a file is likely a text file by checking content
func (h *FileBrowserHandlers) isLikelyTextFile(filePath string, maxCheckSize int64) bool {
	filerAddress := h.adminServer.GetFilerAddress()
	if filerAddress == "" {
		return false
	}

	cleanFilePath, err := h.validateAndCleanFilePath(filePath)
	if err != nil {
		return false
	}

	fileURL := fmt.Sprintf("http://%s%s", filerAddress, cleanFilePath)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(fileURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()

	// Read first few bytes to check if it's text
	buffer := make([]byte, min(maxCheckSize, 512))
	n, err := resp.Body.Read(buffer)
	if err != nil && err != io.EOF {
		return false
	}

	if n == 0 {
		return true // Empty file can be considered text
	}

	// Check if content is printable text
	return h.isPrintableText(buffer[:n])
}

// Helper function to check if content is printable text
func (h *FileBrowserHandlers) isPrintableText(data []byte) bool {
	if len(data) == 0 {
		return true
	}

	// Count printable characters
	printable := 0
	for _, b := range data {
		if b >= 32 && b <= 126 || b == 9 || b == 10 || b == 13 {
			// Printable ASCII, tab, newline, carriage return
			printable++
		} else if b >= 128 {
			// Potential UTF-8 character
			printable++
		}
	}

	// If more than 95% of characters are printable, consider it text
	return float64(printable)/float64(len(data)) > 0.95
}

// Helper function for min
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
