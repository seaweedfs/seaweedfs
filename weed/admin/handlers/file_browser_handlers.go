package handlers

import (
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

type FileBrowserHandlers struct {
	adminServer *dash.AdminServer
	httpClient  *client.HTTPClient
}

func NewFileBrowserHandlers(adminServer *dash.AdminServer) *FileBrowserHandlers {
	// Create HTTP client with TLS support from https.client configuration
	// The client is created without a timeout - each operation will set its own timeout
	// If TLS is enabled but misconfigured, fail fast to alert the operator immediately
	// rather than silently falling back to HTTP and causing confusing runtime errors
	httpClient, err := client.NewHttpClient(client.Client)
	if err != nil {
		glog.Fatalf("Failed to create HTTPS client for file browser: %v", err)
	}

	return &FileBrowserHandlers{
		adminServer: adminServer,
		httpClient:  httpClient,
	}
}

// ShowFileBrowser renders the file browser page
func (h *FileBrowserHandlers) ShowFileBrowser(w http.ResponseWriter, r *http.Request) {
	// Get path from query parameter, default to root
	path := defaultQuery(r.URL.Query().Get("path"), "/")
	// Normalize Windows-style paths for consistency
	path = util.CleanWindowsPath(path)

	// Get pagination parameters
	lastFileName := r.URL.Query().Get("lastFileName")

	pageSize, err := strconv.Atoi(defaultQuery(r.URL.Query().Get("limit"), "20"))
	if err != nil || pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}

	// Get file browser data with cursor-based pagination
	browserData, err := h.adminServer.GetFileBrowser(path, lastFileName, pageSize)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get file browser data: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	browserData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	browserComponent := app.FileBrowser(*browserData)
	viewCtx := layout.NewViewContext(r, username, dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, browserComponent)
	err = layoutComponent.Render(r.Context(), w)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// DeleteFile handles file deletion API requests
func (h *FileBrowserHandlers) DeleteFile(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Path string `json:"path" binding:"required"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &request); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	if strings.TrimSpace(request.Path) == "" {
		writeJSONError(w, http.StatusBadRequest, "path is required")
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
		writeJSONError(w, http.StatusInternalServerError, "Failed to delete file: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"message": "File deleted successfully"})
}

// DeleteMultipleFiles handles multiple file deletion API requests
func (h *FileBrowserHandlers) DeleteMultipleFiles(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Paths []string `json:"paths" binding:"required"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &request); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	if len(request.Paths) == 0 {
		writeJSONError(w, http.StatusBadRequest, "No paths provided")
		return
	}

	for _, path := range request.Paths {
		if strings.TrimSpace(path) == "" {
			writeJSONError(w, http.StatusBadRequest, "path is required")
			return
		}
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
		writeJSON(w, http.StatusOK, response)
	} else {
		response["message"] = "Failed to delete all selected items"
		writeJSON(w, http.StatusInternalServerError, response)
	}
}

// CreateFolder handles folder creation requests
func (h *FileBrowserHandlers) CreateFolder(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Path       string `json:"path" binding:"required"`
		FolderName string `json:"folder_name" binding:"required"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &request); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	if strings.TrimSpace(request.Path) == "" {
		writeJSONError(w, http.StatusBadRequest, "path is required")
		return
	}

	// Clean and validate folder name
	folderName := strings.TrimSpace(request.FolderName)
	if folderName == "" || strings.Contains(folderName, "/") || strings.Contains(folderName, "\\") {
		writeJSONError(w, http.StatusBadRequest, "Invalid folder name")
		return
	}

	// Create full path for new folder
	base := "/" + strings.TrimPrefix(request.Path, "/")
	fullPath := path.Join(base, folderName)

	// Create folder via filer
	err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: filepath.Dir(fullPath),
			Entry: &filer_pb.Entry{
				Name:        filepath.Base(fullPath),
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileMode: uint32(0o755 | os.ModeDir), // Directory mode
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
		writeJSONError(w, http.StatusInternalServerError, "Failed to create folder: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"message": "Folder created successfully"})
}

// UploadFile handles file upload requests
func (h *FileBrowserHandlers) UploadFile(w http.ResponseWriter, r *http.Request) {
	// Get the current path
	currentPath := r.FormValue("path")
	if currentPath == "" {
		currentPath = "/"
	}

	// Parse multipart form
	err := r.ParseMultipartForm(1 << 30) // 1GB max memory for large file uploads
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "Failed to parse multipart form: "+err.Error())
		return
	}

	// Get uploaded files (supports multiple files)
	files := r.MultipartForm.File["files"]
	if len(files) == 0 {
		writeJSONError(w, http.StatusBadRequest, "No files uploaded")
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

		// Normalize Windows-style backslashes to forward slashes
		fileName = util.CleanWindowsPath(fileName)

		// Create full path for the file using path.Join for URL path semantics
		// path.Join handles double slashes and is not OS-specific like filepath.Join
		fullPath := path.Join(currentPath, fileName)
		if !strings.HasPrefix(fullPath, "/") {
			fullPath = "/" + fullPath
		}

		// Upload file to filer
		err = h.uploadFileToFiler(r.Context(), fullPath, fileHeader)

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
		writeJSON(w, http.StatusOK, response)
	} else {
		response["message"] = "All file uploads failed"
		writeJSON(w, http.StatusInternalServerError, response)
	}
}

// uploadFileToFiler uploads a file to the cluster via filer gRPC + volume
// HTTP. This works whether or not the filer is running with -disableHttp=true,
// since the bytes never traverse the filer's HTTP listener. The multipart
// file is streamed through the chunked uploader, so the admin process never
// buffers the entire payload in memory. The caller passes the request
// context so a client disconnect cancels the in-flight chunk uploads instead
// of letting them run to completion against the volume servers.
func (h *FileBrowserHandlers) uploadFileToFiler(ctx context.Context, filePath string, fileHeader *multipart.FileHeader) error {
	file, err := fileHeader.Open()
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return h.uploadFileGrpc(ctx, filePath, fileHeader.Filename, fileHeader.Header.Get("Content-Type"), file)
}

// validateAndCleanFilePath validates and cleans the file path to prevent path traversal
func (h *FileBrowserHandlers) validateAndCleanFilePath(filePath string) (string, error) {
	if filePath == "" {
		return "", fmt.Errorf("file path cannot be empty")
	}

	// Normalize Windows-style backslashes to forward slashes
	filePath = util.CleanWindowsPath(filePath)

	// Clean the path to remove any .. or . components
	// Use path.Clean (not filepath.Clean) since this is a URL path
	cleanPath := path.Clean(filePath)

	// Ensure the path starts with /
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}

	// Prevent path traversal attacks
	if strings.Contains(cleanPath, "..") {
		return "", fmt.Errorf("path traversal not allowed")
	}

	return cleanPath, nil
}

// fetchFileContent fetches file content via the filer gRPC service. It is
// used for the "view as text" path, so the maxBytes cap matches the 1 MB
// limit the caller already applies before invoking us.
func (h *FileBrowserHandlers) fetchFileContent(filePath string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return h.fetchFileContentGrpc(ctx, filePath, 0)
}

// DownloadFile streams a file straight from the volume servers via the filer
// gRPC service, so the admin file browser keeps working even when the filer
// is started with -disableHttp=true.
func (h *FileBrowserHandlers) DownloadFile(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		writeJSONError(w, http.StatusBadRequest, "File path is required")
		return
	}
	inline := r.URL.Query().Get("inline") == "true"
	tracker := &responseWriteTracker{ResponseWriter: w}
	if err := h.downloadFileGrpc(r.Context(), filePath, tracker, inline); err != nil {
		// Once bytes have been written we can't switch to a JSON error body
		// without corrupting the partial response — log and stop. Before any
		// write the response is still uncommitted, so a 502 with details is
		// safe.
		if tracker.committed {
			glog.Errorf("Error streaming file download: %v", err)
			return
		}
		writeJSONError(w, http.StatusBadGateway, "Failed to fetch file: "+err.Error())
	}
}

// responseWriteTracker wraps http.ResponseWriter to record whether the
// response has been committed (status line + headers sent). DownloadFile
// uses this instead of probing Header() so future header-setting code
// reorganization can't silently break the "did we already send bytes?"
// detection.
type responseWriteTracker struct {
	http.ResponseWriter
	committed bool
}

func (t *responseWriteTracker) WriteHeader(code int) {
	t.committed = true
	t.ResponseWriter.WriteHeader(code)
}

func (t *responseWriteTracker) Write(p []byte) (int, error) {
	t.committed = true
	return t.ResponseWriter.Write(p)
}

// ViewFile handles file viewing requests (for text files, images, etc.)
func (h *FileBrowserHandlers) ViewFile(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		writeJSONError(w, http.StatusBadRequest, "File path is required")
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

		mime := dash.ResolveEntryMime(entry)

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
		writeJSONError(w, http.StatusInternalServerError, "Failed to get file metadata: "+err.Error())
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
			// Fetch file content from filer
			var err error
			content, err = h.fetchFileContent(filePath, 30*time.Second)
			if err != nil {
				reason = err.Error()
			}
			viewable = (err == nil)
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

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"file":     fileEntry,
		"content":  content,
		"viewable": viewable,
		"reason":   reason,
	})
}

// GetFileProperties handles file properties requests
func (h *FileBrowserHandlers) GetFileProperties(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		writeJSONError(w, http.StatusBadRequest, "File path is required")
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

			properties["file_mode"] = dash.FormatFileMode(entry.Attributes.FileMode)
			properties["file_mode_formatted"] = dash.FormatFileMode(entry.Attributes.FileMode)
			properties["file_mode_octal"] = fmt.Sprintf("%o", entry.Attributes.FileMode)
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

		if !entry.IsDirectory {
			properties["mime_type"] = dash.ResolveEntryMime(entry)
		}

		return nil
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get file properties: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, properties)
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

// Helper function to check if a file is likely a text file by checking content
func (h *FileBrowserHandlers) isLikelyTextFile(filePath string, maxCheckSize int64) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	content, err := h.fetchFileContentGrpc(ctx, filePath, int(maxCheckSize))
	if err != nil {
		return false
	}
	if len(content) == 0 {
		return true
	}
	return h.isPrintableText([]byte(content))
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

