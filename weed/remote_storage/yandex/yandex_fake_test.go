package yandex

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNode is one file or folder of the fake disk.
type fakeNode struct {
	isDir    bool
	data     []byte
	modified time.Time
	props    map[string]string
}

// fakeDisk is an in-memory Yandex Disk API: enough of the resources,
// transfer links, custom properties and operations endpoints for the client
// to run against.
type fakeDisk struct {
	t  *testing.T
	mu sync.Mutex

	nodes     map[string]*fakeNode // cleaned path -> node, "/" is the root
	uploads   map[string]string    // upload id -> path
	downloads map[string]string    // download id -> path
	ops       map[string]int       // operation id -> remaining in-progress answers

	requests []recordedRequest

	// knobs a test sets
	hidePropertyOnce bool   // the next stat omits custom properties
	unauthorizedOnce bool   // the next API request answers 401
	ignoreRange      bool   // downloads answer 200 with the whole body
	shortBody        bool   // downloads answer fewer bytes than asked
	deleteAccepted   bool   // deletes answer 202 with an operation link
	uploadOperation  bool   // upload links carry an operation id
	lastUploadLength int64  // Content-Length of the last upload
	lastUploadBody   []byte // body of the last upload
	lastUploadAuth   string // Authorization header of the last upload
	// missingParentNotFound answers a missing parent folder with 404
	// DiskNotFoundError instead of the API's 409 DiskPathDoesntExistsError
	missingParentNotFound bool
	unauthorizedOps       bool // operation polls answer 401
	lockedUploadLinks     int  // upload-link requests answered 423 before success
}

type recordedRequest struct {
	method string
	path   string
	query  string
	auth   string
	range_ string
}

func newFakeDisk(t *testing.T) *fakeDisk {
	t.Helper()
	disk := &fakeDisk{
		t:         t,
		nodes:     map[string]*fakeNode{"/": {isDir: true, modified: time.Now()}},
		uploads:   map[string]string{},
		downloads: map[string]string{},
		ops:       map[string]int{},
	}
	return disk
}

func (d *fakeDisk) server() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/disk/resources", d.handleResources)
	mux.HandleFunc("/v1/disk/resources/upload", d.handleUploadLink)
	mux.HandleFunc("/v1/disk/resources/download", d.handleDownloadLink)
	mux.HandleFunc("/v1/disk/resources/copy", d.handleCopy)
	mux.HandleFunc("/v1/disk/operations/", d.handleOperation)
	mux.HandleFunc("/upload/", d.handleUpload)
	mux.HandleFunc("/download/", d.handleDownload)
	return httptest.NewServer(d.record(mux))
}

func (d *fakeDisk) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		d.requests = append(d.requests, recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
			auth:   r.Header.Get("Authorization"),
			range_: r.Header.Get("Range"),
		})
		d.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func cleanPath(raw string) string {
	raw = strings.TrimPrefix(raw, "disk:")
	raw = strings.TrimPrefix(raw, "app:")
	return "/" + strings.Trim(raw, "/")
}

func pathName(path string) string {
	if path == "/" {
		return ""
	}
	return path[strings.LastIndex(path, "/")+1:]
}

func (d *fakeDisk) put(path string, node *fakeNode) {
	d.mkdirAll(parentPath(path))
	if node.modified.IsZero() {
		node.modified = time.Now()
	}
	d.nodes[path] = node
}

func (d *fakeDisk) mkdirAll(path string) {
	current := ""
	for _, segment := range strings.Split(strings.Trim(path, "/"), "/") {
		current += "/" + segment
		if _, found := d.nodes[current]; !found {
			d.nodes[current] = &fakeNode{isDir: true, modified: time.Now()}
		}
	}
}

func (d *fakeDisk) children(dir string) []string {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	var names []string
	for path := range d.nodes {
		if path == "/" || !strings.HasPrefix(path, prefix) {
			continue
		}
		if rest := strings.TrimPrefix(path, prefix); strings.Contains(rest, "/") {
			continue
		}
		names = append(names, path)
	}
	sort.Strings(names)
	return names
}

func (d *fakeDisk) remove(path string) {
	prefix := strings.TrimSuffix(path, "/") + "/"
	for candidate := range d.nodes {
		if candidate == path || strings.HasPrefix(candidate, prefix) {
			delete(d.nodes, candidate)
		}
	}
}

func (d *fakeDisk) resourceJSON(path string, node *fakeNode) map[string]any {
	result := map[string]any{
		"name":      pathName(path),
		"path":      "disk:" + path,
		"type":      "file",
		"size":      len(node.data),
		"created":   node.modified.Format(time.RFC3339Nano),
		"modified":  node.modified.Format(time.RFC3339Nano),
		"md5":       hex.EncodeToString(md5Sum(node.data)),
		"mime_type": "application/octet-stream",
	}
	if node.isDir {
		result["type"] = "dir"
		result["size"] = 0
		result["md5"] = ""
	}
	if len(node.props) > 0 {
		properties := map[string]string{}
		for key, value := range node.props {
			properties[key] = value
		}
		result["custom_properties"] = properties
	}
	return result
}

func md5Sum(data []byte) []byte {
	sum := md5.Sum(data)
	return sum[:]
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, name, description string) {
	writeJSON(w, status, map[string]any{"error": name, "description": description, "message": description})
}

// writeMissingParent answers a request whose parent folder does not exist the
// way the Disk API does: 409 DiskPathDoesntExistsError.
func (d *fakeDisk) writeMissingParent(w http.ResponseWriter) {
	if d.missingParentNotFound {
		writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Parent not found.")
		return
	}
	writeAPIError(w, http.StatusConflict, "DiskPathDoesntExistsError", "Specified path does not exist.")
}

func (d *fakeDisk) handleResources(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.unauthorizedOnce {
		d.unauthorizedOnce = false
		writeAPIError(w, http.StatusUnauthorized, "UnauthorizedError", "Invalid token.")
		return
	}
	path := cleanPath(r.URL.Query().Get("path"))

	switch r.Method {
	case http.MethodGet:
		node, found := d.nodes[path]
		if !found {
			writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Resource not found.")
			return
		}
		payload := d.resourceJSON(path, node)
		if !node.isDir && d.hidePropertyOnce {
			// the service drops the custom property on the read back
			d.hidePropertyOnce = false
			delete(payload, "custom_properties")
		}
		if node.isDir {
			var items []map[string]any
			for _, child := range d.children(path) {
				items = append(items, d.resourceJSON(child, d.nodes[child]))
			}
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if limit <= 0 {
				limit = len(items)
			}
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			total := len(items)
			if offset > total {
				offset = total
			}
			end := offset + limit
			if end > total {
				end = total
			}
			if items == nil {
				items = []map[string]any{}
			}
			payload["_embedded"] = map[string]any{
				"items":  items[offset:end],
				"total":  total,
				"limit":  limit,
				"offset": offset,
			}
		}
		writeJSON(w, http.StatusOK, payload)
	case http.MethodPut:
		if node, found := d.nodes[path]; found {
			name := "DiskPathPointsToExistentDirectoryError"
			if !node.isDir {
				name = "DiskPathPointsToExistentFileError"
			}
			writeAPIError(w, http.StatusConflict, name, "Path already exists.")
			return
		}
		parent := parentPath(path)
		if parentNode, found := d.nodes[parent]; !found || !parentNode.isDir {
			d.writeMissingParent(w)
			return
		}
		d.put(path, &fakeNode{isDir: true, modified: time.Now()})
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		node, found := d.nodes[path]
		if !found {
			writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Resource not found.")
			return
		}
		if node.isDir && len(d.children(path)) > 0 && r.URL.Query().Get("permanently") == "" {
			writeAPIError(w, http.StatusConflict, "DiskResourceLockedError", "Folder not empty.")
			return
		}
		d.remove(path)
		if d.deleteAccepted {
			href := fmt.Sprintf("http://%s/v1/disk/operations/del", r.Host)
			writeJSON(w, http.StatusAccepted, map[string]any{"href": href, "method": "GET"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPatch:
		node, found := d.nodes[path]
		if !found {
			writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Resource not found.")
			return
		}
		var payload struct {
			CustomProperties map[string]string `json:"custom_properties"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeAPIError(w, http.StatusBadRequest, "BadRequest", err.Error())
			return
		}
		if node.props == nil {
			node.props = map[string]string{}
		}
		for key, value := range payload.CustomProperties {
			node.props[key] = value
		}
		writeJSON(w, http.StatusOK, d.resourceJSON(path, node))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (d *fakeDisk) handleUploadLink(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lockedUploadLinks > 0 {
		// a folder still being deleted asynchronously is locked for a moment
		d.lockedUploadLinks--
		writeAPIError(w, http.StatusLocked, "DiskResourceLockedError", "Resource is locked.")
		return
	}
	path := cleanPath(r.URL.Query().Get("path"))
	if _, found := d.nodes[parentPath(path)]; !found {
		d.writeMissingParent(w)
		return
	}
	id := fmt.Sprintf("up%d", len(d.uploads)+1)
	d.uploads[id] = path
	payload := map[string]any{
		"href":   fmt.Sprintf("http://%s/upload/%s", r.Host, id),
		"method": "PUT",
	}
	if d.uploadOperation {
		payload["operation_id"] = id
	}
	writeJSON(w, http.StatusOK, payload)
}

func (d *fakeDisk) handleUpload(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/upload/")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	path, found := d.uploads[id]
	if !found {
		writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Unknown upload.")
		return
	}
	// net/http reports the declared length, or -1 for a chunked body
	if r.ContentLength > 0 && int64(len(body)) != r.ContentLength {
		writeAPIError(w, http.StatusBadRequest, "BadRequest", "body length mismatch")
		return
	}
	d.lastUploadLength = r.ContentLength
	d.lastUploadBody = body
	d.lastUploadAuth = r.Header.Get("Authorization")
	d.put(path, &fakeNode{data: body, modified: time.Now()})
	w.WriteHeader(http.StatusCreated)
}

func (d *fakeDisk) handleDownloadLink(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	path := cleanPath(r.URL.Query().Get("path"))
	node, found := d.nodes[path]
	if !found {
		writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Resource not found.")
		return
	}
	if node.isDir {
		writeAPIError(w, http.StatusBadRequest, "BadRequest", "Resource is a folder.")
		return
	}
	id := fmt.Sprintf("down%d", len(d.downloads)+1)
	d.downloads[id] = path
	writeJSON(w, http.StatusOK, map[string]any{
		"href":   fmt.Sprintf("http://%s/download/%s", r.Host, id),
		"method": "GET",
	})
}

func (d *fakeDisk) handleDownload(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/download/")
	d.mu.Lock()
	path, found := d.downloads[id]
	node := d.nodes[path]
	ignoreRange := d.ignoreRange
	shortBody := d.shortBody
	d.mu.Unlock()
	if !found || node == nil {
		writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Unknown download.")
		return
	}
	data := node.data
	if shortBody {
		// answer one byte less than the requested range
		start, end := 0, len(data)-1
		if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
			_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		chunk := data[start:max(start, end)]
		w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(chunk)
		return
	}
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" && !ignoreRange {
		var start, end int
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err == nil {
			if start > len(data) {
				start = len(data)
			}
			if end >= len(data) {
				end = len(data) - 1
			}
			chunk := data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(chunk)
			return
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (d *fakeDisk) handleCopy(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	from := cleanPath(r.URL.Query().Get("from"))
	to := cleanPath(r.URL.Query().Get("path"))
	node, found := d.nodes[from]
	if !found {
		writeAPIError(w, http.StatusNotFound, "DiskNotFoundError", "Source not found.")
		return
	}
	if parentNode, found := d.nodes[parentPath(to)]; !found || !parentNode.isDir {
		d.writeMissingParent(w)
		return
	}
	if _, exists := d.nodes[to]; exists && r.URL.Query().Get("overwrite") != "true" {
		writeAPIError(w, http.StatusConflict, "DiskPathPointsToExistentFileError", "Destination exists.")
		return
	}
	copied := &fakeNode{isDir: node.isDir, modified: time.Now()}
	if !node.isDir {
		copied.data = append([]byte(nil), node.data...)
	}
	d.put(to, copied)
	href := fmt.Sprintf("http://%s/v1/disk/operations/copy", r.Host)
	writeJSON(w, http.StatusAccepted, map[string]any{"href": href, "method": "GET"})
}

func (d *fakeDisk) handleOperation(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.unauthorizedOps {
		writeAPIError(w, http.StatusUnauthorized, "UnauthorizedError", "Invalid token.")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/disk/operations/")
	if remaining, found := d.ops[id]; found && remaining > 0 {
		d.ops[id] = remaining - 1
		writeJSON(w, http.StatusOK, map[string]any{"status": "in-progress"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "success"})
}
