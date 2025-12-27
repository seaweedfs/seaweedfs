package dash

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// FileEntry represents a file or directory entry in the file browser
type FileEntry struct {
	Name        string    `json:"name"`
	FullPath    string    `json:"full_path"`
	IsDirectory bool      `json:"is_directory"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"mod_time"`
	Mode        string    `json:"mode"`
	Uid         uint32    `json:"uid"`
	Gid         uint32    `json:"gid"`
	Mime        string    `json:"mime"`
	Replication string    `json:"replication"`
	Collection  string    `json:"collection"`
	TtlSec      int32     `json:"ttl_sec"`
}

// BreadcrumbItem represents a single breadcrumb in the navigation
type BreadcrumbItem struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// FileBrowserData contains all data needed for the file browser view
type FileBrowserData struct {
	Username    string           `json:"username"`
	CurrentPath string           `json:"current_path"`
	ParentPath  string           `json:"parent_path"`
	Breadcrumbs []BreadcrumbItem `json:"breadcrumbs"`
	Entries     []FileEntry      `json:"entries"`

	LastUpdated  time.Time `json:"last_updated"`
	IsBucketPath bool      `json:"is_bucket_path"`
	BucketName   string    `json:"bucket_name"`
	// Pagination fields
	PageSize            int    `json:"page_size"`
	HasNextPage         bool   `json:"has_next_page"`
	LastFileName        string `json:"last_file_name"`         // Cursor for next page
	CurrentLastFileName string `json:"current_last_file_name"` // Cursor from current request (for page size changes)
}

// GetFileBrowser retrieves file browser data for a given path with cursor-based pagination
func (s *AdminServer) GetFileBrowser(dir string, lastFileName string, pageSize int) (*FileBrowserData, error) {
	if dir == "" {
		dir = "/"
	}

	// Set defaults for pagination
	if pageSize < 1 {
		pageSize = 20 // Default page size
	}

	var entries []FileEntry

	// Fetch entries using cursor-based pagination
	// We fetch pageSize+1 to determine if there's a next page
	fetchLimit := pageSize + 1
	var fetchedCount int
	var lastEntryName string

	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Fetch entries starting from the cursor (lastFileName)
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             "",
			Limit:              uint32(fetchLimit),
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: false, // Don't include the cursor file itself
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				return err
			}

			entry := resp.Entry
			if entry == nil {
				continue
			}

			fetchedCount++

			// Only add entries up to pageSize (the +1 is just to check for next page)
			if fetchedCount <= pageSize {
				fullPath := path.Join(dir, entry.Name)

				var modTime time.Time
				if entry.Attributes != nil && entry.Attributes.Mtime > 0 {
					modTime = time.Unix(entry.Attributes.Mtime, 0)
				}

				var mode string
				var uid, gid uint32
				var size int64
				var replication, collection string
				var ttlSec int32

				if entry.Attributes != nil {
					mode = FormatFileMode(entry.Attributes.FileMode)
					uid = entry.Attributes.Uid
					gid = entry.Attributes.Gid
					size = int64(entry.Attributes.FileSize)
					ttlSec = entry.Attributes.TtlSec
				}

				// Get replication and collection from entry extended attributes
				if entry.Extended != nil {
					if repl, ok := entry.Extended["replication"]; ok {
						replication = string(repl)
					}
					if coll, ok := entry.Extended["collection"]; ok {
						collection = string(coll)
					}
				}

				// Determine MIME type based on file extension
				mime := "application/octet-stream"
				if entry.IsDirectory {
					mime = "inode/directory"
				} else {
					ext := strings.ToLower(path.Ext(entry.Name))
					switch ext {
					case ".txt", ".log":
						mime = "text/plain"
					case ".html", ".htm":
						mime = "text/html"
					case ".css":
						mime = "text/css"
					case ".js":
						mime = "application/javascript"
					case ".json":
						mime = "application/json"
					case ".xml":
						mime = "application/xml"
					case ".pdf":
						mime = "application/pdf"
					case ".jpg", ".jpeg":
						mime = "image/jpeg"
					case ".png":
						mime = "image/png"
					case ".gif":
						mime = "image/gif"
					case ".svg":
						mime = "image/svg+xml"
					case ".mp4":
						mime = "video/mp4"
					case ".mp3":
						mime = "audio/mpeg"
					case ".zip":
						mime = "application/zip"
					case ".tar":
						mime = "application/x-tar"
					case ".gz":
						mime = "application/gzip"
					}
				}

				fileEntry := FileEntry{
					Name:        entry.Name,
					FullPath:    fullPath,
					IsDirectory: entry.IsDirectory,
					Size:        size,
					ModTime:     modTime,
					Mode:        mode,
					Uid:         uid,
					Gid:         gid,
					Mime:        mime,
					Replication: replication,
					Collection:  collection,
					TtlSec:      ttlSec,
				}

				entries = append(entries, fileEntry)

				lastEntryName = entry.Name

			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Determine if there's a next page
	hasNextPage := fetchedCount > pageSize

	// Generate breadcrumbs
	breadcrumbs := s.generateBreadcrumbs(dir)

	// Calculate parent path
	parentPath := "/"
	if dir != "/" {
		parentPath = path.Dir(dir)
		if parentPath == "." {
			parentPath = "/"
		}
	}

	// Check if this is a bucket path
	isBucketPath := false
	bucketName := ""
	if strings.HasPrefix(dir, "/buckets/") {
		isBucketPath = true
		pathParts := strings.Split(strings.Trim(dir, "/"), "/")
		if len(pathParts) >= 2 {
			bucketName = pathParts[1]
		}
	}

	return &FileBrowserData{
		CurrentPath: dir,
		ParentPath:  parentPath,
		Breadcrumbs: breadcrumbs,
		Entries:     entries,

		LastUpdated:  time.Now(),
		IsBucketPath: isBucketPath,
		BucketName:   bucketName,
		// Pagination metadata
		PageSize:            pageSize,
		HasNextPage:         hasNextPage,
		LastFileName:        lastEntryName, // Store for next page navigation
		CurrentLastFileName: lastFileName,  // Store input cursor for page size changes
	}, nil
}

// generateBreadcrumbs creates breadcrumb navigation for the current path
func (s *AdminServer) generateBreadcrumbs(dir string) []BreadcrumbItem {
	var breadcrumbs []BreadcrumbItem

	// Always start with root
	breadcrumbs = append(breadcrumbs, BreadcrumbItem{
		Name: "Root",
		Path: "/",
	})

	if dir == "/" {
		return breadcrumbs
	}

	// Split path and build breadcrumbs
	parts := strings.Split(strings.Trim(dir, "/"), "/")
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath += "/" + part

		// Special handling for bucket paths
		displayName := part
		if len(breadcrumbs) == 1 && part == "buckets" {
			displayName = "Object Store Buckets"
		} else if len(breadcrumbs) == 2 && strings.HasPrefix(dir, "/buckets/") {
			displayName = "📦 " + part // Add bucket icon to bucket name
		}

		breadcrumbs = append(breadcrumbs, BreadcrumbItem{
			Name: displayName,
			Path: currentPath,
		})
	}

	return breadcrumbs
}
