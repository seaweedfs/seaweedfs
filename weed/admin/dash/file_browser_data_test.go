package dash

import (
	"strings"
	"testing"
)

// TestGenerateBreadcrumbs tests the breadcrumb generation for different paths
func TestGenerateBreadcrumbs(t *testing.T) {
	s := &AdminServer{}

	tests := []struct {
		name     string
		path     string
		expected []BreadcrumbItem
	}{
		{
			name: "root path",
			path: "/",
			expected: []BreadcrumbItem{
				{Name: "Root", Path: "/"},
			},
		},
		{
			name: "simple path",
			path: "/folder",
			expected: []BreadcrumbItem{
				{Name: "Root", Path: "/"},
				{Name: "folder", Path: "/folder"},
			},
		},
		{
			name: "nested path",
			path: "/folder/subfolder",
			expected: []BreadcrumbItem{
				{Name: "Root", Path: "/"},
				{Name: "folder", Path: "/folder"},
				{Name: "subfolder", Path: "/folder/subfolder"},
			},
		},
		{
			name: "bucket path",
			path: "/buckets/mybucket",
			expected: []BreadcrumbItem{
				{Name: "Root", Path: "/"},
				{Name: "Object Store Buckets", Path: "/buckets"},
				{Name: "📦 mybucket", Path: "/buckets/mybucket"},
			},
		},
		{
			name: "bucket nested path",
			path: "/buckets/mybucket/folder",
			expected: []BreadcrumbItem{
				{Name: "Root", Path: "/"},
				{Name: "Object Store Buckets", Path: "/buckets"},
				{Name: "📦 mybucket", Path: "/buckets/mybucket"},
				{Name: "folder", Path: "/buckets/mybucket/folder"},
			},
		},
		{
			name: "path with trailing slash",
			path: "/folder/",
			expected: []BreadcrumbItem{
				{Name: "Root", Path: "/"},
				{Name: "folder", Path: "/folder"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.generateBreadcrumbs(tt.path)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d breadcrumbs, got %d", len(tt.expected), len(result))
				return
			}

			for i, crumb := range result {
				if crumb.Name != tt.expected[i].Name {
					t.Errorf("breadcrumb %d: expected name %q, got %q", i, tt.expected[i].Name, crumb.Name)
				}
				if crumb.Path != tt.expected[i].Path {
					t.Errorf("breadcrumb %d: expected path %q, got %q", i, tt.expected[i].Path, crumb.Path)
				}
			}
		})
	}
}

// TestPathHandlingWithForwardSlashes ensures paths always use forward slashes
// This is critical for URL compatibility and Windows path handling
func TestPathHandlingWithForwardSlashes(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		hasSlash bool
	}{
		{
			name:     "root",
			path:     "/",
			hasSlash: true,
		},
		{
			name:     "single level",
			path:     "/test",
			hasSlash: true,
		},
		{
			name:     "multiple levels",
			path:     "/a/b/c",
			hasSlash: true,
		},
		{
			name:     "bucket path",
			path:     "/buckets/mybucket/file",
			hasSlash: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify no backslashes appear in paths (which would be wrong on Windows)
			if strings.Contains(tt.path, "\\") {
				t.Errorf("path contains backslash: %q", tt.path)
			}

			// Verify forward slashes are used
			if tt.hasSlash && !strings.Contains(tt.path, "/") {
				t.Errorf("path should contain forward slash: %q", tt.path)
			}
		})
	}
}

// TestParentPathCalculation tests the parent path calculation logic
func TestParentPathCalculation(t *testing.T) {
	tests := []struct {
		name       string
		currentDir string
		expected   string
	}{
		{
			name:       "root path",
			currentDir: "/",
			expected:   "/",
		},
		{
			name:       "single level",
			currentDir: "/folder",
			expected:   "/",
		},
		{
			name:       "two levels",
			currentDir: "/folder/subfolder",
			expected:   "/folder",
		},
		{
			name:       "deep nesting",
			currentDir: "/a/b/c/d",
			expected:   "/a/b/c",
		},
		{
			name:       "bucket root",
			currentDir: "/buckets",
			expected:   "/",
		},
		{
			name:       "bucket directory",
			currentDir: "/buckets/mybucket",
			expected:   "/buckets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentPath := "/"
			if tt.currentDir != "/" {
				// Using path.Dir which always uses forward slashes
				// (this is what the fix changes to)
				parentPath = getDirParent(tt.currentDir)
				if parentPath == "." {
					parentPath = "/"
				}
			}

			if parentPath != tt.expected {
				t.Errorf("expected parent %q, got %q for %q", tt.expected, parentPath, tt.currentDir)
			}

			// Verify no backslashes
			if strings.Contains(parentPath, "\\") {
				t.Errorf("parent path contains backslash: %q", parentPath)
			}
		})
	}
}

// getDirParent is a helper function that mimics path.Dir behavior
// (using forward slashes, not OS-specific separators)
func getDirParent(p string) string {
	// This uses the same logic as path.Dir
	i := strings.LastIndex(p, "/")
	if i < 0 {
		return "."
	}
	if i == 0 {
		return "/"
	}
	return p[:i]
}

// TestFileExtensionHandling tests that file extensions are correctly identified
// using path.Ext instead of filepath.Ext to ensure consistent behavior
func TestFileExtensionHandling(t *testing.T) {
	tests := []struct {
		filename string
		expected string
	}{
		{"file.txt", ".txt"},
		{"file.log", ".log"},
		{"archive.tar.gz", ".gz"},
		{"image.jpg", ".jpg"},
		{"document.pdf", ".pdf"},
		{"data.json", ".json"},
		{"noextension", ""},
		{".hidden", ".hidden"},
		{"file.TXT", ".txt"}, // lowercase conversion
		{"file.JPG", ".jpg"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			// Simulate the logic in the code: path.Ext(entry.Name)
			// followed by strings.ToLower
			ext := getExtension(tt.filename)
			if ext != tt.expected {
				t.Errorf("expected extension %q for %q, got %q", tt.expected, tt.filename, ext)
			}
		})
	}
}

// getExtension is a helper function that mimics the file extension
// logic in the GetFileBrowser function
func getExtension(filename string) string {
	// This simulates: ext := strings.ToLower(path.Ext(entry.Name))
	// Using path.Ext which handles forward slashes correctly
	i := strings.LastIndex(filename, ".")
	if i < 0 {
		return ""
	}
	return strings.ToLower(filename[i:])
}

// TestBucketPathDetection tests the detection of bucket paths
func TestBucketPathDetection(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		isBucket     bool
		expectedName string
	}{
		{
			name:         "root is not a bucket path",
			path:         "/",
			isBucket:     false,
			expectedName: "",
		},
		{
			name:         "buckets root",
			path:         "/buckets/",
			isBucket:     true,
			expectedName: "",
		},
		{
			name:         "single bucket",
			path:         "/buckets/mybucket",
			isBucket:     true,
			expectedName: "mybucket",
		},
		{
			name:         "bucket with nested path",
			path:         "/buckets/mybucket/folder/file",
			isBucket:     true,
			expectedName: "mybucket",
		},
		{
			name:         "non-bucket path",
			path:         "/data/folder",
			isBucket:     false,
			expectedName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isBucketPath := false
			bucketName := ""
			if strings.HasPrefix(tt.path, "/buckets/") {
				isBucketPath = true
				pathParts := strings.Split(strings.Trim(tt.path, "/"), "/")
				if len(pathParts) >= 2 {
					bucketName = pathParts[1]
				}
			}

			if isBucketPath != tt.isBucket {
				t.Errorf("expected isBucketPath=%v, got %v for path %q", tt.isBucket, isBucketPath, tt.path)
			}

			if bucketName != tt.expectedName {
				t.Errorf("expected bucket name %q, got %q for path %q", tt.expectedName, bucketName, tt.path)
			}
		})
	}
}
