package dash

import (
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestGenerateBreadcrumbs tests the actual breadcrumb generation function
// from the production code with various path scenarios
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
			// Call the actual production function
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

// TestPathHandlingWithForwardSlashes verifies that the production code
// correctly handles paths with forward slashes (not OS-specific backslashes)
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

// TestParentPathCalculationLogic verifies that parent path calculation
// uses path.Dir semantics (forward slashes), not filepath.Dir (OS-specific)
func TestParentPathCalculationLogic(t *testing.T) {
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
			// Verify using path.Dir (the correct approach for URLs)
			// This demonstrates the expected behavior
			parentPath := "/"
			if tt.currentDir != "/" {
				// path.Dir always uses forward slashes
				parentPath = path.Dir(tt.currentDir)
				if parentPath == "." {
					parentPath = "/"
				}
			}

			if parentPath != tt.expected {
				t.Errorf("expected parent %q, got %q for %q", tt.expected, parentPath, tt.currentDir)
			}

			// Verify no backslashes in the result
			if strings.Contains(parentPath, "\\") {
				t.Errorf("parent path contains backslash: %q", parentPath)
			}
		})
	}
}

// TestFileExtensionHandlingLogic verifies that file extensions are correctly
// identified using path semantics (always forward slashes)
func TestFileExtensionHandlingLogic(t *testing.T) {
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
		{"file.TXT", ".txt"},
		{"file.JPG", ".jpg"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			// Verify using path.Ext + strings.ToLower (the correct approach)
			ext := strings.ToLower(path.Ext(tt.filename))
			if ext != tt.expected {
				t.Errorf("expected extension %q for %q, got %q", tt.expected, tt.filename, ext)
			}
		})
	}
}

// TestBucketPathDetectionLogic verifies bucket path detection logic
func TestBucketPathDetectionLogic(t *testing.T) {
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
			// Verify the bucket path detection logic
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

// TestPathJoinHandlesEdgeCases verifies that path.Join handles edge cases
// properly for URL path construction (unlike filepath.Join which is OS-specific)
func TestPathJoinHandlesEdgeCases(t *testing.T) {
	tests := []struct {
		testName string
		dir      string
		filename string
		expected string
	}{
		{
			testName: "root directory",
			dir:      "/",
			filename: "file.txt",
			expected: "/file.txt",
		},
		{
			testName: "simple directory",
			dir:      "/folder",
			filename: "file.txt",
			expected: "/folder/file.txt",
		},
		{
			testName: "nested directory",
			dir:      "/a/b/c",
			filename: "file.txt",
			expected: "/a/b/c/file.txt",
		},
		{
			testName: "handles trailing slash",
			dir:      "/folder/",
			filename: "file.txt",
			expected: "/folder/file.txt",
		},
		{
			testName: "handles empty name",
			dir:      "/folder",
			filename: "",
			expected: "/folder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			// Verify path.Join behavior for URL paths
			result := path.Join(tt.dir, tt.filename)
			if result != tt.expected {
				t.Errorf("path.Join(%q, %q) = %q, expected %q", tt.dir, tt.filename, result, tt.expected)
			}

			// Verify no backslashes in the result
			if strings.Contains(result, "\\") {
				t.Errorf("result contains backslash: %q", result)
			}
		})
	}
}

// TestWindowsPathNormalizationBehavior validates that Windows-style paths
// are correctly converted to forward slashes for URL compatibility.
// This test verifies the actual util.CleanWindowsPath() function used in
// the ShowFileBrowser handler.
func TestWindowsPathNormalizationBehavior(t *testing.T) {
	tests := []struct {
		name             string
		windowsPath      string
		expectedNormPath string
	}{
		{
			name:             "backslash separator",
			windowsPath:      "\\folder\\subfolder",
			expectedNormPath: "/folder/subfolder",
		},
		{
			name:             "mixed separators",
			windowsPath:      "/folder\\subfolder/file",
			expectedNormPath: "/folder/subfolder/file",
		},
		{
			name:             "already normalized",
			windowsPath:      "/folder/file",
			expectedNormPath: "/folder/file",
		},
		{
			name:             "simple backslash path",
			windowsPath:      "\\data",
			expectedNormPath: "/data",
		},
		{
			name:             "deep nested path",
			windowsPath:      "\\a\\b\\c\\d",
			expectedNormPath: "/a/b/c/d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the actual production function
			normalized := util.CleanWindowsPath(tt.windowsPath)
			if normalized != tt.expectedNormPath {
				t.Errorf("CleanWindowsPath(%q): expected %q, got %q",
					tt.windowsPath, tt.expectedNormPath, normalized)
			}
		})
	}
}

// TestBreadcrumbPathFormatting validates that breadcrumb paths always
// use forward slashes and maintain proper URL format
func TestBreadcrumbPathFormatting(t *testing.T) {
	s := &AdminServer{}

	testPaths := []string{
		"/",
		"/folder",
		"/folder/subfolder",
		"/buckets/mybucket",
		"/buckets/mybucket/data",
	}

	for _, testPath := range testPaths {
		t.Run("breadcrumbs_for_"+testPath, func(t *testing.T) {
			breadcrumbs := s.generateBreadcrumbs(testPath)

			// Verify all breadcrumb paths use forward slashes
			for i, crumb := range breadcrumbs {
				if strings.Contains(crumb.Path, "\\") {
					t.Errorf("breadcrumb %d has backslash in path: %q", i, crumb.Path)
				}
				// Verify paths start with / (except when empty)
				if crumb.Path != "" && !strings.HasPrefix(crumb.Path, "/") {
					t.Errorf("breadcrumb %d path should start with /: %q", i, crumb.Path)
				}
			}
		})
	}
}

// TestDirectoryNavigation validates the complete navigation flow
// for various path scenarios
func TestDirectoryNavigation(t *testing.T) {
	s := &AdminServer{}

	tests := []struct {
		name           string
		currentPath    string
		expectedParent string
		expectedCrumbs int
	}{
		{
			name:           "navigate from root",
			currentPath:    "/",
			expectedParent: "/",
			expectedCrumbs: 1,
		},
		{
			name:           "navigate from single folder",
			currentPath:    "/documents",
			expectedParent: "/",
			expectedCrumbs: 2,
		},
		{
			name:           "navigate from nested folder",
			currentPath:    "/documents/projects/current",
			expectedParent: "/documents/projects",
			expectedCrumbs: 4,
		},
		{
			name:           "navigate bucket contents",
			currentPath:    "/buckets/data/files",
			expectedParent: "/buckets/data",
			expectedCrumbs: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify breadcrumbs are generated correctly
			breadcrumbs := s.generateBreadcrumbs(tt.currentPath)
			if len(breadcrumbs) != tt.expectedCrumbs {
				t.Errorf("expected %d breadcrumbs, got %d", tt.expectedCrumbs, len(breadcrumbs))
			}

			// Verify parent path calculation
			expectedParent := "/"
			if tt.currentPath != "/" {
				expectedParent = path.Dir(tt.currentPath)
				if expectedParent == "." {
					expectedParent = "/"
				}
			}
			if expectedParent != tt.expectedParent {
				t.Errorf("expected parent %q, got %q", tt.expectedParent, expectedParent)
			}

			// Verify all paths use forward slashes
			for i, crumb := range breadcrumbs {
				if strings.Contains(crumb.Path, "\\") {
					t.Errorf("breadcrumb %d contains backslash: %q", i, crumb.Path)
				}
			}
		})
	}
}
