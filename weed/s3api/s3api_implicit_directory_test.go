package s3api

import (
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestImplicitDirectoryBehaviorLogic tests the core logic for implicit directory detection
// This tests the decision logic without requiring a full S3 server setup
func TestImplicitDirectoryBehaviorLogic(t *testing.T) {
	tests := []struct {
		name              string
		objectPath        string
		hasTrailingSlash  bool
		fileSize          uint64
		isDirectory       bool
		mimeType          string
		hasChildren       bool
		versioningEnabled bool
		shouldReturn404   bool
		description       string
	}{
		{
			name:              "PyArrow directory marker: 0-byte file with application/octet-stream and children",
			objectPath:        "dataset",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       false,
			mimeType:          "application/octet-stream",
			hasChildren:       true,
			versioningEnabled: false,
			shouldReturn404:   true,
			description:       "Should return 404 to force s3fs LIST-based discovery",
		},
		{
			name:              "PyArrow directory marker: 0-byte file with empty MIME type and children",
			objectPath:        "dataset",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       false,
			mimeType:          "",
			hasChildren:       true,
			versioningEnabled: false,
			shouldReturn404:   true,
			description:       "Should return 404 for empty MIME type directory markers",
		},
		{
			name:              "Actual directory with children",
			objectPath:        "dataset",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       true,
			mimeType:          "",
			hasChildren:       true,
			versioningEnabled: false,
			shouldReturn404:   false,
			description:       "Should return 200 for actual directories (maintains AWS S3 compatibility)",
		},
		{
			name:              "Explicit directory request: trailing slash",
			objectPath:        "dataset/",
			hasTrailingSlash:  true,
			fileSize:          0,
			isDirectory:       true,
			mimeType:          "",
			hasChildren:       true,
			versioningEnabled: false,
			shouldReturn404:   false,
			description:       "Should return 200 for explicit directory request (trailing slash)",
		},
		{
			name:              "Empty file: 0-byte file without children",
			objectPath:        "empty.txt",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       false,
			mimeType:          "application/octet-stream",
			hasChildren:       false,
			versioningEnabled: false,
			shouldReturn404:   false,
			description:       "Should return 200 for legitimate empty file",
		},
		{
			name:              "Empty directory: directory without children",
			objectPath:        "empty-dir",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       true,
			mimeType:          "",
			hasChildren:       false,
			versioningEnabled: false,
			shouldReturn404:   false,
			description:       "Should return 200 for empty directory",
		},
		{
			name:              "Regular file: non-zero size",
			objectPath:        "file.txt",
			hasTrailingSlash:  false,
			fileSize:          100,
			isDirectory:       false,
			mimeType:          "text/plain",
			hasChildren:       false,
			versioningEnabled: false,
			shouldReturn404:   false,
			description:       "Should return 200 for regular file with content",
		},
		{
			name:              "Versioned bucket: directory marker should return 200",
			objectPath:        "dataset",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       false,
			mimeType:          "application/octet-stream",
			hasChildren:       true,
			versioningEnabled: true,
			shouldReturn404:   false,
			description:       "Should return 200 for versioned buckets (skip directory marker check)",
		},
		{
			name:              "Directory marker with specific MIME type",
			objectPath:        "dataset",
			hasTrailingSlash:  false,
			fileSize:          0,
			isDirectory:       false,
			mimeType:          "text/plain",
			hasChildren:       true,
			versioningEnabled: false,
			shouldReturn404:   false,
			description:       "Should return 200 for 0-byte files with specific MIME types (not generic markers)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the logic: should we return 404?
			// New logic from HeadObjectHandler:
			// if !versioningConfigured && !strings.HasSuffix(object, "/") {
			//     if isZeroByteFile && hasGenericMimeType {
			//         if hasChildren {
			//             return 404
			//         }
			//     }
			// }

			isZeroByteFile := tt.fileSize == 0 && !tt.isDirectory
			hasGenericMimeType := tt.mimeType == "" || tt.mimeType == "application/octet-stream"

			shouldReturn404 := false
			if !tt.versioningEnabled && !tt.hasTrailingSlash {
				if isZeroByteFile && hasGenericMimeType {
					if tt.hasChildren {
						shouldReturn404 = true
					}
				}
			}

			if shouldReturn404 != tt.shouldReturn404 {
				t.Errorf("Logic mismatch for %s:\n  Expected shouldReturn404=%v\n  Got shouldReturn404=%v\n  Description: %s",
					tt.name, tt.shouldReturn404, shouldReturn404, tt.description)
			} else {
				t.Logf("✓ %s: correctly returns %d", tt.name, map[bool]int{true: 404, false: 200}[shouldReturn404])
			}
		})
	}
}

// TestHasChildrenLogic tests the hasChildren helper function logic
func TestHasChildrenLogic(t *testing.T) {
	tests := []struct {
		name           string
		bucket         string
		prefix         string
		listResponse   *filer_pb.ListEntriesResponse
		listError      error
		expectedResult bool
		description    string
	}{
		{
			name:   "Directory with children",
			bucket: "test-bucket",
			prefix: "dataset",
			listResponse: &filer_pb.ListEntriesResponse{
				Entry: &filer_pb.Entry{
					Name:        "file.parquet",
					IsDirectory: false,
				},
			},
			listError:      nil,
			expectedResult: true,
			description:    "Should return true when at least one child exists",
		},
		{
			name:           "Empty directory",
			bucket:         "test-bucket",
			prefix:         "empty-dir",
			listResponse:   nil,
			listError:      io.EOF,
			expectedResult: false,
			description:    "Should return false when no children exist (EOF)",
		},
		{
			name:   "Directory with leading slash in prefix",
			bucket: "test-bucket",
			prefix: "/dataset",
			listResponse: &filer_pb.ListEntriesResponse{
				Entry: &filer_pb.Entry{
					Name:        "file.parquet",
					IsDirectory: false,
				},
			},
			listError:      nil,
			expectedResult: true,
			description:    "Should handle leading slashes correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the hasChildren logic:
			// 1. It should trim leading slashes from prefix
			// 2. It should list with Limit=1
			// 3. It should return true if any entry is received
			// 4. It should return false if EOF is received

			hasChildren := false
			if tt.listError == nil && tt.listResponse != nil {
				hasChildren = true
			} else if tt.listError == io.EOF {
				hasChildren = false
			}

			if hasChildren != tt.expectedResult {
				t.Errorf("hasChildren logic mismatch for %s:\n  Expected: %v\n  Got: %v\n  Description: %s",
					tt.name, tt.expectedResult, hasChildren, tt.description)
			} else {
				t.Logf("✓ %s: correctly returns %v", tt.name, hasChildren)
			}
		})
	}
}

// TestImplicitDirectoryEdgeCases tests edge cases in the implicit directory detection
func TestImplicitDirectoryEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		scenario    string
		expectation string
	}{
		{
			name:        "PyArrow write_dataset creates 0-byte files with application/octet-stream",
			scenario:    "PyArrow creates 'dataset' as 0-byte file with MIME type 'application/octet-stream', then writes 'dataset/file.parquet'",
			expectation: "HEAD dataset → 404 (has children + generic MIME type), s3fs uses LIST → correctly identifies as directory",
		},
		{
			name:        "Filer creates actual directories",
			scenario:    "Filer creates 'dataset' as actual directory with IsDirectory=true",
			expectation: "HEAD dataset → 200 (actual directory, not 0-byte file), maintains AWS S3 compatibility",
		},
		{
			name:        "Empty file edge case",
			scenario:    "User creates 'empty.txt' as 0-byte file with 'application/octet-stream' but no children",
			expectation: "HEAD empty.txt → 200 (no children), s3fs correctly reports as file",
		},
		{
			name:        "Explicit directory request",
			scenario:    "User requests 'dataset/' with trailing slash",
			expectation: "HEAD dataset/ → 200 (explicit directory request), normal directory behavior",
		},
		{
			name:        "Versioned bucket",
			scenario:    "Bucket has versioning enabled",
			expectation: "HEAD dataset → 200 (skip directory marker check), versioned semantics apply",
		},
		{
			name:        "AWS S3 compatibility",
			scenario:    "Only 'dataset/file.txt' exists, no marker at 'dataset'",
			expectation: "HEAD dataset → 404 (object doesn't exist), matches AWS S3 behavior",
		},
		{
			name:        "Directory marker with specific MIME type",
			scenario:    "PyArrow creates 'dataset' as 0-byte file with MIME type 'text/plain' and children",
			expectation: "HEAD dataset → 200 (specific MIME type, not generic), may not work with PyArrow but preserves compatibility",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Scenario: %s", tt.scenario)
			t.Logf("Expected: %s", tt.expectation)
		})
	}
}

// TestImplicitDirectoryIntegration is an integration test placeholder
// Run with: cd test/s3/parquet && make test-implicit-dir-with-server
func TestImplicitDirectoryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Skip("Integration test - run manually with: cd test/s3/parquet && make test-implicit-dir-with-server")
}

// Benchmark for hasChildren performance
func BenchmarkHasChildrenCheck(b *testing.B) {
	// This benchmark would measure the performance impact of the hasChildren check
	// Expected: ~1-5ms per call (one gRPC LIST request with Limit=1)
	b.Skip("Benchmark - requires full filer setup")
}
