package s3api

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestIsInRemoteOnly tests the IsInRemoteOnly method on filer_pb.Entry
func TestIsInRemoteOnly(t *testing.T) {
	tests := []struct {
		name     string
		entry    *filer_pb.Entry
		expected bool
	}{
		{
			name: "remote-only entry with no chunks",
			entry: &filer_pb.Entry{
				Name:   "remote-file.txt",
				Chunks: nil,
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 1024,
				},
			},
			expected: true,
		},
		{
			name: "remote entry with chunks (cached)",
			entry: &filer_pb.Entry{
				Name: "cached-file.txt",
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc123", Size: 1024, Offset: 0},
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 1024,
				},
			},
			expected: false,
		},
		{
			name: "local file with chunks (not remote)",
			entry: &filer_pb.Entry{
				Name: "local-file.txt",
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc123", Size: 1024, Offset: 0},
				},
				RemoteEntry: nil,
			},
			expected: false,
		},
		{
			name: "empty remote entry (size 0)",
			entry: &filer_pb.Entry{
				Name:   "empty-remote.txt",
				Chunks: nil,
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 0,
				},
			},
			expected: false,
		},
		{
			name: "no chunks but nil RemoteEntry",
			entry: &filer_pb.Entry{
				Name:        "empty-local.txt",
				Chunks:      nil,
				RemoteEntry: nil,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.entry.IsInRemoteOnly()
			assert.Equal(t, tt.expected, result,
				"IsInRemoteOnly() for %s should return %v", tt.name, tt.expected)
		})
	}
}

// TestRemoteOnlyEntryDetection tests that the streamFromVolumeServers logic
// correctly distinguishes between remote-only entries and data integrity errors
func TestRemoteOnlyEntryDetection(t *testing.T) {
	tests := []struct {
		name              string
		entry             *filer_pb.Entry
		shouldBeRemote    bool
		shouldBeDataError bool
		shouldBeEmpty     bool
	}{
		{
			name: "remote-only entry (no chunks, has remote entry)",
			entry: &filer_pb.Entry{
				Name:   "remote-file.txt",
				Chunks: nil,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 1024,
				},
			},
			shouldBeRemote:    true,
			shouldBeDataError: false,
			shouldBeEmpty:     false,
		},
		{
			name: "data integrity error (no chunks, no remote, has size)",
			entry: &filer_pb.Entry{
				Name:   "corrupt-file.txt",
				Chunks: nil,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				RemoteEntry: nil,
			},
			shouldBeRemote:    false,
			shouldBeDataError: true,
			shouldBeEmpty:     false,
		},
		{
			name: "empty local file (no chunks, no remote, size 0)",
			entry: &filer_pb.Entry{
				Name:   "empty-file.txt",
				Chunks: nil,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 0,
				},
				RemoteEntry: nil,
			},
			shouldBeRemote:    false,
			shouldBeDataError: false,
			shouldBeEmpty:     true,
		},
		{
			name: "normal file with chunks",
			entry: &filer_pb.Entry{
				Name: "normal-file.txt",
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc123", Size: 1024, Offset: 0},
				},
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				RemoteEntry: nil,
			},
			shouldBeRemote:    false,
			shouldBeDataError: false,
			shouldBeEmpty:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := tt.entry.GetChunks()
			totalSize := int64(filer.FileSize(tt.entry))

			if len(chunks) == 0 {
				// This mirrors the logic in streamFromVolumeServers
				if tt.entry.IsInRemoteOnly() {
					assert.True(t, tt.shouldBeRemote,
						"Entry should be detected as remote-only")
				} else if totalSize > 0 && len(tt.entry.Content) == 0 {
					assert.True(t, tt.shouldBeDataError,
						"Entry should be detected as data integrity error")
				} else {
					assert.True(t, tt.shouldBeEmpty,
						"Entry should be detected as empty")
				}
			} else {
				assert.False(t, tt.shouldBeRemote, "Entry with chunks should not be remote-only")
				assert.False(t, tt.shouldBeDataError, "Entry with chunks should not be data error")
				assert.False(t, tt.shouldBeEmpty, "Entry with chunks should not be empty")
			}
		})
	}
}

// TestVersionedRemoteObjectPathBuilding tests that the path building logic
// correctly handles versioned objects stored in .versions/ directory
func TestVersionedRemoteObjectPathBuilding(t *testing.T) {
	bucketsPath := "/buckets"

	tests := []struct {
		name         string
		bucket       string
		object       string
		versionId    string
		expectedDir  string
		expectedName string
	}{
		{
			name:         "non-versioned object (empty versionId)",
			bucket:       "mybucket",
			object:       "myobject.txt",
			versionId:    "",
			expectedDir:  "/buckets/mybucket",
			expectedName: "myobject.txt",
		},
		{
			name:         "null version",
			bucket:       "mybucket",
			object:       "myobject.txt",
			versionId:    "null",
			expectedDir:  "/buckets/mybucket",
			expectedName: "myobject.txt",
		},
		{
			name:         "specific version",
			bucket:       "mybucket",
			object:       "myobject.txt",
			versionId:    "abc123",
			expectedDir:  "/buckets/mybucket/myobject.txt" + s3_constants.VersionsFolder,
			expectedName: "v_abc123",
		},
		{
			name:         "nested object with version",
			bucket:       "mybucket",
			object:       "folder/subfolder/file.txt",
			versionId:    "xyz789",
			expectedDir:  "/buckets/mybucket/folder/subfolder/file.txt" + s3_constants.VersionsFolder,
			expectedName: "v_xyz789",
		},
		{
			name:         "object with leading slash and version",
			bucket:       "mybucket",
			object:       "/path/to/file.txt",
			versionId:    "ver456",
			expectedDir:  "/buckets/mybucket/path/to/file.txt" + s3_constants.VersionsFolder,
			expectedName: "v_ver456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dir, name string

			// This mirrors the logic in cacheRemoteObjectForStreaming
			if tt.versionId != "" && tt.versionId != "null" {
				// Versioned object path
				normalizedObject := strings.TrimPrefix(removeDuplicateSlashesTest(tt.object), "/")
				dir = bucketsPath + "/" + tt.bucket + "/" + normalizedObject + s3_constants.VersionsFolder
				name = "v_" + tt.versionId
			} else {
				// Non-versioned path (simplified - just for testing)
				dir = bucketsPath + "/" + tt.bucket
				normalizedObject := strings.TrimPrefix(removeDuplicateSlashesTest(tt.object), "/")
				if idx := strings.LastIndex(normalizedObject, "/"); idx > 0 {
					dir = dir + "/" + normalizedObject[:idx]
					name = normalizedObject[idx+1:]
				} else {
					name = normalizedObject
				}
			}

			assert.Equal(t, tt.expectedDir, dir, "Directory path should match")
			assert.Equal(t, tt.expectedName, name, "Name should match")
		})
	}
}

// removeDuplicateSlashesTest is a test helper that mirrors production code
func removeDuplicateSlashesTest(s string) string {
	for strings.Contains(s, "//") {
		s = strings.ReplaceAll(s, "//", "/")
	}
	return s
}

// TestResolvedSourceVersionId pins that latest-version reads in a versioning-
// enabled bucket fall back to the entry's recorded version id when the
// request carried none, so cache paths target .versions/v_<id> correctly.
func TestResolvedSourceVersionId(t *testing.T) {
	tests := []struct {
		name      string
		requested string
		entry     *filer_pb.Entry
		expected  string
	}{
		{
			name:      "explicit request versionId wins",
			requested: "abc123",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.ExtVersionIdKey: []byte("ignored"),
				},
			},
			expected: "abc123",
		},
		{
			name:      "empty request falls back to entry version (latest in versioned bucket)",
			requested: "",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.ExtVersionIdKey: []byte("xyz789"),
				},
			},
			expected: "xyz789",
		},
		{
			name:      "empty request and pre-versioning entry stays empty",
			requested: "",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{},
			},
			expected: "",
		},
		{
			name:      "empty request and nil Extended stays empty",
			requested: "",
			entry:     &filer_pb.Entry{Extended: nil},
			expected:  "",
		},
		{
			name:      "nil entry tolerated when no request version",
			requested: "",
			entry:     nil,
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, resolvedSourceVersionId(tt.requested, tt.entry))
		})
	}
}

func TestCachedEntryHasLocalData(t *testing.T) {
	tests := []struct {
		name     string
		entry    *filer_pb.Entry
		expected bool
	}{
		{
			name:     "nil entry is not a hit",
			entry:    nil,
			expected: false,
		},
		{
			name: "entry with chunks is a hit",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{{FileId: "1,abc", Size: 10}},
			},
			expected: true,
		},
		{
			name: "entry with inline content is a hit",
			entry: &filer_pb.Entry{
				Content: []byte("small file body"),
			},
			expected: true,
		},
		{
			name:     "entry with neither chunks nor content is not a hit",
			entry:    &filer_pb.Entry{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, cachedEntryHasLocalData(tt.entry))
		})
	}
}

// TestCopyObjectRemoteOnlySourceDetection is a regression test for #9304: a
// remote-only source used to fall through to CopyObject's inline branch with
// empty Content, producing a destination with FileSize > 0 but no chunks.
func TestCopyObjectRemoteOnlySourceDetection(t *testing.T) {
	tests := []struct {
		name                   string
		entry                  *filer_pb.Entry
		expectRemoteOnly       bool
		expectInlineBranchHit  bool
		expectBrokenWithoutFix bool
	}{
		{
			name: "remote-only mp3 (matches #9304 reproduction)",
			entry: &filer_pb.Entry{
				Name: "file-1234-audio.mp3",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 16018804,
				},
				Chunks:  nil,
				Content: nil,
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 16018804,
				},
			},
			expectRemoteOnly:       true,
			expectInlineBranchHit:  true,
			expectBrokenWithoutFix: true,
		},
		{
			name: "local file with chunks - copy works fine, fix does not engage",
			entry: &filer_pb.Entry{
				Name: "local.bin",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc", Size: 1024, Offset: 0},
				},
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  false,
			expectBrokenWithoutFix: false,
		},
		{
			name: "small inline file (no chunks, has Content) - hits inline branch but not broken",
			entry: &filer_pb.Entry{
				Name: "tiny.txt",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 5,
				},
				Content: []byte("hello"),
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  true,
			expectBrokenWithoutFix: false,
		},
		{
			name: "remote entry already cached (has chunks) - fix does not engage",
			entry: &filer_pb.Entry{
				Name: "cached.dat",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 2048,
				},
				Chunks: []*filer_pb.FileChunk{
					{FileId: "2,def", Size: 2048, Offset: 0},
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 2048,
				},
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  false,
			expectBrokenWithoutFix: false,
		},
		{
			// Zero-byte remote objects must not enter the cache branch:
			// IsInRemoteOnly requires RemoteSize > 0, so CopyObject's
			// pre-existing inline branch handles them with no 503.
			name: "zero-byte remote object - fix does not engage, inline branch handles it",
			entry: &filer_pb.Entry{
				Name: "empty-on-remote.txt",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 0,
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 0,
				},
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  true,
			expectBrokenWithoutFix: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectRemoteOnly, tt.entry.IsInRemoteOnly())

			// Mirror the inline branch in s3api_object_handlers_copy.go:
			//   if entry.Attributes.FileSize == 0 || len(entry.GetChunks()) == 0
			inlineBranchHit := tt.entry.Attributes != nil &&
				(tt.entry.Attributes.FileSize == 0 || len(tt.entry.GetChunks()) == 0)
			assert.Equal(t, tt.expectInlineBranchHit, inlineBranchHit)

			// The #9304 shape: inline branch fires, no inline content, FileSize > 0.
			brokenWithoutFix := inlineBranchHit &&
				len(tt.entry.Content) == 0 &&
				tt.entry.Attributes != nil &&
				tt.entry.Attributes.FileSize > 0
			assert.Equal(t, tt.expectBrokenWithoutFix, brokenWithoutFix)
		})
	}
}
