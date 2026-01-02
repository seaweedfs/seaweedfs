package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

// TestDirectoryListedAsCommonPrefix tests that regular directories (without MIME)
// are correctly listed as CommonPrefixes when using delimiter "/", not as objects.
func TestDirectoryListedAsCommonPrefix(t *testing.T) {
	testCases := []struct {
		name      string
		delimiter string
	}{
		{"delimiter slash", "/"},
		{"delimiter underscore", "_"},
		{"delimiter colon", ":"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s3a := &S3ApiServer{
				option: &S3ApiServerOption{
					BucketsPath: "/buckets",
				},
			}

			// Regular directory (no MIME) - matching actual user metadata
			regularDir := &filer_pb.Entry{
				Name:        "f2f1237f-0e69-4e0b-8f01-d4fa299787e1.vhd",
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mime: "", // Empty MIME - IsDirectoryKeyObject() returns false
				},
			}

			client := &testFilerClient{
				entriesByDir: map[string][]*filer_pb.Entry{
					"/buckets/xoa-bucket/xo-vm-backups/data": {regularDir},
				},
			}

			cursor := &ListingCursor{maxKeys: 1000}
			var seenDirs []string
			var seenFiles []string

			_, err := s3a.doListFilerEntries(
				client,
				"/buckets/xoa-bucket/xo-vm-backups/data",
				"", // prefix
				cursor,
				"",           // marker
				tc.delimiter, // delimiter - should create CommonPrefixes
				false,
				"xoa-bucket",
				func(dir string, entry *filer_pb.Entry) {
					if entry.IsDirectory {
						seenDirs = append(seenDirs, entry.Name)
					} else {
						seenFiles = append(seenFiles, entry.Name)
					}
				},
			)

			assert.NoError(t, err)

			// The directory should be passed to the callback for ANY delimiter
			assert.Contains(t, seenDirs, "f2f1237f-0e69-4e0b-8f01-d4fa299787e1.vhd",
				"Directory should be passed to callback for CommonPrefix processing with delimiter=%s", tc.delimiter)
			assert.Empty(t, seenFiles, "No files should be seen, only the directory")
		})
	}
}
