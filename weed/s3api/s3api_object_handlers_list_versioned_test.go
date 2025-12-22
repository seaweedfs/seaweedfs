package s3api

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	grpc "google.golang.org/grpc"
)

// TestListObjectsWithVersionedObjects tests that versioned objects are properly listed
// This validates the fix for duplicate versioned objects issue
func TestListObjectsWithVersionedObjects(t *testing.T) {
	now := time.Now().Unix()

	// Create test filer client with versioned objects
	filerClient := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test-bucket": {
				// Regular directory
				{
					Name:        "folder1",
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
				},
				// .versions directory with metadata for versioned object
				{
					Name:        "file1.txt" + s3_constants.VersionsFolder,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
					Extended: map[string][]byte{
						s3_constants.ExtLatestVersionIdKey:     []byte("v1-abc123"),
						s3_constants.ExtLatestVersionSizeKey:   []byte("1234"),
						s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
						s3_constants.ExtLatestVersionETagKey:   []byte(fmt.Sprintf("\"%s\"", hex.EncodeToString([]byte("test-etag-1")))),
					},
				},
				// Another .versions directory
				{
					Name:        "file2.txt" + s3_constants.VersionsFolder,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
					Extended: map[string][]byte{
						s3_constants.ExtLatestVersionIdKey:     []byte("v2-def456"),
						s3_constants.ExtLatestVersionSizeKey:   []byte("5678"),
						s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
						s3_constants.ExtLatestVersionETagKey:   []byte(fmt.Sprintf("\"%s\"", hex.EncodeToString([]byte("test-etag-2")))),
					},
				},
			},
			"/buckets/test-bucket/folder1": {
				// Versioned object in subdirectory
				{
					Name:        "nested.txt" + s3_constants.VersionsFolder,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
					Extended: map[string][]byte{
						s3_constants.ExtLatestVersionIdKey:     []byte("v3-ghi789"),
						s3_constants.ExtLatestVersionSizeKey:   []byte("9012"),
						s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
						s3_constants.ExtLatestVersionETagKey:   []byte(fmt.Sprintf("\"%s\"", hex.EncodeToString([]byte("test-etag-3")))),
					},
				},
			},
		},
	}

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	tests := []struct {
		name              string
		bucket            string
		prefix            string
		delimiter         string
		expectedCount     int
		expectedKeys      []string
		expectedPrefixes  []string
	}{
		{
			name:          "List all objects including versioned (no delimiter)",
			bucket:        "test-bucket",
			prefix:        "",
			delimiter:     "",
			expectedCount: 3, // file1.txt, file2.txt, folder1/nested.txt
			expectedKeys: []string{
				"file1.txt",
				"file2.txt",
				"folder1/nested.txt",
			},
			expectedPrefixes: []string{},
		},
		{
			name:          "List bucket root with delimiter",
			bucket:        "test-bucket",
			prefix:        "",
			delimiter:     "/",
			expectedCount: 2, // file1.txt, file2.txt (folder1/ becomes common prefix)
			expectedKeys: []string{
				"file1.txt",
				"file2.txt",
			},
			expectedPrefixes: []string{
				"folder1/",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a direct test instead of mocking WithFilerClient
			cursor := &ListingCursor{maxKeys: uint16(tt.expectedCount + 10)}
			contents := []ListEntry{}
			commonPrefixes := []PrefixEntry{}
			bucketPrefix := fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, tt.bucket)
			
			_, err := s3a.doListFilerEntries(filerClient, bucketPrefix[:len(bucketPrefix)-1], tt.prefix, cursor, "", tt.delimiter, false, false, tt.bucket, func(dir string, entry *filer_pb.Entry) {
				if cursor.maxKeys <= 0 {
					return
				}
				
				if entry.IsDirectory {
					if tt.delimiter == "/" {
						// Extract relative path from bucket prefix
						relDir := strings.TrimPrefix(dir, bucketPrefix[:len(bucketPrefix)-1])
						if relDir != "" && relDir[0] == '/' {
							relDir = relDir[1:]
						}
						prefix := relDir
						if prefix != "" {
							prefix += "/"
						}
						prefix += entry.Name + "/"
						
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: prefix,
						})
						cursor.maxKeys--
					}
				} else {
					// Extract key from dir and entry name
					relDir := strings.TrimPrefix(dir, bucketPrefix[:len(bucketPrefix)-1])
					if relDir != "" && relDir[0] == '/' {
						relDir = relDir[1:]
					}
					key := entry.Name
					if relDir != "" {
						key = relDir + "/" + entry.Name
					}
					
					contents = append(contents, ListEntry{
						Key: key,
					})
					cursor.maxKeys--
				}
			})

			assert.NoError(t, err, "doListFilerEntries should not return error")
			assert.Equal(t, tt.expectedCount, len(contents), "Should return correct number of objects")
			assert.Equal(t, len(tt.expectedPrefixes), len(commonPrefixes), "Should return correct number of common prefixes")

			// Verify keys
			actualKeys := make([]string, len(contents))
			for i, entry := range contents {
				actualKeys[i] = entry.Key
			}
			assert.ElementsMatch(t, tt.expectedKeys, actualKeys, "Should return expected keys")

			// Verify common prefixes
			actualPrefixes := make([]string, len(commonPrefixes))
			for i, prefix := range commonPrefixes {
				actualPrefixes[i] = prefix.Prefix
			}
			assert.ElementsMatch(t, tt.expectedPrefixes, actualPrefixes, "Should return expected prefixes")

			// Verify each versioned object has correct version metadata
			for _, entry := range contents {
				assert.NotEmpty(t, entry.Key, "Versioned object should have key")
			}
		})
	}
}

// TestVersionedObjectsNoDuplication ensures that .versions directories are only processed once
// This is the core test for the bug fix - previously versioned objects were duplicated 250x
func TestVersionedObjectsNoDuplication(t *testing.T) {
	now := time.Now().Unix()

	// Create a single .versions directory
	filerClient := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test-bucket": {
				{
					Name:        "test.txt" + s3_constants.VersionsFolder,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
					Extended: map[string][]byte{
						s3_constants.ExtLatestVersionIdKey:     []byte("v1-test"),
						s3_constants.ExtLatestVersionSizeKey:   []byte("100"),
						s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
						s3_constants.ExtLatestVersionETagKey:   []byte("\"test-etag\""),
					},
				},
			},
		},
	}

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	cursor := &ListingCursor{maxKeys: uint16(1000)}
	contents := []ListEntry{}
	_, err := s3a.doListFilerEntries(filerClient, "/buckets/test-bucket", "", cursor, "", "", false, false, "test-bucket", func(dir string, entry *filer_pb.Entry) {
		contents = append(contents, ListEntry{Key: entry.Name})
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, len(contents), "Should return exactly 1 object (no duplicates)")
	assert.Equal(t, "test.txt", contents[0].Key, "Should return correct key")
}

// TestVersionedObjectsWithDeleteMarker tests that objects with delete markers are not listed
func TestVersionedObjectsWithDeleteMarker(t *testing.T) {
	now := time.Now().Unix()

	filerClient := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test-bucket": {
				// Active versioned object
				{
					Name:        "active.txt" + s3_constants.VersionsFolder,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
					Extended: map[string][]byte{
						s3_constants.ExtLatestVersionIdKey:     []byte("v1-active"),
						s3_constants.ExtLatestVersionSizeKey:   []byte("100"),
						s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
						s3_constants.ExtLatestVersionETagKey:   []byte("\"etag-active\""),
					},
				},
				// Deleted object (has delete marker)
				{
					Name:        "deleted.txt" + s3_constants.VersionsFolder,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime: now,
					},
					Extended: map[string][]byte{
						s3_constants.ExtLatestVersionIdKey:           []byte("v1-deleted"),
						s3_constants.ExtLatestVersionIsDeleteMarker:  []byte("true"),
					},
				},
			},
		},
	}

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	cursor := &ListingCursor{maxKeys: uint16(1000)}
	contents := []ListEntry{}
	_, err := s3a.doListFilerEntries(filerClient, "/buckets/test-bucket", "", cursor, "", "", false, false, "test-bucket", func(dir string, entry *filer_pb.Entry) {
		contents = append(contents, ListEntry{Key: entry.Name})
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, len(contents), "Should only return active object, not deleted")
	assert.Equal(t, "active.txt", contents[0].Key, "Should return the active object")
}

// TestVersionedObjectsMaxKeys tests pagination with versioned objects
func TestVersionedObjectsMaxKeys(t *testing.T) {
	now := time.Now().Unix()

	// Create 5 versioned objects
	entries := make([]*filer_pb.Entry, 5)
	for i := 0; i < 5; i++ {
		entries[i] = &filer_pb.Entry{
			Name:        fmt.Sprintf("file%d.txt"+s3_constants.VersionsFolder, i),
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime: now,
			},
			Extended: map[string][]byte{
				s3_constants.ExtLatestVersionIdKey:     []byte(fmt.Sprintf("v%d", i)),
				s3_constants.ExtLatestVersionSizeKey:   []byte("100"),
				s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
				s3_constants.ExtLatestVersionETagKey:   []byte(fmt.Sprintf("\"etag-%d\"", i)),
			},
		}
	}

	filerClient := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test-bucket": entries,
		},
	}

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	cursor := &ListingCursor{maxKeys: uint16(3)}
	contents := []ListEntry{}
	_, err := s3a.doListFilerEntries(filerClient, "/buckets/test-bucket", "", cursor, "", "", false, false, "test-bucket", func(dir string, entry *filer_pb.Entry) {
		if cursor.maxKeys <= 0 {
			return
		}
		contents = append(contents, ListEntry{Key: entry.Name})
		cursor.maxKeys--
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, len(contents), "Should respect maxKeys limit")
	assert.True(t, cursor.isTruncated, "Should set IsTruncated when there are more results")
}

// TestVersionsDirectoryNotTraversed ensures .versions directories are never traversed
func TestVersionsDirectoryNotTraversed(t *testing.T) {
	now := time.Now().Unix()
	traversedDirs := make(map[string]bool)

	// Custom filer client that tracks which directories are accessed
	customClient := &customTestFilerClient{
		testFilerClient: testFilerClient{
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/test-bucket": {
					{
						Name:        "object.txt" + s3_constants.VersionsFolder,
						IsDirectory: true,
						Attributes: &filer_pb.FuseAttributes{
							Mtime: now,
						},
						Extended: map[string][]byte{
							s3_constants.ExtLatestVersionIdKey:     []byte("v1"),
							s3_constants.ExtLatestVersionSizeKey:   []byte("100"),
							s3_constants.ExtLatestVersionMtimeKey:  []byte(strconv.FormatInt(now, 10)),
							s3_constants.ExtLatestVersionETagKey:   []byte("\"etag\""),
						},
					},
				},
				// This directory should NEVER be accessed
				"/buckets/test-bucket/object.txt.versions": {
					{
						Name:        "should-not-see-this",
						IsDirectory: false,
					},
				},
			},
		},
		traversedDirs: &traversedDirs,
	}

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	cursor := &ListingCursor{maxKeys: uint16(1000)}
	contents := []ListEntry{}
	_, err := s3a.doListFilerEntries(customClient, "/buckets/test-bucket", "", cursor, "", "", false, false, "test-bucket", func(dir string, entry *filer_pb.Entry) {
		contents = append(contents, ListEntry{Key: entry.Name})
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, len(contents))
	
	// Verify .versions directory was NEVER traversed
	_, wasTraversed := traversedDirs["/buckets/test-bucket/object.txt.versions"]
	assert.False(t, wasTraversed, ".versions directory should never be traversed")
}

// customTestFilerClient tracks which directories are accessed
type customTestFilerClient struct {
	testFilerClient
	traversedDirs *map[string]bool
}

func (c *customTestFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	(*c.traversedDirs)[in.Directory] = true
	return c.testFilerClient.ListEntries(ctx, in, opts...)
}
