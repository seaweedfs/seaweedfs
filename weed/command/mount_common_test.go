//go:build linux || darwin || freebsd || windows

package command

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func Test_volumeName(t *testing.T) {
	tests := []struct {
		name               string
		filer              string
		filerMountRootPath string
		dir                string
		override           string
		expected           string
	}{
		{
			name:               "an override outranks the mounted path and the mount point",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/buckets/videos",
			dir:                `\\seaweedfs\Images`,
			override:           "MyDisk",
			expected:           "MyDisk",
		},
		{
			name:               "an override still gets commas replaced",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/",
			override:           "My, Disk",
			expected:           "My+ Disk",
		},
		{
			name:               "a drive letter leaves only the filer to fall back on",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/",
			dir:                "S:",
			expected:           "127.0.0.1:8888",
		},
		{
			name:               "empty path falls back to the filer",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "",
			expected:           "127.0.0.1:8888",
		},
		{
			name:               "several filers stay parseable as one option",
			filer:              "127.0.0.1:8888,127.0.0.1:8889",
			filerMountRootPath: "/",
			expected:           "127.0.0.1:8888+127.0.0.1:8889",
		},
		{
			name:               "a whole-tree mount takes the name of its network share",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/",
			dir:                `\\seaweedfs\Images`,
			expected:           "Images",
		},
		{
			name:               "a whole-tree mount takes the name of its directory",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/",
			dir:                "/mnt/seaweedfs",
			expected:           "seaweedfs",
		},
		{
			name:               "the mounted path outranks the mount point",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/buckets/videos",
			dir:                "/mnt/seaweedfs",
			expected:           "videos",
		},
		{
			name:               "a relative mount point is not a name",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/",
			dir:                ".",
			expected:           "127.0.0.1:8888",
		},
		{
			name:               "mounted directory names the disk",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/buckets/images",
			expected:           "images",
		},
		{
			name:               "trailing slash is not a name",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/buckets/videos/",
			expected:           "videos",
		},
		{
			name:               "spaces are kept, commas are not",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/Image, Disk",
			expected:           "Image+ Disk",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := volumeName(tt.filer, tt.filerMountRootPath, tt.dir, tt.override); got != tt.expected {
				t.Errorf("volumeName(%q, %q, %q, %q) = %q, want %q", tt.filer, tt.filerMountRootPath, tt.dir, tt.override, got, tt.expected)
			}
		})
	}
}

type allowEmptyFoldersInnerClient struct {
	filer_pb.SeaweedFilerClient
	entry          *filer_pb.Entry
	beforeUpdate   func()
	lookups        int
	updateAttempts int
	updates        []*filer_pb.Entry
}

func (c *allowEmptyFoldersInnerClient) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.lookups++
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(c.entry).(*filer_pb.Entry)}, nil
}

func (c *allowEmptyFoldersInnerClient) UpdateEntry(_ context.Context, in *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	c.updateAttempts++
	if c.beforeUpdate != nil {
		c.beforeUpdate()
	}
	for key, expected := range in.ExpectedExtended {
		actual, found := c.entry.Extended[key]
		if found && !bytes.Equal(actual, expected) || !found && len(expected) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "extended attribute changed")
		}
	}
	c.entry = proto.Clone(in.Entry).(*filer_pb.Entry)
	c.updates = append(c.updates, in.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

type allowEmptyFoldersFilerClient struct {
	inner *allowEmptyFoldersInnerClient
}

func (c *allowEmptyFoldersFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c.inner)
}
func (c *allowEmptyFoldersFilerClient) AdjustedUrl(_ *filer_pb.Location) string { return "" }
func (c *allowEmptyFoldersFilerClient) GetDataCenter() string                   { return "" }

func Test_ensureBucketAllowEmptyFolders(t *testing.T) {
	bucketEntry := func(attrValue string) *filer_pb.Entry {
		entry := &filer_pb.Entry{Name: "b1", IsDirectory: true, Extended: map[string][]byte{}}
		if attrValue != "" {
			entry.Extended[s3_constants.ExtAllowEmptyFolders] = []byte(attrValue)
		}
		return entry
	}

	tests := []struct {
		name          string
		mountRoot     string
		attrValue     string
		beforeUpdate  func(inner *allowEmptyFoldersInnerClient)
		wantLookups   int
		wantUpdates   int
		wantAttrValue string
	}{
		{name: "no policy allows empty folders", mountRoot: "/buckets/b1", wantLookups: 1, wantUpdates: 1, wantAttrValue: "true"},
		{name: "explicit true is kept", mountRoot: "/buckets/b1", attrValue: "true", wantLookups: 1, wantUpdates: 0},
		{name: "explicit false is kept", mountRoot: "/buckets/b1", attrValue: "false", wantLookups: 1, wantUpdates: 0},
		{name: "subdirectory mount leaves the bucket alone", mountRoot: "/buckets/b1/sub", wantLookups: 0, wantUpdates: 0},
		{
			name:      "a concurrent false wins over the mount default",
			mountRoot: "/buckets/b1",
			beforeUpdate: func(inner *allowEmptyFoldersInnerClient) {
				inner.entry.Extended[s3_constants.ExtAllowEmptyFolders] = []byte("false")
			},
			wantLookups:   2,
			wantUpdates:   0,
			wantAttrValue: "false",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &allowEmptyFoldersInnerClient{entry: bucketEntry(tt.attrValue)}
			if tt.beforeUpdate != nil {
				inner.beforeUpdate = func() { tt.beforeUpdate(inner) }
			}
			filerClient := &allowEmptyFoldersFilerClient{inner: inner}

			if err := ensureBucketAllowEmptyFolders(context.Background(), filerClient, tt.mountRoot, "/buckets"); err != nil {
				t.Fatalf("ensureBucketAllowEmptyFolders: %v", err)
			}
			if inner.lookups != tt.wantLookups {
				t.Errorf("lookups = %d, want %d", inner.lookups, tt.wantLookups)
			}
			if len(inner.updates) != tt.wantUpdates {
				t.Fatalf("updates = %d, want %d", len(inner.updates), tt.wantUpdates)
			}
			if tt.wantAttrValue != "" {
				got := string(inner.entry.Extended[s3_constants.ExtAllowEmptyFolders])
				if got != tt.wantAttrValue {
					t.Errorf("%s = %q, want %q", s3_constants.ExtAllowEmptyFolders, got, tt.wantAttrValue)
				}
			}
		})
	}
}
