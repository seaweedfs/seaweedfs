//go:build linux || darwin || freebsd || windows

package command

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
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

func Test_mountSocketCleanup(t *testing.T) {
	dir, err := os.MkdirTemp("", "swmnt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "mount.sock")
	listen := func() {
		l, err := net.Listen("unix", socketPath)
		if err != nil {
			t.Skipf("unix sockets unavailable: %v", err)
		}
		t.Cleanup(func() { l.Close() })
	}
	exists := func() bool {
		_, err := os.Lstat(socketPath)
		return err == nil
	}

	listen()
	removeMountSocket := mountSocketCleanup(socketPath)
	removeMountSocket()
	removeMountSocket()
	if exists() {
		t.Fatalf("%s is still there after the mount exited", socketPath)
	}

	listen()
	removeMountSocket = mountSocketCleanup(socketPath)
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	listen()
	removeMountSocket()
	if !exists() {
		t.Fatalf("exiting mount removed the socket of a later mount of the same directory")
	}
}

// mountRootFiler serves the filer calls a mount start makes on its root path.
// Like the filer, it answers a non-exclusive create of an existing path by
// replacing the entry, keeping only its crtime and inode.
type mountRootFiler struct {
	filer_pb.SeaweedFilerClient
	entry          *filer_pb.Entry // nil while the path is missing
	textOnlyErrors bool            // a filer that predates error codes
	beforeCreate   func(f *mountRootFiler)
	lookups        int
	creates        []*filer_pb.CreateEntryRequest
	updates        int
}

func (f *mountRootFiler) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(f)
}
func (f *mountRootFiler) AdjustedUrl(_ *filer_pb.Location) string { return "" }
func (f *mountRootFiler) GetDataCenter() string                   { return "" }

func (f *mountRootFiler) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.lookups++
	if f.entry == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(f.entry).(*filer_pb.Entry)}, nil
}

func (f *mountRootFiler) CreateEntry(_ context.Context, in *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	f.creates = append(f.creates, proto.Clone(in).(*filer_pb.CreateEntryRequest))
	if f.beforeCreate != nil {
		f.beforeCreate(f)
	}
	failed := func(code filer_pb.FilerError, text string) (*filer_pb.CreateEntryResponse, error) {
		resp := &filer_pb.CreateEntryResponse{Error: text}
		if !f.textOnlyErrors {
			resp.ErrorCode = code
		}
		return resp, nil
	}
	next := proto.Clone(in.Entry).(*filer_pb.Entry)
	if f.entry != nil {
		if in.OExcl {
			return failed(filer_pb.FilerError_ENTRY_ALREADY_EXISTS, "EEXIST: entry already exists")
		}
		if !f.entry.IsDirectory && next.IsDirectory {
			return failed(filer_pb.FilerError_EXISTING_IS_FILE, "existing entry is a file")
		}
		next.Attributes.Crtime = f.entry.Attributes.Crtime
		next.Attributes.Inode = f.entry.Attributes.Inode
	}
	f.entry = next
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *mountRootFiler) UpdateEntry(_ context.Context, in *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	for key, expected := range in.ExpectedExtended {
		actual, found := f.entry.Extended[key]
		if found && !bytes.Equal(actual, expected) || !found && len(expected) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "extended attribute changed")
		}
	}
	f.updates++
	f.entry = proto.Clone(in.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func Test_ensureMountRootDirectory(t *testing.T) {
	bucket := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:        "b1",
			IsDirectory: true,
			Quota:       1 << 30,
			Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0750), Uid: 1000, Gid: 1000, Crtime: 1, Inode: 7},
			Extended: map[string][]byte{
				s3_constants.ExtAllowEmptyFolders: []byte("false"),
				s3_constants.ExtAmzOwnerKey:       []byte("alice"),
				s3_constants.ExtVersioningKey:     []byte("Enabled"),
			},
		}
	}
	file := func() *filer_pb.Entry {
		return &filer_pb.Entry{Name: "b1", Attributes: &filer_pb.FuseAttributes{FileMode: 0644, Crtime: 1, Inode: 7}}
	}

	t.Run("an existing bucket root survives the mount start", func(t *testing.T) {
		f := &mountRootFiler{entry: bucket()}
		if err := ensureMountRootDirectory(context.Background(), f, "/buckets/b1"); err != nil {
			t.Fatalf("ensureMountRootDirectory: %v", err)
		}
		if err := ensureBucketAllowEmptyFolders(context.Background(), f, "/buckets/b1", "/buckets"); err != nil {
			t.Fatalf("ensureBucketAllowEmptyFolders: %v", err)
		}
		if len(f.creates) != 0 || f.updates != 0 {
			t.Errorf("creates = %d, updates = %d, want none", len(f.creates), f.updates)
		}
		if !proto.Equal(f.entry, bucket()) {
			t.Errorf("bucket entry changed:\n got %v\nwant %v", f.entry, bucket())
		}
	})

	t.Run("a missing root is created exclusively", func(t *testing.T) {
		f := &mountRootFiler{}
		if err := ensureMountRootDirectory(context.Background(), f, "/buckets/b1/data"); err != nil {
			t.Fatalf("ensureMountRootDirectory: %v", err)
		}
		if len(f.creates) != 1 || !f.creates[0].OExcl {
			t.Fatalf("creates = %v, want one exclusive create", f.creates)
		}
		if got := f.creates[0]; got.Directory != "/buckets/b1" || got.Entry.Name != "data" || !got.Entry.IsDirectory {
			t.Errorf("created %s/%s (directory %v), want directory /buckets/b1/data", got.Directory, got.Entry.Name, got.Entry.IsDirectory)
		}
	})

	t.Run("a file at the root path is refused", func(t *testing.T) {
		f := &mountRootFiler{entry: file()}
		err := ensureMountRootDirectory(context.Background(), f, "/buckets/b1")
		if !errors.Is(err, filer_pb.ErrExistingIsFile) {
			t.Fatalf("err = %v, want %v", err, filer_pb.ErrExistingIsFile)
		}
		if len(f.creates) != 0 {
			t.Errorf("creates = %d, want none", len(f.creates))
		}
	})

	for _, textOnly := range []bool{false, true} {
		f := &mountRootFiler{textOnlyErrors: textOnly, beforeCreate: func(f *mountRootFiler) { f.entry = bucket() }}
		t.Run("a root created meanwhile is kept, text-only errors "+strconv.FormatBool(textOnly), func(t *testing.T) {
			if err := ensureMountRootDirectory(context.Background(), f, "/buckets/b1"); err != nil {
				t.Fatalf("ensureMountRootDirectory: %v", err)
			}
			if len(f.creates) != 1 || !proto.Equal(f.entry, bucket()) {
				t.Errorf("creates = %d, entry %v; want one refused exclusive create and the other client's entry", len(f.creates), f.entry)
			}
		})
	}

	t.Run("a file created meanwhile is refused", func(t *testing.T) {
		f := &mountRootFiler{beforeCreate: func(f *mountRootFiler) { f.entry = file() }}
		err := ensureMountRootDirectory(context.Background(), f, "/buckets/b1")
		if !errors.Is(err, filer_pb.ErrExistingIsFile) {
			t.Fatalf("err = %v, want %v", err, filer_pb.ErrExistingIsFile)
		}
	})

	t.Run("the filer root is never created", func(t *testing.T) {
		f := &mountRootFiler{}
		if err := ensureMountRootDirectory(context.Background(), f, "/"); err != nil {
			t.Fatalf("ensureMountRootDirectory: %v", err)
		}
		if f.lookups != 0 || len(f.creates) != 0 {
			t.Errorf("lookups = %d, creates = %d, want none", f.lookups, len(f.creates))
		}
	})
}
