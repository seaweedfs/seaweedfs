package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestVersionedFilePathRewrittenForRemote verifies that the fix for
// https://github.com/seaweedfs/seaweedfs/discussions/8481#discussioncomment-16209342
// works correctly: internal .versions/v_{id} paths are rewritten to the
// original S3 object key before syncing to the remote.
func TestVersionedFilePathRewrittenForRemote(t *testing.T) {
	bucketsDir := "/buckets"
	bucketName := "devicetransaction"
	bucket := util.FullPath(bucketsDir).Child(bucketName)

	objectPath := "9e149757-2363-11f1-bfa6-11c8ff31b539/transactionlog-2026-03-19-16-30-00.xml"
	versionId := "6761c63812bd9b64704acf08a3ba5800"
	versionFileName := "v_" + versionId

	// The filer event for the version file creation
	versionedParentPath := string(bucket) + "/" + objectPath + s3_constants.VersionsFolder

	// The CREATE event that remote_gateway receives
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: versionedParentPath,
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: versionedParentPath,
			NewEntry: &filer_pb.Entry{
				Name:    versionFileName,
				Content: []byte("test content"),
			},
		},
	}

	// Verify preconditions
	if !filer_pb.IsCreate(event) {
		t.Fatal("expected create event")
	}
	if isMultipartUploadFile(event.EventNotification.NewParentPath, event.EventNotification.NewEntry.Name) {
		t.Fatal("should not be detected as multipart upload")
	}
	if !filer.HasData(event.EventNotification.NewEntry) {
		t.Fatal("version file should have data")
	}
	if !shouldSendToRemote(event.EventNotification.NewEntry) {
		t.Fatal("version file should be eligible for remote sync")
	}

	// Apply the versioned path rewriting (as remote_gateway now does)
	parentPath, entryName := event.EventNotification.NewParentPath, event.EventNotification.NewEntry.Name
	if newParent, newName, ok := rewriteVersionedSourcePath(parentPath, entryName); ok {
		parentPath, entryName = newParent, newName
	}

	// Compute the remote destination with the rewritten path
	remoteStorageMountLocation := &remote_pb.RemoteStorageLocation{
		Name:   "central",
		Bucket: bucketName,
		Path:   "/",
	}
	sourcePath := util.NewFullPath(parentPath, entryName)
	dest := toRemoteStorageLocation(bucket, sourcePath, remoteStorageMountLocation)

	// Verify the destination does NOT contain internal .versions structure
	if strings.Contains(dest.Path, s3_constants.VersionsFolder) {
		t.Errorf("remote destination path still contains .versions: %s", dest.Path)
	}

	expectedPath := "/" + objectPath
	if dest.Path != expectedPath {
		t.Errorf("remote destination path = %q, want %q", dest.Path, expectedPath)
	}
}

// TestVersionsDirectoryFilteredByHasData verifies that the .versions
// directory creation event is correctly filtered out (no data), so only
// the version file event needs path rewriting.
func TestVersionsDirectoryFilteredByHasData(t *testing.T) {
	bucket := "/buckets/devicetransaction"
	objectPath := "9e149757-2363-11f1-bfa6-11c8ff31b539/transactionlog-2026-03-19-16-30-00.xml"

	dirEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: bucket + "/9e149757-2363-11f1-bfa6-11c8ff31b539",
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: bucket + "/9e149757-2363-11f1-bfa6-11c8ff31b539",
			NewEntry: &filer_pb.Entry{
				Name:        objectPath[strings.LastIndex(objectPath, "/")+1:] + s3_constants.VersionsFolder,
				IsDirectory: true,
			},
		},
	}

	if filer.HasData(dirEvent.EventNotification.NewEntry) {
		t.Error(".versions directory should not have data")
	}
}

func TestIsVersionedPath(t *testing.T) {
	tests := []struct {
		label    string
		dir      string
		name     string
		isDir    bool
		expected bool
	}{
		// Version file inside .versions directory (file, v_ prefix)
		{
			label:    "version file in .versions dir",
			dir:      "/buckets/mybucket/path/to/file.xml" + s3_constants.VersionsFolder,
			name:     "v_6761c63812bd9b64704acf08a3ba5800",
			isDir:    false,
			expected: true,
		},
		// Regular file (not versioned)
		{
			label:    "regular file",
			dir:      "/buckets/mybucket/path/to",
			name:     "file.xml",
			isDir:    false,
			expected: false,
		},
		// .versions directory entry itself
		{
			label:    ".versions directory entry",
			dir:      "/buckets/mybucket/path/to",
			name:     "file.xml" + s3_constants.VersionsFolder,
			isDir:    true,
			expected: true,
		},
		// Non-version file inside .versions dir (no v_ prefix) — not internal
		{
			label:    "non-version file in .versions dir",
			dir:      "/buckets/mybucket/file.xml" + s3_constants.VersionsFolder,
			name:     "some_other_file",
			isDir:    false,
			expected: false,
		},
		// User-created directory whose name ends with .versions — not
		// treated as versioned when isDir=false (file inside it)
		{
			label:    "file in user dir ending with .versions but no v_ prefix",
			dir:      "/buckets/mybucket/my" + s3_constants.VersionsFolder,
			name:     "data.txt",
			isDir:    false,
			expected: false,
		},
		// Regular directory (not .versions)
		{
			label:    "regular directory",
			dir:      "/buckets/mybucket/path/to",
			name:     "subdir",
			isDir:    true,
			expected: false,
		},
		// Entry whose name ends with .versions but is a file, not a dir
		{
			label:    "file named like .versions dir",
			dir:      "/buckets/mybucket/path/to",
			name:     "file.xml" + s3_constants.VersionsFolder,
			isDir:    false,
			expected: false,
		},
		// Non-bucket mount: .versions dir should not match
		{
			label:    ".versions dir outside /buckets/",
			dir:      "/mnt/remote/path/to",
			name:     "file.xml" + s3_constants.VersionsFolder,
			isDir:    true,
			expected: false,
		},
		// Non-bucket mount: v_ file should not match
		{
			label:    "v_ file outside /buckets/",
			dir:      "/data/archive/file.xml" + s3_constants.VersionsFolder,
			name:     "v_abc123",
			isDir:    false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got := isVersionedPath(tt.dir, tt.name, tt.isDir)
			if got != tt.expected {
				t.Errorf("isVersionedPath(%q, %q, %v) = %v, want %v",
					tt.dir, tt.name, tt.isDir, got, tt.expected)
			}
		})
	}
}

// TestDeleteMarkerDetectedBeforeHasDataFilter verifies that a delete marker
// (zero-content version entry with ExtDeleteMarkerKey="true") is detected
// and can be propagated as a deletion, rather than being silently dropped
// by the HasData() check.
func TestDeleteMarkerDetectedBeforeHasDataFilter(t *testing.T) {
	bucketsDir := "/buckets"
	bucketName := "devicetransaction"
	bucket := util.FullPath(bucketsDir).Child(bucketName)

	objectPath := "docs/report.pdf"
	versionId := "aabb112233445566"
	versionFileName := "v_" + versionId
	versionedParentPath := string(bucket) + "/" + objectPath + s3_constants.VersionsFolder

	// A delete marker CREATE event: has ExtDeleteMarkerKey but no content
	deleteMarkerEntry := &filer_pb.Entry{
		Name: versionFileName,
		Extended: map[string][]byte{
			s3_constants.ExtDeleteMarkerKey: []byte("true"),
			s3_constants.ExtVersionIdKey:    []byte(versionId),
		},
		// Content and Chunks are nil → HasData() returns false
	}

	// Preconditions
	if filer.HasData(deleteMarkerEntry) {
		t.Fatal("delete marker should have no data")
	}
	if !isDeleteMarker(deleteMarkerEntry) {
		t.Fatal("should be detected as a delete marker")
	}

	// The versioned path should be rewritable to the original key
	newParent, newName, ok := rewriteVersionedSourcePath(versionedParentPath, deleteMarkerEntry.Name)
	if !ok {
		t.Fatal("delete marker path should be rewritable")
	}

	// Verify the rewritten path points to the original object
	remoteStorageMountLocation := &remote_pb.RemoteStorageLocation{
		Name:   "central",
		Bucket: bucketName,
		Path:   "/",
	}
	dest := toRemoteStorageLocation(bucket, util.NewFullPath(newParent, newName), remoteStorageMountLocation)

	expectedPath := "/" + objectPath
	if dest.Path != expectedPath {
		t.Errorf("delete marker destination = %q, want %q", dest.Path, expectedPath)
	}
	if strings.Contains(dest.Path, s3_constants.VersionsFolder) {
		t.Errorf("delete marker destination should not contain .versions: %s", dest.Path)
	}
}

func TestRewriteVersionedSourcePath(t *testing.T) {
	tests := []struct {
		name        string
		dir         string
		entryName   string
		wantDir     string
		wantName    string
		wantChanged bool
	}{
		{
			name:        "version file in .versions dir",
			dir:         "/buckets/bucket/path/to/file.xml" + s3_constants.VersionsFolder,
			entryName:   "v_6761c63812bd9b64704acf08a3ba5800",
			wantDir:     "/buckets/bucket/path/to",
			wantName:    "file.xml",
			wantChanged: true,
		},
		{
			name:        "regular file",
			dir:         "/buckets/bucket/path/to",
			entryName:   "file.xml",
			wantDir:     "/buckets/bucket/path/to",
			wantName:    "file.xml",
			wantChanged: false,
		},
		{
			name:        "version file at bucket root",
			dir:         "/buckets/bucket/report.pdf" + s3_constants.VersionsFolder,
			entryName:   "v_abc123",
			wantDir:     "/buckets/bucket",
			wantName:    "report.pdf",
			wantChanged: true,
		},
		{
			name:        "non-bucket path not rewritten",
			dir:         "/file.xml" + s3_constants.VersionsFolder,
			entryName:   "v_abc123",
			wantDir:     "/file.xml" + s3_constants.VersionsFolder,
			wantName:    "v_abc123",
			wantChanged: false,
		},
		{
			name:        "non-bucket mount not rewritten",
			dir:         "/mnt/remote/file.xml" + s3_constants.VersionsFolder,
			entryName:   "v_abc123",
			wantDir:     "/mnt/remote/file.xml" + s3_constants.VersionsFolder,
			wantName:    "v_abc123",
			wantChanged: false,
		},
		{
			name:        "non-version file in .versions dir",
			dir:         "/buckets/bucket/file.xml" + s3_constants.VersionsFolder,
			entryName:   "some_other_file",
			wantDir:     "/buckets/bucket/file.xml" + s3_constants.VersionsFolder,
			wantName:    "some_other_file",
			wantChanged: false,
		},
		{
			name:        "dir not ending in .versions",
			dir:         "/buckets/bucket/path/to",
			entryName:   "v_abc123",
			wantDir:     "/buckets/bucket/path/to",
			wantName:    "v_abc123",
			wantChanged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDir, gotName, gotChanged := rewriteVersionedSourcePath(tt.dir, tt.entryName)
			if gotDir != tt.wantDir || gotName != tt.wantName || gotChanged != tt.wantChanged {
				t.Errorf("rewriteVersionedSourcePath(%q, %q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.dir, tt.entryName, gotDir, gotName, gotChanged, tt.wantDir, tt.wantName, tt.wantChanged)
			}
		})
	}
}

func chunkedEntry(name string, remote *filer_pb.RemoteEntry, etags ...string) *filer_pb.Entry {
	entry := &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}, RemoteEntry: remote}
	for i, etag := range etags {
		entry.Chunks = append(entry.Chunks, &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%02x", i), Offset: int64(i) * 1024, Size: 1024, ETag: etag})
	}
	return entry
}

func TestIsMetadataOnlyUpdate(t *testing.T) {
	const dir = "/buckets/media"
	replicated := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 2048, RemoteMtime: 1786096669}

	tests := []struct {
		name     string
		dir      string
		oldEntry *filer_pb.Entry
		newEntry *filer_pb.Entry
		want     bool
	}{
		{
			name:     "same chunks",
			dir:      dir,
			oldEntry: chunkedEntry("output.pdf", replicated, "e1", "e2"),
			newEntry: chunkedEntry("output.pdf", replicated, "e1", "e2"),
			want:     true,
		},
		{
			name:     "same chunks, no RemoteEntry",
			dir:      dir,
			oldEntry: chunkedEntry("output.pdf", nil, "e1", "e2"),
			newEntry: chunkedEntry("output.pdf", nil, "e1", "e2"),
			want:     true,
		},
		{
			name:     "rewritten chunks",
			dir:      dir,
			oldEntry: chunkedEntry("output.pdf", replicated, "e1", "e2"),
			newEntry: chunkedEntry("output.pdf", replicated, "e1", "e3"),
			want:     false,
		},
		{
			name:     "first write into an empty entry",
			dir:      dir,
			oldEntry: chunkedEntry("output.pdf", nil),
			newEntry: chunkedEntry("output.pdf", nil, "e1"),
			want:     false,
		},
		{
			name:     "rename",
			dir:      dir,
			oldEntry: chunkedEntry("output.pdf", replicated, "e1"),
			newEntry: chunkedEntry("renamed.pdf", replicated, "e1"),
			want:     false,
		},
		{
			name:     "move to another directory",
			dir:      "/buckets/media/inbox",
			oldEntry: chunkedEntry("output.pdf", replicated, "e1"),
			newEntry: chunkedEntry("output.pdf", replicated, "e1"),
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := &filer_pb.EventNotification{NewParentPath: dir, OldEntry: tt.oldEntry, NewEntry: tt.newEntry}
			if got := isMetadataOnlyUpdate(tt.dir, message); got != tt.want {
				t.Errorf("isMetadataOnlyUpdate = %v, want %v", got, tt.want)
			}
		})
	}
}

// stubFilerClient serves one entry; nil means not found. A non-nil err fails
// every lookup instead. entries, when set, answers lookups by "dir/name"
// before entry is consulted.
type stubFilerClient struct {
	filer_pb.SeaweedFilerClient
	entry     *filer_pb.Entry
	entries   map[string]*filer_pb.Entry
	err       error
	updateErr error
	lookups   int
	updates   int
	updated   *filer_pb.UpdateEntryRequest
}

func (c *stubFilerClient) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.lookups++
	if c.err != nil {
		return nil, c.err
	}
	if c.entries != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: c.entries[req.Directory+"/"+req.Name]}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: c.entry}, nil
}

func (c *stubFilerClient) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	c.updates++
	c.updated = req
	if c.updateErr != nil {
		return &filer_pb.UpdateEntryResponse{}, c.updateErr
	}
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (c *stubFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c)
}

func (c *stubFilerClient) AdjustedUrl(location *filer_pb.Location) string { return location.Url }

func (c *stubFilerClient) GetDataCenter() string { return "" }

// TestLiveRemoteEntry tells apart the two update events that carry no
// RemoteEntry: a file never replicated (#11139), which must be uploaded, and a
// chmod logged before the sync finished uploading the preceding write, which
// must not be uploaded again.
func TestLiveRemoteEntry(t *testing.T) {
	const dir = "/buckets/media"
	stamped := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 2048, RemoteMtime: 1786096669}

	t.Run("event carries the RemoteEntry, no lookup", func(t *testing.T) {
		filerClient := &stubFilerClient{}
		got, err := liveRemoteEntry(filerClient, dir, chunkedEntry("output.pdf", stamped, "e1"))
		if err != nil {
			t.Fatal(err)
		}
		if got != stamped {
			t.Errorf("got %+v, want the event's own RemoteEntry", got)
		}
		if filerClient.lookups != 0 {
			t.Errorf("looked up the filer %d times with the answer already in hand", filerClient.lookups)
		}
	})

	t.Run("never replicated", func(t *testing.T) {
		event := chunkedEntry("output.pdf", nil, "e1")
		if !shouldSendToRemote(event) {
			t.Fatal("an entry with no RemoteEntry should be eligible for sending")
		}
		filerClient := &stubFilerClient{entry: chunkedEntry("output.pdf", nil, "e1")}
		got, err := liveRemoteEntry(filerClient, dir, event)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("got %+v, want nil so the update takes the write path", got)
		}
	})

	t.Run("stamped after the event was logged", func(t *testing.T) {
		filerClient := &stubFilerClient{entry: chunkedEntry("output.pdf", stamped, "e1")}
		got, err := liveRemoteEntry(filerClient, dir, chunkedEntry("output.pdf", nil, "e1"))
		if err != nil {
			t.Fatal(err)
		}
		if got != stamped {
			t.Errorf("got %+v, want the filer's current RemoteEntry so the update stays on the metadata path", got)
		}
		if filerClient.lookups != 1 {
			t.Errorf("looked up the filer %d times, want 1", filerClient.lookups)
		}
	})

	t.Run("deleted since", func(t *testing.T) {
		_, err := liveRemoteEntry(&stubFilerClient{}, dir, chunkedEntry("output.pdf", nil, "e1"))
		if !errors.Is(err, filer_pb.ErrNotFound) {
			t.Errorf("err = %v, want filer_pb.ErrNotFound so the update is skipped rather than uploaded from a deleted entry", err)
		}
	})
}

func chunk(fileId, etag string) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileId, Size: 1024, ETag: etag}
}

func chunkAt(fileId, etag string, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileId, Offset: offset, Size: 1024, ETag: etag}
}

func entryWith(name string, remote *filer_pb.RemoteEntry, chunks ...*filer_pb.FileChunk) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}, RemoteEntry: remote, Chunks: chunks}
}

// TestIsSuperseded decides what a failed upload means for its event (#11148).
// A replay from an earlier offset re-emits creates for entries the filer has
// since deleted or rewritten; their chunks are gone, so the upload can never
// succeed, and failing the event pins the offset before it forever. Those are
// skipped. A failure to upload an entry the filer still holds as described is
// a real failure and must keep failing the event so the subscription retries.
//
// Every write stores its chunks under new file ids, so the entry is compared
// by chunk identity, the way the filer itself decides which chunks an update
// leaves for deletion. Content fingerprints would miss a delete-and-recreate
// of identical bytes and fail the event forever on its dead chunks.
//
// Chunks the filer has added since count too. The event was then a snapshot
// of a write in progress: a mount flushes metadata while chunk uploads are
// still running, so the snapshot can have holes, and the stream reader stops
// at the first one on every retry.
func TestIsSuperseded(t *testing.T) {
	const dir = "/buckets/ingest"
	event := entryWith("tmpjt9req69__Alan_Doe_Resume-1.pdf", nil, chunk("3,01", "e1"), chunk("3,02", "e2"))
	stamped := &filer_pb.RemoteEntry{StorageName: "b2", RemoteMtime: 1786096669}

	tests := []struct {
		name  string
		filer *stubFilerClient
		want  bool
	}{
		{name: "deleted since", filer: &stubFilerClient{}, want: true},
		{name: "rewritten since", filer: &stubFilerClient{entry: entryWith(event.Name, nil, chunk("3,01", "e1"), chunk("4,07", "e3"))}, want: true},
		{name: "recreated with identical content", filer: &stubFilerClient{entry: entryWith(event.Name, nil, chunk("5,11", "e1"), chunk("5,12", "e2"))}, want: true},
		{name: "recreated with other content", filer: &stubFilerClient{entry: entryWith(event.Name, nil, chunk("6,01", "e9"))}, want: true},
		{name: "truncated since", filer: &stubFilerClient{entry: entryWith(event.Name, nil, chunk("3,01", "e1"))}, want: true},
		{name: "still as described", filer: &stubFilerClient{entry: entryWith(event.Name, nil, chunk("3,01", "e1"), chunk("3,02", "e2"))}, want: false},
		{name: "still as described, since replicated", filer: &stubFilerClient{entry: entryWith(event.Name, stamped, chunk("3,01", "e1"), chunk("3,02", "e2"))}, want: false},
		{name: "appended since", filer: &stubFilerClient{entry: entryWith(event.Name, nil, chunk("3,01", "e1"), chunk("3,02", "e2"), chunk("3,03", "e3"))}, want: true},
		{name: "filer lookup failed", filer: &stubFilerClient{err: errors.New("rpc error: code = Unavailable")}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSuperseded(tt.filer, dir, event); got != tt.want {
				t.Errorf("isSuperseded = %v, want %v", got, tt.want)
			}
			if tt.filer.lookups != 1 {
				t.Errorf("looked up the filer %d times, want 1", tt.filer.lookups)
			}
		})
	}

	t.Run("written since: the event described an entry without data", func(t *testing.T) {
		remoteOnly := entryWith("video.mp4", &filer_pb.RemoteEntry{StorageName: "b2", RemoteSize: 20971520, RemoteMtime: 1786096669})
		if !isSuperseded(&stubFilerClient{entry: entryWith("video.mp4", remoteOnly.RemoteEntry, chunk("3,09", "e9"))}, dir, remoteOnly) {
			t.Error("isSuperseded = false, want true: the filer wrote local data to a remote-only entry")
		}
		if isSuperseded(&stubFilerClient{entry: entryWith("video.mp4", remoteOnly.RemoteEntry)}, dir, remoteOnly) {
			t.Error("isSuperseded = true, want false: the entry is still remote-only")
		}
		empty := &filer_pb.Entry{Name: "touch.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}}
		if !isSuperseded(&stubFilerClient{entry: &filer_pb.Entry{Name: "touch.txt", Content: []byte("now")}}, dir, empty) {
			t.Error("isSuperseded = false, want true: the empty file was written to")
		}
		if isSuperseded(&stubFilerClient{entry: &filer_pb.Entry{Name: "touch.txt"}}, dir, empty) {
			t.Error("isSuperseded = true, want false: the file is still empty")
		}
	})

	t.Run("mid-write snapshot with a hole, filled in since", func(t *testing.T) {
		snapshot := entryWith("IMG_6526.mov", nil, chunkAt("222,01", "e1", 0), chunkAt("220,03", "e3", 2048))
		filled := entryWith("IMG_6526.mov", nil, chunkAt("222,01", "e1", 0), chunkAt("216,02", "e2", 1024), chunkAt("220,03", "e3", 2048))
		if !isSuperseded(&stubFilerClient{entry: filled}, dir, snapshot) {
			t.Error("isSuperseded = false, want true: the filer holds the finished write, the snapshot can never be read past its hole")
		}
		if isSuperseded(&stubFilerClient{entry: filled}, dir, filled) {
			t.Error("isSuperseded = true, want false for the event that describes the finished write")
		}
	})

	t.Run("inline content rewritten since", func(t *testing.T) {
		event := &filer_pb.Entry{Name: "note.txt", Content: []byte("v1")}
		if !isSuperseded(&stubFilerClient{entry: &filer_pb.Entry{Name: "note.txt", Content: []byte("v2")}}, dir, event) {
			t.Error("isSuperseded = false, want true for an entry whose inline content has changed")
		}
		if isSuperseded(&stubFilerClient{entry: &filer_pb.Entry{Name: "note.txt", Content: []byte("v1")}}, dir, event) {
			t.Error("isSuperseded = true, want false for an entry whose inline content is unchanged")
		}
	})
}

// failingRemote fails every WriteFile with err and counts the attempts.
type failingRemote struct {
	remote_storage.RemoteStorageClient
	err    error
	writes int
}

func (r *failingRemote) WriteFile(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, io.Reader) (*filer_pb.RemoteEntry, error) {
	r.writes++
	return nil, r.err
}

// TestRetriedWriteFileStopsWhenSuperseded checks that a failed upload asks the
// filer at once, and stops retrying when the entry is gone: the ~13s of backoff
// util.Retry would spend on a dead chunk buys nothing. An upload of an entry the
// filer still holds keeps the retry policy it had.
func TestRetriedWriteFileStopsWhenSuperseded(t *testing.T) {
	const dir = "/buckets/ingest"
	event := entryWith("tmpjt9req69__Alan_Doe_Resume-1.pdf", nil, chunk("3,01", "e1"))
	dest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "tier", Path: "/" + event.Name}
	// What the S3 SDK reports when the volume server no longer has the chunk:
	// "requesterror" makes IsTransientError retry it to exhaustion.
	deadChunk := errors.New("RequestError: send request failed\ncaused by: Put \"https://s3.example.com/tier/x\": http://volume:8444/3,01?readDeleted=true: 404 Not Found: not found")

	t.Run("superseded before the first attempt, no write", func(t *testing.T) {
		remote := &failingRemote{err: deadChunk}
		filerClient := &stubFilerClient{}
		start := time.Now()
		_, err := retriedWriteFile(remote, filerClient, dir, event, dest)
		if !errors.Is(err, errSuperseded) {
			t.Errorf("err = %v, want errSuperseded", err)
		}
		if remote.writes != 0 {
			t.Errorf("wrote %d times, want 0: a superseded version is not worth uploading", remote.writes)
		}
		if filerClient.lookups != 1 {
			t.Errorf("looked up the filer %d times, want 1", filerClient.lookups)
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Errorf("took %v, want no backoff", elapsed)
		}
	})

	t.Run("superseded during the attempt, one write", func(t *testing.T) {
		remote := &failingRemote{err: deadChunk}
		filerClient := &supersedingFilerClient{live: entryWith(event.Name, nil, chunk("3,01", "e1"))}
		_, err := retriedWriteFile(remote, filerClient, dir, event, dest)
		if !errors.Is(err, errSuperseded) {
			t.Errorf("err = %v, want errSuperseded", err)
		}
		if !strings.Contains(err.Error(), "404 Not Found") {
			t.Errorf("err = %v, want it to keep the write failure", err)
		}
		if remote.writes != 1 {
			t.Errorf("wrote %d times, want 1: retrying a dead chunk cannot succeed", remote.writes)
		}
		if filerClient.lookups != 2 {
			t.Errorf("looked up the filer %d times, want one before the attempt and one after it failed", filerClient.lookups)
		}
	})

	t.Run("mid-write snapshot filled in during the attempt, one write", func(t *testing.T) {
		snapshot := entryWith("IMG_6526.mov", nil, chunkAt("222,01", "e1", 0), chunkAt("220,03", "e3", 2048))
		filled := entryWith("IMG_6526.mov", nil, chunkAt("222,01", "e1", 0), chunkAt("216,02", "e2", 1024), chunkAt("220,03", "e3", 2048))
		// What the S3 SDK reports when the stream reader stops at the hole.
		shortBody := errors.New("RequestError: send request failed\ncaused by: Put \"https://s3.example.com/tier/IMG_6526.mov?partNumber=12\": net/http: HTTP/1.x transport connection broken: http: ContentLength=8388608 with Body length 4194304")
		remote := &failingRemote{err: shortBody}
		filerClient := &supersedingFilerClient{live: snapshot, then: filled}
		_, err := retriedWriteFile(remote, filerClient, dir, snapshot, dest)
		if !errors.Is(err, errSuperseded) {
			t.Errorf("err = %v, want errSuperseded", err)
		}
		if remote.writes != 1 {
			t.Errorf("wrote %d times, want 1: the snapshot reads short on every retry", remote.writes)
		}
	})

	t.Run("still as described, transient failure keeps retrying", func(t *testing.T) {
		prev := util.RetryWaitTime
		util.RetryWaitTime = 1600 * time.Millisecond // two attempts: 1s, then 1.5s
		t.Cleanup(func() { util.RetryWaitTime = prev })
		remote := &failingRemote{err: deadChunk}
		filerClient := &stubFilerClient{entry: entryWith(event.Name, nil, chunk("3,01", "e1"))}
		_, err := retriedWriteFile(remote, filerClient, dir, event, dest)
		if errors.Is(err, errSuperseded) {
			t.Errorf("err = %v, want the write failure itself", err)
		}
		if remote.writes != 2 {
			t.Errorf("wrote %d times, want 2: a transient failure on a live entry is still retried", remote.writes)
		}
		if filerClient.lookups != 3 {
			t.Errorf("looked up the filer %d times, want one before the first attempt and one per failed attempt", filerClient.lookups)
		}
	})

	t.Run("still as described, permanent failure not retried", func(t *testing.T) {
		remote := &failingRemote{err: errors.New("AccessDenied: Access Denied")}
		filerClient := &stubFilerClient{entry: entryWith(event.Name, nil, chunk("3,01", "e1"))}
		_, err := retriedWriteFile(remote, filerClient, dir, event, dest)
		if err == nil || errors.Is(err, errSuperseded) {
			t.Errorf("err = %v, want the write failure itself", err)
		}
		if remote.writes != 1 {
			t.Errorf("wrote %d times, want 1", remote.writes)
		}
	})
}

type recordingRemote struct {
	remote_storage.RemoteStorageClient
	deletes         []*remote_pb.RemoteStorageLocation
	writes          []*remote_pb.RemoteStorageLocation
	metadataUpdates []*remote_pb.RemoteStorageLocation
	written         [][]byte
	// chunk ids of each written entry: which version of the file went up
	writtenChunks [][]string
	deleteErr     error
	writeErr      error
	// objects present on the remote, by path: StatFile and ReadFile answer
	// from it, everything else is ErrRemoteObjectNotFound.
	objects map[string][]byte
	// statETag is the ETag StatFile reports for a present object, unless
	// statETags names one for its path.
	statETag  string
	statETags map[string]string
	statMtime int64
	// metadataUpdateResult is the RemoteEntry UpdateFileMetadata reports; nil
	// means the update left the object untouched
	metadataUpdateResult *filer_pb.RemoteEntry
}

func (r *recordingRemote) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (*filer_pb.RemoteEntry, error) {
	if r.writeErr != nil {
		return nil, r.writeErr
	}
	r.writes = append(r.writes, loc)
	var ids []string
	for _, c := range entry.GetChunks() {
		ids = append(ids, c.GetFileIdString())
	}
	r.writtenChunks = append(r.writtenChunks, ids)
	// A chunked upload's reader walks volume servers the stub filer cannot
	// name, so it is not read; the copy path hands over the old object's
	// stream for a chunkless entry, which is drained.
	var body []byte
	if rc, ok := reader.(io.ReadCloser); ok && len(entry.GetChunks()) == 0 && r.objects != nil {
		body, _ = io.ReadAll(rc)
	}
	r.written = append(r.written, body)
	return &filer_pb.RemoteEntry{StorageName: loc.Name, RemoteETag: "etag", RemoteSize: int64(len(entry.Content)), RemoteMtime: entry.Attributes.GetMtime()}, nil
}

func (r *recordingRemote) StatFile(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	body, ok := r.objects[loc.Path]
	if !ok {
		return nil, remote_storage.ErrRemoteObjectNotFound
	}
	etag := r.statETag
	if e, ok := r.statETags[loc.Path]; ok {
		etag = e
	}
	return &filer_pb.RemoteEntry{StorageName: loc.Name, RemoteSize: int64(len(body)), RemoteETag: etag, RemoteMtime: r.statMtime}, nil
}

func (r *recordingRemote) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) ([]byte, error) {
	body, ok := r.objects[loc.Path]
	if !ok {
		return nil, remote_storage.ErrRemoteObjectNotFound
	}
	return body[offset : offset+size], nil
}

func (r *recordingRemote) DeleteFile(loc *remote_pb.RemoteStorageLocation) error {
	r.deletes = append(r.deletes, loc)
	return r.deleteErr
}

func (r *recordingRemote) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry, newEntry *filer_pb.Entry) (*filer_pb.RemoteEntry, error) {
	r.metadataUpdates = append(r.metadataUpdates, loc)
	return r.metadataUpdateResult, nil
}

// TestRenameWithInheritedRemoteEntryWritesNewKey reproduces #11261: a rename
// arrives as an update whose NewEntry carries the RemoteEntry inherited from
// the source. shouldSendToRemote returns false for it (the sync stamp covers
// the entry's mtime), so the old code skipped the event and never wrote the
// new key, while the filer had already deleted the old object. A path change
// must always write.
func TestRenameWithInheritedRemoteEntryWritesNewKey(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}

	replicated := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 2048, RemoteMtime: 1786096669, LastLocalSyncTsNs: 1786096669*1e9 + 1}
	oldEntry := &filer_pb.Entry{Name: "probe.bin", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}, RemoteEntry: replicated}
	newEntry := &filer_pb.Entry{
		Name:        "probe.bin",
		Content:     []byte("payload"),
		Attributes:  &filer_pb.FuseAttributes{Mtime: 1786096669},
		RemoteEntry: replicated,
	}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      oldEntry,
			NewParentPath: "/buckets/b/dst",
			NewEntry:      newEntry,
		},
	}

	if shouldSendToRemote(newEntry) {
		t.Fatal("precondition: inherited RemoteEntry should make shouldSendToRemote false, the bug's trigger")
	}

	remote := &recordingRemote{}
	// the filer still holds the renamed entry as the event described it
	filerClient := &stubFilerClient{entry: newEntry}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
		t.Fatal(err)
	}

	wantDelete := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/src/probe.bin"}
	if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
		t.Errorf("deletes = %+v, want the old key %s deleted", remote.deletes, remote_storage.FormatLocation(wantDelete))
	}
	wantWrite := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/dst/probe.bin"}
	if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) {
		t.Errorf("writes = %+v, want the new key %s written", remote.writes, remote_storage.FormatLocation(wantWrite))
	}
}

// TestRenameRemoteOnlyEntrySkipsEmptyUpload guards the edge case from the
// review of #11261: a remote-only entry (no local chunks, content lives only
// on the remote object the filer already deleted) must not be re-uploaded,
// since NewFileReader would supply EOF and create a zero-byte object.
func TestRenameRemoteOnlyEntrySkipsEmptyUpload(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}

	remoteOnly := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 20971520, RemoteMtime: 1786096669}
	oldEntry := &filer_pb.Entry{Name: "video.mp4", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}, RemoteEntry: remoteOnly}
	newEntry := &filer_pb.Entry{
		Name:        "video.mp4",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 1786096669},
		RemoteEntry: remoteOnly,
	}
	if !newEntry.IsInRemoteOnly() {
		t.Fatal("precondition: entry should be remote-only")
	}
	if filer.HasData(newEntry) {
		t.Fatal("precondition: remote-only entry should have no local data")
	}

	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      oldEntry,
			NewParentPath: "/buckets/b/dst",
			NewEntry:      newEntry,
		},
	}

	remote := &recordingRemote{}
	filerClient := &stubFilerClient{}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
		t.Fatal(err)
	}
	if len(remote.writes) != 0 {
		t.Errorf("writes = %+v, want none: a remote-only rename must not upload an empty object", remote.writes)
	}
}

// TestRenameDeleteOldKeyFailureReturnsError checks that a failed delete of the
// old key on a rename is returned so MetadataProcessor retries the event,
// instead of silently continuing and leaving both keys on the remote.
func TestRenameDeleteOldKeyFailureReturnsError(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}

	newEntry := &filer_pb.Entry{
		Name:       "probe.bin",
		Content:    []byte("payload"),
		Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669},
	}
	oldEntry := &filer_pb.Entry{Name: "probe.bin", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      oldEntry,
			NewParentPath: "/buckets/b/dst",
			NewEntry:      newEntry,
		},
	}

	deleteErr := errors.New("AccessDenied: Access Denied")
	remote := &recordingRemote{deleteErr: deleteErr}
	filerClient := &stubFilerClient{}
	err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp)
	if !errors.Is(err, deleteErr) {
		t.Errorf("err = %v, want the delete failure returned so the event is retried", err)
	}
	if len(remote.writes) != 0 {
		t.Errorf("writes = %+v, want none: must not write the new key when the old key delete failed", remote.writes)
	}
}

// TestRenameDeleteOldKeyNotFoundStillWrites checks that an already-deleted old
// key (the filer deletes the source object synchronously during rename) does
// not block the destination write. GCS reports a missing object as
// ErrRemoteObjectNotFound, unlike S3/Azure whose deletes are idempotent, so
// treating it as a real error would pin the sync offset and never write the
// new key.
func TestRenameDeleteOldKeyNotFoundStillWrites(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/"}

	newEntry := &filer_pb.Entry{
		Name:       "probe.bin",
		Content:    []byte("payload"),
		Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669},
	}
	oldEntry := &filer_pb.Entry{Name: "probe.bin", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      oldEntry,
			NewParentPath: "/buckets/b/dst",
			NewEntry:      newEntry,
		},
	}

	remote := &recordingRemote{deleteErr: remote_storage.ErrRemoteObjectNotFound}
	// the filer still holds the renamed entry as the event described it
	filerClient := &stubFilerClient{entry: newEntry}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
		t.Fatalf("err = %v, want nil: an already-deleted old key must not block the write", err)
	}
	wantWrite := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/b/dst/probe.bin"}
	if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) {
		t.Errorf("writes = %+v, want the new key %s written", remote.writes, remote_storage.FormatLocation(wantWrite))
	}
}

func TestIfEntryEqualCarriesTheEventEntry(t *testing.T) {
	entry := &filer_pb.Entry{
		Name:    "f",
		Content: []byte("inline"),
		Chunks: []*filer_pb.FileChunk{
			{FileId: "3,01a"},
			{Fid: &filer_pb.FileId{VolumeId: 4, FileKey: 0x2b, Cookie: 0x0c}},
		},
	}
	cond := ifEntryEqual(entry)
	if len(cond.Clauses) != 1 || cond.Clauses[0].Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL {
		t.Fatalf("condition = %v, want one IF_ENTRY_EQUAL clause", cond)
	}
	if got := cond.Clauses[0].ExpectedEntry; !proto.Equal(got, entry) {
		t.Fatalf("expected_entry = %v, want %v", got, entry)
	}
}

// The remote-bound entry loses (or gains) the storage class attribute while
// the event entry keeps it, so IF_ENTRY_EQUAL still sees the entry the filer
// stored — otherwise every stamp on an S3-written object reads as stale.
func TestRemoteWriteEntryLeavesEventEntryUntouched(t *testing.T) {
	entry := &filer_pb.Entry{
		Name:       "f",
		Attributes: &filer_pb.FuseAttributes{Mtime: 1},
		Extended:   map[string][]byte{s3_constants.AmzStorageClass: []byte("STANDARD")},
	}

	stripped := remoteWriteEntry(entry, "")
	if _, ok := stripped.Extended[s3_constants.AmzStorageClass]; ok {
		t.Fatalf("remote entry still carries %s", s3_constants.AmzStorageClass)
	}
	if string(entry.Extended[s3_constants.AmzStorageClass]) != "STANDARD" {
		t.Fatalf("event entry Extended mutated: %v", entry.Extended)
	}

	overridden := remoteWriteEntry(entry, "GLACIER")
	if got := string(overridden.Extended[s3_constants.AmzStorageClass]); got != "GLACIER" {
		t.Fatalf("override = %q, want GLACIER", got)
	}
	if string(entry.Extended[s3_constants.AmzStorageClass]) != "STANDARD" {
		t.Fatalf("event entry Extended mutated: %v", entry.Extended)
	}

	bare := &filer_pb.Entry{Name: "g", Attributes: &filer_pb.FuseAttributes{Mtime: 1}}
	if got := string(remoteWriteEntry(bare, "GLACIER").Extended[s3_constants.AmzStorageClass]); got != "GLACIER" {
		t.Fatalf("override on nil Extended = %q, want GLACIER", got)
	}
}

func TestIsFailedPrecondition(t *testing.T) {
	refused := status.Error(codes.FailedPrecondition, "precondition failed: /buckets/b/f")
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"status", refused, true},
		{"wrapped status", fmt.Errorf("update entry: %w", refused), true},
		{"message only", errors.New("rpc error: code = FailedPrecondition desc = precondition failed: /f"), false},
		{"other", status.Error(codes.Unavailable, "filer down"), false},
	}
	for _, c := range cases {
		if got := isFailedPrecondition(c.err); got != c.want {
			t.Errorf("%s: isFailedPrecondition = %v, want %v", c.name, got, c.want)
		}
	}
}

// supersedingFilerClient answers the first lookup with the live entry and
// every later one with "not found": the entry is deleted while the upload is
// in flight.
type supersedingFilerClient struct {
	filer_pb.SeaweedFilerClient
	live    *filer_pb.Entry
	then    *filer_pb.Entry // returned after the first lookup; nil means deleted
	lookups int
}

func (c *supersedingFilerClient) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest, ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.lookups++
	if c.lookups == 1 {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: c.live}, nil
	}
	if c.then != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: c.then}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (c *supersedingFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c)
}

func (c *supersedingFilerClient) AdjustedUrl(location *filer_pb.Location) string { return location.Url }

func (c *supersedingFilerClient) GetDataCenter() string { return "" }

// TestDeleteEventAbsentRemoteObjectIsSuccess: an entry created and deleted
// before the sync uploaded it has no remote object. GCS reports the delete as
// ErrRemoteObjectNotFound; the event has nothing left to do, so it must not
// fail (a failed event pins the sync offset and is replayed on every restart).
func TestDeleteEventAbsentRemoteObjectIsSuccess(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/"}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/vol",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "state.db.tmp", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}},
			DeleteChunks: true,
		},
	}
	wantDelete := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/b/vol/state.db.tmp"}

	t.Run("absent object", func(t *testing.T) {
		remote := &recordingRemote{deleteErr: remote_storage.ErrRemoteObjectNotFound}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil: deleting an absent object is complete", err)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
			t.Errorf("deletes = %+v, want %s", remote.deletes, remote_storage.FormatLocation(wantDelete))
		}
	})

	t.Run("metadata-only delete keeps the remote object", func(t *testing.T) {
		marked := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		marked.EventNotification.OldEntry.Extended = map[string][]byte{filer.ExtKeepRemoteObjectKey: []byte("true")}
		remote := &recordingRemote{}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, marked); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none", remote.deletes)
		}
	})

	t.Run("a remote-only entry that took the path keeps the object", func(t *testing.T) {
		// deleted, then a remote-only file renamed onto the path: the rename
		// copied its content to this key before the sync got to the delete
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: &filer_pb.Entry{
			Name:        "state.db.tmp",
			Attributes:  &filer_pb.FuseAttributes{FileSize: 10},
			RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 10},
		}}
		if err := processDeleteEvent(filerClient, remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none", remote.deletes)
		}
	})

	t.Run("a replaced object is kept", func(t *testing.T) {
		// the delete recorded the old object's etag; a rename copied another
		// entry's content onto the key since, and it must not go with it
		deleted := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		deleted.EventNotification.OldEntry.RemoteEntry = &filer_pb.RemoteEntry{RemoteETag: `"orig"`, RemoteSize: 10}
		remote := &recordingRemote{objects: map[string][]byte{"/b/vol/state.db.tmp": []byte("copied")}, statETag: `"copy"`}
		filerClient := &stubFilerClient{entry: &filer_pb.Entry{Name: "state.db.tmp", Content: []byte("copied")}}
		if err := processDeleteEvent(filerClient, remote, mountedDir, mountLoc, deleted); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none", remote.deletes)
		}
	})

	t.Run("a changed object with no entry at the path yet is kept", func(t *testing.T) {
		// a directory rename copies a child onto the key before its new entry
		// becomes visible; the copy may be the content's only remaining home
		deleted := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		deleted.EventNotification.OldEntry.RemoteEntry = &filer_pb.RemoteEntry{RemoteETag: `"orig"`, RemoteSize: 10}
		remote := &recordingRemote{objects: map[string][]byte{"/b/vol/state.db.tmp": []byte("copied")}, statETag: `"copy"`}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, deleted); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none", remote.deletes)
		}
	})

	t.Run("an unchanged object still goes", func(t *testing.T) {
		deleted := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		deleted.EventNotification.OldEntry.RemoteEntry = &filer_pb.RemoteEntry{RemoteETag: `"orig"`, RemoteSize: 3}
		remote := &recordingRemote{objects: map[string][]byte{"/b/vol/state.db.tmp": []byte("old")}, statETag: `"orig"`}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, deleted); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 1 {
			t.Errorf("deletes = %+v, want one", remote.deletes)
		}
	})

	t.Run("a copy that landed before its entry is kept", func(t *testing.T) {
		// the deleted entry was never replicated, so its event records no
		// stamp to match the fresh rename copy waiting for its filer entry
		remote := &recordingRemote{objects: map[string][]byte{"/b/vol/state.db.tmp": []byte("copied")}, statETag: `"copy"`}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none", remote.deletes)
		}
	})

	t.Run("an unverifiable object is kept", func(t *testing.T) {
		// the delete recorded an etag the live stat does not report; the
		// object cannot be confirmed as the one the delete meant
		deleted := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		deleted.EventNotification.OldEntry.RemoteEntry = &filer_pb.RemoteEntry{RemoteETag: `"orig"`, RemoteSize: 3}
		remote := &recordingRemote{objects: map[string][]byte{"/b/vol/state.db.tmp": []byte("old")}}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, deleted); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none", remote.deletes)
		}
	})

	t.Run("an entry with local data at the path does not stop the delete", func(t *testing.T) {
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: &filer_pb.Entry{
			Name:        "state.db.tmp",
			Content:     []byte("local"),
			RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 10},
		}}
		if err := processDeleteEvent(filerClient, remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(remote.deletes) != 1 {
			t.Errorf("deletes = %+v, want one", remote.deletes)
		}
	})

	t.Run("other failure still fails", func(t *testing.T) {
		remote := &recordingRemote{deleteErr: errors.New("AccessDenied: Access Denied")}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, resp); err == nil {
			t.Fatal("err = nil, want the delete failure")
		}
	})

	t.Run("delete marker, absent object", func(t *testing.T) {
		remote := &recordingRemote{deleteErr: remote_storage.ErrRemoteObjectNotFound}
		filerClient := &stubFilerClient{}
		markerDelete := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/b/vol/state.db"}
		message := &filer_pb.EventNotification{
			NewParentPath: "/buckets/b/vol",
			NewEntry:      &filer_pb.Entry{Name: "state.db", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}},
		}
		if err := syncDeleteMarker(remote, filerClient, message, markerDelete); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], markerDelete) {
			t.Errorf("deletes = %+v, want %s", remote.deletes, remote_storage.FormatLocation(markerDelete))
		}
		if filerClient.updates != 1 {
			t.Errorf("stamped %d times, want 1: the marker is recorded locally so a replay is a no-op", filerClient.updates)
		}
	})
}

// TestUpdateLocalEntrySkipsDeletedEntry: the stamp that follows an upload lands
// on an entry a later event already deleted. The filer answers "not found"; the
// delete's own event follows in the log, so the stamp is skipped rather than
// failed.
func TestUpdateLocalEntrySkipsDeletedEntry(t *testing.T) {
	const dir = "/buckets/b/vol"
	remoteEntry := &filer_pb.RemoteEntry{StorageName: "gcs", RemoteSize: 7}

	t.Run("plain not found from the filer", func(t *testing.T) {
		filerClient := &stubFilerClient{updateErr: fmt.Errorf("not found %s: %w", dir+"/state.db", filer_pb.ErrNotFound)}
		if err := updateLocalEntry(filerClient, dir, entryWith("state.db", nil, chunk("3,01", "e1")), remoteEntry); err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	})

	t.Run("not found over grpc", func(t *testing.T) {
		filerClient := &stubFilerClient{updateErr: status.Error(codes.Unknown, "not found "+dir+"/state.db: "+filer_pb.ErrNotFound.Error())}
		if err := updateLocalEntry(filerClient, dir, entryWith("state.db", nil, chunk("3,01", "e1")), remoteEntry); err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	})

	t.Run("typed not found from the filer", func(t *testing.T) {
		filerClient := &stubFilerClient{updateErr: status.Errorf(codes.NotFound, "not found %s/state.db: %v", dir, filer_pb.ErrNotFound)}
		if err := updateLocalEntry(filerClient, dir, entryWith("state.db", nil, chunk("3,01", "e1")), remoteEntry); err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	})

	t.Run("the sentinel inside the path is not a missing entry", func(t *testing.T) {
		name := filer_pb.ErrNotFound.Error()
		filerClient := &stubFilerClient{updateErr: status.Error(codes.Unknown, "not found "+dir+"/"+name+": database unavailable")}
		if err := updateLocalEntry(filerClient, dir, entryWith(name, nil, chunk("3,01", "e1")), remoteEntry); err == nil {
			t.Error("err = nil, want the store failure: the stamp must be retried, not dropped")
		}
	})

	t.Run("other failure still fails", func(t *testing.T) {
		filerClient := &stubFilerClient{updateErr: status.Error(codes.Unavailable, "filer is shutting down")}
		if err := updateLocalEntry(filerClient, dir, entryWith("state.db", nil, chunk("3,01", "e1")), remoteEntry); err == nil {
			t.Error("err = nil, want the update failure")
		}
	})
}

// TestSupersededRenameUploadsCurrentEntry: a rename A -> B queued behind a
// rewrite of B. The rename's snapshot is superseded (B's chunks changed) and
// the old key A is already deleted. The rewrite's own event uploads B unless
// the entry already reads synced (the stamp covers its mtime, or the remote
// is newer); only then does the rename upload what the filer holds now, so
// the content goes up exactly once.
func TestSupersededRenameUploadsCurrentEntry(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/"}
	eventEntry := entryWith("b.txt", nil, chunk("3,01", "e1"))
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      entryWith("a.txt", nil, chunk("3,01", "e1")),
			NewParentPath: "/buckets/b/dir",
			NewEntry:      eventEntry,
		},
	}
	wantDelete := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/b/dir/a.txt"}
	wantWrite := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/b/dir/b.txt"}

	t.Run("rewrite already synced: upload once here", func(t *testing.T) {
		// The stamp covers the rewrite's mtime, so its own event skips.
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 1024, LastLocalSyncTsNs: 1786096669*1e9 + 1}, chunk("3,02", "e2"))
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
			t.Errorf("deletes = %+v, want the old key %s deleted", remote.deletes, remote_storage.FormatLocation(wantDelete))
		}
		if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) {
			t.Fatalf("writes = %+v, want the new key %s written once with the current entry", remote.writes, remote_storage.FormatLocation(wantWrite))
		}
		if filerClient.updates != 1 {
			t.Errorf("stamped %d times, want 1: the current entry is stamped", filerClient.updates)
		}
	})

	t.Run("rewrite newer than the stamp: its own event uploads, not this one", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096000, RemoteSize: 1024}, chunk("3,02", "e2"))
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.deletes) != 1 {
			t.Errorf("deletes = %+v, want the old key deleted", remote.deletes)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none: the rewrite event behind this one uploads b.txt", remote.writes)
		}
	})

	t.Run("deleted meanwhile: nothing to upload", func(t *testing.T) {
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none: the delete event follows", remote.writes)
		}
	})

	t.Run("uncached meanwhile, old object present: copied to the destination, stamped, old key deleted", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7})
		remote := &recordingRemote{objects: map[string][]byte{"/b/dir/a.txt": []byte("payload")}}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) || string(remote.written[0]) != "payload" {
			t.Fatalf("writes = %+v (%q), want the old object's bytes written at %s", remote.writes, remote.written, remote_storage.FormatLocation(wantWrite))
		}
		if filerClient.updates != 1 {
			t.Errorf("stamped %d times, want 1: the entry now points at the destination object", filerClient.updates)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
			t.Errorf("deletes = %+v, want the old key deleted after the copy", remote.deletes)
		}
	})

	t.Run("uncached after an earlier run completed the rename: destination present, old key deleted, nothing written", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7})
		remote := &recordingRemote{objects: map[string][]byte{"/b/dir/a.txt": []byte("payload"), "/b/dir/b.txt": []byte("payload")}}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil: the replayed rename is already complete", err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none", remote.writes)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
			t.Errorf("deletes = %+v, want the old key deleted", remote.deletes)
		}
	})

	t.Run("uncached and neither object exists: the event fails and holds the offset", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7})
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: current}
		err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp)
		if err == nil || !strings.Contains(err.Error(), "neither") {
			t.Fatalf("err = %v, want the lost-content failure", err)
		}
		if len(remote.writes) != 0 || len(remote.deletes) != 0 {
			t.Errorf("writes = %+v deletes = %+v, want none", remote.writes, remote.deletes)
		}
	})

	t.Run("destination holds an object of another size: replaced from the old key", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7})
		remote := &recordingRemote{objects: map[string][]byte{"/b/dir/a.txt": []byte("payload"), "/b/dir/b.txt": []byte("something else")}}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.writes) != 1 || string(remote.written[0]) != "payload" {
			t.Fatalf("writes = %+v (%q), want the old object copied over the destination", remote.writes, remote.written)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
			t.Errorf("deletes = %+v, want the old key deleted after the copy", remote.deletes)
		}
	})

	t.Run("snapshot remote-only but rewritten since: the rewrite's own event uploads", func(t *testing.T) {
		remoteOnly := &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7}
		snapshot := &filer_pb.SubscribeMetadataResponse{
			Directory: "/buckets/b/dir",
			EventNotification: &filer_pb.EventNotification{
				OldEntry:      entryWith("a.txt", remoteOnly),
				NewParentPath: "/buckets/b/dir",
				NewEntry:      entryWith("b.txt", remoteOnly),
			},
		}
		// rewritten since the stamp; its own event uploads, not this one
		rewritten := proto.Clone(remoteOnly).(*filer_pb.RemoteEntry)
		rewritten.LastLocalSyncTsNs = 1
		current := entryWith("b.txt", rewritten, chunk("3,09", "e9"))
		remote := &recordingRemote{objects: map[string][]byte{"/b/dir/a.txt": []byte("payload")}}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, snapshot); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none: the rewrite event behind this one uploads b.txt", remote.writes)
		}
		if len(remote.deletes) != 1 {
			t.Errorf("deletes = %+v, want the old key deleted", remote.deletes)
		}
	})

	t.Run("snapshot already remote-only: completed the same way", func(t *testing.T) {
		remoteOnly := &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7}
		snapshot := &filer_pb.SubscribeMetadataResponse{
			Directory: "/buckets/b/dir",
			EventNotification: &filer_pb.EventNotification{
				OldEntry:      entryWith("a.txt", remoteOnly),
				NewParentPath: "/buckets/b/dir",
				NewEntry:      entryWith("b.txt", remoteOnly),
			},
		}
		remote := &recordingRemote{objects: map[string][]byte{"/b/dir/a.txt": []byte("payload")}}
		filerClient := &stubFilerClient{entry: entryWith("b.txt", remoteOnly)}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, snapshot); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(remote.writes) != 1 || string(remote.written[0]) != "payload" {
			t.Errorf("writes = %+v (%q), want the old object copied to the destination", remote.writes, remote.written)
		}
		if len(remote.deletes) != 1 {
			t.Errorf("deletes = %+v, want the old key deleted after the copy", remote.deletes)
		}
	})

	t.Run("uncached after the old key was deleted: the event fails and holds the offset", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 1024})
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: current}
		err := uploadCurrentEntry(filerClient, filerClient, remote, "/buckets/b/dir", "b.txt", renameSource{loc: wantDelete}, wantWrite, "", eventSource)
		if err == nil || !strings.Contains(err.Error(), "neither") {
			t.Errorf("err = %v, want the lost-content failure", err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none", remote.writes)
		}
	})

	t.Run("uncached with the destination already synced: complete without a write", func(t *testing.T) {
		current := entryWith("b.txt", &filer_pb.RemoteEntry{StorageName: "gcs", RemoteMtime: 1786096669, RemoteSize: 7})
		remote := &recordingRemote{objects: map[string][]byte{"/b/dir/a.txt": []byte("payload"), "/b/dir/b.txt": []byte("payload")}}
		filerClient := &stubFilerClient{entry: current}
		if err := uploadCurrentEntry(filerClient, filerClient, remote, "/buckets/b/dir", "b.txt", renameSource{loc: wantDelete}, wantWrite, "", eventSource); err != nil {
			t.Fatalf("err = %v, want nil: the destination object matches the entry's stamp", err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none", remote.writes)
		}
		if len(remote.deletes) != 1 || !proto.Equal(remote.deletes[0], wantDelete) {
			t.Errorf("deletes = %+v, want the old key deleted", remote.deletes)
		}
	})
}

func TestCompleteRemoteOnlyRenameClearsThePendingSourceFirst(t *testing.T) {
	oldDest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/src/a.jpg"}
	dest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/dst/a.jpg"}
	pending := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:        "a.jpg",
			Attributes:  &filer_pb.FuseAttributes{FileSize: 4},
			RemoteEntry: &filer_pb.RemoteEntry{RemoteETag: `"copy"`, RemoteSize: 4},
			Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/buckets/b/src/a.jpg")},
		}
	}
	objects := func() map[string][]byte {
		return map[string][]byte{"/b/src/a.jpg": []byte("data"), "/b/dst/a.jpg": []byte("data")}
	}

	t.Run("stamped, then the source goes", func(t *testing.T) {
		// the destination already holds the copy; the entry still reads the
		// source until its stamp clears the marker
		current := pending()
		filerClient := &stubFilerClient{entry: current}
		remote := &recordingRemote{objects: objects(), statETag: `"copy"`}
		if err := completeRemoteOnlyRename(filerClient, remote, "/buckets/b/dst", current, renameSource{path: "/buckets/b/src/a.jpg", loc: oldDest}, dest, ""); err != nil {
			t.Fatalf("err = %v", err)
		}
		if filerClient.updates != 1 {
			t.Errorf("updates = %d, want the stamp that clears the marker", filerClient.updates)
		}
		if len(remote.deletes) != 1 || remote.deletes[0].Path != "/b/src/a.jpg" {
			t.Errorf("deletes = %+v, want the source", remote.deletes)
		}
	})

	t.Run("a marker still in place keeps the source", func(t *testing.T) {
		// the stamp did not land: the filer still has the marker
		filerClient := &stubFilerClient{entry: pending()}
		remote := &recordingRemote{objects: objects(), statETag: `"copy"`}
		if err := completeRemoteOnlyRename(filerClient, remote, "/buckets/b/dst", pending(), renameSource{path: "/buckets/b/src/a.jpg", loc: oldDest}, dest, ""); err == nil {
			t.Fatal("err = nil, want the event retried")
		}
		if len(remote.deletes) != 0 {
			t.Errorf("deletes = %+v, want none while the entry reads the source", remote.deletes)
		}
	})
}

// eventSource resolves a rename's source to the event's old key.
func eventSource(_ *filer_pb.Entry, source renameSource) renameSource {
	return source
}

func TestRenameOfAPendingEntryFollowsItsSource(t *testing.T) {
	// A was renamed over B and B over C, both copies failing: C reads A's
	// object through the marker, and B's key holds the old target's bytes
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}
	remoteOnly := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: `"a"`, RemoteSize: 6}
	pendingC := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:        "c.jpg",
			Attributes:  &filer_pb.FuseAttributes{FileSize: 6},
			RemoteEntry: remoteOnly,
			Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/buckets/b/a.jpg")},
		}
	}
	event := func(oldName, newName string) *filer_pb.SubscribeMetadataResponse {
		newEntry := pendingC()
		newEntry.Name = newName
		return &filer_pb.SubscribeMetadataResponse{
			Directory: "/buckets/b",
			EventNotification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: oldName, RemoteEntry: remoteOnly},
				NewParentPath: "/buckets/b",
				NewEntry:      newEntry,
			},
		}
	}
	objects := func() map[string][]byte {
		return map[string][]byte{"/b/a.jpg": []byte("data-a"), "/b/b.jpg": []byte("old-b"), "/b/c.jpg": []byte("old-c")}
	}

	t.Run("the first rename leaves the source to the entry that moved on", func(t *testing.T) {
		remote := &recordingRemote{objects: objects()}
		filerClient := &stubFilerClient{} // nothing at b.jpg any more
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, event("a.jpg", "b.jpg")); err != nil {
			t.Fatal(err)
		}
		if len(remote.deletes) != 0 || len(remote.writes) != 0 {
			t.Errorf("deletes = %+v, writes = %+v, want neither: c.jpg still reads a.jpg", remote.deletes, remote.writes)
		}
	})

	t.Run("the second rename copies from the marker's source", func(t *testing.T) {
		current := pendingC()
		remote := &recordingRemote{objects: objects(), statETag: `"a"`}
		filerClient := &stubFilerClient{entry: current}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, event("b.jpg", "c.jpg")); err != nil {
			t.Fatal(err)
		}
		if len(remote.written) != 1 || string(remote.written[0]) != "data-a" {
			t.Errorf("written = %q, want a.jpg's content", remote.written)
		}
		if len(remote.deletes) != 1 || remote.deletes[0].Path != "/b/a.jpg" {
			t.Errorf("deletes = %+v, want the source once the stamp cleared the marker", remote.deletes)
		}
	})
}

func TestPendingRenameSourceIsReleasedOnlyWhileUnchanged(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}
	// b.jpg's copy failed after it replaced a target: it read a.jpg's object
	pendingB := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:        "b.jpg",
			RemoteEntry: &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: `"a"`, RemoteSize: 6},
			Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/buckets/b/a.jpg")},
		}
	}
	deleted := func() *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{Directory: "/buckets/b", EventNotification: &filer_pb.EventNotification{OldEntry: pendingB()}}
	}
	deletedPaths := func(r *recordingRemote) (paths []string) {
		for _, d := range r.deletes {
			paths = append(paths, d.Path)
		}
		return
	}

	t.Run("deleting the pending entry releases its source", func(t *testing.T) {
		remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-a")}, statETag: `"a"`}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, deleted()); err != nil {
			t.Fatal(err)
		}
		if got := deletedPaths(remote); !slices.Contains(got, "/b/a.jpg") {
			t.Errorf("deletes = %v, want the source", got)
		}
	})

	t.Run("a source replaced since is kept", func(t *testing.T) {
		// another rename copied onto a.jpg before its entry became visible
		remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-d")}, statETag: `"d"`}
		if err := processDeleteEvent(&stubFilerClient{}, remote, mountedDir, mountLoc, deleted()); err != nil {
			t.Fatal(err)
		}
		if got := deletedPaths(remote); slices.Contains(got, "/b/a.jpg") {
			t.Errorf("deletes = %v, want the replaced source kept", got)
		}
	})

	t.Run("a destination cached since releases the source too", func(t *testing.T) {
		// c.jpg was cached before the sync reached the rename: it is no
		// longer remote-only, and a.jpg is still left over
		cached := &filer_pb.Entry{Name: "c.jpg", Content: []byte("data-a"), Attributes: &filer_pb.FuseAttributes{FileSize: 6}, RemoteEntry: &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: `"a"`, RemoteSize: 6}}
		remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-a"), "/b/b.jpg": []byte("old-b"), "/b/c.jpg": []byte("data-a")}, statETag: `"a"`}
		event := &filer_pb.SubscribeMetadataResponse{Directory: "/buckets/b", EventNotification: &filer_pb.EventNotification{
			OldEntry: pendingB(), NewParentPath: "/buckets/b", NewEntry: cached,
		}}
		filerClient := &pathFilerClient{entries: map[string]*filer_pb.Entry{"/buckets/b/c.jpg": cached}}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, event); err != nil {
			t.Fatal(err)
		}
		if got := deletedPaths(remote); !slices.Contains(got, "/b/a.jpg") {
			t.Errorf("deletes = %v, want the pending source released", got)
		}
	})

	t.Run("a cached entry still reading the source keeps it until its upload lands", func(t *testing.T) {
		// c.jpg was cached through its marker; until the upload is stamped
		// and the marker cleared, a.jpg is what remote.uncache would fall
		// back to
		marked := func() *filer_pb.Entry {
			return &filer_pb.Entry{Name: "c.jpg", Content: []byte("data-a"), Attributes: &filer_pb.FuseAttributes{FileSize: 6},
				RemoteEntry: &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: `"a"`, RemoteSize: 6},
				Extended:    map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte("/buckets/b/a.jpg")}}
		}
		for name, writeErr := range map[string]error{"upload fails": errors.New("upload failed"), "stamp does not land": nil} {
			t.Run(name, func(t *testing.T) {
				remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-a"), "/b/b.jpg": []byte("old-b")}, statETag: `"a"`, writeErr: writeErr}
				event := &filer_pb.SubscribeMetadataResponse{Directory: "/buckets/b", EventNotification: &filer_pb.EventNotification{
					OldEntry: pendingB(), NewParentPath: "/buckets/b", NewEntry: marked(),
				}}
				// the filer keeps the marker: the stub does not store updates
				filerClient := &pathFilerClient{entries: map[string]*filer_pb.Entry{"/buckets/b/c.jpg": marked()}}
				_ = processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, event)
				if got := deletedPaths(remote); slices.Contains(got, "/b/a.jpg") {
					t.Errorf("deletes = %v, want the source kept while the entry reads it", got)
				}
			})
		}
	})

	t.Run("a later rename the filer copied releases the source", func(t *testing.T) {
		// b.jpg was renamed to c.jpg and the filer copied a.jpg there at once
		copied := &filer_pb.Entry{Name: "c.jpg", Attributes: &filer_pb.FuseAttributes{FileSize: 6}, RemoteEntry: &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: `"a"`, RemoteSize: 6}}
		remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-a"), "/b/b.jpg": []byte("old-b"), "/b/c.jpg": []byte("data-a")}, statETag: `"a"`}
		event := &filer_pb.SubscribeMetadataResponse{Directory: "/buckets/b", EventNotification: &filer_pb.EventNotification{
			OldEntry: pendingB(), NewParentPath: "/buckets/b", NewEntry: copied,
		}}
		filerClient := &pathFilerClient{entries: map[string]*filer_pb.Entry{"/buckets/b/c.jpg": copied}}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, event); err != nil {
			t.Fatal(err)
		}
		if got := deletedPaths(remote); !slices.Contains(got, "/b/a.jpg") {
			t.Errorf("deletes = %v, want the pending source released", got)
		}
	})
}

// pathFilerClient serves entries by path; a path it lacks is not found.
type pathFilerClient struct {
	stubFilerClient
	entries map[string]*filer_pb.Entry
}

func (c *pathFilerClient) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: c.entries[req.Directory+"/"+req.Name]}, nil
}

func (c *pathFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c)
}

func TestShouldSendToRemote(t *testing.T) {
	const mtime = 1786096669

	tests := []struct {
		name    string
		mtime   int64
		mtimeNs int32
		remote  *filer_pb.RemoteEntry
		want    bool
	}{
		{name: "no RemoteEntry", mtime: mtime, want: true},
		{name: "remote older", mtime: mtime, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime - 1}, want: true},
		{name: "remote newer", mtime: mtime, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime + 1}, want: false},
		{name: "same second, no sync stamp", mtime: mtime, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime}, want: true},
		{name: "same second, synced at the second", mtime: mtime, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime, LastLocalSyncTsNs: mtime * 1e9}, want: false},
		{name: "same second, synced after", mtime: mtime, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime, LastLocalSyncTsNs: mtime*1e9 + 1}, want: false},
		{name: "same second, rewritten after the sync", mtime: mtime, mtimeNs: 337, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime, LastLocalSyncTsNs: mtime*1e9 + 100}, want: true},
		{name: "same second, rewrite synced", mtime: mtime, mtimeNs: 337, remote: &filer_pb.RemoteEntry{RemoteMtime: mtime, LastLocalSyncTsNs: mtime*1e9 + 400}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: tt.mtime, MtimeNs: tt.mtimeNs}, RemoteEntry: tt.remote}
			if got := shouldSendToRemote(entry); got != tt.want {
				t.Errorf("shouldSendToRemote = %v, want %v", got, tt.want)
			}
		})
	}
}

// A metadata-only update pushed to the remote must advance the sync stamp.
// Without the restamp a same-second edit stays dirty forever and every
// following event re-sends the metadata update.
func TestMetadataOnlyUpdateRestampsSyncTs(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/"}

	replicated := &filer_pb.RemoteEntry{StorageName: "gcs", RemoteETag: "abc", RemoteSize: 2048, RemoteMtime: 1786096669, LastLocalSyncTsNs: 1}
	oldEntry := entryWith("a.txt", replicated, chunk("3,01", "e1"))
	newEntry := entryWith("a.txt", replicated, chunk("3,01", "e1"))
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      oldEntry,
			NewParentPath: "/buckets/b/dir",
			NewEntry:      newEntry,
		},
	}

	remote := &recordingRemote{}
	filerClient := &stubFilerClient{entry: newEntry}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
		t.Fatal(err)
	}
	if len(remote.metadataUpdates) != 1 {
		t.Fatalf("metadata updates = %+v, want 1", remote.metadataUpdates)
	}
	if len(remote.writes) != 0 {
		t.Errorf("writes = %+v, want none: the content did not change", remote.writes)
	}
	if filerClient.updates != 1 {
		t.Fatalf("stamped %d times, want 1: the entry is restamped after the metadata update", filerClient.updates)
	}
	stamp := filerClient.updated.Entry.GetRemoteEntry().GetLastLocalSyncTsNs()
	if stamp <= 1 {
		t.Errorf("LastLocalSyncTsNs = %d, want it advanced past the previous stamp", stamp)
	}
	expected := filerClient.updated.Condition.Clauses[0].ExpectedEntry
	if got := expected.GetRemoteEntry().GetLastLocalSyncTsNs(); got != 1 {
		t.Errorf("precondition stamp = %d, want the stored stamp 1 or IF_ENTRY_EQUAL rejects the write", got)
	}
	if got := replicated.GetLastLocalSyncTsNs(); got != 1 {
		t.Errorf("shared RemoteEntry stamp = %d, want 1: stamping must not mutate the caller's RemoteEntry", got)
	}
	if shouldSendToRemote(filerClient.updated.Entry) {
		t.Error("the restamped entry should read synced")
	}
}

// The droppedCopy fixtures are a small synced file under a mount: a Delta log
// entry whose remote object was written in the second of its mtime, as a quick
// sync of a small file is.
const droppedCopyMtime = 1786096669

var (
	droppedCopyDir    = "/buckets/gold/sales/_delta_log"
	droppedCopyName   = "00000000000000000007.json"
	droppedCopyMount  = &remote_pb.RemoteStorageLocation{Name: "s3", Bucket: "gold", Path: "/"}
	droppedCopyDest   = &remote_pb.RemoteStorageLocation{Name: "s3", Bucket: "gold", Path: "/sales/_delta_log/" + droppedCopyName}
	droppedCopySynced = &filer_pb.RemoteEntry{StorageName: "s3", RemoteETag: `"abc"`, RemoteSize: 1024, RemoteMtime: droppedCopyMtime, LastLocalSyncTsNs: droppedCopyMtime*1e9 + 500}
)

// droppedCopyStamp is the synced stamp with LastLocalSyncTsNs cleared, as
// remote.uncache and the eviction leave it.
func droppedCopyStamp() *filer_pb.RemoteEntry {
	stamp := proto.Clone(droppedCopySynced).(*filer_pb.RemoteEntry)
	stamp.LastLocalSyncTsNs = 0
	return stamp
}

// droppedCopyEntry is the fixture file with the given stamp and chunks, sized
// as its remote object.
func droppedCopyEntry(stamp *filer_pb.RemoteEntry, chunks ...*filer_pb.FileChunk) *filer_pb.Entry {
	entry := entryWith(droppedCopyName, stamp, chunks...)
	entry.Attributes.FileSize = uint64(droppedCopySynced.RemoteSize)
	return entry
}

func droppedCopyUpdate(oldEntry, newEntry *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory:         droppedCopyDir,
		EventNotification: &filer_pb.EventNotification{OldEntry: oldEntry, NewParentPath: droppedCopyDir, NewEntry: newEntry},
	}
}

// TestDroppedLocalCopyIsNotUploaded: remote.uncache and the filer's eviction
// under storage pressure drop a synced entry's chunks and clear its sync
// stamp; remote.meta.sync does the same when it pulls a newer remote version.
// With the remote mtime in the entry's second, shouldSendToRemote falls back
// to the cleared stamp and reads the entry as dirty. The object at the same
// key is the entry's only content, so uploading the chunkless entry would
// replace it with a zero-byte object. The update is skipped, and the stamp
// stays cleared so remote.cache and eviction still see the entry as uncached.
func TestDroppedLocalCopyIsNotUploaded(t *testing.T) {
	// the eviction's entry, built the way uncacheRemoteEntry builds it, and
	// the event the way the filer's CreateEntry emits it
	cached := &filer.Entry{
		FullPath: util.NewFullPath(droppedCopyDir, droppedCopyName),
		Attr:     filer.Attr{Mtime: time.Unix(droppedCopyMtime, 0), Mode: 0660, FileSize: 1024},
		Chunks:   []*filer_pb.FileChunk{chunk("3,01", "e1")},
		Remote:   droppedCopySynced,
	}
	evicted := cached.ShallowClone()
	evicted.Chunks = nil
	evicted.Remote = proto.Clone(cached.Remote).(*filer_pb.RemoteEntry)
	evicted.Remote.LastLocalSyncTsNs = 0

	// remote.meta.sync found a newer object: its mtime becomes the entry's
	newer := &filer_pb.RemoteEntry{StorageName: "s3", RemoteETag: `"def"`, RemoteSize: 4096, RemoteMtime: droppedCopyMtime + 60}
	pulled := &filer_pb.Entry{Name: droppedCopyName, Attributes: &filer_pb.FuseAttributes{Mtime: droppedCopyMtime + 60, FileSize: 4096}, RemoteEntry: newer}

	tests := []struct {
		name     string
		oldEntry *filer_pb.Entry
		newEntry *filer_pb.Entry
	}{
		{name: "remote.uncache", oldEntry: droppedCopyEntry(droppedCopySynced, chunk("3,01", "e1")), newEntry: droppedCopyEntry(droppedCopyStamp())},
		{name: "eviction under storage pressure", oldEntry: cached.ToProtoEntry(), newEntry: evicted.ToProtoEntry()},
		{name: "remote.meta.sync pulls a newer version", oldEntry: entryWith(droppedCopyName, droppedCopySynced, chunk("3,01", "e1")), newEntry: pulled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := droppedCopyUpdate(tt.oldEntry, tt.newEntry)
			if !shouldSendToRemote(tt.newEntry) {
				t.Fatal("precondition: the cleared stamp in the remote mtime's second reads as dirty, the bug's trigger")
			}
			if isMetadataOnlyUpdate(resp.Directory, resp.EventNotification) {
				t.Fatal("precondition: the dropped chunks make the update a content change")
			}

			remote := &recordingRemote{}
			filerClient := &stubFilerClient{entry: tt.newEntry}
			if err := processUpdateEvent(filerClient, filerClient, "", remote, "/buckets/gold", droppedCopyMount, resp); err != nil {
				t.Fatal(err)
			}
			if len(remote.writes) != 0 {
				t.Errorf("writes = %+v, want none: the remote object is the entry's content and an upload would zero it", remote.writes)
			}
			if len(remote.deletes) != 0 || len(remote.metadataUpdates) != 0 {
				t.Errorf("deletes = %+v, metadata updates = %+v, want the remote object left alone", remote.deletes, remote.metadataUpdates)
			}
			if filerClient.updates != 0 {
				t.Errorf("stamped the entry %d times, want 0: a stamp would mark the uncached entry as cached", filerClient.updates)
			}
		})
	}

	t.Run("a metadata edit of a remote-only entry still reaches the remote", func(t *testing.T) {
		tagged := droppedCopyEntry(droppedCopyStamp())
		tagged.Extended = map[string][]byte{"X-Amz-Tagging-tier": []byte("gold")}
		resp := droppedCopyUpdate(droppedCopyEntry(droppedCopyStamp()), tagged)
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: tagged}
		if err := processUpdateEvent(filerClient, filerClient, "", remote, "/buckets/gold", droppedCopyMount, resp); err != nil {
			t.Fatal(err)
		}
		if len(remote.metadataUpdates) != 1 || len(remote.writes) != 0 {
			t.Errorf("metadata updates = %+v, writes = %+v, want one metadata update and no upload", remote.metadataUpdates, remote.writes)
		}
	})
}

// TestSameSecondRewriteIsStillUploaded: a rewrite that lands in the remote
// mtime's second holds local data, so the skip for dropped local copies does
// not apply and it is uploaded and restamped (#11715).
func TestSameSecondRewriteIsStillUploaded(t *testing.T) {
	stale := proto.Clone(droppedCopySynced).(*filer_pb.RemoteEntry)
	stale.LastLocalSyncTsNs = droppedCopyMtime*1e9 + 100
	rewrite := func(stamp *filer_pb.RemoteEntry) *filer_pb.Entry {
		entry := entryWith(droppedCopyName, stamp, chunk("4,02", "e2"))
		entry.Attributes.MtimeNs = 337
		return entry
	}

	tests := []struct {
		name     string
		oldEntry *filer_pb.Entry
		newEntry *filer_pb.Entry
	}{
		// Filer.UpdateEntry clears the stamp of a content change
		{name: "synced entry, stamp cleared by the filer", oldEntry: entryWith(droppedCopyName, droppedCopySynced, chunk("3,01", "e1")), newEntry: rewrite(droppedCopyStamp())},
		{name: "synced entry, stamp older than the rewrite", oldEntry: entryWith(droppedCopyName, stale, chunk("3,01", "e1")), newEntry: rewrite(stale)},
		{name: "uncached entry", oldEntry: entryWith(droppedCopyName, droppedCopyStamp()), newEntry: rewrite(droppedCopyStamp())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.newEntry.Attributes.Mtime != tt.newEntry.RemoteEntry.RemoteMtime {
				t.Fatal("precondition: the rewrite lands in the remote mtime's second")
			}
			remote := &recordingRemote{}
			filerClient := &stubFilerClient{entry: tt.newEntry}
			if err := processUpdateEvent(filerClient, filerClient, "", remote, "/buckets/gold", droppedCopyMount, droppedCopyUpdate(tt.oldEntry, tt.newEntry)); err != nil {
				t.Fatal(err)
			}
			if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], droppedCopyDest) {
				t.Fatalf("writes = %+v, want the rewrite uploaded to %s", remote.writes, remote_storage.FormatLocation(droppedCopyDest))
			}
			if !slices.Equal(remote.writtenChunks[0], []string{"4,02"}) {
				t.Errorf("uploaded chunks %v, want the rewrite's [4,02]", remote.writtenChunks[0])
			}
			if filerClient.updates != 1 {
				t.Errorf("stamped the entry %d times, want 1 after the upload", filerClient.updates)
			}
		})
	}
}

// TestTruncateToZeroIsUploaded: a FUSE truncate of a cached file to zero
// (truncateEntry, or SetAttr with size 0) drops the chunks and keeps the
// sized RemoteEntry, so the entry looks remote-only. Its size is no longer
// the remote object's, and the empty file has to replace the object.
func TestTruncateToZeroIsUploaded(t *testing.T) {
	truncated := func(mtime int64, mtimeNs int32) *filer_pb.Entry {
		// the filer clears the stamp of a content change
		entry := entryWith(droppedCopyName, droppedCopyStamp())
		entry.Attributes.Mtime, entry.Attributes.MtimeNs = mtime, mtimeNs
		return entry
	}
	tests := []struct {
		name     string
		newEntry *filer_pb.Entry
	}{
		{name: "in a later second", newEntry: truncated(droppedCopyMtime+5, 0)},
		{name: "in the remote mtime's second", newEntry: truncated(droppedCopyMtime, 337)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !isRemoteOnly(tt.newEntry) {
				t.Fatal("precondition: the truncated entry looks remote-only")
			}
			remote := &recordingRemote{}
			filerClient := &stubFilerClient{entry: tt.newEntry}
			resp := droppedCopyUpdate(droppedCopyEntry(droppedCopySynced, chunk("3,01", "e1")), tt.newEntry)
			if err := processUpdateEvent(filerClient, filerClient, "", remote, "/buckets/gold", droppedCopyMount, resp); err != nil {
				t.Fatal(err)
			}
			if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], droppedCopyDest) {
				t.Fatalf("writes = %+v, want the empty file uploaded to %s", remote.writes, remote_storage.FormatLocation(droppedCopyDest))
			}
			if len(remote.writtenChunks[0]) != 0 {
				t.Errorf("uploaded chunks %v, want none: a zero-byte object", remote.writtenChunks[0])
			}
			if filerClient.updates != 1 {
				t.Errorf("stamped the entry %d times, want 1 after the upload", filerClient.updates)
			}
		})
	}
}

func TestCompleteRemoteOnlyRenameKeepsAReusedOldKey(t *testing.T) {
	// A was renamed to B, then D to A before the sync got to the first
	// rename: the object at A is now D's, and the only copy of it
	oldDest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/a.jpg"}
	dest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/b.jpg"}
	current := &filer_pb.Entry{Name: "b.jpg", RemoteEntry: &filer_pb.RemoteEntry{RemoteETag: `"a"`, RemoteSize: 6}}
	remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-d"), "/b/b.jpg": []byte("data-a")}, statETag: `"a"`}
	remote.statETags = map[string]string{"/b/a.jpg": `"d"`}
	filerClient := &stubFilerClient{entry: current}
	if err := completeRemoteOnlyRename(filerClient, remote, "/buckets/b", current, renameSource{path: "/buckets/b/a.jpg", loc: oldDest}, dest, ""); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 0 {
		t.Errorf("deletes = %+v, want the reused key kept", remote.deletes)
	}
}

func TestCompleteRemoteOnlyRenameRetryUsesTheSourceStamp(t *testing.T) {
	// an earlier run copied a multipart source and stamped the entry with
	// the copy's ETag, then failed to delete the source
	oldDest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/a.jpg"}
	dest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/b/b.jpg"}
	current := &filer_pb.Entry{Name: "b.jpg", RemoteEntry: &filer_pb.RemoteEntry{RemoteETag: `"copy"`, RemoteSize: 6}}
	remote := &recordingRemote{objects: map[string][]byte{"/b/a.jpg": []byte("data-a"), "/b/b.jpg": []byte("data-a")},
		statETags: map[string]string{"/b/a.jpg": `"a-2"`, "/b/b.jpg": `"copy"`}}
	source := renameSource{path: "/buckets/b/a.jpg", loc: oldDest, stamp: &filer_pb.RemoteEntry{RemoteETag: `"a-2"`, RemoteSize: 6}}
	if err := completeRemoteOnlyRename(&stubFilerClient{entry: current}, remote, "/buckets/b", current, source, dest, ""); err != nil {
		t.Fatal(err)
	}
	if len(remote.deletes) != 1 || remote.deletes[0].Path != "/b/a.jpg" {
		t.Errorf("deletes = %+v, want the unchanged source deleted on the retry", remote.deletes)
	}
}

// TestMetadataUpdateRestampsTheUpdatedObject: a metadata-only update can
// rewrite the remote object itself (S3 changes Content-Encoding through an
// in-place copy, minting a new ETag and mtime). The stamp saved back to the
// filer must be the one the update produced — a stat instead could fail the
// freshness comparison on the legitimately new ETag and keep the stale
// stamp, so a later delete would keep the object.
func TestMetadataUpdateRestampsTheUpdatedObject(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/"}
	stamp := &filer_pb.RemoteEntry{StorageName: "gcs", RemoteETag: `"orig"`, RemoteSize: 3, RemoteMtime: 100}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/vol",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 100}, RemoteEntry: stamp},
			NewParentPath: "/buckets/b/vol",
			NewEntry: &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 100}, RemoteEntry: stamp,
				Extended: map[string][]byte{"Content-Encoding": []byte("gzip")}},
		},
	}
	remote := &recordingRemote{
		objects:              map[string][]byte{"/b/vol/f": []byte("old")},
		metadataUpdateResult: &filer_pb.RemoteEntry{StorageName: "gcs", RemoteETag: `"new"`, RemoteSize: 3, RemoteMtime: 200},
	}
	filerClient := &stubFilerClient{}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
		t.Fatal(err)
	}
	if len(remote.metadataUpdates) != 1 {
		t.Fatalf("metadataUpdates = %+v, want one", remote.metadataUpdates)
	}
	got := filerClient.updated.Entry.RemoteEntry
	if filerClient.updates != 1 || got.RemoteMtime != 200 || got.RemoteETag != `"new"` {
		t.Fatalf("stamped RemoteEntry = %+v, want the update's result (mtime 200, etag \"new\")", got)
	}
}

// TestMetadataUpdateKeepsTheStoredStampOnNilResult: a backend that did not
// rewrite the object reports no stamp; the entry keeps its own.
func TestMetadataUpdateKeepsTheStoredStampOnNilResult(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "gcs", Bucket: "bucket", Path: "/"}
	stamp := &filer_pb.RemoteEntry{StorageName: "gcs", RemoteETag: `"orig"`, RemoteSize: 3, RemoteMtime: 100}
	resp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b/vol",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 100}, RemoteEntry: stamp},
			NewParentPath: "/buckets/b/vol",
			NewEntry: &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 100}, RemoteEntry: stamp,
				Extended: map[string][]byte{"Content-Encoding": []byte("gzip")}},
		},
	}
	remote := &recordingRemote{objects: map[string][]byte{"/b/vol/f": []byte("old")}}
	filerClient := &stubFilerClient{}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp); err != nil {
		t.Fatal(err)
	}
	if filerClient.updates != 1 || filerClient.updated.Entry.RemoteEntry.RemoteMtime != 100 {
		t.Fatalf("stamped RemoteEntry = %+v, want the entry's own stamp kept (mtime 100)", filerClient.updated.Entry.RemoteEntry)
	}
}

// TestReshapeCrossMountRename covers an update event with a path outside
// the mount. The subscription delivers a rename when either path is under
// the mount, and an update of the /etc/remote directory entry itself through
// the daemon's /etc/remote prefix; processUpdateEvent then sliced the mount
// prefix off both paths, which panics on a path shorter than the mount (seen
// on a replay: "slice bounds out of range [20:11]" on the 11-byte
// /etc/remote, every restart dying on the same event) and names a key in the
// wrong tree on a longer one.
func TestReshapeCrossMountRename(t *testing.T) {
	const mount = "/buckets/mount"
	chunked := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669, FileSize: 7}, Chunks: []*filer_pb.FileChunk{{FileId: "3,01", Size: 7}}}
	}
	event := func(oldDir, oldName, newDir, newName string, oldEntry *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
		if oldEntry == nil {
			oldEntry = chunked(oldName)
		}
		return &filer_pb.SubscribeMetadataResponse{
			Directory:         oldDir,
			TsNs:              42,
			EventNotification: &filer_pb.EventNotification{OldEntry: oldEntry, NewParentPath: newDir, NewEntry: chunked(newName), Signatures: []int32{7}},
		}
	}

	t.Run("into the mount from a shorter path is a create of the new entry", func(t *testing.T) {
		resp := event("/plain", "in.txt", mount+"/lock", "in.txt", nil)
		got, inMount := reshapeCrossMountRename(mount, resp)
		if !inMount || !filer_pb.IsCreate(got) {
			t.Fatalf("got %+v inMount=%v, want a create", got, inMount)
		}
		if got.Directory != mount+"/lock" || got.EventNotification.NewParentPath != mount+"/lock" || got.EventNotification.NewEntry.Name != "in.txt" {
			t.Errorf("create at %s/%s (dir %s), want %s/in.txt", got.EventNotification.NewParentPath, got.EventNotification.NewEntry.Name, got.Directory, mount+"/lock")
		}
		if got.TsNs != 42 || len(got.EventNotification.Signatures) != 1 {
			t.Errorf("timestamp and signatures must carry over: %+v", got)
		}
	})

	t.Run("into the mount from a longer path outside it is a create, not a key in the wrong tree", func(t *testing.T) {
		resp := event("/buckets/other/deeper/tree", "x.txt", mount, "x.txt", nil)
		got, inMount := reshapeCrossMountRename(mount, resp)
		if !inMount || !filer_pb.IsCreate(got) || got.EventNotification.NewParentPath != mount {
			t.Fatalf("got %+v inMount=%v, want a create under %s", got, inMount, mount)
		}
	})

	t.Run("out of the mount is a delete of the old entry", func(t *testing.T) {
		resp := event(mount+"/lock", "out.txt", "/plain", "out.txt", nil)
		got, inMount := reshapeCrossMountRename(mount, resp)
		if !inMount || !filer_pb.IsDelete(got) {
			t.Fatalf("got %+v inMount=%v, want a delete", got, inMount)
		}
		if got.Directory != mount+"/lock" || got.EventNotification.OldEntry.Name != "out.txt" {
			t.Errorf("delete of %s/%s, want %s/out.txt", got.Directory, got.EventNotification.OldEntry.Name, mount+"/lock")
		}
	})

	t.Run("out of the mount keeps the object of a remote-only entry", func(t *testing.T) {
		remoteOnly := &filer_pb.Entry{Name: "out.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669, FileSize: 7}, RemoteEntry: &filer_pb.RemoteEntry{StorageName: "b2", RemoteSize: 7, RemoteMtime: 1786096669}}
		resp := event(mount+"/lock", "out.txt", "/plain", "out.txt", remoteOnly)
		if got, inMount := reshapeCrossMountRename(mount, resp); inMount || got != nil {
			t.Fatalf("got %+v inMount=%v, want the event dropped", got, inMount)
		}
	})

	t.Run("both paths inside is the update itself", func(t *testing.T) {
		resp := event(mount+"/a", "x.txt", mount+"/b", "y.txt", nil)
		if got, inMount := reshapeCrossMountRename(mount, resp); !inMount || got != resp {
			t.Fatalf("got %+v inMount=%v, want the same event", got, inMount)
		}
	})

	t.Run("an update of the /etc/remote directory entry itself is not the mount's event", func(t *testing.T) {
		dirEntry := &filer_pb.Entry{Name: "remote", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}}
		resp := &filer_pb.SubscribeMetadataResponse{
			Directory:         "/etc",
			EventNotification: &filer_pb.EventNotification{OldEntry: dirEntry, NewParentPath: "/etc", NewEntry: dirEntry},
		}
		if got, inMount := reshapeCrossMountRename("/buckets/c4x-storage", resp); inMount || got != nil {
			t.Fatalf("got %+v inMount=%v, want the event dropped", got, inMount)
		}
	})

	t.Run("neither path inside is not the mount's event", func(t *testing.T) {
		resp := event("/plain", "x.txt", "/buckets/mounted-not", "x.txt", nil)
		if got, inMount := reshapeCrossMountRename(mount, resp); inMount || got != nil {
			t.Fatalf("got %+v inMount=%v, want the event dropped", got, inMount)
		}
	})

	t.Run("a trailing slash on the mount flag changes nothing", func(t *testing.T) {
		resp := event("/plain", "in.txt", mount, "in.txt", nil)
		if got, inMount := reshapeCrossMountRename(mount+"/", resp); !inMount || !filer_pb.IsCreate(got) {
			t.Fatalf("got %+v inMount=%v, want a create", got, inMount)
		}
	})
}

// TestRenameIntoMountUploadsDespiteTheOldStamp covers a file synced at a path
// under the mount, renamed out (which deleted its remote object), and renamed
// back in: the entry still carries the RemoteEntry the first sync stamped, so
// shouldSendToRemote would skip the create the reshape made, leaving the new
// key without an object. A reshaped create must send unconditionally.
func TestRenameIntoMountUploadsDespiteTheOldStamp(t *testing.T) {
	const mountedDir = "/buckets/mount"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}

	stamp := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 7, RemoteMtime: 1786096669, LastLocalSyncTsNs: 1786096669*1e9 + 1}
	newEntry := &filer_pb.Entry{
		Name:        "back.txt",
		Content:     []byte("payload"),
		Attributes:  &filer_pb.FuseAttributes{Mtime: 1786096669},
		RemoteEntry: stamp,
	}
	resp, inMount := reshapeCrossMountRename(mountedDir, &filer_pb.SubscribeMetadataResponse{
		Directory: "/plain",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "back.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}, RemoteEntry: stamp},
			NewParentPath: mountedDir + "/lock",
			NewEntry:      newEntry,
		},
	})
	if !inMount || !filer_pb.IsCreate(resp) {
		t.Fatalf("reshape = %+v inMount=%v, want a create", resp, inMount)
	}
	if shouldSendToRemote(resp.EventNotification.NewEntry) {
		t.Fatal("precondition: the stamp the rename carried should fail shouldSendToRemote")
	}

	remote := &recordingRemote{}
	filerClient := &stubFilerClient{entry: newEntry}
	if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp, true, "/plain/back.txt"); err != nil {
		t.Fatal(err)
	}
	wantWrite := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/lock/back.txt"}
	if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) {
		t.Fatalf("writes = %+v, want the new key %s written", remote.writes, remote_storage.FormatLocation(wantWrite))
	}

	// a plain create keeps trusting the stamp
	remote = &recordingRemote{}
	if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, resp, false, ""); err != nil {
		t.Fatal(err)
	}
	if len(remote.writes) != 0 {
		t.Errorf("writes = %+v, want the stamped create skipped", remote.writes)
	}
}

// TestRenameIntoMountWritesAnEmptyFile: a rename into the mount is reshaped
// to a create with mustSend, and an empty entry has no data for the HasData
// gate; the new key still takes a zero-byte object. A remote-only entry holds
// nothing local to send and is skipped as before.
func TestRenameIntoMountWritesAnEmptyFile(t *testing.T) {
	const mountedDir = "/buckets/mount"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/"}
	renameIn := func(entry *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
		resp, inMount := reshapeCrossMountRename(mountedDir, &filer_pb.SubscribeMetadataResponse{
			Directory:         "/plain",
			EventNotification: &filer_pb.EventNotification{OldEntry: entry, NewParentPath: mountedDir + "/lock", NewEntry: entry},
		})
		if !inMount || !filer_pb.IsCreate(resp) {
			t.Fatalf("reshape = %+v inMount=%v, want a create", resp, inMount)
		}
		return resp
	}
	wantWrite := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/lock/empty.txt"}

	t.Run("an empty entry writes a zero-byte object", func(t *testing.T) {
		entry := &filer_pb.Entry{Name: "empty.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}}
		if filer.HasData(entry) {
			t.Fatal("precondition: an empty entry carries no data")
		}
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: entry}
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn(entry), true, "/plain/empty.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) {
			t.Errorf("writes = %+v, want the new key %s written", remote.writes, remote_storage.FormatLocation(wantWrite))
		}
	})

	t.Run("a remote-only entry still skips", func(t *testing.T) {
		stamp := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 7, RemoteMtime: 1786096669, LastLocalSyncTsNs: 1786096669*1e9 + 1}
		entry := &filer_pb.Entry{Name: "empty.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669, FileSize: 7}, RemoteEntry: stamp}
		if !isRemoteOnly(entry) {
			t.Fatal("precondition: the entry should be remote-only")
		}
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: entry}
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn(entry), true, "/plain/empty.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none for a remote-only entry", remote.writes)
		}
	})

	// a stamped entry truncated to zero is not remote-only content: its stamp
	// describes bytes the file no longer holds, so the new key takes a
	// zero-byte object
	t.Run("a truncated stamped entry writes a zero-byte object", func(t *testing.T) {
		stamp := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 7, RemoteMtime: 1786096669, LastLocalSyncTsNs: 1786096669*1e9 + 1}
		entry := &filer_pb.Entry{Name: "empty.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}, RemoteEntry: stamp}
		if !isRemoteOnly(entry) || isDroppedLocalCopy(entry) {
			t.Fatal("precondition: a truncated stamped entry reports remote-only but is not a dropped copy")
		}
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: entry}
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn(entry), true, "/plain/empty.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], wantWrite) {
			t.Errorf("writes = %+v, want the new key %s written", remote.writes, remote_storage.FormatLocation(wantWrite))
		}
	})

	// an ordinary create of an empty entry is unchanged: no object
	t.Run("a plain empty create still writes nothing", func(t *testing.T) {
		entry := &filer_pb.Entry{Name: "empty.txt", Attributes: &filer_pb.FuseAttributes{Mtime: 1786096669}}
		remote := &recordingRemote{}
		filerClient := &stubFilerClient{entry: entry}
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn(entry), false, ""); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want the empty create skipped", remote.writes)
		}
	})
}

// TestSupersededRenameInRecoversTheCurrentEntry covers a rename into the
// mount whose create event the sync reaches only after remote.uncache dropped
// the moved file's chunks: the upload is superseded, and skipping it leaves
// the destination object missing once the source side's delete runs.
func TestSupersededRenameInRecoversTheCurrentEntry(t *testing.T) {
	const mountedDir = "/buckets/mount"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/mnt"}
	srcMountLoc := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/plain-remote"}

	stamp := &filer_pb.RemoteEntry{StorageName: "b2", RemoteETag: "abc", RemoteSize: 4, RemoteMtime: 1786096669, LastLocalSyncTsNs: 1786096669*1e9 + 1}
	// the entry as the rename logged it: local chunks plus the stamp earned
	// under the source mount
	eventEntry := &filer_pb.Entry{
		Name:        "a.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 1786096669, FileSize: 4},
		Chunks:      []*filer_pb.FileChunk{{FileId: "1,01111111", Size: 4}},
		RemoteEntry: stamp,
	}
	// the entry as remote.uncache left it: the chunks are gone and the stamp
	// still describes the source mount's object
	uncached := &filer_pb.Entry{
		Name:        "a.txt",
		Attributes:  &filer_pb.FuseAttributes{Mtime: 1786096669, FileSize: 4},
		RemoteEntry: stamp,
	}
	mappingContent, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		mountedDir: mountLoc,
		"/plain":   srcMountLoc,
	}})
	if err != nil {
		t.Fatal(err)
	}
	mappingEntry := &filer_pb.Entry{Name: filer.REMOTE_STORAGE_MOUNT_FILE, Content: mappingContent}
	renameIn := &filer_pb.SubscribeMetadataResponse{
		Directory:         mountedDir + "/lock",
		EventNotification: &filer_pb.EventNotification{NewParentPath: mountedDir + "/lock", NewEntry: eventEntry},
	}
	dest := &remote_pb.RemoteStorageLocation{Name: "b2", Bucket: "bucket", Path: "/mnt/lock/a.txt"}

	newFilerClient := func(entry *filer_pb.Entry) *stubFilerClient {
		return &stubFilerClient{entries: map[string]*filer_pb.Entry{
			mountedDir + "/lock/a.txt":                                       proto.Clone(entry).(*filer_pb.Entry),
			filer.DirectoryEtcRemote + "/" + filer.REMOTE_STORAGE_MOUNT_FILE: mappingEntry,
		}}
	}

	t.Run("uncached meanwhile: the source object is copied and the entry stamped", func(t *testing.T) {
		remote := &recordingRemote{objects: map[string][]byte{"/plain-remote/a.txt": []byte("data")}}
		filerClient := newFilerClient(uncached)
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn, true, "/plain/a.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], dest) {
			t.Fatalf("writes = %+v, want the source copied to %s", remote.writes, remote_storage.FormatLocation(dest))
		}
		if string(remote.written[0]) != "data" {
			t.Errorf("written = %q, want the source object's bytes", remote.written[0])
		}
		if filerClient.updates != 1 {
			t.Errorf("updates = %d, want the entry stamped with the copy", filerClient.updates)
		}
	})

	t.Run("the destination already holds it: only a stamp", func(t *testing.T) {
		remote := &recordingRemote{objects: map[string][]byte{"/mnt/lock/a.txt": []byte("data")}, statETags: map[string]string{"/mnt/lock/a.txt": "abc"}}
		filerClient := newFilerClient(uncached)
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn, true, "/plain/a.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want none once the destination holds the content", remote.writes)
		}
		if filerClient.updates != 1 {
			t.Errorf("updates = %d, want the entry stamped", filerClient.updates)
		}
	})

	t.Run("no object on either side: the event fails", func(t *testing.T) {
		remote := &recordingRemote{objects: map[string][]byte{}}
		filerClient := newFilerClient(uncached)
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn, true, "/plain/a.txt"); err == nil {
			t.Fatal("want the event to fail and hold the offset")
		}
	})

	t.Run("a pending marker under the mount names the source", func(t *testing.T) {
		marked := proto.Clone(uncached).(*filer_pb.Entry)
		marked.Extended = map[string][]byte{filer.ExtRemoteRenameSourceKey: []byte(mountedDir + "/lock/old.txt")}
		remote := &recordingRemote{objects: map[string][]byte{"/mnt/lock/old.txt": []byte("data")}}
		filerClient := newFilerClient(marked)
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn, true, "/plain/a.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 1 || !proto.Equal(remote.writes[0], dest) {
			t.Fatalf("writes = %+v, want the marked source copied to %s", remote.writes, remote_storage.FormatLocation(dest))
		}
	})

	t.Run("rewritten since: the superseding event uploads", func(t *testing.T) {
		rewritten := &filer_pb.Entry{
			Name:        "a.txt",
			Attributes:  &filer_pb.FuseAttributes{Mtime: 1786096700, FileSize: 4},
			Chunks:      []*filer_pb.FileChunk{{FileId: "2,02222222", Size: 4}},
			RemoteEntry: stamp,
		}
		remote := &recordingRemote{}
		filerClient := newFilerClient(rewritten)
		if err := processCreateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, renameIn, true, "/plain/a.txt"); err != nil {
			t.Fatal(err)
		}
		if len(remote.writes) != 0 {
			t.Errorf("writes = %+v, want the rewrite's own event to upload", remote.writes)
		}
	})
}

func TestIsSystemPath(t *testing.T) {
	tests := []struct {
		dir      string
		name     string
		expected bool
	}{
		{dir: "/buckets", name: ".system", expected: true},
		{dir: "/buckets/.system", name: "owners", expected: true},
		{dir: "/buckets/.system/owners", name: "alice", expected: true},
		{dir: "/buckets/.system/owners/alice", name: "mybucket", expected: true},
		{dir: "/", name: ".system", expected: true},
		{dir: "/.system", name: "config", expected: true},
		{dir: "/custom-mount", name: ".system", expected: true},
		{dir: "/custom-mount/.system/owners", name: "alice", expected: true},
		// Bucket contents should not match
		{dir: "/buckets/mybucket", name: "file.txt", expected: false},
		{dir: "/buckets/mybucket", name: ".system", expected: false},
		{dir: "/buckets/mybucket/.system", name: "data", expected: false},
		{dir: "/buckets", name: ".system-other", expected: false},
		{dir: "/buckets/.system-other", name: "sub", expected: false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s", tt.dir, tt.name), func(t *testing.T) {
			got := isSystemPath(tt.dir, tt.name)
			if got != tt.expected {
				t.Errorf("isSystemPath(%q, %q) = %v, want %v", tt.dir, tt.name, got, tt.expected)
			}
		})
	}
}

func TestRemoteSyncSkipsSystemFolder(t *testing.T) {
	const mountedDir = "/buckets"
	mountLoc := &remote_pb.RemoteStorageLocation{Name: "azure", Bucket: "project-storage", Path: "/"}

	// Test update event on /buckets/.system/owners/... is skipped
	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/.system/owners/structify-api",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "structify-copy-canary"},
			NewParentPath: "/buckets/.system/owners/structify-api",
			NewEntry:      &filer_pb.Entry{Name: "structify-copy-canary"},
		},
	}
	remote := &recordingRemote{}
	filerClient := &stubFilerClient{}
	if err := processUpdateEvent(filerClient, filerClient, "", remote, mountedDir, mountLoc, updateResp); err != nil {
		t.Fatalf("processUpdateEvent failed: %v", err)
	}
	if len(remote.writes) != 0 || len(remote.metadataUpdates) != 0 {
		t.Fatalf("expected 0 remote writes or metadata updates for system path, got %d writes and %d metadata updates",
			len(remote.writes), len(remote.metadataUpdates))
	}

	// Test delete event on /buckets/.system/owners/... is skipped
	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/.system/owners/structify-api",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "structify-copy-canary"},
		},
	}
	if err := processDeleteEvent(filerClient, remote, mountedDir, mountLoc, deleteResp); err != nil {
		t.Fatalf("processDeleteEvent failed: %v", err)
	}
	if len(remote.deletes) != 0 {
		t.Fatalf("expected 0 remote deletes for system path, got %d deletes", len(remote.deletes))
	}
}
