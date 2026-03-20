package command

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
