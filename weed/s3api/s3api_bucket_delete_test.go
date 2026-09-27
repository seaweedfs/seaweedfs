package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeBucketDeleteFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entriesByDir map[string][]*filer_pb.Entry
	deleteReq    *filer_pb.DeleteEntryRequest
}

func (f *fakeBucketDeleteFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	entries, ok := f.entriesByDir[req.Directory]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	for _, e := range entries {
		if e.Name == req.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, filer_pb.ErrNotFound
}

func (f *fakeBucketDeleteFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	entries := f.entriesByDir[req.Directory]
	if inPrefix := req.Prefix; inPrefix != "" && inPrefix != "/" {
		filtered := make([]*filer_pb.Entry, 0)
		for _, e := range entries {
			if strings.HasPrefix(e.Name, inPrefix) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}
	if req.StartFromFileName != "" {
		filtered := make([]*filer_pb.Entry, 0)
		for _, e := range entries {
			if e.Name > req.StartFromFileName || (req.InclusiveStartFrom && e.Name == req.StartFromFileName) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}
	if req.Limit > 0 && int(req.Limit) < len(entries) {
		entries = entries[:req.Limit]
	}
	for _, entry := range entries {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBucketDeleteFiler) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.deleteReq = req
	return &filer_pb.DeleteEntryResponse{}, nil
}

func newBucketDeleteTestServer(t *testing.T, f *fakeBucketDeleteFiler, allowDeleteBucketNotEmpty bool) *S3ApiServer {
	t.Helper()
	s3a := newFailoverTestServer(t, startFakeFiler(t, f))
	s3a.option.BucketsPath = "/buckets"
	s3a.option.AllowDeleteBucketNotEmpty = allowDeleteBucketNotEmpty
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.iam = &IdentityAccessManagement{}
	return s3a
}

func TestBucketHasUserObjects_EmptyDirectories(t *testing.T) {
	cases := []struct {
		name         string
		entriesByDir map[string][]*filer_pb.Entry
		wantHasUser  bool
	}{
		{
			name: "completely empty bucket",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {},
			},
			wantHasUser: false,
		},
		{
			name: "bucket with root file",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "file.txt", IsDirectory: false},
				},
			},
			wantHasUser: true,
		},
		{
			name: "bucket with leftover empty directory from rm --recursive",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "data", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
				},
				"/buckets/b/data": {},
			},
			wantHasUser: false,
		},
		{
			name: "bucket with nested leftover empty directories",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "data", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
				},
				"/buckets/b/data": {
					{Name: "2026", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
				},
				"/buckets/b/data/2026": {},
			},
			wantHasUser: false,
		},
		{
			name: "bucket with nested directory containing a file",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "data", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
				},
				"/buckets/b/data": {
					{Name: "2026", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
				},
				"/buckets/b/data/2026": {
					{Name: "report.pdf", IsDirectory: false},
				},
			},
			wantHasUser: true,
		},
		{
			name: "bucket with explicit directory object (MIME set)",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "logs", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/x-directory"}},
				},
				"/buckets/b/logs": {},
			},
			wantHasUser: true,
		},
		{
			name: "bucket with only reserved folders (.uploads, *.versions)",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: s3_constants.MultipartUploadsFolder, IsDirectory: true},
					{Name: "oldfile.txt" + s3_constants.VersionsFolder, IsDirectory: true},
				},
			},
			wantHasUser: false,
		},
		{
			name: "nested versions-named directories are internal",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "logs", IsDirectory: true},
				},
				"/buckets/b/logs": {
					{Name: "foo" + s3_constants.VersionsFolder, IsDirectory: true},
				},
				"/buckets/b/logs/foo" + s3_constants.VersionsFolder: {
					{Name: "v1", IsDirectory: false},
				},
			},
			wantHasUser: false,
		},
		{
			name: "nested uploads-named directories are internal",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "data", IsDirectory: true},
				},
				"/buckets/b/data": {
					{Name: s3_constants.MultipartUploadsFolder, IsDirectory: true},
				},
				"/buckets/b/data/" + s3_constants.MultipartUploadsFolder: {
					{Name: "part-1", IsDirectory: false},
				},
			},
			wantHasUser: false,
		},
		{
			name: "directory object with a reserved name counts",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "data", IsDirectory: true},
				},
				"/buckets/b/data": {
					{Name: s3_constants.MultipartUploadsFolder, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: s3_constants.FolderMimeType}},
				},
			},
			wantHasUser: true,
		},
		{
			name: "file with a reserved name counts",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: "dir", IsDirectory: true},
				},
				"/buckets/b/dir": {
					{Name: "data" + s3_constants.VersionsFolder, IsDirectory: false},
				},
			},
			wantHasUser: true,
		},
		{
			name: "file with backslash in name",
			entriesByDir: map[string][]*filer_pb.Entry{
				"/buckets/b": {
					{Name: `a\b`, IsDirectory: false},
				},
			},
			wantHasUser: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeBucketDeleteFiler{entriesByDir: tc.entriesByDir}
			s3a := newBucketDeleteTestServer(t, f, false)

			got, err := s3a.bucketHasUserObjects("b")
			require.NoError(t, err)
			assert.Equal(t, tc.wantHasUser, got)
		})
	}
}

func TestBucketHasUserObjects_DeepEmptyChain(t *testing.T) {
	entriesByDir := map[string][]*filer_pb.Entry{
		"/buckets/b": {{Name: "d0", IsDirectory: true}},
	}
	dir := "/buckets/b/d0"
	for i := 1; i < 500; i++ {
		child := fmt.Sprintf("d%d", i)
		entriesByDir[dir] = []*filer_pb.Entry{{Name: child, IsDirectory: true}}
		dir = dir + "/" + child
	}
	entriesByDir[dir] = []*filer_pb.Entry{}

	f := &fakeBucketDeleteFiler{entriesByDir: entriesByDir}
	s3a := newBucketDeleteTestServer(t, f, false)

	got, err := s3a.bucketHasUserObjects("b")
	require.NoError(t, err)
	assert.False(t, got)
}

func TestDeleteBucketHandler_EmptyDirectoriesAllowedWhenNotEmptyFalse(t *testing.T) {
	// A bucket that holds only an empty directory leftover from deleted objects
	// must succeed when AllowDeleteBucketNotEmpty is false.
	f := &fakeBucketDeleteFiler{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets": {
				{Name: "repro-bucket", IsDirectory: true},
			},
			"/buckets/repro-bucket": {
				{Name: "data", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
			},
			"/buckets/repro-bucket/data": {},
		},
	}
	s3a := newBucketDeleteTestServer(t, f, false)
	s3a.bucketConfigCache.Set("repro-bucket", &BucketConfig{Name: "repro-bucket"})

	req := httptest.NewRequest(http.MethodDelete, "/repro-bucket", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": "repro-bucket"})
	rr := httptest.NewRecorder()

	s3a.DeleteBucketHandler(rr, req)

	assert.Equal(t, http.StatusNoContent, rr.Code, "deleting bucket with only empty directory must succeed: %s", rr.Body.String())
	assert.NotNil(t, f.deleteReq)
	assert.Equal(t, "/buckets", f.deleteReq.Directory)
	assert.Equal(t, "repro-bucket", f.deleteReq.Name)
	assert.True(t, f.deleteReq.IsRecursive)
}

func TestDeleteBucketHandler_RefusesBucketWithFilesWhenNotEmptyFalse(t *testing.T) {
	f := &fakeBucketDeleteFiler{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets": {
				{Name: "non-empty-bucket", IsDirectory: true},
			},
			"/buckets/non-empty-bucket": {
				{Name: "data", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: ""}},
			},
			"/buckets/non-empty-bucket/data": {
				{Name: "hello.txt", IsDirectory: false},
			},
		},
	}
	s3a := newBucketDeleteTestServer(t, f, false)
	s3a.bucketConfigCache.Set("non-empty-bucket", &BucketConfig{Name: "non-empty-bucket"})

	req := httptest.NewRequest(http.MethodDelete, "/non-empty-bucket", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": "non-empty-bucket"})
	rr := httptest.NewRecorder()

	s3a.DeleteBucketHandler(rr, req)

	assert.Equal(t, http.StatusConflict, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Code>BucketNotEmpty</Code>")
	assert.Nil(t, f.deleteReq, "delete request must not have been sent to filer")
}

func TestDeleteBucketHandler_RefusesBucketWithDirectoryObjectWhenNotEmptyFalse(t *testing.T) {
	f := &fakeBucketDeleteFiler{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets": {
				{Name: "dir-obj-bucket", IsDirectory: true},
			},
			"/buckets/dir-obj-bucket": {
				{Name: "photos", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/x-directory"}},
			},
			"/buckets/dir-obj-bucket/photos": {},
		},
	}
	s3a := newBucketDeleteTestServer(t, f, false)
	s3a.bucketConfigCache.Set("dir-obj-bucket", &BucketConfig{Name: "dir-obj-bucket"})

	req := httptest.NewRequest(http.MethodDelete, "/dir-obj-bucket", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": "dir-obj-bucket"})
	rr := httptest.NewRecorder()

	s3a.DeleteBucketHandler(rr, req)

	assert.Equal(t, http.StatusConflict, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Code>BucketNotEmpty</Code>")
	assert.Nil(t, f.deleteReq, "delete request must not have been sent to filer")
}
