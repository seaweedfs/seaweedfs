package shell

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type stubFilerClient struct {
	client filer_pb.SeaweedFilerClient
}

func (s stubFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(s.client)
}

func (s stubFilerClient) AdjustedUrl(location *filer_pb.Location) string { return "" }

func (s stubFilerClient) GetDataCenter() string { return "" }

type stubFilerSvc struct {
	filer_pb.SeaweedFilerClient
	entries    map[string]*filer_pb.Entry
	lookupResp *filer_pb.LookupDirectoryEntryResponse
	lookupErr  error
	listResp   []*filer_pb.Entry
	listDir    string
	listErr    error
	lookupReq  *filer_pb.LookupDirectoryEntryRequest
	listReq    *filer_pb.ListEntriesRequest
	txnResp    *filer_pb.ObjectTransactionResponse
	txnReqs    []*filer_pb.ObjectTransactionRequest
	deleteReqs []*filer_pb.DeleteEntryRequest
}

func (s *stubFilerSvc) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest, opts ...grpc.CallOption) (*filer_pb.ObjectTransactionResponse, error) {
	s.txnReqs = append(s.txnReqs, req)
	return s.txnResp, nil
}

func (s *stubFilerSvc) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest, opts ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	s.deleteReqs = append(s.deleteReqs, req)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (s *stubFilerSvc) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if entry, ok := s.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	s.lookupReq = req
	return s.lookupResp, s.lookupErr
}

func (s *stubFilerSvc) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	s.listReq = req
	if s.listErr != nil {
		return nil, s.listErr
	}
	if s.listDir != "" && req.Directory != s.listDir {
		return &stubListStream{}, nil
	}
	return &stubListStream{entries: s.listResp}, nil
}

type stubListStream struct {
	entries []*filer_pb.Entry
	idx     int
}

func (s *stubListStream) Header() (metadata.MD, error) { return nil, nil }
func (s *stubListStream) Trailer() metadata.MD         { return nil }
func (s *stubListStream) CloseSend() error             { return nil }
func (s *stubListStream) Context() context.Context     { return context.Background() }
func (s *stubListStream) SendMsg(m any) error          { return nil }
func (s *stubListStream) RecvMsg(m any) error          { return nil }

func (s *stubListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.idx >= len(s.entries) {
		return nil, io.EOF
	}
	entry := s.entries[s.idx]
	s.idx++
	return &filer_pb.ListEntriesResponse{Entry: entry}, nil
}

func uploadEntry(uploadId, objectKey string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:     uploadId,
		Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte(objectKey)},
	}
}

func completedEntry(uploadId string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.SeaweedFSUploadId: []byte(uploadId)},
	}
}

func TestUploadCompletedVersionPaths(t *testing.T) {
	for _, tc := range []struct {
		key       string
		objectDir string
		name      string
	}{
		{key: "obj", objectDir: "/buckets/b", name: "obj"},
		{key: "dir/obj", objectDir: "/buckets/b/dir", name: "obj"},
		{key: "dir/", objectDir: "/buckets/b/dir", name: "dir"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			svc := &stubFilerSvc{
				lookupErr: filer_pb.ErrNotFound,
				listDir:   tc.objectDir + "/" + tc.name + s3_constants.VersionsFolder,
				listResp:  []*filer_pb.Entry{completedEntry("up1")},
			}
			c := &commandS3CleanUploads{}
			completed, err := c.uploadCompleted(stubFilerClient{client: svc}, "/buckets/b", uploadEntry("up1", tc.key))
			if err != nil || !completed {
				t.Fatalf("got (%v, %v), want (true, nil)", completed, err)
			}
			if svc.lookupReq.Directory != tc.objectDir || svc.lookupReq.Name != tc.name {
				t.Fatalf("lookup = %s/%s, want %s/%s", svc.lookupReq.Directory, svc.lookupReq.Name, tc.objectDir, tc.name)
			}
		})
	}
}

func TestUploadCompleted(t *testing.T) {
	c := &commandS3CleanUploads{}

	t.Run("object entry carries the upload id", func(t *testing.T) {
		fc := stubFilerClient{client: &stubFilerSvc{
			lookupResp: &filer_pb.LookupDirectoryEntryResponse{Entry: completedEntry("up1")},
		}}
		completed, err := c.uploadCompleted(fc, "/buckets/b", uploadEntry("up1", "obj"))
		if err != nil || !completed {
			t.Fatalf("got (%v, %v), want (true, nil)", completed, err)
		}
	})

	t.Run("object entry has a different upload id", func(t *testing.T) {
		fc := stubFilerClient{client: &stubFilerSvc{
			lookupResp: &filer_pb.LookupDirectoryEntryResponse{Entry: completedEntry("up2")},
		}}
		completed, err := c.uploadCompleted(fc, "/buckets/b", uploadEntry("up1", "obj"))
		if err != nil || completed {
			t.Fatalf("got (%v, %v), want (false, nil)", completed, err)
		}
	})

	t.Run("a version file carries the upload id", func(t *testing.T) {
		fc := stubFilerClient{client: &stubFilerSvc{
			lookupErr: filer_pb.ErrNotFound,
			listResp: []*filer_pb.Entry{
				completedEntry("up0"),
				completedEntry("up1"),
			},
		}}
		completed, err := c.uploadCompleted(fc, "/buckets/b", uploadEntry("up1", "obj"))
		if err != nil || !completed {
			t.Fatalf("got (%v, %v), want (true, nil)", completed, err)
		}
	})

	t.Run("no object and no versions", func(t *testing.T) {
		fc := stubFilerClient{client: &stubFilerSvc{
			lookupErr: filer_pb.ErrNotFound,
		}}
		completed, err := c.uploadCompleted(fc, "/buckets/b", uploadEntry("up1", "obj"))
		if err != nil || completed {
			t.Fatalf("got (%v, %v), want (false, nil)", completed, err)
		}
	})

	t.Run("upload dir without an object key", func(t *testing.T) {
		completed, err := c.uploadCompleted(stubFilerClient{}, "/buckets/b", &filer_pb.Entry{Name: "up1"})
		if err != nil || completed {
			t.Fatalf("got (%v, %v), want (false, nil)", completed, err)
		}
	})

	t.Run("lookup error propagates", func(t *testing.T) {
		fc := stubFilerClient{client: &stubFilerSvc{
			lookupErr: errors.New("store unavailable"),
		}}
		completed, err := c.uploadCompleted(fc, "/buckets/b", uploadEntry("up1", "obj"))
		if err == nil || completed {
			t.Fatalf("got (%v, %v), want (false, error)", completed, err)
		}
	})

	t.Run("versions list not found means no completed object", func(t *testing.T) {
		fc := stubFilerClient{client: &stubFilerSvc{
			lookupErr: filer_pb.ErrNotFound,
			listErr:   filer_pb.ErrNotFound,
		}}
		completed, err := c.uploadCompleted(fc, "/buckets/b", uploadEntry("up1", "obj"))
		if err != nil || completed {
			t.Fatalf("got (%v, %v), want (false, nil)", completed, err)
		}
	})

	t.Run("trailing slash key resolves inside its directory", func(t *testing.T) {
		svc := &stubFilerSvc{
			lookupResp: &filer_pb.LookupDirectoryEntryResponse{Entry: completedEntry("up1")},
		}
		completed, err := c.uploadCompleted(stubFilerClient{client: svc}, "/buckets/b", uploadEntry("up1", "dir/"))
		if err != nil || !completed {
			t.Fatalf("got (%v, %v), want (true, nil)", completed, err)
		}
		if svc.lookupReq.Directory != "/buckets/b/dir" || svc.lookupReq.Name != "dir" {
			t.Fatalf("lookup = %s/%s, want /buckets/b/dir/dir", svc.lookupReq.Directory, svc.lookupReq.Name)
		}
	})
}

func bucketEntry(versioning string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: "b", IsDirectory: true, Extended: map[string][]byte{s3_constants.ExtVersioningKey: []byte(versioning)}}
}

// openUploadSvc answers an open upload's lookups: no object, no versions, and
// a bucket with the given versioning state.
func openUploadSvc(versioning string, txnResp *filer_pb.ObjectTransactionResponse) *stubFilerSvc {
	return &stubFilerSvc{
		entries:   map[string]*filer_pb.Entry{"/buckets/b": bucketEntry(versioning)},
		lookupErr: filer_pb.ErrNotFound,
		listErr:   filer_pb.ErrNotFound,
		txnResp:   txnResp,
	}
}

func TestRemoveUploadDir(t *testing.T) {
	c := &commandS3CleanUploads{}
	const uploadsDir = "/buckets/b/" + s3_constants.MultipartUploadsFolder
	lockHeld := func() bool { return true }

	t.Run("open upload is freed in a transaction on the object's lock", func(t *testing.T) {
		svc := openUploadSvc("", &filer_pb.ObjectTransactionResponse{})
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "/dir//obj"), lockHeld); err != nil {
			t.Fatal(err)
		}
		if len(svc.txnReqs) != 1 || len(svc.deleteReqs) != 0 {
			t.Fatalf("%d transactions, %d deletes; want 1, 0", len(svc.txnReqs), len(svc.deleteReqs))
		}
		req := svc.txnReqs[0]
		if req.LockKey != "/buckets/b/dir/obj" || req.ConditionKey != req.LockKey || req.RouteKey != s3_constants.ObjectWriteRouteKeyPrefix+req.LockKey {
			t.Fatalf("lock %q condition %q route %q", req.LockKey, req.ConditionKey, req.RouteKey)
		}
		clause := req.Condition.GetClauses()[0]
		if clause.Kind != filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL || clause.ExtKey != s3_constants.SeaweedFSUploadId || string(clause.ExtValue) != "up1" {
			t.Fatalf("condition %v", clause)
		}
		m := req.Mutations[0]
		if m.Type != filer_pb.ObjectMutation_DELETE || m.Directory != uploadsDir || m.Name != "up1" || !m.IsDeleteData || !m.IsRecursive {
			t.Fatalf("mutation %v", m)
		}
	})

	t.Run("trailing-slash key locks the entry its completion writes as versioning changes", func(t *testing.T) {
		svc := openUploadSvc("", &filer_pb.ObjectTransactionResponse{})
		for _, tc := range []struct {
			versioning string
			objectLock bool
			lockKey    string
		}{
			{"", false, "/buckets/b/dir/dir"},
			{s3_constants.VersioningEnabled, false, "/buckets/b/dir/"},
			{s3_constants.VersioningSuspended, false, "/buckets/b/dir/dir"},
			{"", true, "/buckets/b/dir/"},
		} {
			bucket := bucketEntry(tc.versioning)
			if tc.objectLock {
				bucket.Extended[s3_constants.ExtObjectLockEnabledKey] = []byte(s3_constants.ObjectLockEnabled)
			}
			svc.entries["/buckets/b"] = bucket
			svc.txnReqs = nil
			if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "dir/"), lockHeld); err != nil {
				t.Fatal(err)
			}
			req := svc.txnReqs[0]
			if req.LockKey != tc.lockKey || req.ConditionKey != tc.lockKey || req.RouteKey != s3_constants.ObjectWriteRouteKeyPrefix+"/buckets/b/dir" {
				t.Fatalf("versioning=%q objectLock=%v: lock %q condition %q route %q", tc.versioning, tc.objectLock, req.LockKey, req.ConditionKey, req.RouteKey)
			}
		}
	})

	t.Run("unreadable bucket deletes nothing", func(t *testing.T) {
		svc := &stubFilerSvc{lookupErr: filer_pb.ErrNotFound, listErr: filer_pb.ErrNotFound, txnResp: &filer_pb.ObjectTransactionResponse{}}
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "obj"), lockHeld); err != nil {
			t.Fatal(err)
		}
		if len(svc.txnReqs) != 0 || len(svc.deleteReqs) != 0 {
			t.Fatalf("%d transactions, %d deletes", len(svc.txnReqs), len(svc.deleteReqs))
		}
	})

	t.Run("upload completed before the transaction keeps its chunks", func(t *testing.T) {
		svc := openUploadSvc("", &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED})
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "obj"), lockHeld); err != nil {
			t.Fatal(err)
		}
		if len(svc.deleteReqs) != 1 || svc.deleteReqs[0].IsDeleteData || svc.deleteReqs[0].Name != "up1" {
			t.Fatalf("deletes %v, want one metadata-only delete of up1", svc.deleteReqs)
		}
	})

	t.Run("completed upload goes metadata-only without a transaction", func(t *testing.T) {
		svc := &stubFilerSvc{lookupResp: &filer_pb.LookupDirectoryEntryResponse{Entry: completedEntry("up1")}}
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "obj"), lockHeld); err != nil {
			t.Fatal(err)
		}
		if len(svc.txnReqs) != 0 || len(svc.deleteReqs) != 1 || svc.deleteReqs[0].IsDeleteData {
			t.Fatalf("%d transactions, deletes %v", len(svc.txnReqs), svc.deleteReqs)
		}
	})

	t.Run("undecidable lookup deletes nothing", func(t *testing.T) {
		svc := &stubFilerSvc{lookupErr: errors.New("store unavailable")}
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "obj"), lockHeld); err != nil {
			t.Fatal(err)
		}
		if len(svc.txnReqs) != 0 || len(svc.deleteReqs) != 0 {
			t.Fatalf("%d transactions, %d deletes", len(svc.txnReqs), len(svc.deleteReqs))
		}
	})

	t.Run("lost lock deletes nothing", func(t *testing.T) {
		svc := openUploadSvc("", &filer_pb.ObjectTransactionResponse{})
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "obj"), func() bool { return false }); err != nil {
			t.Fatal(err)
		}
		if len(svc.txnReqs) != 0 || len(svc.deleteReqs) != 0 {
			t.Fatalf("%d transactions, %d deletes", len(svc.txnReqs), len(svc.deleteReqs))
		}
	})

	t.Run("transaction error fails the purge", func(t *testing.T) {
		svc := openUploadSvc("", &filer_pb.ObjectTransactionResponse{Error: "boom"})
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, uploadEntry("up1", "obj"), lockHeld); err == nil {
			t.Fatal("want an error")
		}
		if len(svc.deleteReqs) != 0 {
			t.Fatalf("deletes %v", svc.deleteReqs)
		}
	})

	t.Run("upload without an object key frees its chunks directly", func(t *testing.T) {
		svc := &stubFilerSvc{}
		if err := c.removeUploadDir(stubFilerClient{client: svc}, io.Discard, "/buckets/b", uploadsDir, &filer_pb.Entry{Name: "up1"}, lockHeld); err != nil {
			t.Fatal(err)
		}
		if len(svc.txnReqs) != 0 || len(svc.deleteReqs) != 1 || !svc.deleteReqs[0].IsDeleteData {
			t.Fatalf("%d transactions, deletes %v", len(svc.txnReqs), svc.deleteReqs)
		}
	})
}
