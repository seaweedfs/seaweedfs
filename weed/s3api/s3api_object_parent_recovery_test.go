package s3api

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func parentRecoveryServer(t *testing.T, f *parentFailFiler) *S3ApiServer {
	s3a := ambiguousRouteServer(t, startFakeFiler(t, f))
	s3a.option.BucketsPath = "/buckets"
	f.put("/buckets", &filer_pb.Entry{Name: "b", IsDirectory: true})
	return s3a
}

// parentFailFiler is a memCompleteFiler whose directory creates fail while
// mkdirFails is set. Lookups and directory creates are counted.
type parentFailFiler struct {
	*memCompleteFiler
	mkdirFails bool
	mkdirs     int
	lookups    int
}

func (f *parentFailFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	f.lookups++
	f.mu.Unlock()
	return f.memCompleteFiler.LookupDirectoryEntry(ctx, req)
}

func (f *parentFailFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if !req.Entry.IsDirectory {
		return f.memCompleteFiler.CreateEntry(ctx, req)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mkdirs++
	if f.mkdirFails {
		return nil, status.Error(codes.Internal, "mkdir failed")
	}
	if f.entries[req.Directory][req.Entry.Name] != nil {
		return &filer_pb.CreateEntryResponse{Error: filer_pb.ErrEntryAlreadyExists.Error(), ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS}, nil
	}
	f.put(req.Directory, req.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func newParentFailFiler() *parentFailFiler {
	return &parentFailFiler{memCompleteFiler: &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}}}
}

func (f *parentFailFiler) isDir(dir, name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := f.entries[dir][name]
	return entry != nil && entry.IsDirectory
}

func TestAmbiguousRouteRecoveryCreatesMissingParent(t *testing.T) {
	uploaded := []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}
	chunksAlive := func(context.Context, []*filer_pb.FileChunk) bool { return true }
	recover := func(t *testing.T, f *parentFailFiler) (s3err.ErrorCode, bool) {
		s3a := parentRecoveryServer(t, f)
		addr := s3a.option.Filers[0]
		f.put("/buckets/b/d", &filer_pb.Entry{Name: "o", Chunks: []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}})
		entryCreated := false
		code := s3a.createAfterAmbiguousRoute("/buckets/b/d/o", "b", "d/o", addr, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive,
			func(*filer_pb.WriteCondition) s3err.ErrorCode {
				t.Fatal("re-committed an entry the route already stored")
				return s3err.ErrNone
			})
		return code, entryCreated
	}

	t.Run("missing parent is created", func(t *testing.T) {
		f := newParentFailFiler()
		if code, entryCreated := recover(t, f); code != s3err.ErrNone || !entryCreated {
			t.Fatalf("code=%v entryCreated=%v", code, entryCreated)
		}
		if !f.isDir("/buckets/b", "d") {
			t.Fatal("the recovered object's directory was not created")
		}
	})

	t.Run("parent that cannot be created fails the write", func(t *testing.T) {
		f := newParentFailFiler()
		f.mkdirFails = true
		if code, entryCreated := recover(t, f); code == s3err.ErrNone || entryCreated {
			t.Fatalf("code=%v entryCreated=%v; want an error while the object is unlisted", code, entryCreated)
		}
	})

	t.Run("existing parent is left alone", func(t *testing.T) {
		f := newParentFailFiler()
		f.put("/buckets/b", &filer_pb.Entry{Name: "d", IsDirectory: true})
		if code, entryCreated := recover(t, f); code != s3err.ErrNone || !entryCreated {
			t.Fatalf("code=%v entryCreated=%v", code, entryCreated)
		}
		if f.mkdirs != 0 {
			t.Fatalf("mkdirs = %d, want none for a present directory", f.mkdirs)
		}
	})
}

func TestConfirmCreateLandedCreatesMissingParent(t *testing.T) {
	uploaded := []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}
	for _, mkdirFails := range []bool{false, true} {
		f := newParentFailFiler()
		f.mkdirFails = mkdirFails
		s3a := parentRecoveryServer(t, f)
		f.put("/buckets/b/d", &filer_pb.Entry{Name: "o", Chunks: []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}})
		landed, absent := s3a.confirmCreateLanded("/buckets/b/d/o", "b", "d/o", "d/o", &filer_pb.Entry{Name: "o"}, uploaded, nil)
		if absent || landed == mkdirFails || f.isDir("/buckets/b", "d") == mkdirFails {
			t.Fatalf("mkdirFails=%v: landed=%v absent=%v dir=%v", mkdirFails, landed, absent, f.isDir("/buckets/b", "d"))
		}
	}
}

func TestEnsureObjectParentChecksEveryLevel(t *testing.T) {
	t.Run("lower directory stored without its parent", func(t *testing.T) {
		f := newParentFailFiler()
		s3a := parentRecoveryServer(t, f)
		f.put("/buckets/b/a", &filer_pb.Entry{Name: "c", IsDirectory: true})
		if err := s3a.ensureObjectParent(context.Background(), "b", "/buckets/b/a/c"); err != nil {
			t.Fatal(err)
		}
		if !f.isDir("/buckets/b", "a") {
			t.Fatal("the missing ancestor was not created")
		}
	})
	t.Run("parent left a file", func(t *testing.T) {
		f := newParentFailFiler()
		s3a := parentRecoveryServer(t, f)
		f.put("/buckets/b", &filer_pb.Entry{Name: "d"})
		if err := s3a.ensureObjectParent(context.Background(), "b", "/buckets/b/d"); err == nil {
			t.Fatal("accepted a file as the object's directory")
		}
	})
	t.Run("missing bucket is not recreated", func(t *testing.T) {
		f := newParentFailFiler()
		s3a := parentRecoveryServer(t, f)
		delete(f.entries["/buckets"], "b")
		if err := s3a.ensureObjectParent(context.Background(), "b", "/buckets/b/d"); err == nil {
			t.Fatal("repaired an object's directory under a deleted bucket")
		}
		if f.mkdirs != 0 {
			t.Fatalf("mkdirs = %d, want none under a missing bucket", f.mkdirs)
		}
	})
}

// A part's upload directory is never recreated: it is gone only when the
// upload was aborted or completed.
func TestEnsureObjectParentSkipsBucketAndUploads(t *testing.T) {
	f := newParentFailFiler()
	s3a := parentRecoveryServer(t, f)
	for _, dir := range []string{s3a.bucketDir("b"), s3a.genUploadsFolder("b"), s3a.genUploadsFolder("b") + "/u1", "/buckets"} {
		if err := s3a.ensureObjectParent(context.Background(), "b", dir); err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
	}
	if f.mkdirs != 0 || f.lookups != 0 {
		t.Fatalf("mkdirs=%d lookups=%d, want none", f.mkdirs, f.lookups)
	}
}

// A completion that committed its object but not the object's directory
// answers an error, and the retry that finds the commit creates the directory.
func TestCompleteMultipartUploadCreatesCommittedObjectsParent(t *testing.T) {
	f := newParentFailFiler()
	f.createFailure = status.Error(codes.Internal, "mkdir after the entry failed")
	f.storedOnFailure = func(entry *filer_pb.Entry) *filer_pb.Entry { return entry }
	f.mkdirFails = true
	s3a := newFailoverTestServer(t, startFakeFiler(t, f))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	uploadsDir := s3a.genUploadsFolder("b")
	f.put(s3a.bucketRoot("b"), &filer_pb.Entry{Name: "b", IsDirectory: true})
	f.put(uploadsDir, &filer_pb.Entry{Name: "u1", IsDirectory: true, Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("d/a.bin")}})
	f.put(uploadsDir+"/u1", &filer_pb.Entry{
		Name:       "0001.part",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 11}},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("5eb63bbbe01eeed093cb22bb8f5acdc3")},
	})
	f.put(uploadsDir+"/u1", &filer_pb.Entry{
		Name:       "0001_old.part",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,02637037d6", Size: 11}},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("0f343b0931126a20f133d67c2b018a3b")},
	})
	complete := func() (*CompleteMultipartUploadResult, s3err.ErrorCode) {
		input := &s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("d/a.bin"), UploadId: aws.String("u1")}
		parts := &CompleteMultipartUpload{Parts: []CompletedPart{{PartNumber: 1, ETag: `"5eb63bbbe01eeed093cb22bb8f5acdc3"`}}}
		return s3a.completeMultipartUpload(httptest.NewRequest("POST", "/b/d/a.bin?uploadId=u1", nil), input, parts)
	}

	if _, code := complete(); code == s3err.ErrNone {
		t.Fatal("completion answered success with the object's directory missing")
	}
	if f.entries[s3a.bucketDir("b")+"/d"]["a.bin"] == nil {
		t.Fatal("test setup: the failed commit did not store the object")
	}
	if f.entries[uploadsDir]["u1"] == nil {
		t.Fatal("the upload directory was removed before the object was listed")
	}
	if deleteData, ok := f.deletedData["0001_old.part"]; !ok || !deleteData {
		t.Errorf("unused part deleted = %v, with data = %v; want its data freed before the retry", ok, deleteData)
	}

	f.mkdirFails = false
	output, code := complete()
	if code != s3err.ErrNone || output == nil {
		t.Fatalf("retry code = %v, want success", s3err.GetAPIError(code).Code)
	}
	if !f.isDir(s3a.bucketDir("b"), "d") {
		t.Fatal("the retry did not create the committed object's directory")
	}
	if f.entries[uploadsDir]["u1"] != nil {
		t.Error("the upload directory survived the completed retry")
	}
}
