package s3api

import (
	"context"
	"net/http/httptest"
	"sync"
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

// memCompleteFiler keeps entries in memory. Its CreateEntry can store the
// entry and still fail, as a filer that committed but lost the response.
type memCompleteFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu            sync.Mutex
	entries       map[string]map[string]*filer_pb.Entry
	storeOnFail   bool
	createFailure error
}

func (f *memCompleteFiler) put(dir string, entry *filer_pb.Entry) {
	if f.entries[dir] == nil {
		f.entries[dir] = map[string]*filer_pb.Entry{}
	}
	f.entries[dir][entry.Name] = entry
}

func (f *memCompleteFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry := f.entries[req.Directory][req.Name]; entry != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func (f *memCompleteFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	f.mu.Lock()
	var entries []*filer_pb.Entry
	for _, entry := range f.entries[req.Directory] {
		entries = append(entries, entry)
	}
	f.mu.Unlock()
	for _, entry := range entries {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (f *memCompleteFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createFailure == nil || f.storeOnFail {
		f.put(req.Directory, req.Entry)
	}
	if f.createFailure != nil {
		return nil, f.createFailure
	}
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *memCompleteFiler) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries[req.Directory], req.Name)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func newCompleteTestServer(t *testing.T, f *memCompleteFiler) *S3ApiServer {
	t.Helper()
	s3a := newFailoverTestServer(t, startFakeFiler(t, f))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	uploadsDir := s3a.genUploadsFolder("b")
	f.put(uploadsDir, &filer_pb.Entry{
		Name:        "u1",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("a.bin")},
	})
	f.put(uploadsDir+"/u1", &filer_pb.Entry{
		Name:       "0001.part",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 11}},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("5eb63bbbe01eeed093cb22bb8f5acdc3")},
	})
	return s3a
}

func completeU1(s3a *S3ApiServer) (*CompleteMultipartUploadResult, s3err.ErrorCode) {
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("a.bin"), UploadId: aws.String("u1")}
	parts := &CompleteMultipartUpload{Parts: []CompletedPart{{PartNumber: 1, ETag: `"5eb63bbbe01eeed093cb22bb8f5acdc3"`}}}
	return s3a.completeMultipartUpload(httptest.NewRequest("POST", "/b/a.bin?uploadId=u1", nil), input, parts)
}

// A completion whose commit landed but whose call failed answers success, as
// its retry would, instead of a 500 the client cannot interpret.
func TestCompleteMultipartUploadCommittedDespiteFailure(t *testing.T) {
	f := &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}, storeOnFail: true, createFailure: status.Error(codes.Internal, "response lost after commit")}
	s3a := newCompleteTestServer(t, f)

	output, code := completeU1(s3a)
	if code != s3err.ErrNone || output == nil {
		t.Fatalf("code = %v, want success for the committed object", s3err.GetAPIError(code).Code)
	}
	entry := f.entries[s3a.bucketDir("b")]["a.bin"]
	if entry == nil || string(entry.Extended[s3_constants.SeaweedFSUploadId]) != "u1" {
		t.Fatalf("object entry = %v, want the committed upload u1", entry)
	}
	if got, want := aws.StringValue(output.ETag), getEtagFromEntry(entry); got != want {
		t.Errorf("ETag = %s, want the committed object's %s", got, want)
	}
	if _, ok := f.entries[s3a.genUploadsFolder("b")]["u1"]; ok {
		t.Error("the upload directory survived the completed upload")
	}
}

// A failure that left another object at the key still reports the failure.
func TestCompleteMultipartUploadFailureOverAnotherObject(t *testing.T) {
	f := &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}, createFailure: status.Error(codes.Internal, "store failed")}
	s3a := newCompleteTestServer(t, f)
	f.put(s3a.bucketDir("b"), &filer_pb.Entry{Name: "a.bin", Attributes: &filer_pb.FuseAttributes{}, Extended: map[string][]byte{s3_constants.SeaweedFSUploadId: []byte("u0")}})

	if output, code := completeU1(s3a); code != s3err.ErrInternalError || output != nil {
		t.Fatalf("code = %v, output = %v, want InternalError", s3err.GetAPIError(code).Code, output)
	}
	if _, ok := f.entries[s3a.genUploadsFolder("b")]["u1"]; !ok {
		t.Error("the failed completion removed its upload directory")
	}
}
