package s3api

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeAbortFiler answers the three calls abortMultipartUpload makes: the
// .uploads/<id> record lookup, the completed-object lookup, the .versions
// listing, and records the DeleteEntry request it receives.
type fakeAbortFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	uploadsDir  string
	uploadEntry *filer_pb.Entry
	objectDir   string
	objectName  string
	objectEntry *filer_pb.Entry
	objectErr   error
	versionsDir string
	versions    []*filer_pb.Entry
	listErr     error
	deleteReq   *filer_pb.DeleteEntryRequest
}

func (f *fakeAbortFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	switch req.Directory {
	case f.uploadsDir:
		if f.uploadEntry != nil && req.Name == f.uploadEntry.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: f.uploadEntry}, nil
		}
		return nil, filer_pb.ErrNotFound
	case f.objectDir:
		if f.objectErr != nil {
			return nil, f.objectErr
		}
		if req.Name == f.objectName && f.objectEntry != nil {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: f.objectEntry}, nil
		}
		return nil, filer_pb.ErrNotFound
	}
	return nil, status.Errorf(codes.Internal, "unexpected lookup in %s", req.Directory)
}

func (f *fakeAbortFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	if req.Directory != f.versionsDir {
		return status.Errorf(codes.Internal, "unexpected listing of %s", req.Directory)
	}
	if f.listErr != nil {
		return f.listErr
	}
	for _, entry := range f.versions {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeAbortFiler) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.deleteReq = req
	return &filer_pb.DeleteEntryResponse{}, nil
}

func newAbortTestServer(t *testing.T, f *fakeAbortFiler) *S3ApiServer {
	t.Helper()
	bucketDir := (&S3ApiServer{option: &S3ApiServerOption{}}).bucketDir("b")
	f.uploadsDir = bucketDir + "/" + s3_constants.MultipartUploadsFolder
	f.objectDir = bucketDir
	f.objectName = "a.bin"
	f.versionsDir = bucketDir + "/a.bin" + s3_constants.VersionsFolder
	return newFailoverTestServer(t, startFakeFiler(t, f))
}

func abortInput(uploadId string) *s3.AbortMultipartUploadInput {
	return &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("b"),
		Key:      aws.String("a.bin"),
		UploadId: aws.String(uploadId),
	}
}

func versionEntry(name, uploadId string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:     name,
		Extended: map[string][]byte{s3_constants.SeaweedFSUploadId: []byte(uploadId)},
	}
}

// A leftover .uploads/<id> whose object entry still carries the upload id
// shares chunks with that object; abort must drop the metadata only.
func TestAbortCompletedUploadDeletesMetadataOnly(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		objectEntry: versionEntry("a.bin", "up1"),
	}
	s3a := newAbortTestServer(t, f)

	_, code := s3a.abortMultipartUpload(abortInput("up1"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if f.deleteReq == nil || f.deleteReq.IsDeleteData {
		t.Fatalf("deleteReq = %+v, want IsDeleteData=false", f.deleteReq)
	}
}

// A version file carrying the upload id is the same case: the versioned
// object's chunks are the part chunks.
func TestAbortCompletedVersionDeletesMetadataOnly(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		versions:    []*filer_pb.Entry{versionEntry("v_123", "up1")},
	}
	s3a := newAbortTestServer(t, f)

	_, code := s3a.abortMultipartUpload(abortInput("up1"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if f.deleteReq == nil || f.deleteReq.IsDeleteData {
		t.Fatalf("deleteReq = %+v, want IsDeleteData=false", f.deleteReq)
	}
}

// An upload record missing its object-key stamp can still have completed;
// the abort's Key is the fallback lookup path.
func TestAbortCompletedUploadWithoutRecordedKey(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: &filer_pb.Entry{Name: "up1", IsDirectory: true},
		objectEntry: versionEntry("a.bin", "up1"),
	}
	s3a := newAbortTestServer(t, f)

	_, code := s3a.abortMultipartUpload(abortInput("up1"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if f.deleteReq == nil || f.deleteReq.IsDeleteData {
		t.Fatalf("deleteReq = %+v, want IsDeleteData=false", f.deleteReq)
	}
}

// An upload that never completed owns its part chunks; abort frees them.
func TestAbortOpenUploadDeletesData(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		objectEntry: versionEntry("a.bin", "other-upload"),
	}
	s3a := newAbortTestServer(t, f)

	_, code := s3a.abortMultipartUpload(abortInput("up1"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if f.deleteReq == nil || !f.deleteReq.IsDeleteData {
		t.Fatalf("deleteReq = %+v, want IsDeleteData=true", f.deleteReq)
	}
}

// When the completed check cannot decide, abort must refuse rather than
// risk freeing chunks a live object references.
func TestAbortUndecidableCheckRefuses(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		objectErr:   status.Error(codes.Unavailable, "store down"),
	}
	s3a := newAbortTestServer(t, f)

	_, code := s3a.abortMultipartUpload(abortInput("up1"))

	if code != s3err.ErrInternalError {
		t.Fatalf("code = %v, want ErrInternalError", code)
	}
	if f.deleteReq != nil {
		t.Fatalf("delete issued despite undecidable check: %+v", f.deleteReq)
	}
}

// No upload directory at all answers success, the same as before.
func TestAbortGoneUploadAnswersSuccess(t *testing.T) {
	s3a := newAbortTestServer(t, &fakeAbortFiler{})

	_, code := s3a.abortMultipartUpload(abortInput("gone"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
}

// A failed .versions listing is undecidable the same way a failed object
// lookup is: refuse instead of guessing.
func TestAbortVersionsListErrorRefuses(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		listErr:     status.Error(codes.Internal, "store down"),
	}
	s3a := newAbortTestServer(t, f)

	_, code := s3a.abortMultipartUpload(abortInput("up1"))

	if code != s3err.ErrInternalError {
		t.Fatalf("code = %v, want ErrInternalError", code)
	}
	if f.deleteReq != nil {
		t.Fatalf("delete issued despite undecidable check: %+v", f.deleteReq)
	}
}

func lifecycleAbortRequest(uploadId string) *s3_lifecycle_pb.LifecycleDeleteRequest {
	return &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:     "b",
		ObjectPath: s3_constants.MultipartUploadsFolder + "/" + uploadId,
	}
}

func TestLifecycleAbortCompletedUploadDeletesMetadataOnly(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		objectEntry: versionEntry("a.bin", "up1"),
	}
	s3a := newAbortTestServer(t, f)

	resp, err := s3a.lifecycleAbortMPU(context.Background(), lifecycleAbortRequest("up1"))

	if err != nil || resp.Outcome != s3_lifecycle_pb.LifecycleDeleteOutcome_DONE {
		t.Fatalf("resp = %v, err = %v, want DONE", resp, err)
	}
	if f.deleteReq == nil || f.deleteReq.IsDeleteData {
		t.Fatalf("deleteReq = %+v, want IsDeleteData=false", f.deleteReq)
	}
}

func TestLifecycleAbortUndecidableCheckRetriesLater(t *testing.T) {
	f := &fakeAbortFiler{
		uploadEntry: uploadRecordEntry("up1"),
		objectErr:   status.Error(codes.Unavailable, "store down"),
	}
	s3a := newAbortTestServer(t, f)

	resp, err := s3a.lifecycleAbortMPU(context.Background(), lifecycleAbortRequest("up1"))

	if err != nil || resp.Outcome != s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER {
		t.Fatalf("resp = %v, err = %v, want RETRY_LATER", resp, err)
	}
	if f.deleteReq != nil {
		t.Fatalf("delete issued despite undecidable check: %+v", f.deleteReq)
	}
}
