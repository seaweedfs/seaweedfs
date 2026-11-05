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

// memCompleteFiler keeps entries in memory. A failing CreateEntry still
// stores what storedOnFailure returns, and a failing ObjectTransaction still
// applies its mutations, as a filer whose commit landed before the call failed.
type memCompleteFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu              sync.Mutex
	entries         map[string]map[string]*filer_pb.Entry
	createFailure   error
	storedOnFailure func(*filer_pb.Entry) *filer_pb.Entry
	deletedData     map[string]bool
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
	if f.createFailure == nil {
		f.put(req.Directory, req.Entry)
		return &filer_pb.CreateEntryResponse{}, nil
	}
	if f.storedOnFailure != nil {
		if entry := f.storedOnFailure(req.Entry); entry != nil {
			f.put(req.Directory, entry)
		}
	}
	return nil, f.createFailure
}

func (f *memCompleteFiler) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remove(req.Directory, req.Name, req.IsDeleteData)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *memCompleteFiler) remove(dir, name string, isDeleteData bool) {
	delete(f.entries[dir], name)
	if f.deletedData == nil {
		f.deletedData = map[string]bool{}
	}
	f.deletedData[name] = isDeleteData
}

// ObjectTransaction applies PUT and DELETE as given and points a recompute at
// the transaction's PUT, the newest version.
func (f *memCompleteFiler) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var put *filer_pb.Entry
	for _, m := range req.Mutations {
		switch m.Type {
		case filer_pb.ObjectMutation_PUT:
			put = m.Entry
			f.put(m.Directory, m.Entry)
		case filer_pb.ObjectMutation_DELETE:
			f.remove(m.Directory, m.Name, m.IsDeleteData)
		case filer_pb.ObjectMutation_RECOMPUTE_LATEST:
			f.put(m.Directory, &filer_pb.Entry{Name: m.Name, IsDirectory: true, Extended: map[string][]byte{
				s3_constants.ExtLatestVersionIdKey:       put.Extended[s3_constants.ExtVersionIdKey],
				s3_constants.ExtLatestVersionFileNameKey: []byte(put.Name),
			}})
		}
	}
	if f.createFailure != nil {
		return nil, f.createFailure
	}
	return &filer_pb.ObjectTransactionResponse{}, nil
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
	// An earlier upload of part 1 the completion does not use.
	f.put(uploadsDir+"/u1", &filer_pb.Entry{
		Name:       "0001_old.part",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,02637037d6", Size: 11}},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("0f343b0931126a20f133d67c2b018a3b")},
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
	f := &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}, createFailure: status.Error(codes.Internal, "response lost after commit"),
		storedOnFailure: func(entry *filer_pb.Entry) *filer_pb.Entry { return entry }}
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
	if deleteData, ok := f.deletedData["0001_old.part"]; !ok || !deleteData {
		t.Errorf("unused part deleted = %v, with data = %v; want its data freed as on success", ok, deleteData)
	}
	if deleteData, ok := f.deletedData["u1"]; !ok || deleteData {
		t.Errorf("upload directory deleted = %v, with data = %v; want metadata only, its parts are the object's", ok, deleteData)
	}
}

func newVersionedCommitTestServer(t *testing.T, versioning string) (*S3ApiServer, *memCompleteFiler) {
	t.Helper()
	f := &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}}
	s3a := newFailoverTestServer(t, startFakeFiler(t, f))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Versioning: versioning})
	return s3a, f
}

func pointLatestVersion(f *memCompleteFiler, s3a *S3ApiServer, versionId string, version *filer_pb.Entry) {
	f.put(s3a.bucketDir("b"), &filer_pb.Entry{Name: "a.bin" + s3_constants.VersionsFolder, IsDirectory: true, Extended: map[string][]byte{
		s3_constants.ExtLatestVersionIdKey:       []byte(versionId),
		s3_constants.ExtLatestVersionFileNameKey: []byte(version.Name),
	}})
	f.put(s3a.bucketDir("b")+"/a.bin"+s3_constants.VersionsFolder, version)
}

// On a versioned bucket the committed version is read through the latest
// pointer, and the answer carries its version id and checksum.
func TestCommittedCompletionReadsTheLatestVersion(t *testing.T) {
	s3a, f := newVersionedCommitTestServer(t, s3_constants.VersioningEnabled)
	pointLatestVersion(f, s3a, "3a1b", &filer_pb.Entry{Name: "v_3a1b", Attributes: &filer_pb.FuseAttributes{FileSize: 11}, Extended: map[string][]byte{
		s3_constants.SeaweedFSUploadId:    []byte("u1"),
		s3_constants.ExtVersionIdKey:      []byte("3a1b"),
		s3_constants.ExtETagKey:           []byte("8a6d5f0e5c8f7b2f4e1d0c9b8a7f6e5d-1"),
		s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC32C),
		s3_constants.ExtChecksumValue:     []byte("fx4FQw==-1"),
		s3_constants.ExtChecksumType:      []byte(s3_constants.ChecksumTypeComposite),
	}})

	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("a.bin"), UploadId: aws.String("u1")}
	output, entry := s3a.committedCompletion(httptest.NewRequest("POST", "/b/a.bin?uploadId=u1", nil), input)
	if output == nil || entry == nil {
		t.Fatal("did not find the committed version")
	}
	if got := aws.StringValue(output.VersionId); got != "3a1b" {
		t.Errorf("VersionId = %q, want 3a1b", got)
	}
	if output.ChecksumCRC32C != "fx4FQw==-1" || output.ChecksumType != s3_constants.ChecksumTypeComposite {
		t.Errorf("checksum = %q %q, want the committed version's", output.ChecksumCRC32C, output.ChecksumType)
	}
}

// A versioned finalize whose transaction landed but whose call failed answers
// with the committed version's id and checksum.
func TestCompleteMultipartUploadVersionedCommittedDespiteFailure(t *testing.T) {
	f := &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}, createFailure: status.Error(codes.Internal, "response lost after commit")}
	s3a := newCompleteTestServer(t, f)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Versioning: s3_constants.VersioningEnabled})
	uploadsDir := s3a.genUploadsFolder("b")
	f.entries[uploadsDir]["u1"].Extended[s3_constants.ExtChecksumAlgorithm] = []byte(s3_constants.AmzChecksumCRC32C)
	part := f.entries[uploadsDir+"/u1"]["0001.part"]
	part.Extended[s3_constants.ExtChecksumAlgorithm] = []byte(s3_constants.AmzChecksumCRC32C)
	part.Extended[s3_constants.ExtChecksumValue] = []byte("yZRlqg==")

	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("a.bin"), UploadId: aws.String("u1")}
	parts := &CompleteMultipartUpload{Parts: []CompletedPart{{PartNumber: 1, ETag: `"5eb63bbbe01eeed093cb22bb8f5acdc3"`, ChecksumResult: ChecksumResult{ChecksumCRC32C: "yZRlqg=="}}}}
	output, code := s3a.completeMultipartUpload(httptest.NewRequest("POST", "/b/a.bin?uploadId=u1", nil), input, parts)
	if code != s3err.ErrNone || output == nil {
		t.Fatalf("code = %v, want success for the committed version", s3err.GetAPIError(code).Code)
	}
	pointer := f.entries[s3a.bucketDir("b")]["a.bin"+s3_constants.VersionsFolder]
	version := f.entries[s3a.bucketDir("b")+"/a.bin"+s3_constants.VersionsFolder][string(pointer.Extended[s3_constants.ExtLatestVersionFileNameKey])]
	if version == nil {
		t.Fatal("the transaction did not store the version")
	}
	if got, want := aws.StringValue(output.VersionId), string(version.Extended[s3_constants.ExtVersionIdKey]); got == "" || got != want {
		t.Errorf("VersionId = %q, want the committed version's %q", got, want)
	}
	if got, want := output.ChecksumCRC32C, string(version.Extended[s3_constants.ExtChecksumValue]); got == "" || got != want {
		t.Errorf("ChecksumCRC32C = %q, want the committed version's %q", got, want)
	}
	if output.ChecksumType != s3_constants.ChecksumTypeComposite {
		t.Errorf("ChecksumType = %q, want %q", output.ChecksumType, s3_constants.ChecksumTypeComposite)
	}
}

// Suspended versioning whose null write left the delete marker current reads
// as deleted, so the upload's object behind it is not a completed answer.
func TestCommittedCompletionBehindADeleteMarker(t *testing.T) {
	s3a, f := newVersionedCommitTestServer(t, s3_constants.VersioningSuspended)
	f.put(s3a.bucketDir("b"), &filer_pb.Entry{Name: "a.bin", Attributes: &filer_pb.FuseAttributes{FileSize: 11}, Extended: map[string][]byte{
		s3_constants.SeaweedFSUploadId: []byte("u1"),
		s3_constants.ExtVersionIdKey:   []byte("null"),
	}})
	pointLatestVersion(f, s3a, "7c2d", &filer_pb.Entry{Name: "v_7c2d", Attributes: &filer_pb.FuseAttributes{}, Extended: map[string][]byte{
		s3_constants.ExtVersionIdKey:    []byte("7c2d"),
		s3_constants.ExtDeleteMarkerKey: []byte("true"),
	}})

	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("a.bin"), UploadId: aws.String("u1")}
	if output, _ := s3a.committedCompletion(httptest.NewRequest("POST", "/b/a.bin?uploadId=u1", nil), input); output != nil {
		t.Fatalf("answered %+v for a key that reads as deleted", output)
	}
}

// The object found may be another attempt's commit of the same upload, whose
// parts this attempt can count as unused: their data must stay.
func TestCompleteMultipartUploadKeepsPartDataUnderAnotherAttemptsCommit(t *testing.T) {
	other := &filer_pb.Entry{
		Name:       "a.bin",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,02637037d6", Size: 11}},
		Extended: map[string][]byte{
			s3_constants.SeaweedFSUploadId: []byte("u1"),
			s3_constants.ExtETagKey:        []byte("8a6d5f0e5c8f7b2f4e1d0c9b8a7f6e5d-1"),
		},
	}
	f := &memCompleteFiler{entries: map[string]map[string]*filer_pb.Entry{}, createFailure: status.Error(codes.Internal, "lost the race"),
		storedOnFailure: func(*filer_pb.Entry) *filer_pb.Entry { return other }}
	s3a := newCompleteTestServer(t, f)

	output, code := completeU1(s3a)
	if code != s3err.ErrNone || output == nil {
		t.Fatalf("code = %v, want success for the upload's committed object", s3err.GetAPIError(code).Code)
	}
	if got, want := aws.StringValue(output.ETag), getEtagFromEntry(other); got != want {
		t.Errorf("ETag = %s, want the committed object's %s", got, want)
	}
	if deleteData, ok := f.deletedData["0001_old.part"]; ok && deleteData {
		t.Error("freed the data of a part the committed object references")
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
