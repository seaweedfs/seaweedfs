package s3api

import (
	"context"
	"math"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakePartsFiler serves the two lookups listObjectParts makes: the .uploads/<id>
// record via LookupDirectoryEntry, and the part listing via ListEntries. A nil
// uploadEntry answers the lookup with not-found, the way a filer does after
// complete/abort removed the directory or for an upload id that never existed.
// Any other directory is refused, so a path built wrong fails the test rather
// than passing on the fixture.
type fakePartsFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	uploadsDir  string
	uploadEntry *filer_pb.Entry
	parts       []*filer_pb.Entry
}

func (f *fakePartsFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if req.Directory != f.uploadsDir {
		return nil, status.Errorf(codes.Internal, "unexpected lookup in %s", req.Directory)
	}
	if f.uploadEntry != nil && req.Name == f.uploadEntry.Name {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: f.uploadEntry}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (f *fakePartsFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	if f.uploadEntry == nil || req.Directory != f.uploadsDir+"/"+f.uploadEntry.Name {
		return status.Errorf(codes.Internal, "unexpected listing of %s", req.Directory)
	}
	// most stores list a missing directory as empty rather than erroring, which
	// is exactly the behavior under test
	count := uint32(0)
	for _, entry := range f.parts {
		if req.StartFromFileName != "" {
			if req.InclusiveStartFrom {
				if entry.Name < req.StartFromFileName {
					continue
				}
			} else {
				if entry.Name <= req.StartFromFileName {
					continue
				}
			}
		}
		if req.Limit > 0 && count >= req.Limit {
			break
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
		count++
	}
	return nil
}

func newListPartsServer(t *testing.T, f *fakePartsFiler) *S3ApiServer {
	t.Helper()
	f.uploadsDir = (&S3ApiServer{option: &S3ApiServerOption{}}).genUploadsFolder("b")
	return newFailoverTestServer(t, startFakeFiler(t, f))
}

func uploadRecordEntry(uploadId string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        uploadId,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("a.bin")},
	}
}

func partEntry(name string, size uint64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileSize: size},
	}
}

func listPartsInput(uploadId string) *s3.ListPartsInput {
	return &s3.ListPartsInput{
		Bucket:           aws.String("b"),
		Key:              aws.String("a.bin"),
		UploadId:         aws.String(uploadId),
		MaxParts:         aws.Int64(1000),
		PartNumberMarker: aws.Int64(0),
	}
}

// A completed (or aborted, or never-created) upload has no .uploads/<id>
// record, and AWS answers ListParts on it with NoSuchUpload. Answering 200
// with an empty list instead is indistinguishable from an open upload with no
// parts yet, and clients that derive upload state from ListParts (tusd derives
// the resumable-upload offset from the part sizes) read every completed upload
// as one with zero bytes received.
func TestListPartsGoneUploadAnswersNoSuchUpload(t *testing.T) {
	s3a := newListPartsServer(t, &fakePartsFiler{})

	_, code := s3a.listObjectParts(listPartsInput("gone-upload"))

	if code != s3err.ErrNoSuchUpload {
		t.Fatalf("code = %v, want ErrNoSuchUpload", code)
	}
}

// Only createMultipartUpload stamps the destination key on the record; a
// directory a late part write resurrected after an abort is not an upload.
func TestListPartsResurrectedDirectoryAnswersNoSuchUpload(t *testing.T) {
	entry := uploadRecordEntry("resurrected-upload")
	entry.Extended = nil
	s3a := newListPartsServer(t, &fakePartsFiler{uploadEntry: entry})

	_, code := s3a.listObjectParts(listPartsInput("resurrected-upload"))

	if code != s3err.ErrNoSuchUpload {
		t.Fatalf("code = %v, want ErrNoSuchUpload", code)
	}
}

// An open upload that has not received a part yet must keep answering 200 with
// an empty list: the upload directory is the marker that the upload exists,
// and an offset of zero is the truth here.
func TestListPartsOpenUploadWithNoPartsAnswersEmptyList(t *testing.T) {
	s3a := newListPartsServer(t, &fakePartsFiler{
		uploadEntry: uploadRecordEntry("open-upload"),
	})

	output, code := s3a.listObjectParts(listPartsInput("open-upload"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if len(output.Part) != 0 {
		t.Fatalf("parts = %d, want 0", len(output.Part))
	}
}

func TestListPartsOpenUploadListsParts(t *testing.T) {
	s3a := newListPartsServer(t, &fakePartsFiler{
		uploadEntry: uploadRecordEntry("open-upload"),
		parts: []*filer_pb.Entry{
			partEntry("0001.part", 5),
			partEntry("0002.part", 3),
		},
	})

	output, code := s3a.listObjectParts(listPartsInput("open-upload"))

	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if len(output.Part) != 2 {
		t.Fatalf("parts = %d, want 2", len(output.Part))
	}
	if *output.Part[0].PartNumber != 1 || *output.Part[0].Size != 5 {
		t.Fatalf("part[0] = %d/%d, want 1/5", *output.Part[0].PartNumber, *output.Part[0].Size)
	}
}

func TestListPartsPaginationWithUUIDParts(t *testing.T) {
	s3a := newListPartsServer(t, &fakePartsFiler{
		uploadEntry: uploadRecordEntry("open-upload"),
		parts: []*filer_pb.Entry{
			partEntry("0001_c3a1e204.part", 5),
			partEntry("0002_d4b2f315.part", 5),
			partEntry("0003_e5c3a426.part", 5),
		},
	})

	// Page 1: marker = 0, MaxParts = 1. Returns part 1, IsTruncated = true, NextMarker = 1
	input := listPartsInput("open-upload")
	input.PartNumberMarker = aws.Int64(0)
	input.MaxParts = aws.Int64(1)

	output, code := s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("page 1 code = %v, want ErrNone", code)
	}
	if len(output.Part) != 1 || *output.Part[0].PartNumber != 1 {
		t.Fatalf("page 1 parts = %v, want [part 1]", output.Part)
	}
	if !*output.IsTruncated {
		t.Fatalf("page 1 IsTruncated = false, want true")
	}
	if output.NextPartNumberMarker == nil || *output.NextPartNumberMarker != 1 {
		t.Fatalf("page 1 NextPartNumberMarker = %v, want 1", output.NextPartNumberMarker)
	}

	// Page 2: marker = 1, MaxParts = 1. Advances to part 2, IsTruncated = true, NextMarker = 2
	input.PartNumberMarker = output.NextPartNumberMarker
	output, code = s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("page 2 code = %v, want ErrNone", code)
	}
	if len(output.Part) != 1 || *output.Part[0].PartNumber != 2 {
		t.Fatalf("page 2 parts = %v, want [part 2]", output.Part)
	}
	if !*output.IsTruncated {
		t.Fatalf("page 2 IsTruncated = false, want true")
	}
	if output.NextPartNumberMarker == nil || *output.NextPartNumberMarker != 2 {
		t.Fatalf("page 2 NextPartNumberMarker = %v, want 2", output.NextPartNumberMarker)
	}

	// Page 3: marker = 2, MaxParts = 1. Returns part 3, IsTruncated = false, NextMarker = nil
	input.PartNumberMarker = output.NextPartNumberMarker
	output, code = s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("page 3 code = %v, want ErrNone", code)
	}
	if len(output.Part) != 1 || *output.Part[0].PartNumber != 3 {
		t.Fatalf("page 3 parts = %v, want [part 3]", output.Part)
	}
	if *output.IsTruncated {
		t.Fatalf("page 3 IsTruncated = true, want false")
	}
	if output.NextPartNumberMarker != nil {
		t.Fatalf("page 3 NextPartNumberMarker = %v, want nil", output.NextPartNumberMarker)
	}

	// Extreme boundary test: MaxInt marker returns empty unpageable response safely
	input.PartNumberMarker = aws.Int64(math.MaxInt64)
	output, code = s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("max marker code = %v, want ErrNone", code)
	}
	if len(output.Part) != 0 {
		t.Fatalf("max marker parts = %d, want 0", len(output.Part))
	}
	if *output.IsTruncated {
		t.Fatalf("max marker IsTruncated = true, want false")
	}
}
