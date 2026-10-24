package s3api

import (
	"context"
	"math"
	"sort"
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
	parts := append([]*filer_pb.Entry(nil), f.parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Name < parts[j].Name })
	var sent uint32
	for _, entry := range parts {
		name := entry.Name
		if req.StartFromFileName != "" {
			if name < req.StartFromFileName {
				continue
			}
			if name == req.StartFromFileName && !req.InclusiveStartFrom {
				continue
			}
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
		sent++
		if req.Limit > 0 && sent >= req.Limit {
			return nil
		}
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

// S3 part-number-marker is exclusive. Part objects are stored as
// NNNN_<uuid>.part, which sorts after the NNNN.part prefix, so a page of
// max-parts=1 must advance to the next part number instead of repeating it.
func TestListPartsPartNumberMarkerIsExclusive(t *testing.T) {
	s3a := newListPartsServer(t, &fakePartsFiler{
		uploadEntry: uploadRecordEntry("open-upload"),
		parts: []*filer_pb.Entry{
			partEntry("0001_11111111-1111-1111-1111-111111111111.part", 5),
			partEntry("0002_22222222-2222-2222-2222-222222222222.part", 5),
			partEntry("0003_33333333-3333-3333-3333-333333333333.part", 5),
		},
	})

	input := listPartsInput("open-upload")
	input.PartNumberMarker = aws.Int64(1)
	input.MaxParts = aws.Int64(1)
	output, code := s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if got := partNumbers(output); len(got) != 1 || got[0] != 2 {
		t.Fatalf("parts = %v, want [2]", got)
	}
	if output.IsTruncated == nil || !*output.IsTruncated {
		t.Fatal("IsTruncated = false, want true")
	}
	if output.NextPartNumberMarker == nil || *output.NextPartNumberMarker != 2 {
		t.Fatalf("NextPartNumberMarker = %v, want 2", output.NextPartNumberMarker)
	}

	input.MaxParts = aws.Int64(1000)
	output, code = s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("code = %v, want ErrNone", code)
	}
	if got := partNumbers(output); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("parts = %v, want [2 3]", got)
	}
}

// A client paging one part at a time must walk marker=0 through every part and
// terminate: each page returns the next part and a NextPartNumberMarker that
// the following page starts strictly after.
func TestListPartsPaginationWithUUIDParts(t *testing.T) {
	s3a := newListPartsServer(t, &fakePartsFiler{
		uploadEntry: uploadRecordEntry("open-upload"),
		parts: []*filer_pb.Entry{
			partEntry("0001_c3a1e204-1111-1111-1111-111111111111.part", 5),
			partEntry("0002_d4b2f315-2222-2222-2222-222222222222.part", 5),
			partEntry("0003_e5c3a426-3333-3333-3333-333333333333.part", 5),
		},
	})

	input := listPartsInput("open-upload")
	input.MaxParts = aws.Int64(1)

	for page, want := range []int64{1, 2, 3} {
		output, code := s3a.listObjectParts(input)
		if code != s3err.ErrNone {
			t.Fatalf("page %d code = %v, want ErrNone", page+1, code)
		}
		if got := partNumbers(output); len(got) != 1 || got[0] != want {
			t.Fatalf("page %d parts = %v, want [%d]", page+1, got, want)
		}
		if page < 2 {
			if output.IsTruncated == nil || !*output.IsTruncated {
				t.Fatalf("page %d IsTruncated = false, want true", page+1)
			}
			if output.NextPartNumberMarker == nil || *output.NextPartNumberMarker != want {
				t.Fatalf("page %d NextPartNumberMarker = %v, want %d", page+1, output.NextPartNumberMarker, want)
			}
			input.PartNumberMarker = output.NextPartNumberMarker
		} else {
			if output.IsTruncated == nil || *output.IsTruncated {
				t.Fatalf("page %d IsTruncated = true, want false", page+1)
			}
			if output.NextPartNumberMarker != nil {
				t.Fatalf("page %d NextPartNumberMarker = %v, want nil", page+1, output.NextPartNumberMarker)
			}
		}
	}

	// a marker at the int64 ceiling cannot be incremented; answer an empty,
	// untruncated page instead of wrapping the start name negative
	input.PartNumberMarker = aws.Int64(math.MaxInt64)
	output, code := s3a.listObjectParts(input)
	if code != s3err.ErrNone {
		t.Fatalf("max marker code = %v, want ErrNone", code)
	}
	if len(output.Part) != 0 {
		t.Fatalf("max marker parts = %d, want 0", len(output.Part))
	}
	if output.IsTruncated == nil || *output.IsTruncated {
		t.Fatal("max marker IsTruncated = true, want false")
	}
}

func partNumbers(output *ListPartsResult) []int64 {
	if output == nil {
		return nil
	}
	nums := make([]int64, len(output.Part))
	for i, part := range output.Part {
		if part.PartNumber != nil {
			nums[i] = *part.PartNumber
		}
	}
	return nums
}
