package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// uploadsListingFiler also lists the .uploads folder itself, which
// ListMultipartUploads reads.
type uploadsListingFiler struct {
	*fakePartsFiler
}

func (f uploadsListingFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	if req.Directory == f.uploadsDir {
		return stream.Send(&filer_pb.ListEntriesResponse{Entry: f.uploadEntry})
	}
	return f.fakePartsFiler.ListEntries(req, stream)
}

// max-parts, part-number-marker and max-uploads must be integers, as their
// InvalidArgument errors say and as max-keys is already validated. A value that
// does not parse used to be read as 0: a max-parts of 0 reaches the filer as
// "no limit" and lists every part, a part-number-marker of 0 restarts the
// listing from the first part, and a max-uploads of 0 lists no uploads.
func TestMultipartListingsRejectNonIntegerArguments(t *testing.T) {
	const bucket, object = "b", "a.bin"
	f := &fakePartsFiler{parts: []*filer_pb.Entry{
		partEntry("0001.part", 5),
		partEntry("0002.part", 5),
		partEntry("0003.part", 5),
	}}
	f.uploadsDir = (&S3ApiServer{option: &S3ApiServerOption{}}).genUploadsFolder(bucket)
	s3a := newFailoverTestServer(t, startFakeFiler(t, uploadsListingFiler{f}))
	s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket})
	uploadID := s3a.generateUploadID(object)
	f.uploadEntry = uploadRecordEntry(uploadID)

	cases := []struct {
		name    string
		query   string
		handler http.HandlerFunc
		object  string
	}{
		{"ListParts max-parts", "uploadId=" + uploadID + "&max-parts=abc", s3a.ListObjectPartsHandler, object},
		{"ListParts max-parts out of range", "uploadId=" + uploadID + "&max-parts=2147483648", s3a.ListObjectPartsHandler, object},
		{"ListParts part-number-marker", "uploadId=" + uploadID + "&part-number-marker=abc", s3a.ListObjectPartsHandler, object},
		{"ListParts part-number-marker out of range", "uploadId=" + uploadID + "&part-number-marker=2147483648", s3a.ListObjectPartsHandler, object},
		{"ListMultipartUploads max-uploads", "uploads&max-uploads=abc", s3a.ListMultipartUploadsHandler, ""},
		{"ListMultipartUploads max-uploads out of range", "uploads&max-uploads=2147483648", s3a.ListMultipartUploadsHandler, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+tc.object+"?"+tc.query, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": tc.object})
			rr := httptest.NewRecorder()
			tc.handler(rr, req)

			if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "<Code>InvalidArgument</Code>") {
				t.Fatalf("got %d %s, want 400 InvalidArgument", rr.Code, rr.Body.String())
			}
		})
	}

	// well-formed arguments still page as before
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+object+"?uploadId="+uploadID+"&part-number-marker=1&max-parts=1", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
	rr := httptest.NewRecorder()
	s3a.ListObjectPartsHandler(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "<PartNumber>2</PartNumber>") || strings.Contains(rr.Body.String(), "<PartNumber>3</PartNumber>") {
		t.Fatalf("got %d %s, want 200 listing only part 2", rr.Code, rr.Body.String())
	}
}
