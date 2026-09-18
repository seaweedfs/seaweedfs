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
	lookupResp *filer_pb.LookupDirectoryEntryResponse
	lookupErr  error
	listResp   []*filer_pb.Entry
	listErr    error
	lookupReq  *filer_pb.LookupDirectoryEntryRequest
	listReq    *filer_pb.ListEntriesRequest
}

func (s *stubFilerSvc) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.lookupReq = req
	return s.lookupResp, s.lookupErr
}

func (s *stubFilerSvc) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	s.listReq = req
	if s.listErr != nil {
		return nil, s.listErr
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
