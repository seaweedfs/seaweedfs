package s3tables

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteEntryStub answers DeleteEntry the way the filer does: a rejected delete
// comes back as a nil transport error with the reason in the response.
type deleteEntryStub struct {
	filer_pb.SeaweedFilerClient
	resp *filer_pb.DeleteEntryResponse
	err  error
	req  *filer_pb.DeleteEntryRequest
}

func (s *deleteEntryStub) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	s.req = req
	return s.resp, s.err
}

func TestDeleteDirectoryReportsRejectedDelete(t *testing.T) {
	stub := &deleteEntryStub{resp: &filer_pb.DeleteEntryResponse{Error: "fail to delete non-empty folder"}}

	err := (&S3TablesHandler{}).deleteDirectory(context.Background(), stub, "/buckets/b/ns/t")

	require.Error(t, err, "a delete the filer rejected must not be reported as success")
	assert.Contains(t, err.Error(), "fail to delete non-empty folder")
}

func TestDeleteDirectoryToleratesMissingEntry(t *testing.T) {
	t.Run("reported in the response", func(t *testing.T) {
		stub := &deleteEntryStub{resp: &filer_pb.DeleteEntryResponse{Error: filer_pb.ErrNotFound.Error()}}
		assert.NoError(t, (&S3TablesHandler{}).deleteDirectory(context.Background(), stub, "/buckets/b/ns/t"))
	})
	t.Run("reported as a transport error", func(t *testing.T) {
		stub := &deleteEntryStub{err: status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())}
		assert.NoError(t, (&S3TablesHandler{}).deleteDirectory(context.Background(), stub, "/buckets/b/ns/t"))
	})
}

func TestDeleteDirectoryPropagatesTransportError(t *testing.T) {
	stub := &deleteEntryStub{err: errors.New("filer unreachable")}

	err := (&S3TablesHandler{}).deleteDirectory(context.Background(), stub, "/buckets/b/ns/t")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "filer unreachable")
}

// deleteDirectory drops a whole subtree, so the delete has to stay recursive and
// take the data with it.
func TestDeleteDirectoryDeletesTheSubtree(t *testing.T) {
	stub := &deleteEntryStub{resp: &filer_pb.DeleteEntryResponse{}}

	require.NoError(t, (&S3TablesHandler{}).deleteDirectory(context.Background(), stub, "/buckets/b/ns/t"))

	require.NotNil(t, stub.req)
	assert.Equal(t, "/buckets/b/ns", stub.req.Directory)
	assert.Equal(t, "t", stub.req.Name)
	assert.True(t, stub.req.IsDeleteData)
	assert.True(t, stub.req.IsRecursive)
	assert.True(t, stub.req.IgnoreRecursiveError)
}
