package s3vectors

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// DescribeFn resolves a table id to its Lance location and the storage
// options engines need to open it — the gateway supplies the catalog's
// in-process describe, so no unauthenticated HTTP call is needed.
type DescribeFn func(ctx context.Context, id []string) (string, map[string]string, error)

// SetDescribe resolves every table this server sends to the engine, so the
// engine opens it without a catalog round trip of its own.
func (s *Server) SetDescribe(describe DescribeFn) {
	s.vectors = &describedClient{VectorServiceClient: s.vectors, describe: describe, encrypts: s.bucketEncrypts}
}

// bucketEncrypts reports whether the vector bucket declares SSE-S3, which the
// S3 write path applies to every file of a Lance bucket and the engine must
// apply to the files it writes itself.
func (s *Server) bucketEncrypts(ctx context.Context, bucket string) (bool, error) {
	record, err := s.vectorBucket(ctx, bucket)
	if err != nil {
		return false, err
	}
	return s3tables.DeclaredEncryption(record.Format, record.EncryptionConfiguration) != "", nil
}

// describedClient fills in the table's location and the bucket's encryption
// before a request reaches the engine.
type describedClient struct {
	vector_pb.VectorServiceClient
	describe DescribeFn
	encrypts func(ctx context.Context, bucket string) (bool, error)
}

func (c *describedClient) resolve(ctx context.Context, table *vector_pb.Table) error {
	if table == nil || table.Location != "" || len(table.Id) == 0 {
		return nil
	}
	location, _, err := c.describe(ctx, table.Id)
	if err != nil {
		return describeErr(err)
	}
	sseS3, err := c.encrypts(ctx, table.Id[0])
	if err != nil {
		return describeErr(err)
	}
	table.Location, table.SseS3 = location, sseS3
	return nil
}

// describeErr gives a catalog failure the gRPC code the engine would have
// answered with, so vectorErr maps it the same way.
func describeErr(err error) error {
	var apiErr *s3tables.S3TablesError
	if !errors.As(err, &apiErr) {
		return status.Error(codes.Internal, err.Error())
	}
	switch apiErr.Type {
	case s3tables.ErrCodeNoSuchTable, s3tables.ErrCodeNoSuchNamespace, s3tables.ErrCodeNoSuchBucket:
		return status.Error(codes.NotFound, apiErr.Message)
	case s3tables.ErrCodeAccessDenied:
		return status.Error(codes.PermissionDenied, apiErr.Message)
	}
	return status.Error(codes.Internal, apiErr.Message)
}

func (c *describedClient) CreateIndex(ctx context.Context, in *vector_pb.CreateIndexRequest, opts ...grpc.CallOption) (*vector_pb.CreateIndexResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.CreateIndex(ctx, in, opts...)
}

func (c *describedClient) DropIndex(ctx context.Context, in *vector_pb.DropIndexRequest, opts ...grpc.CallOption) (*vector_pb.DropIndexResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.DropIndex(ctx, in, opts...)
}

func (c *describedClient) PutVectors(ctx context.Context, in *vector_pb.PutVectorsRequest, opts ...grpc.CallOption) (*vector_pb.PutVectorsResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.PutVectors(ctx, in, opts...)
}

func (c *describedClient) GetVectors(ctx context.Context, in *vector_pb.GetVectorsRequest, opts ...grpc.CallOption) (*vector_pb.GetVectorsResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.GetVectors(ctx, in, opts...)
}

func (c *describedClient) DeleteVectors(ctx context.Context, in *vector_pb.DeleteVectorsRequest, opts ...grpc.CallOption) (*vector_pb.DeleteVectorsResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.DeleteVectors(ctx, in, opts...)
}

func (c *describedClient) ListVectors(ctx context.Context, in *vector_pb.ListVectorsRequest, opts ...grpc.CallOption) (*vector_pb.ListVectorsResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.ListVectors(ctx, in, opts...)
}

func (c *describedClient) QueryVectors(ctx context.Context, in *vector_pb.QueryVectorsRequest, opts ...grpc.CallOption) (*vector_pb.QueryVectorsResponse, error) {
	if err := c.resolve(ctx, in.Table); err != nil {
		return nil, err
	}
	return c.VectorServiceClient.QueryVectors(ctx, in, opts...)
}
