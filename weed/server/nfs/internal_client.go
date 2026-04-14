package nfs

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

type filerClientExecutor func(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error
type internalClientExecutor func(streamingMode bool, fn func(nfsFilerClient) error) error

type nfsListEntriesClient interface {
	Recv() (*filer_pb.ListEntriesResponse, error)
}

type nfsSubscribeMetadataClient interface {
	Recv() (*filer_pb.SubscribeMetadataResponse, error)
}

type nfsFilerClient interface {
	KvGet(ctx context.Context, in *filer_pb.KvGetRequest, opts ...grpc.CallOption) (*filer_pb.KvGetResponse, error)
	LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error)
	ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (nfsListEntriesClient, error)
	SubscribeMetadata(ctx context.Context, in *filer_pb.SubscribeMetadataRequest, opts ...grpc.CallOption) (nfsSubscribeMetadataClient, error)
	CreateEntry(ctx context.Context, in *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error)
	UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error)
	DeleteEntry(ctx context.Context, in *filer_pb.DeleteEntryRequest, opts ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error)
	AtomicRenameEntry(ctx context.Context, in *filer_pb.AtomicRenameEntryRequest, opts ...grpc.CallOption) (*filer_pb.AtomicRenameEntryResponse, error)
	Statistics(ctx context.Context, in *filer_pb.StatisticsRequest, opts ...grpc.CallOption) (*filer_pb.StatisticsResponse, error)
}

type grpcNFSFilerClient struct {
	client filer_pb.SeaweedFilerClient
}

func (c grpcNFSFilerClient) KvGet(ctx context.Context, in *filer_pb.KvGetRequest, opts ...grpc.CallOption) (*filer_pb.KvGetResponse, error) {
	return c.client.KvGet(ctx, in, opts...)
}

func (c grpcNFSFilerClient) LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return c.client.LookupDirectoryEntry(ctx, in, opts...)
}

func (c grpcNFSFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (nfsListEntriesClient, error) {
	return c.client.ListEntries(ctx, in, opts...)
}

func (c grpcNFSFilerClient) SubscribeMetadata(ctx context.Context, in *filer_pb.SubscribeMetadataRequest, opts ...grpc.CallOption) (nfsSubscribeMetadataClient, error) {
	return c.client.SubscribeMetadata(ctx, in, opts...)
}

func (c grpcNFSFilerClient) CreateEntry(ctx context.Context, in *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	return c.client.CreateEntry(ctx, in, opts...)
}

func (c grpcNFSFilerClient) UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	return c.client.UpdateEntry(ctx, in, opts...)
}

func (c grpcNFSFilerClient) DeleteEntry(ctx context.Context, in *filer_pb.DeleteEntryRequest, opts ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	return c.client.DeleteEntry(ctx, in, opts...)
}

func (c grpcNFSFilerClient) AtomicRenameEntry(ctx context.Context, in *filer_pb.AtomicRenameEntryRequest, opts ...grpc.CallOption) (*filer_pb.AtomicRenameEntryResponse, error) {
	return c.client.AtomicRenameEntry(ctx, in, opts...)
}

func (c grpcNFSFilerClient) Statistics(ctx context.Context, in *filer_pb.StatisticsRequest, opts ...grpc.CallOption) (*filer_pb.StatisticsResponse, error) {
	return c.client.Statistics(ctx, in, opts...)
}

func newFilerClientExecutor(option *Option, signature int32) filerClientExecutor {
	return func(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
		return pb.WithGrpcClient(streamingMode, signature, func(grpcConnection *grpc.ClientConn) error {
			return fn(filer_pb.NewSeaweedFilerClient(grpcConnection))
		}, option.Filer.ToGrpcAddress(), false, option.GrpcDialOption)
	}
}

func newInternalClientExecutor(option *Option, signature int32) internalClientExecutor {
	return func(streamingMode bool, fn func(nfsFilerClient) error) error {
		return pb.WithGrpcClient(streamingMode, signature, func(grpcConnection *grpc.ClientConn) error {
			return fn(grpcNFSFilerClient{client: filer_pb.NewSeaweedFilerClient(grpcConnection)})
		}, option.Filer.ToGrpcAddress(), false, option.GrpcDialOption)
	}
}
