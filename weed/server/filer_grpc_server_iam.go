package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetS3ApiConfiguration returns the full S3 API configuration stored in the credential store.
func (fs *FilerServer) GetS3ApiConfiguration(ctx context.Context, req *iam_pb.GetS3ApiConfigurationRequest) (*iam_pb.GetS3ApiConfigurationResponse, error) {
	if fs.credentialManager == nil {
		return nil, status.Error(codes.FailedPrecondition, "credential manager not initialized")
	}
	config, err := fs.credentialManager.LoadConfiguration(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load S3 configuration: %v", err)
	}
	return &iam_pb.GetS3ApiConfigurationResponse{Config: config}, nil
}

// PutS3ApiConfiguration persists the full S3 API configuration to the credential store.
func (fs *FilerServer) PutS3ApiConfiguration(ctx context.Context, req *iam_pb.PutS3ApiConfigurationRequest) (*iam_pb.PutS3ApiConfigurationResponse, error) {
	if fs.credentialManager == nil {
		return nil, status.Error(codes.FailedPrecondition, "credential manager not initialized")
	}
	if req.GetConfig() == nil {
		return nil, status.Error(codes.InvalidArgument, "config is required")
	}
	if err := fs.credentialManager.SaveConfiguration(ctx, req.GetConfig()); err != nil {
		return nil, status.Errorf(codes.Internal, "save S3 configuration: %v", err)
	}
	return &iam_pb.PutS3ApiConfigurationResponse{}, nil
}
