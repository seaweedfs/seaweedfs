package s3api

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SeaweedS3IamCacheServer Implementation
// This interface is dedicated to UNIDIRECTIONAL updates from Filer to S3 Server.
// S3 Server acts purely as a cache.

func (s3a *S3ApiServer) PutIdentity(ctx context.Context, req *iam_pb.PutIdentityRequest) (*iam_pb.PutIdentityResponse, error) {
	if req.Identity == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	// Direct in-memory cache update
	glog.V(1).Infof("IAM: received identity update for %s", req.Identity.Name)
	if err := s3a.iam.UpsertIdentity(req.Identity); err != nil {
		glog.Errorf("failed to update identity cache for %s: %v", req.Identity.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to update identity cache: %v", err)
	}
	return &iam_pb.PutIdentityResponse{}, nil
}

func (s3a *S3ApiServer) RemoveIdentity(ctx context.Context, req *iam_pb.RemoveIdentityRequest) (*iam_pb.RemoveIdentityResponse, error) {
	if req.Username == "" {
		return nil, status.Errorf(codes.InvalidArgument, "username is required")
	}
	// Direct in-memory cache update
	glog.V(1).Infof("IAM: received identity removal for %s", req.Username)
	s3a.iam.RemoveIdentity(req.Username)
	return &iam_pb.RemoveIdentityResponse{}, nil
}

func (s3a *S3ApiServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if req.Content == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy content is required")
	}

	// Update IAM policy cache
	glog.V(1).Infof("IAM: received policy update for %s", req.Name)
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}

	if err := s3a.iam.PutPolicy(req.Name, req.Content); err != nil {
		glog.Errorf("failed to update policy cache for %s: %v", req.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to update policy cache: %v", err)
	}
	return &iam_pb.PutPolicyResponse{}, nil
}

func (s3a *S3ApiServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}

	// Delete from IAM policy cache
	glog.V(1).Infof("IAM: received policy removal for %s", req.Name)
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}

	if err := s3a.iam.DeletePolicy(req.Name); err != nil {
		glog.Errorf("failed to delete policy cache for %s: %v", req.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to delete policy cache: %v", err)
	}
	return &iam_pb.DeletePolicyResponse{}, nil
}

func (s3a *S3ApiServer) GetPolicy(ctx context.Context, req *iam_pb.GetPolicyRequest) (*iam_pb.GetPolicyResponse, error) {
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}
	policy, err := s3a.iam.GetPolicy(req.Name)
	if err != nil {
		return &iam_pb.GetPolicyResponse{}, nil // Not found is fine for cache
	}
	return &iam_pb.GetPolicyResponse{
		Name:    policy.Name,
		Content: policy.Content,
	}, nil
}

func (s3a *S3ApiServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	resp := &iam_pb.ListPoliciesResponse{}
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}
	policies := s3a.iam.ListPolicies()
	for _, policy := range policies {
		resp.Policies = append(resp.Policies, policy)
	}
	return resp, nil
}
