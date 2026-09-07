package s3api

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SeaweedS3IamCacheServer Implementation
// This interface is dedicated to UNIDIRECTIONAL updates from Filer to S3 Server.
// S3 Server acts purely as a cache.

// checkAdminAuth verifies the caller presented a Bearer token signed by the
// filer write-signing key (jwt.filer_signing.key). It mirrors the filer's
// IamGrpcServer.checkAdminAuth so the same operator knob that locks down the
// filer IAM gRPC service also locks down this cache. With no key configured the
// check is a no-op, matching the rest of SeaweedFS's gRPC surface.
func (s3a *S3ApiServer) checkAdminAuth(ctx context.Context) error {
	if s3a.filerGuard == nil {
		return nil
	}
	signingKey := s3a.filerGuard.SigningKey()
	if len(signingKey) == 0 {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	raw := strings.TrimSpace(authHeaders[0])
	parts := strings.Fields(raw)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return status.Error(codes.Unauthenticated, "authorization header must use Bearer scheme")
	}
	parsed, err := security.DecodeJwt(signingKey, security.EncodedJwt(parts[1]), &security.SeaweedFilerAdminClaims{})
	if err != nil || parsed == nil || !parsed.Valid {
		return status.Error(codes.Unauthenticated, "invalid admin token")
	}
	return nil
}

func (s3a *S3ApiServer) PutIdentity(ctx context.Context, req *iam_pb.PutIdentityRequest) (*iam_pb.PutIdentityResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req.Identity == nil {
		return nil, status.Errorf(codes.InvalidArgument, "identity is required")
	}
	glog.V(1).Infof("IAM: received identity update for %s", req.Identity.Name)
	if err := s3a.iam.UpsertIdentity(req.Identity); err != nil {
		glog.Errorf("failed to update identity cache for %s: %v", req.Identity.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to update identity cache: %v", err)
	}
	return &iam_pb.PutIdentityResponse{}, nil
}

func (s3a *S3ApiServer) RemoveIdentity(ctx context.Context, req *iam_pb.RemoveIdentityRequest) (*iam_pb.RemoveIdentityResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req.Username == "" {
		return nil, status.Errorf(codes.InvalidArgument, "username is required")
	}
	glog.V(1).Infof("IAM: received identity removal for %s", req.Username)
	s3a.iam.RemoveIdentity(req.Username)
	return &iam_pb.RemoveIdentityResponse{}, nil
}

func (s3a *S3ApiServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}
	if req.Content == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy content is required")
	}

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
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "policy name is required")
	}

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
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
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

func (s3a *S3ApiServer) PutGroup(ctx context.Context, req *iam_pb.PutGroupRequest) (*iam_pb.PutGroupResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req.Group == nil {
		return nil, status.Errorf(codes.InvalidArgument, "group is required")
	}
	glog.V(1).Infof("IAM: received group update for %s", req.Group.Name)
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}
	if err := s3a.iam.PutGroup(req.Group); err != nil {
		glog.Errorf("failed to update group cache for %s: %v", req.Group.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to update group cache: %v", err)
	}
	return &iam_pb.PutGroupResponse{}, nil
}

func (s3a *S3ApiServer) RemoveGroup(ctx context.Context, req *iam_pb.RemoveGroupRequest) (*iam_pb.RemoveGroupResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
	if req.GroupName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "group name is required")
	}
	glog.V(1).Infof("IAM: received group removal for %s", req.GroupName)
	if s3a.iam == nil {
		return nil, status.Errorf(codes.Internal, "IAM not initialized")
	}
	s3a.iam.RemoveGroup(req.GroupName)
	return &iam_pb.RemoveGroupResponse{}, nil
}

func (s3a *S3ApiServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	if err := s3a.checkAdminAuth(ctx); err != nil {
		return nil, err
	}
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
