package s3api

import (
	"context"
	"encoding/json"

	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// SeaweedS3IamCacheServer Implementation
// This interface is dedicated to UNIDIRECTIONAL updates from Filer to S3 Server.
// S3 Server acts purely as a cache.

func (s3a *S3ApiServer) PutIdentity(ctx context.Context, req *iam_pb.PutIdentityRequest) (*iam_pb.PutIdentityResponse, error) {
	if req.Identity == nil {
		return nil, fmt.Errorf("identity is required")
	}
	// Direct in-memory cache update
	glog.V(0).Infof("IAM: received identity update for %s", req.Identity.Name)
	if err := s3a.iam.UpsertIdentity(req.Identity); err != nil {
		glog.Errorf("failed to update identity cache for %s: %v", req.Identity.Name, err)
		return nil, err
	}
	return &iam_pb.PutIdentityResponse{}, nil
}

func (s3a *S3ApiServer) RemoveIdentity(ctx context.Context, req *iam_pb.RemoveIdentityRequest) (*iam_pb.RemoveIdentityResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	// Direct in-memory cache update
	glog.V(0).Infof("IAM: received identity removal for %s", req.Username)
	s3a.iam.RemoveIdentity(req.Username)
	return &iam_pb.RemoveIdentityResponse{}, nil
}

func (s3a *S3ApiServer) PutPolicy(ctx context.Context, req *iam_pb.PutPolicyRequest) (*iam_pb.PutPolicyResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	var doc policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(req.Content), &doc); err != nil {
		return nil, fmt.Errorf("invalid policy content: %v", err)
	}

	// Update policy engine directly (Cache only)
	glog.V(0).Infof("IAM: received policy update for %s", req.Name)
	if s3a.policyEngine != nil {
		if err := s3a.policyEngine.LoadBucketPolicyFromCache(req.Name, &doc); err != nil {
			glog.Errorf("failed to reload policy cache for %s: %v", req.Name, err)
			return nil, err
		}
	}
	return &iam_pb.PutPolicyResponse{}, nil
}

func (s3a *S3ApiServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	// Delete from policy engine directly (Cache only)
	glog.V(0).Infof("IAM: received policy removal for %s", req.Name)
	if s3a.policyEngine != nil {
		if err := s3a.policyEngine.DeleteBucketPolicy(req.Name); err != nil {
			glog.Errorf("failed to delete policy cache for %s: %v", req.Name, err)
			return nil, err
		}
	}
	return &iam_pb.DeletePolicyResponse{}, nil
}
