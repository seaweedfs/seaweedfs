package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

type IamGrpcServer struct {
	iam_pb.UnimplementedSeaweedIdentityAccessManagementServer
	iamStorage iam.IamStorage
}

func NewIamGrpcServer(iamStorage iam.IamStorage) *IamGrpcServer {
	return &IamGrpcServer{
		iamStorage: iamStorage,
	}
}

// Identity RPCs

func (s *IamGrpcServer) CreateIdentity(ctx context.Context, req *iam_pb.CreateIdentityRequest) (*iam_pb.CreateIdentityResponse, error) {
	if err := s.iamStorage.CreateIdentity(ctx, req.Identity); err != nil {
		return nil, err
	}
	return &iam_pb.CreateIdentityResponse{}, nil
}

func (s *IamGrpcServer) UpdatesIdentity(ctx context.Context, req *iam_pb.UpdateIdentityRequest) (*iam_pb.UpdateIdentityResponse, error) {
	if err := s.iamStorage.UpdateIdentity(ctx, req.Identity); err != nil {
		return nil, err
	}
	return &iam_pb.UpdateIdentityResponse{}, nil
}

func (s *IamGrpcServer) GetIdentity(ctx context.Context, req *iam_pb.GetIdentityRequest) (*iam_pb.GetIdentityResponse, error) {
	identity, err := s.iamStorage.GetIdentity(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	return &iam_pb.GetIdentityResponse{Identity: identity}, nil
}

func (s *IamGrpcServer) DeleteIdentity(ctx context.Context, req *iam_pb.DeleteIdentityRequest) (*iam_pb.DeleteIdentityResponse, error) {
	if err := s.iamStorage.DeleteIdentity(ctx, req.Name); err != nil {
		return nil, err
	}
	return &iam_pb.DeleteIdentityResponse{}, nil
}

func (s *IamGrpcServer) ListIdentities(ctx context.Context, req *iam_pb.ListIdentitiesRequest) (*iam_pb.ListIdentitiesResponse, error) {
	identities, err := s.iamStorage.ListIdentities(ctx, int(req.Limit), req.Offset)
	if err != nil {
		return nil, err
	}
	return &iam_pb.ListIdentitiesResponse{Identities: identities}, nil
}

// Policy RPCs

func (s *IamGrpcServer) CreatePolicy(ctx context.Context, req *iam_pb.CreatePolicyRequest) (*iam_pb.CreatePolicyResponse, error) {
	// Convert proto Policy to policy.PolicyDocument
	// Proto policy structure is simple (just strings).
	// PolicyDocument structure matches json.
	// We need to decide how to persist.
	// Ideally we accept the policy document JSON string or structured.
	// In the proto I updated, Policy has repeated statements.
	// PolicyDocument has []Statement (struct).

	// Mapping:
	doc := &policy.PolicyDocument{
		Version:   req.Policy.Version,
		Id:        req.Policy.Id,
		Statement: make([]policy.Statement, len(req.Policy.Statements)),
	}
	for i, stmt := range req.Policy.Statements {
		doc.Statement[i] = policy.Statement{
			Sid:         stmt.Sid,
			Effect:      stmt.Effect,
			Action:      stmt.Action,
			NotAction:   stmt.NotAction,
			Resource:    stmt.Resource,
			NotResource: stmt.NotResource,
			// Condition: stmt.ConditionJson ? We need to unmarshal condition JSON if present
		}
		// NOTE: simplified condition handling.
	}

	if err := s.iamStorage.CreatePolicy(ctx, req.Policy.Name, doc); err != nil {
		return nil, err
	}
	return &iam_pb.CreatePolicyResponse{}, nil
}

func (s *IamGrpcServer) GetPolicy(ctx context.Context, req *iam_pb.GetPolicyRequest) (*iam_pb.GetPolicyResponse, error) {
	doc, err := s.iamStorage.GetPolicy(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	// Convert PolicyDocument to proto Policy
	p := &iam_pb.Policy{
		Name:       req.Name, // PolicyDocument doesn't have Name usually, it's the filename/key
		Version:    doc.Version,
		Id:         doc.Id,
		Statements: make([]*iam_pb.Statement, len(doc.Statement)),
	}

	for i, stmt := range doc.Statement {
		p.Statements[i] = &iam_pb.Statement{
			Sid:         stmt.Sid,
			Effect:      stmt.Effect,
			Action:      stmt.Action,
			NotAction:   stmt.NotAction,
			Resource:    stmt.Resource,
			NotResource: stmt.NotResource,
			// Condition ignored for now
		}
	}

	return &iam_pb.GetPolicyResponse{Policy: p}, nil
}

func (s *IamGrpcServer) DeletePolicy(ctx context.Context, req *iam_pb.DeletePolicyRequest) (*iam_pb.DeletePolicyResponse, error) {
	if err := s.iamStorage.DeletePolicy(ctx, req.Name); err != nil {
		return nil, err
	}
	return &iam_pb.DeletePolicyResponse{}, nil
}

func (s *IamGrpcServer) ListPolicies(ctx context.Context, req *iam_pb.ListPoliciesRequest) (*iam_pb.ListPoliciesResponse, error) {
	names, err := s.iamStorage.ListPolicies(ctx, int(req.Limit), req.Offset)
	if err != nil {
		return nil, err
	}

	policies := make([]*iam_pb.Policy, len(names))
	for i, name := range names {
		policies[i] = &iam_pb.Policy{
			Name: name,
		}
	}

	return &iam_pb.ListPoliciesResponse{Policies: policies}, nil
}
