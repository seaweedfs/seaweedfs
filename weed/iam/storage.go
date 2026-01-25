package iam

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

type IamStorage interface {
	// Identity Management
	CreateIdentity(ctx context.Context, identity *iam_pb.Identity) error
	GetIdentity(ctx context.Context, name string) (*iam_pb.Identity, error)
	UpdateIdentity(ctx context.Context, identity *iam_pb.Identity) error
	DeleteIdentity(ctx context.Context, name string) error
	ListIdentities(ctx context.Context, limit int, offset string) ([]*iam_pb.Identity, error)

	// Policy Management
	CreatePolicy(ctx context.Context, name string, policy *policy.PolicyDocument) error
	GetPolicy(ctx context.Context, name string) (*policy.PolicyDocument, error)
	DeletePolicy(ctx context.Context, name string) error
	ListPolicies(ctx context.Context, limit int, offset string) ([]string, error)
}
