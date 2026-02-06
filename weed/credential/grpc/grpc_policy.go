package grpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (store *IamGrpcStore) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	policies := make(map[string]policy_engine.PolicyDocument)
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.ListPolicies(ctx, &iam_pb.ListPoliciesRequest{})
		if err != nil {
			return err
		}
		for _, p := range resp.Policies {
			var doc policy_engine.PolicyDocument
			if err := json.Unmarshal([]byte(p.Content), &doc); err != nil {
				return fmt.Errorf("failed to unmarshal policy %s: %v", p.Name, err)
			}
			policies[p.Name] = doc
		}
		return nil
	})
	return policies, err
}

func (store *IamGrpcStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	content, err := json.Marshal(document)
	if err != nil {
		return err
	}
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.PutPolicy(ctx, &iam_pb.PutPolicyRequest{
			Name:    name,
			Content: string(content),
		})
		return err
	})
}

func (store *IamGrpcStore) DeletePolicy(ctx context.Context, name string) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.DeletePolicy(ctx, &iam_pb.DeletePolicyRequest{
			Name: name,
		})
		return err
	})
}

func (store *IamGrpcStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	var doc policy_engine.PolicyDocument
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetPolicy(ctx, &iam_pb.GetPolicyRequest{
			Name: name,
		})
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(resp.Content), &doc)
	})
	if err != nil {
		// If policy not found, return nil instead of error (consistent with other stores)
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	return &doc, nil
}

// CreatePolicy creates a new policy (delegates to PutPolicy)
func (store *IamGrpcStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.PutPolicy(ctx, name, document)
}

// UpdatePolicy updates an existing policy (delegates to PutPolicy)
func (store *IamGrpcStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.PutPolicy(ctx, name, document)
}
