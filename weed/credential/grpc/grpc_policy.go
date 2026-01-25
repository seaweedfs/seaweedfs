package grpc

import (
	"context"
	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
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
			if err := json.Unmarshal([]byte(p.Content), &doc); err == nil {
				policies[p.Name] = doc
			}
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
			Name:   name,
			Policy: string(content),
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
		return json.Unmarshal([]byte(resp.Policy), &doc)
	})
	if err != nil {
		return nil, err
	}
	return &doc, nil
}
