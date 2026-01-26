package grpc

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *IamGrpcStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.CreateServiceAccount(ctx, &iam_pb.CreateServiceAccountRequest{
			ServiceAccount: sa,
		})
		return err
	})
}

func (store *IamGrpcStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.UpdateServiceAccount(ctx, &iam_pb.UpdateServiceAccountRequest{
			Id:             id,
			ServiceAccount: sa,
		})
		return err
	})
}

func (store *IamGrpcStore) DeleteServiceAccount(ctx context.Context, id string) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.DeleteServiceAccount(ctx, &iam_pb.DeleteServiceAccountRequest{
			Id: id,
		})
		return err
	})
}

func (store *IamGrpcStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	var sa *iam_pb.ServiceAccount
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetServiceAccount(ctx, &iam_pb.GetServiceAccountRequest{
			Id: id,
		})
		if err != nil {
			return err
		}
		sa = resp.ServiceAccount
		return nil
	})
	return sa, err
}

func (store *IamGrpcStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	var accounts []*iam_pb.ServiceAccount
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.ListServiceAccounts(ctx, &iam_pb.ListServiceAccountsRequest{})
		if err != nil {
			return err
		}
		accounts = resp.ServiceAccounts
		return nil
	})
	return accounts, err
}

func (store *IamGrpcStore) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error) {
	// Fallback to client-side filtering since RPC might not exist yet
	accounts, err := store.ListServiceAccounts(ctx)
	if err != nil {
		return nil, err
	}
	for _, sa := range accounts {
		if sa.Credential != nil && sa.Credential.AccessKey == accessKey {
			return sa, nil
		}
	}
	// TODO: Add GetServiceAccountByAccessKey RPC to iam.proto for efficiency
	return nil, fmt.Errorf("access key not found")
}
