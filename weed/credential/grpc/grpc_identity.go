package grpc

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *IamGrpcStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	var config *iam_pb.S3ApiConfiguration
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}
		config = resp.Configuration
		return nil
	})
	return config, err
}

func (store *IamGrpcStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.PutConfiguration(ctx, &iam_pb.PutConfigurationRequest{
			Configuration: config,
		})
		return err
	})
}

func (store *IamGrpcStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.CreateUser(ctx, &iam_pb.CreateUserRequest{
			Identity: identity,
		})
		return err
	})
}

func (store *IamGrpcStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	var identity *iam_pb.Identity
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{
			Username: username,
		})
		if err != nil {
			return err
		}
		identity = resp.Identity
		return nil
	})
	return identity, err
}

func (store *IamGrpcStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
			Username: username,
			Identity: identity,
		})
		return err
	})
}

func (store *IamGrpcStore) DeleteUser(ctx context.Context, username string) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.DeleteUser(ctx, &iam_pb.DeleteUserRequest{
			Username: username,
		})
		return err
	})
}

func (store *IamGrpcStore) ListUsers(ctx context.Context) ([]string, error) {
	var usernames []string
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.ListUsers(ctx, &iam_pb.ListUsersRequest{})
		if err != nil {
			return err
		}
		usernames = resp.Usernames
		return nil
	})
	return usernames, err
}

func (store *IamGrpcStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	var identity *iam_pb.Identity
	err := store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetUserByAccessKey(ctx, &iam_pb.GetUserByAccessKeyRequest{
			AccessKey: accessKey,
		})
		if err != nil {
			return err
		}
		identity = resp.Identity
		return nil
	})
	return identity, err
}

func (store *IamGrpcStore) CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.CreateAccessKey(ctx, &iam_pb.CreateAccessKeyRequest{
			Username:   username,
			Credential: credential,
		})
		return err
	})
}

func (store *IamGrpcStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return store.withIamClient(func(client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.DeleteAccessKey(ctx, &iam_pb.DeleteAccessKeyRequest{
			Username:  username,
			AccessKey: accessKey,
		})
		return err
	})
}
