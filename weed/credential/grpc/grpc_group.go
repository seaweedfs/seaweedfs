package grpc

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// NOTE: The gRPC store uses a load-modify-save pattern for all operations,
// which is inherently subject to race conditions under concurrent access.
// This matches the existing pattern used for identities and policies.
// A future improvement would add dedicated gRPC RPCs for atomic group operations.

func (store *IamGrpcStore) CreateGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil || group.Name == "" {
		return fmt.Errorf("group name is required")
	}
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return err
	}
	for _, g := range config.Groups {
		if g.Name == group.Name {
			return credential.ErrGroupAlreadyExists
		}
	}
	config.Groups = append(config.Groups, group)
	return store.SaveConfiguration(ctx, config)
}

func (store *IamGrpcStore) GetGroup(ctx context.Context, groupName string) (*iam_pb.Group, error) {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range config.Groups {
		if g.Name == groupName {
			return g, nil
		}
	}
	return nil, credential.ErrGroupNotFound
}

func (store *IamGrpcStore) DeleteGroup(ctx context.Context, groupName string) error {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return err
	}
	for i, g := range config.Groups {
		if g.Name == groupName {
			config.Groups = append(config.Groups[:i], config.Groups[i+1:]...)
			return store.SaveConfiguration(ctx, config)
		}
	}
	return credential.ErrGroupNotFound
}

func (store *IamGrpcStore) ListGroups(ctx context.Context) ([]string, error) {
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, g := range config.Groups {
		names = append(names, g.Name)
	}
	return names, nil
}

func (store *IamGrpcStore) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil || group.Name == "" {
		return fmt.Errorf("group name is required")
	}
	config, err := store.LoadConfiguration(ctx)
	if err != nil {
		return err
	}
	for i, g := range config.Groups {
		if g.Name == group.Name {
			config.Groups[i] = group
			return store.SaveConfiguration(ctx, config)
		}
	}
	return credential.ErrGroupNotFound
}
