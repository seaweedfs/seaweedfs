package memory

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// cloneGroup creates a deep copy of an iam_pb.Group.
func cloneGroup(g *iam_pb.Group) *iam_pb.Group {
	if g == nil {
		return nil
	}
	clone := &iam_pb.Group{
		Name:     g.Name,
		Disabled: g.Disabled,
	}
	if g.Members != nil {
		clone.Members = make([]string, len(g.Members))
		copy(clone.Members, g.Members)
	}
	if g.PolicyNames != nil {
		clone.PolicyNames = make([]string, len(g.PolicyNames))
		copy(clone.PolicyNames, g.PolicyNames)
	}
	return clone
}

func (store *MemoryStore) CreateGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil || group.Name == "" {
		return fmt.Errorf("group name is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.groups[group.Name]; exists {
		return credential.ErrGroupAlreadyExists
	}
	store.groups[group.Name] = cloneGroup(group)
	return nil
}

func (store *MemoryStore) GetGroup(ctx context.Context, groupName string) (*iam_pb.Group, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if g, exists := store.groups[groupName]; exists {
		return cloneGroup(g), nil
	}
	return nil, credential.ErrGroupNotFound
}

func (store *MemoryStore) DeleteGroup(ctx context.Context, groupName string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.groups[groupName]; !exists {
		return credential.ErrGroupNotFound
	}
	delete(store.groups, groupName)
	return nil
}

func (store *MemoryStore) ListGroups(ctx context.Context) ([]string, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	var names []string
	for name := range store.groups {
		names = append(names, name)
	}
	return names, nil
}

func (store *MemoryStore) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	if group == nil || group.Name == "" {
		return fmt.Errorf("group name is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.groups[group.Name]; !exists {
		return credential.ErrGroupNotFound
	}
	store.groups[group.Name] = cloneGroup(group)
	return nil
}
