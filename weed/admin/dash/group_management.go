package dash

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// cloneGroup creates a deep copy of an iam_pb.Group to avoid mutating stored state.
func cloneGroup(g *iam_pb.Group) *iam_pb.Group {
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

func (s *AdminServer) GetGroups(ctx context.Context) ([]GroupData, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	groupNames, err := s.credentialManager.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}

	var groups []GroupData
	for _, name := range groupNames {
		g, err := s.credentialManager.GetGroup(ctx, name)
		if err != nil {
			glog.V(1).Infof("Failed to get group %s: %v", name, err)
			continue
		}
		status := "enabled"
		if g.Disabled {
			status = "disabled"
		}
		groups = append(groups, GroupData{
			Name:        g.Name,
			MemberCount: len(g.Members),
			PolicyCount: len(g.PolicyNames),
			Status:      status,
			Members:     g.Members,
			PolicyNames: g.PolicyNames,
		})
	}
	return groups, nil
}

func (s *AdminServer) GetGroupDetails(ctx context.Context, name string) (*GroupData, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	g, err := s.credentialManager.GetGroup(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get group: %w", err)
	}
	status := "enabled"
	if g.Disabled {
		status = "disabled"
	}
	return &GroupData{
		Name:        g.Name,
		MemberCount: len(g.Members),
		PolicyCount: len(g.PolicyNames),
		Status:      status,
		Members:     g.Members,
		PolicyNames: g.PolicyNames,
	}, nil
}

func (s *AdminServer) CreateGroup(ctx context.Context, name string) (*GroupData, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	group := &iam_pb.Group{Name: name}
	if err := s.credentialManager.CreateGroup(ctx, group); err != nil {
		return nil, fmt.Errorf("failed to create group: %w", err)
	}
	glog.V(1).Infof("Created group %s", name)
	return &GroupData{
		Name:   name,
		Status: "enabled",
	}, nil
}

func (s *AdminServer) DeleteGroup(ctx context.Context, name string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}
	if err := s.credentialManager.DeleteGroup(ctx, name); err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}
	glog.V(1).Infof("Deleted group %s", name)
	return nil
}

func (s *AdminServer) AddGroupMember(ctx context.Context, groupName, username string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}
	g, err := s.credentialManager.GetGroup(ctx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group: %w", err)
	}
	g = cloneGroup(g)
	if _, err := s.credentialManager.GetUser(ctx, username); err != nil {
		return fmt.Errorf("user %s not found: %w", username, err)
	}
	for _, m := range g.Members {
		if m == username {
			return nil // already a member
		}
	}
	g.Members = append(g.Members, username)
	if err := s.credentialManager.UpdateGroup(ctx, g); err != nil {
		return fmt.Errorf("failed to update group: %w", err)
	}
	glog.V(1).Infof("Added user %s to group %s", username, groupName)
	return nil
}

func (s *AdminServer) RemoveGroupMember(ctx context.Context, groupName, username string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}
	g, err := s.credentialManager.GetGroup(ctx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group: %w", err)
	}
	g = cloneGroup(g)
	found := false
	var newMembers []string
	for _, m := range g.Members {
		if m == username {
			found = true
		} else {
			newMembers = append(newMembers, m)
		}
	}
	if !found {
		return fmt.Errorf("user %s is not a member of group %s", username, groupName)
	}
	g.Members = newMembers
	if err := s.credentialManager.UpdateGroup(ctx, g); err != nil {
		return fmt.Errorf("failed to update group: %w", err)
	}
	glog.V(1).Infof("Removed user %s from group %s", username, groupName)
	return nil
}

func (s *AdminServer) AttachGroupPolicy(ctx context.Context, groupName, policyName string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}
	g, err := s.credentialManager.GetGroup(ctx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group: %w", err)
	}
	g = cloneGroup(g)
	if _, err := s.credentialManager.GetPolicy(ctx, policyName); err != nil {
		return fmt.Errorf("policy %s not found: %w", policyName, err)
	}
	for _, p := range g.PolicyNames {
		if p == policyName {
			return nil // already attached
		}
	}
	g.PolicyNames = append(g.PolicyNames, policyName)
	if err := s.credentialManager.UpdateGroup(ctx, g); err != nil {
		return fmt.Errorf("failed to update group: %w", err)
	}
	glog.V(1).Infof("Attached policy %s to group %s", policyName, groupName)
	return nil
}

func (s *AdminServer) DetachGroupPolicy(ctx context.Context, groupName, policyName string) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}
	g, err := s.credentialManager.GetGroup(ctx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group: %w", err)
	}
	g = cloneGroup(g)
	found := false
	var newPolicies []string
	for _, p := range g.PolicyNames {
		if p == policyName {
			found = true
		} else {
			newPolicies = append(newPolicies, p)
		}
	}
	if !found {
		return fmt.Errorf("policy %s is not attached to group %s", policyName, groupName)
	}
	g.PolicyNames = newPolicies
	if err := s.credentialManager.UpdateGroup(ctx, g); err != nil {
		return fmt.Errorf("failed to update group: %w", err)
	}
	glog.V(1).Infof("Detached policy %s from group %s", policyName, groupName)
	return nil
}

func (s *AdminServer) SetGroupStatus(ctx context.Context, groupName string, enabled bool) error {
	if s.credentialManager == nil {
		return fmt.Errorf("credential manager not available")
	}
	g, err := s.credentialManager.GetGroup(ctx, groupName)
	if err != nil {
		return fmt.Errorf("failed to get group: %w", err)
	}
	g = cloneGroup(g)
	g.Disabled = !enabled
	if err := s.credentialManager.UpdateGroup(ctx, g); err != nil {
		return fmt.Errorf("failed to update group: %w", err)
	}
	glog.V(1).Infof("Set group %s status to enabled=%v", groupName, enabled)
	return nil
}
