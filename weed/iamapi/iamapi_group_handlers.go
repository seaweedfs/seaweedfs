package iamapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func (iama *IamApiServer) CreateGroup(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*CreateGroupResponse, *IamError) {
	resp := &CreateGroupResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("group %s already exists", groupName)}
		}
	}
	s3cfg.Groups = append(s3cfg.Groups, &iam_pb.Group{Name: groupName})
	resp.CreateGroupResult.Group.GroupName = &groupName
	return resp, nil
}

func (iama *IamApiServer) DeleteGroup(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*DeleteGroupResponse, *IamError) {
	resp := &DeleteGroupResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	for i, g := range s3cfg.Groups {
		if g.Name == groupName {
			if len(g.Members) > 0 {
				return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("cannot delete group %s: group has %d member(s)", groupName, len(g.Members))}
			}
			if len(g.PolicyNames) > 0 {
				return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("cannot delete group %s: group has %d attached policy(ies)", groupName, len(g.PolicyNames))}
			}
			// Check for inline policies
			policies := Policies{}
			if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
				return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
			}
			if gp := policies.GroupInlinePolicies[groupName]; len(gp) > 0 {
				return resp, &IamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("cannot delete group %s: group has %d inline policy(ies)", groupName, len(gp))}
			}
			s3cfg.Groups = append(s3cfg.Groups[:i], s3cfg.Groups[i+1:]...)
			// Clean up any empty inline policy entries, reuse already-fetched policies
			if err := cleanupGroupInlinePolicies(iama, groupName, &policies); err != nil {
				glog.Warningf("Failed to cleanup inline policies for group %s: %v", groupName, err)
			}
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) UpdateGroup(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*UpdateGroupResponse, *IamError) {
	resp := &UpdateGroupResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			if disabled := values.Get("Disabled"); disabled != "" {
				if disabled != "true" && disabled != "false" {
					return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("Disabled must be 'true' or 'false'")}
				}
				g.Disabled = disabled == "true"
			}
			if newName := values.Get("NewGroupName"); newName != "" && newName != g.Name {
				for _, other := range s3cfg.Groups {
					if other.Name == newName {
						return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("group %s already exists", newName)}
					}
				}
				oldName := g.Name
				g.Name = newName
				if err := migrateGroupInlinePolicies(iama, oldName, newName); err != nil {
					glog.Warningf("Failed to migrate inline policies for group rename %s -> %s: %v", oldName, newName, err)
				}
			}
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) GetGroup(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*GetGroupResponse, *IamError) {
	resp := &GetGroupResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			resp.GetGroupResult.Group.GroupName = &g.Name
			for _, member := range g.Members {
				m := member
				resp.GetGroupResult.Users = append(resp.GetGroupResult.Users, &iam.User{UserName: &m})
			}
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) ListGroups(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) *ListGroupsResponse {
	resp := &ListGroupsResponse{}
	for _, g := range s3cfg.Groups {
		name := g.Name
		resp.ListGroupsResult.Groups = append(resp.ListGroupsResult.Groups, &iam.Group{GroupName: &name})
	}
	return resp
}

func (iama *IamApiServer) AddUserToGroup(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*AddUserToGroupResponse, *IamError) {
	resp := &AddUserToGroupResponse{}
	groupName := values.Get("GroupName")
	userName := values.Get("UserName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	if userName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	userFound := false
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			userFound = true
			break
		}
	}
	if !userFound {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("user %s does not exist", userName)}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			for _, m := range g.Members {
				if m == userName {
					return resp, nil
				}
			}
			g.Members = append(g.Members, userName)
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) RemoveUserFromGroup(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*RemoveUserFromGroupResponse, *IamError) {
	resp := &RemoveUserFromGroupResponse{}
	groupName := values.Get("GroupName")
	userName := values.Get("UserName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	if userName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			for i, m := range g.Members {
				if m == userName {
					g.Members = append(g.Members[:i], g.Members[i+1:]...)
					return resp, nil
				}
			}
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("user %s is not a member of group %s", userName, groupName)}
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) AttachGroupPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*AttachGroupPolicyResponse, *IamError) {
	resp := &AttachGroupPolicyResponse{}
	groupName := values.Get("GroupName")
	policyArn := values.Get("PolicyArn")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	policyName, iamErr := parsePolicyArn(policyArn)
	if iamErr != nil {
		return resp, iamErr
	}
	// Verify policy exists in the persisted policies store
	policies := Policies{}
	if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}
	if _, exists := policies.Policies[policyName]; !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found", policyName)}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			for _, p := range g.PolicyNames {
				if p == policyName {
					return resp, nil
				}
			}
			g.PolicyNames = append(g.PolicyNames, policyName)
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) DetachGroupPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*DetachGroupPolicyResponse, *IamError) {
	resp := &DetachGroupPolicyResponse{}
	groupName := values.Get("GroupName")
	policyArn := values.Get("PolicyArn")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	policyName, iamErr := parsePolicyArn(policyArn)
	if iamErr != nil {
		return resp, iamErr
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			for i, p := range g.PolicyNames {
				if p == policyName {
					g.PolicyNames = append(g.PolicyNames[:i], g.PolicyNames[i+1:]...)
					return resp, nil
				}
			}
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s is not attached to group %s", policyName, groupName)}
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) ListAttachedGroupPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*ListAttachedGroupPoliciesResponse, *IamError) {
	resp := &ListAttachedGroupPoliciesResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			for _, policyName := range g.PolicyNames {
				pn := policyName
				policyArn := policyArnPrefix + pn
				resp.ListAttachedGroupPoliciesResult.AttachedPolicies = append(resp.ListAttachedGroupPoliciesResult.AttachedPolicies, &iam.AttachedPolicy{
					PolicyName: &pn,
					PolicyArn:  &policyArn,
				})
			}
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
}

func (iama *IamApiServer) ListGroupsForUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*ListGroupsForUserResponse, *IamError) {
	resp := &ListGroupsForUserResponse{}
	userName := values.Get("UserName")
	if userName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	userFound := false
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			userFound = true
			break
		}
	}
	if !userFound {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("user %s does not exist", userName)}
	}
	// Build reverse index for efficient lookup
	userGroupsIndex := buildUserGroupsIndex(s3cfg)
	for _, gName := range userGroupsIndex[userName] {
		name := gName
		resp.ListGroupsForUserResult.Groups = append(resp.ListGroupsForUserResult.Groups, &iam.Group{GroupName: &name})
	}
	return resp, nil
}

// removeUserFromAllGroups removes a user from all groups they belong to.
// Uses a reverse index for efficient lookup of which groups to modify.
func removeUserFromAllGroups(s3cfg *iam_pb.S3ApiConfiguration, userName string) {
	userGroupsIndex := buildUserGroupsIndex(s3cfg)
	groupNames, found := userGroupsIndex[userName]
	if !found {
		return
	}
	// Build a set for fast group name lookup
	targetGroups := make(map[string]bool, len(groupNames))
	for _, gn := range groupNames {
		targetGroups[gn] = true
	}
	for _, g := range s3cfg.Groups {
		if !targetGroups[g.Name] {
			continue
		}
		for i, m := range g.Members {
			if m == userName {
				g.Members = append(g.Members[:i], g.Members[i+1:]...)
				break
			}
		}
	}
}

// updateUserInGroups updates group membership references when a user is renamed.
func updateUserInGroups(s3cfg *iam_pb.S3ApiConfiguration, oldUserName, newUserName string) {
	for _, g := range s3cfg.Groups {
		for i, m := range g.Members {
			if m == oldUserName {
				g.Members[i] = newUserName
				break
			}
		}
	}
}

// isPolicyAttachedToAnyGroup checks if a policy is attached to any group.
func isPolicyAttachedToAnyGroup(s3cfg *iam_pb.S3ApiConfiguration, policyName string) (string, bool) {
	for _, g := range s3cfg.Groups {
		for _, p := range g.PolicyNames {
			if p == policyName {
				return g.Name, true
			}
		}
	}
	return "", false
}

// buildUserGroupsIndex builds a reverse index mapping usernames to group names.
func buildUserGroupsIndex(s3cfg *iam_pb.S3ApiConfiguration) map[string][]string {
	index := make(map[string][]string)
	for _, g := range s3cfg.Groups {
		for _, m := range g.Members {
			index[m] = append(index[m], g.Name)
		}
	}
	return index
}

// PutGroupPolicy attaches an inline policy to a group.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_PutGroupPolicy.html
func (iama *IamApiServer) PutGroupPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*PutGroupPolicyResponse, *IamError) {
	resp := &PutGroupPolicyResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	policyName := values.Get("PolicyName")
	if policyName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("PolicyName is required")}
	}
	policyDocumentString := values.Get("PolicyDocument")
	if policyDocumentString == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("PolicyDocument is required")}
	}
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	if _, err := GetActions(&policyDocument); err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}

	// Find group and get its members for action recomputation
	var targetGroup *iam_pb.Group
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			targetGroup = g
			break
		}
	}
	if targetGroup == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
	}

	// Persist inline policy
	policies := Policies{}
	if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}
	groupPolicies := policies.getOrCreateGroupPolicies(groupName)
	groupPolicies[policyName] = policyDocument
	if pErr := iama.s3ApiConfig.PutPolicies(&policies); pErr != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}

	// Recompute actions for all group members
	recomputeActionsForGroupMembers(iama, s3cfg, targetGroup, &policies)

	return resp, nil
}

// GetGroupPolicy gets an inline policy attached to a group.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetGroupPolicy.html
func (iama *IamApiServer) GetGroupPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*GetGroupPolicyResponse, *IamError) {
	resp := &GetGroupPolicyResponse{}
	groupName := values.Get("GroupName")
	policyName := values.Get("PolicyName")

	// Verify group exists
	found := false
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			found = true
			break
		}
	}
	if !found {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
	}

	policies := Policies{}
	if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}

	if groupPolicies := policies.GroupInlinePolicies[groupName]; groupPolicies != nil {
		if policyDocument, exists := groupPolicies[policyName]; exists {
			policyDocumentJSON, err := json.Marshal(policyDocument)
			if err != nil {
				return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
			}
			resp.GetGroupPolicyResult.GroupName = groupName
			resp.GetGroupPolicyResult.PolicyName = policyName
			resp.GetGroupPolicyResult.PolicyDocument = string(policyDocumentJSON)
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found on group %s", policyName, groupName)}
}

// DeleteGroupPolicy removes an inline policy from a group.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteGroupPolicy.html
func (iama *IamApiServer) DeleteGroupPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*DeleteGroupPolicyResponse, *IamError) {
	resp := &DeleteGroupPolicyResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}
	policyName := values.Get("PolicyName")
	if policyName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("PolicyName is required")}
	}

	// Find group for member action recomputation
	var targetGroup *iam_pb.Group
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			targetGroup = g
			break
		}
	}
	if targetGroup == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
	}

	policies := Policies{}
	if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}
	if groupPolicies := policies.GroupInlinePolicies[groupName]; groupPolicies != nil {
		delete(groupPolicies, policyName)
		if pErr := iama.s3ApiConfig.PutPolicies(&policies); pErr != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
		}
	}

	// Recompute actions for all group members
	recomputeActionsForGroupMembers(iama, s3cfg, targetGroup, &policies)

	return resp, nil
}

// ListGroupPolicies lists the names of inline policies attached to a group.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListGroupPolicies.html
func (iama *IamApiServer) ListGroupPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (*ListGroupPoliciesResponse, *IamError) {
	resp := &ListGroupPoliciesResponse{}
	groupName := values.Get("GroupName")
	if groupName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("GroupName is required")}
	}

	// Verify group exists
	found := false
	for _, g := range s3cfg.Groups {
		if g.Name == groupName {
			found = true
			break
		}
	}
	if !found {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("group %s does not exist", groupName)}
	}

	policies := Policies{}
	if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}
	if groupPolicies := policies.GroupInlinePolicies[groupName]; groupPolicies != nil {
		for policyName := range groupPolicies {
			resp.ListGroupPoliciesResult.PolicyNames = append(resp.ListGroupPoliciesResult.PolicyNames, policyName)
		}
		sort.Strings(resp.ListGroupPoliciesResult.PolicyNames)
	}
	resp.ListGroupPoliciesResult.IsTruncated = false
	return resp, nil
}

// cleanupGroupInlinePolicies removes all inline policies for a group from persistent storage.
// If policies is provided, it uses that to avoid redundant I/O; otherwise fetches from storage.
func cleanupGroupInlinePolicies(iama *IamApiServer, groupName string, policies *Policies) error {
	if policies == nil {
		policies = &Policies{}
		if err := iama.s3ApiConfig.GetPolicies(policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}
	}
	if _, exists := policies.GroupInlinePolicies[groupName]; exists {
		delete(policies.GroupInlinePolicies, groupName)
		return iama.s3ApiConfig.PutPolicies(policies)
	}
	return nil
}

// migrateGroupInlinePolicies renames the inline policies key when a group is renamed.
func migrateGroupInlinePolicies(iama *IamApiServer, oldName, newName string) error {
	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return err
	}
	if oldPolicies, exists := policies.GroupInlinePolicies[oldName]; exists {
		if policies.GroupInlinePolicies == nil {
			policies.GroupInlinePolicies = make(map[string]map[string]policy_engine.PolicyDocument)
		}
		policies.GroupInlinePolicies[newName] = oldPolicies
		delete(policies.GroupInlinePolicies, oldName)
		return iama.s3ApiConfig.PutPolicies(&policies)
	}
	return nil
}

// recomputeActionsForGroupMembers recomputes the aggregated actions for all members of a group.
// Uses an identity index for O(N+M) complexity instead of O(N*M).
func recomputeActionsForGroupMembers(iama *IamApiServer, s3cfg *iam_pb.S3ApiConfiguration, group *iam_pb.Group, policies *Policies) {
	// Build name -> identity index for O(1) lookup
	identIndex := make(map[string]*iam_pb.Identity, len(s3cfg.Identities))
	for _, ident := range s3cfg.Identities {
		identIndex[ident.Name] = ident
	}
	for _, memberName := range group.Members {
		if ident, ok := identIndex[memberName]; ok {
			aggregatedActions, err := computeAllActionsForUser(iama, memberName, policies, ident, s3cfg)
			if err != nil {
				glog.Warningf("Failed to recompute actions for user %s after group policy change: %v", memberName, err)
			} else {
				ident.Actions = aggregatedActions
			}
		}
	}
}
