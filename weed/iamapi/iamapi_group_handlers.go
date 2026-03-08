package iamapi

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
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
			s3cfg.Groups = append(s3cfg.Groups[:i], s3cfg.Groups[i+1:]...)
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
	// Verify policy exists
	policyFound := false
	for _, p := range s3cfg.Policies {
		if p.Name == policyName {
			policyFound = true
			break
		}
	}
	if !policyFound {
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
	for _, g := range s3cfg.Groups {
		for _, m := range g.Members {
			if m == userName {
				name := g.Name
				resp.ListGroupsForUserResult.Groups = append(resp.ListGroupsForUserResult.Groups, &iam.Group{GroupName: &name})
				break
			}
		}
	}
	return resp, nil
}

// removeUserFromAllGroups removes a user from all groups they belong to.
func removeUserFromAllGroups(s3cfg *iam_pb.S3ApiConfiguration, userName string) {
	for _, g := range s3cfg.Groups {
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

// policyNameFromArn extracts policy name from ARN for standalone handlers.
func policyNameFromArn(policyArn string) string {
	return strings.TrimPrefix(policyArn, policyArnPrefix)
}
