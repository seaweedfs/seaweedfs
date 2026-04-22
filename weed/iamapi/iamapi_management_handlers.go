package iamapi

// This file provides IAM API handlers for the standalone IAM server.
// Common IAM types and helpers are imported from the shared weed/iam package.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

// Constants from shared package
const (
	charsetUpper            = iamlib.CharsetUpper
	charset                 = iamlib.Charset
	policyDocumentVersion   = iamlib.PolicyDocumentVersion
	StatementActionAdmin    = iamlib.StatementActionAdmin
	StatementActionWrite    = iamlib.StatementActionWrite
	StatementActionWriteAcp = iamlib.StatementActionWriteAcp
	StatementActionRead     = iamlib.StatementActionRead
	StatementActionReadAcp  = iamlib.StatementActionReadAcp
	StatementActionList     = iamlib.StatementActionList
	StatementActionTagging  = iamlib.StatementActionTagging
	StatementActionDelete   = iamlib.StatementActionDelete
	USER_DOES_NOT_EXIST     = iamlib.UserDoesNotExist
	accessKeyStatusActive   = iamlib.AccessKeyStatusActive
	accessKeyStatusInactive = iamlib.AccessKeyStatusInactive
)

var policyLock = sync.RWMutex{}

const policyArnPrefix = "arn:aws:iam:::policy/"

// parsePolicyArn validates an IAM policy ARN and extracts the policy name.
func parsePolicyArn(policyArn string) (string, *IamError) {
	if !strings.HasPrefix(policyArn, policyArnPrefix) {
		return "", &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("invalid policy ARN: %s", policyArn)}
	}
	policyName := strings.TrimPrefix(policyArn, policyArnPrefix)
	if policyName == "" {
		return "", &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("invalid policy ARN: %s", policyArn)}
	}
	return policyName, nil
}

// userPolicyKey returns a namespaced key for inline user policies to prevent collision with managed policies.
// getOrCreateUserPolicies returns the policy map for a user, creating it if needed.
// Returns a pointer to the user's policy map from Policies.InlinePolicies.
func (p *Policies) getOrCreateUserPolicies(userName string) map[string]policy_engine.PolicyDocument {
	if p.InlinePolicies == nil {
		p.InlinePolicies = make(map[string]map[string]policy_engine.PolicyDocument)
	}
	if p.InlinePolicies[userName] == nil {
		p.InlinePolicies[userName] = make(map[string]policy_engine.PolicyDocument)
	}
	return p.InlinePolicies[userName]
}

// getOrCreateGroupPolicies returns the policy map for a group, creating it if needed.
func (p *Policies) getOrCreateGroupPolicies(groupName string) map[string]policy_engine.PolicyDocument {
	if p.GroupInlinePolicies == nil {
		p.GroupInlinePolicies = make(map[string]map[string]policy_engine.PolicyDocument)
	}
	if p.GroupInlinePolicies[groupName] == nil {
		p.GroupInlinePolicies[groupName] = make(map[string]policy_engine.PolicyDocument)
	}
	return p.GroupInlinePolicies[groupName]
}

// computeAggregatedActionsForUser computes the union of actions across all inline policies for a user.
// Directly accesses user's policies from Policies.InlinePolicies[userName] for O(1) lookup.
// If policies is non-nil, it uses that instead of fetching from storage (for I/O optimization).
// When policies is nil, it fetches from storage using GetPolicies.
//
// Performance: O(user_policies) instead of O(all_policies) with per-user index.
//
// Best-effort aggregation: If GetActions fails for a policy document, that policy is logged at Warning level
// but is NOT removed from persistent storage. This intentional choice ensures:
// - Stored policy documents survive even if they temporarily fail to parse
// - The policy data is preserved for potential future fixes or manual inspection
// - Only the runtime action set (ident.Actions) is affected when GetActions fails
// This keeps persistent state consistent while gracefully handling parsing errors.
func computeAggregatedActionsForUser(iama *IamApiServer, userName string, policies *Policies) ([]string, error) {
	var aggregatedActions []string
	actionSet := make(map[string]bool)

	var policiesToUse Policies
	if policies != nil {
		// Use provided Policies (caller already fetched, avoids redundant I/O)
		policiesToUse = *policies
	} else {
		// Fetch from storage
		if err := iama.s3ApiConfig.GetPolicies(&policiesToUse); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			return nil, err
		}
	}

	// Direct O(1) access to user's policies using per-user index
	userPolicies := policiesToUse.InlinePolicies[userName]
	if len(userPolicies) == 0 {
		return aggregatedActions, nil
	}

	for policyName, policyDocument := range userPolicies {
		actions, err := GetActions(&policyDocument)
		if err != nil {
			// Best-effort: policy stored successfully but failed to parse; log and skip from aggregation
			glog.Warningf("Failed to get actions from stored policy '%s' for user %s (policy retained in storage): %v", policyName, userName, err)
			continue
		}
		for _, action := range actions {
			if !actionSet[action] {
				actionSet[action] = true
				aggregatedActions = append(aggregatedActions, action)
			}
		}
	}

	return aggregatedActions, nil
}

// Helper function wrappers using shared package
func MapToStatementAction(action string) string {
	return iamlib.MapToStatementAction(action)
}

func MapToIdentitiesAction(action string) string {
	return iamlib.MapToIdentitiesAction(action)
}

type Policies struct {
	// Policies: managed policies (flat map, unchanged for backward compatibility)
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`

	// InlinePolicies: user-indexed inline policies for O(1) lookup
	// Structure: [userName][policyName] -> PolicyDocument
	// Enables fast access without iterating all policies
	InlinePolicies map[string]map[string]policy_engine.PolicyDocument `json:"inlinePolicies"`

	// GroupInlinePolicies: group-indexed inline policies for O(1) lookup
	// Structure: [groupName][policyName] -> PolicyDocument
	GroupInlinePolicies map[string]map[string]policy_engine.PolicyDocument `json:"groupInlinePolicies,omitempty"`
}

func Hash(s *string) string {
	return iamlib.Hash(s)
}

func StringWithCharset(length int, charset string) (string, error) {
	return iamlib.GenerateRandomString(length, charset)
}

func stringSlicesEqual(a, b []string) bool {
	return iamlib.StringSlicesEqual(a, b)
}

func validateAccessKeyStatus(status string) error {
	switch status {
	case accessKeyStatusActive, accessKeyStatusInactive:
		return nil
	case "":
		return fmt.Errorf("Status parameter is required")
	default:
		return fmt.Errorf("Status must be '%s' or '%s'", accessKeyStatusActive, accessKeyStatusInactive)
	}
}

func (iama *IamApiServer) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *ListUsersResponse) {
	resp = &ListUsersResponse{}
	for _, ident := range s3cfg.Identities {
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &ident.Name})
	}
	return resp
}

func (iama *IamApiServer) ListAccessKeys(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *ListAccessKeysResponse) {
	resp = &ListAccessKeysResponse{}
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if userName != "" && userName != ident.Name {
			continue
		}
		for _, cred := range ident.Credentials {
			status := cred.Status
			if status == "" {
				status = accessKeyStatusActive
			}
			identName := ident.Name
			accessKey := cred.AccessKey
			resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
				&iam.AccessKeyMetadata{UserName: &identName, AccessKeyId: &accessKey, Status: &status},
			)
		}
	}
	return resp
}

func (iama *IamApiServer) CreateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *CreateUserResponse) {
	resp = &CreateUserResponse{}
	userName := values.Get("UserName")
	resp.CreateUserResult.User.UserName = &userName
	s3cfg.Identities = append(s3cfg.Identities, &iam_pb.Identity{Name: userName})
	return resp
}

func (iama *IamApiServer) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp *DeleteUserResponse, err *IamError) {
	resp = &DeleteUserResponse{}
	for i, ident := range s3cfg.Identities {
		if userName == ident.Name {
			// Clean up any inline policies stored for this user
			policies := Policies{}
			if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
				return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
			}
			if policies.InlinePolicies != nil {
				if _, exists := policies.InlinePolicies[userName]; exists {
					delete(policies.InlinePolicies, userName)
					if pErr := iama.s3ApiConfig.PutPolicies(&policies); pErr != nil {
						return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
					}
				}
			}
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			// Remove user from all groups
			removeUserFromAllGroups(s3cfg, userName)
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func (iama *IamApiServer) GetUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp *GetUserResponse, err *IamError) {
	resp = &GetUserResponse{}
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			resp.GetUserResult.User = iam.User{UserName: &ident.Name}
			return resp, nil
		}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func (iama *IamApiServer) UpdateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *UpdateUserResponse, err *IamError) {
	resp = &UpdateUserResponse{}
	userName := values.Get("UserName")
	newUserName := values.Get("NewUserName")
	if newUserName == "" {
		return resp, nil
	}

	// Find the source identity first
	var sourceIdent *iam_pb.Identity
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			sourceIdent = ident
			break
		}
	}
	if sourceIdent == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}

	// No-op if renaming to the same name
	if newUserName == userName {
		return resp, nil
	}

	// Check for name collision before renaming
	for _, ident := range s3cfg.Identities {
		if ident.Name == newUserName {
			return resp, &IamError{
				Code:  iam.ErrCodeEntityAlreadyExistsException,
				Error: fmt.Errorf("user %s already exists", newUserName),
			}
		}
	}
	// Check for inline policy collision
	policies := Policies{}
	if pErr := iama.s3ApiConfig.GetPolicies(&policies); pErr != nil && !errors.Is(pErr, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
	}
	if policies.InlinePolicies != nil {
		if _, exists := policies.InlinePolicies[newUserName]; exists {
			return resp, &IamError{
				Code:  iam.ErrCodeEntityAlreadyExistsException,
				Error: fmt.Errorf("inline policies already exist for user %s", newUserName),
			}
		}
	}

	sourceIdent.Name = newUserName
	// Move any inline policies from old username to new username
	if policies.InlinePolicies != nil {
		if userPolicies, exists := policies.InlinePolicies[userName]; exists {
			delete(policies.InlinePolicies, userName)
			policies.InlinePolicies[newUserName] = userPolicies
			if pErr := iama.s3ApiConfig.PutPolicies(&policies); pErr != nil {
				// Rollback: restore identity name and inline policies
				sourceIdent.Name = userName
				delete(policies.InlinePolicies, newUserName)
				policies.InlinePolicies[userName] = userPolicies
				return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: pErr}
			}
		}
	}
	// Update group membership references
	updateUserInGroups(s3cfg, userName, newUserName)
	// Update service account parent references
	for _, sa := range s3cfg.ServiceAccounts {
		if sa.ParentUser == userName {
			sa.ParentUser = newUserName
		}
	}
	return resp, nil
}

func GetPolicyDocument(policy *string) (policy_engine.PolicyDocument, error) {
	var policyDocument policy_engine.PolicyDocument
	if err := json.Unmarshal([]byte(*policy), &policyDocument); err != nil {
		return policy_engine.PolicyDocument{}, err
	}
	return policyDocument, nil
}

func (iama *IamApiServer) CreatePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *CreatePolicyResponse, iamError *IamError) {
	resp = &CreatePolicyResponse{}
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	policyId := Hash(&policyName)
	arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
	resp.CreatePolicyResult.Policy.PolicyName = &policyName
	resp.CreatePolicyResult.Policy.Arn = &arn
	resp.CreatePolicyResult.Policy.PolicyId = &policyId
	policies := Policies{}
	// Note: Lock is already held by DoActions, no need to acquire here
	if err = iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	if policies.Policies == nil {
		policies.Policies = make(map[string]policy_engine.PolicyDocument)
	}
	policies.Policies[policyName] = policyDocument
	if err = iama.s3ApiConfig.PutPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return resp, nil
}

type IamError struct {
	Code  string
	Error error
}

// https://docs.aws.amazon.com/IAM/latest/APIReference/API_PutUserPolicy.html
func (iama *IamApiServer) PutUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *PutUserPolicyResponse, iamError *IamError) {
	resp = &PutUserPolicyResponse{}
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	if _, err := GetActions(&policyDocument); err != nil {
		return resp, &IamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}

	// Verify the user exists before persisting the policy
	var targetIdent *iam_pb.Identity
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			targetIdent = ident
			break
		}
	}
	if targetIdent == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the user with name %s cannot be found", userName)}
	}

	// Persist inline policy to storage using per-user indexed structure
	policies := Policies{}
	if err = iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	userPolicies := policies.getOrCreateUserPolicies(userName)
	userPolicies[policyName] = policyDocument

	if err = iama.s3ApiConfig.PutPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Recompute aggregated actions (inline + managed)
	aggregatedActions, computeErr := computeAllActionsForUser(iama, userName, &policies, targetIdent, s3cfg)
	if computeErr != nil {
		glog.Warningf("Failed to compute aggregated actions for user %s: %v; keeping existing actions", userName, computeErr)
	} else {
		targetIdent.Actions = aggregatedActions
	}
	return resp, nil
}

func (iama *IamApiServer) GetUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *GetUserPolicyResponse, err *IamError) {
	resp = &GetUserPolicyResponse{}
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}

		resp.GetUserPolicyResult.UserName = userName
		resp.GetUserPolicyResult.PolicyName = policyName

		// Try to retrieve stored inline policy from persistent storage using per-user index
		policies := Policies{}
		if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			// Propagate storage errors
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}

		// Direct O(1) access to user's policy using per-user index
		if userPolicies := policies.InlinePolicies[userName]; userPolicies != nil {
			if policyDocument, exists := userPolicies[policyName]; exists {
				policyDocumentJSON, err := json.Marshal(policyDocument)
				if err != nil {
					return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
				}
				resp.GetUserPolicyResult.PolicyDocument = string(policyDocumentJSON)
				return resp, nil
			} else {
				// User's inline policies exist but this specific policy does not
				return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: errors.New("policy not found")}
			}
		}

		if len(ident.Actions) == 0 {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: errors.New("no actions found")}
		}

		policyDocument := policy_engine.PolicyDocument{Version: policyDocumentVersion}
		statements := make(map[string][]string)
		seenAction := make(map[string]map[string]bool)
		for _, action := range ident.Actions {
			// parse "Read:EXAMPLE-BUCKET" or "Read:EXAMPLE-BUCKET/prefix/*"
			// Use SplitN so the path component (which may contain ':') is preserved intact.
			act := strings.SplitN(action, ":", 2)

			resource := "*"
			if len(act) == 2 {
				// Preserve the stored path verbatim so bucket-level and
				// object-level resources remain distinguishable. GetActions
				// stores the path exactly as parsed from the original ARN
				// (e.g. "b-le*" for the bucket, "b-le*/*" for objects), and
				// reconstruction should not rewrite one into the other.
				resource = fmt.Sprintf("arn:aws:s3:::%s", act[1])
			}
			s3Action := fmt.Sprintf("s3:%s", MapToIdentitiesAction(act[0]))
			// Dedupe actions per resource: the Read/Write/List internal verbs map to
			// coarse wildcards (s3:Get*, s3:Put*, s3:List*), so multiple distinct
			// original actions can collapse to the same reconstructed verb.
			if seenAction[resource] == nil {
				seenAction[resource] = make(map[string]bool)
			}
			if seenAction[resource][s3Action] {
				continue
			}
			seenAction[resource][s3Action] = true
			statements[resource] = append(statements[resource], s3Action)
		}
		for resource, actions := range statements {
			isEqAction := false
			for i, statement := range policyDocument.Statement {
				// Use order-independent comparison to avoid duplicates from different action orderings
				if stringSlicesEqual(statement.Action.Strings(), actions) {
					policyDocument.Statement[i].Resource = policy_engine.NewStringOrStringSlicePtr(append(
						policyDocument.Statement[i].Resource.Strings(), resource)...)
					isEqAction = true
					break
				}
			}
			if isEqAction {
				continue
			}
			policyDocumentStatement := policy_engine.PolicyStatement{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice(actions...),
				Resource: policy_engine.NewStringOrStringSlicePtr(resource),
			}
			policyDocument.Statement = append(policyDocument.Statement, policyDocumentStatement)
		}
		policyDocumentJSON, err := json.Marshal(policyDocument)
		if err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
		resp.GetUserPolicyResult.PolicyDocument = string(policyDocumentJSON)
		return resp, nil
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

// DeleteUserPolicy removes the inline policy from a user (clears their actions).
func (iama *IamApiServer) DeleteUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *DeleteUserPolicyResponse, err *IamError) {
	resp = &DeleteUserPolicyResponse{}
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")

	// First, verify the user exists in identities before modifying storage
	var targetIdent *iam_pb.Identity
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			targetIdent = ident
			break
		}
	}
	if targetIdent == nil {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}

	// User exists; now proceed with removing the stored inline policy from persistent storage
	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		// Propagate storage errors immediately
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	// Direct O(1) access to user's policy map using per-user index
	if userPolicies := policies.InlinePolicies[userName]; userPolicies != nil {
		delete(userPolicies, policyName)
		// Note: userPolicies is a map, so the delete modifies the map in policies.InlinePolicies[userName]
		if err := iama.s3ApiConfig.PutPolicies(&policies); err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}

	// Recompute aggregated actions from remaining inline + managed policies
	aggregatedActions, computeErr := computeAllActionsForUser(iama, userName, &policies, targetIdent, s3cfg)
	if computeErr != nil {
		glog.Warningf("Failed to recompute aggregated actions for user %s: %v; keeping existing actions", userName, computeErr)
	} else {
		targetIdent.Actions = aggregatedActions
	}
	return resp, nil
}

// ListUserPolicies lists the names of inline policies attached to a user.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListUserPolicies.html
func (iama *IamApiServer) ListUserPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *ListUserPoliciesResponse, iamError *IamError) {
	resp = &ListUserPoliciesResponse{}
	userName := values.Get("UserName")
	if userName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}

	// Verify the user exists
	found := false
	for _, ident := range s3cfg.Identities {
		if ident.Name == userName {
			found = true
			break
		}
	}
	if !found {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
	}

	// List inline policy names from persistent storage
	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	if userPolicies := policies.InlinePolicies[userName]; userPolicies != nil {
		for policyName := range userPolicies {
			resp.ListUserPoliciesResult.PolicyNames = append(resp.ListUserPoliciesResult.PolicyNames, policyName)
		}
		sort.Strings(resp.ListUserPoliciesResult.PolicyNames)
	}
	resp.ListUserPoliciesResult.IsTruncated = false
	return resp, nil
}

// GetPolicy retrieves a managed policy by ARN.
func (iama *IamApiServer) GetPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *GetPolicyResponse, iamError *IamError) {
	resp = &GetPolicyResponse{}
	policyArn := values.Get("PolicyArn")
	policyName, iamError := parsePolicyArn(policyArn)
	if iamError != nil {
		return resp, iamError
	}

	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	if _, exists := policies.Policies[policyName]; !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found", policyName)}
	}

	policyId := Hash(&policyName)
	resp.GetPolicyResult.Policy.PolicyName = &policyName
	resp.GetPolicyResult.Policy.Arn = &policyArn
	resp.GetPolicyResult.Policy.PolicyId = &policyId
	return resp, nil
}

// DeletePolicy removes a managed policy. Rejects deletion if the policy is still attached to any user
// (matching AWS IAM behavior: must detach before deleting).
func (iama *IamApiServer) DeletePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *DeletePolicyResponse, iamError *IamError) {
	resp = &DeletePolicyResponse{}
	policyArn := values.Get("PolicyArn")
	policyName, iamError := parsePolicyArn(policyArn)
	if iamError != nil {
		return resp, iamError
	}

	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	if _, exists := policies.Policies[policyName]; !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found", policyName)}
	}

	// Reject deletion if the policy is still attached to any user
	for _, ident := range s3cfg.Identities {
		for _, name := range ident.PolicyNames {
			if name == policyName {
				return resp, &IamError{
					Code:  iam.ErrCodeDeleteConflictException,
					Error: fmt.Errorf("policy %s is still attached to user %s", policyName, ident.Name),
				}
			}
		}
	}

	// Reject deletion if the policy is attached to any group
	if groupName, attached := isPolicyAttachedToAnyGroup(s3cfg, policyName); attached {
		return resp, &IamError{
			Code:  iam.ErrCodeDeleteConflictException,
			Error: fmt.Errorf("policy %s is still attached to group %s", policyName, groupName),
		}
	}

	delete(policies.Policies, policyName)
	if err := iama.s3ApiConfig.PutPolicies(&policies); err != nil {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	return resp, nil
}

// ListPolicies lists all managed policies.
func (iama *IamApiServer) ListPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *ListPoliciesResponse, iamError *IamError) {
	resp = &ListPoliciesResponse{}
	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}

	for policyName := range policies.Policies {
		name := policyName
		arn := fmt.Sprintf("arn:aws:iam:::policy/%s", name)
		policyId := Hash(&name)
		resp.ListPoliciesResult.Policies = append(resp.ListPoliciesResult.Policies, &iam.Policy{
			PolicyName: &name,
			Arn:        &arn,
			PolicyId:   &policyId,
		})
	}
	return resp, nil
}

// AttachUserPolicy attaches a managed policy to a user.
func (iama *IamApiServer) AttachUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *AttachUserPolicyResponse, iamError *IamError) {
	resp = &AttachUserPolicyResponse{}
	userName := values.Get("UserName")
	policyArn := values.Get("PolicyArn")
	policyName, iamError := parsePolicyArn(policyArn)
	if iamError != nil {
		return resp, iamError
	}

	// Verify managed policy exists
	policies := Policies{}
	if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	if _, exists := policies.Policies[policyName]; !exists {
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found", policyName)}
	}

	// Find user and attach policy
	for _, ident := range s3cfg.Identities {
		if ident.Name != userName {
			continue
		}
		// Check if already attached
		for _, name := range ident.PolicyNames {
			if name == policyName {
				return resp, nil // Already attached, idempotent
			}
		}
		prevPolicyNames := ident.PolicyNames
		ident.PolicyNames = append(ident.PolicyNames, policyName)

		// Recompute aggregated actions (inline + managed + group)
		aggregatedActions, err := computeAllActionsForUser(iama, userName, &policies, ident, s3cfg)
		if err != nil {
			// Roll back PolicyNames to keep identity consistent
			ident.PolicyNames = prevPolicyNames
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to compute actions after attaching policy: %w", err)}
		}
		ident.Actions = aggregatedActions
		return resp, nil
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

// DetachUserPolicy detaches a managed policy from a user.
func (iama *IamApiServer) DetachUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *DetachUserPolicyResponse, iamError *IamError) {
	resp = &DetachUserPolicyResponse{}
	userName := values.Get("UserName")
	policyArn := values.Get("PolicyArn")
	policyName, iamError := parsePolicyArn(policyArn)
	if iamError != nil {
		return resp, iamError
	}

	for _, ident := range s3cfg.Identities {
		if ident.Name != userName {
			continue
		}
		// Find and remove policy name from the list
		prevPolicyNames := make([]string, len(ident.PolicyNames))
		copy(prevPolicyNames, ident.PolicyNames)

		found := false
		for i, name := range ident.PolicyNames {
			if name == policyName {
				ident.PolicyNames = append(ident.PolicyNames[:i], ident.PolicyNames[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s is not attached to user %s", policyName, userName)}
		}

		// Recompute aggregated actions (inline + managed)
		policies := Policies{}
		if err := iama.s3ApiConfig.GetPolicies(&policies); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			// Roll back PolicyNames on storage error
			ident.PolicyNames = prevPolicyNames
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
		aggregatedActions, err := computeAllActionsForUser(iama, userName, &policies, ident, s3cfg)
		if err != nil {
			// Roll back PolicyNames to keep identity consistent
			ident.PolicyNames = prevPolicyNames
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to compute actions after detaching policy: %w", err)}
		}
		ident.Actions = aggregatedActions
		return resp, nil
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

// ListAttachedUserPolicies lists the managed policies attached to a user.
func (iama *IamApiServer) ListAttachedUserPolicies(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *ListAttachedUserPoliciesResponse, iamError *IamError) {
	resp = &ListAttachedUserPoliciesResponse{}
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if ident.Name != userName {
			continue
		}
		for _, policyName := range ident.PolicyNames {
			name := policyName
			arn := fmt.Sprintf("arn:aws:iam:::policy/%s", name)
			resp.ListAttachedUserPoliciesResult.AttachedPolicies = append(
				resp.ListAttachedUserPoliciesResult.AttachedPolicies,
				&iam.AttachedPolicy{PolicyName: &name, PolicyArn: &arn},
			)
		}
		return resp, nil
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

// computeAllActionsForUser computes the union of actions from user inline policies,
// user managed policies, group inline policies, and group managed policies.
// If s3cfg is provided, group memberships are resolved to include group policies.
func computeAllActionsForUser(iama *IamApiServer, userName string, policies *Policies, ident *iam_pb.Identity, s3cfgs ...*iam_pb.S3ApiConfiguration) ([]string, error) {
	actionSet := make(map[string]bool)
	var aggregatedActions []string

	addUniqueActions := func(actions []string) {
		for _, action := range actions {
			if !actionSet[action] {
				actionSet[action] = true
				aggregatedActions = append(aggregatedActions, action)
			}
		}
	}

	// Include user inline policy actions
	inlineActions, err := computeAggregatedActionsForUser(iama, userName, policies)
	if err != nil {
		return nil, err
	}
	addUniqueActions(inlineActions)

	// Include user managed policy actions
	for _, policyName := range ident.PolicyNames {
		if policyDoc, exists := policies.Policies[policyName]; exists {
			actions, err := GetActions(&policyDoc)
			if err != nil {
				glog.Warningf("Failed to get actions from managed policy '%s' for user %s: %v", policyName, userName, err)
				continue
			}
			addUniqueActions(actions)
		}
	}

	// Include group policies (both inline and managed) if s3cfg is available
	if len(s3cfgs) > 0 && s3cfgs[0] != nil {
		s3cfg := s3cfgs[0]
		for _, g := range s3cfg.Groups {
			if g.Disabled {
				continue
			}
			isMember := false
			for _, m := range g.Members {
				if m == userName {
					isMember = true
					break
				}
			}
			if !isMember {
				continue
			}
			// Group managed policies
			for _, policyName := range g.PolicyNames {
				if policyDoc, exists := policies.Policies[policyName]; exists {
					actions, err := GetActions(&policyDoc)
					if err != nil {
						glog.Warningf("Failed to get actions from group managed policy '%s' (group %s) for user %s: %v", policyName, g.Name, userName, err)
						continue
					}
					addUniqueActions(actions)
				}
			}
			// Group inline policies
			if groupPolicies := policies.GroupInlinePolicies[g.Name]; groupPolicies != nil {
				for policyName, policyDoc := range groupPolicies {
					actions, err := GetActions(&policyDoc)
					if err != nil {
						glog.Warningf("Failed to get actions from group inline policy '%s' (group %s) for user %s: %v", policyName, g.Name, userName, err)
						continue
					}
					addUniqueActions(actions)
				}
			}
		}
	}

	return aggregatedActions, nil
}

func GetActions(policy *policy_engine.PolicyDocument) ([]string, error) {
	var actions []string

	for _, statement := range policy.Statement {
		if statement.Effect != policy_engine.PolicyEffectAllow {
			return nil, fmt.Errorf("not a valid effect: '%s'. Only 'Allow' is possible", statement.Effect)
		}
		for _, resource := range statement.Resource.Strings() {
			// Parse "arn:aws:s3:::my-bucket/shared/*"
			res := strings.Split(resource, ":")
			if len(res) != 6 || res[0] != "arn" || res[1] != "aws" || res[2] != "s3" {
				continue
			}
			for _, action := range statement.Action.Strings() {
				// Parse "s3:Get*"
				act := strings.Split(action, ":")
				if len(act) != 2 || act[0] != "s3" {
					continue
				}
				statementAction := MapToStatementAction(act[1])

				if statementAction == "" {
					return nil, fmt.Errorf("not a valid action: '%s'", act[1])
				}

				path := res[5]
				if path == "*" {
					actions = append(actions, statementAction)
					continue
				}
				actions = append(actions, fmt.Sprintf("%s:%s", statementAction, path))
			}
		}
	}
	return actions, nil
}

func (iama *IamApiServer) CreateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *CreateAccessKeyResponse, iamErr *IamError) {
	resp = &CreateAccessKeyResponse{}
	userName := values.Get("UserName")
	status := iam.StatusTypeActive

	accessKeyId := values.Get("AccessKeyId")
	secretAccessKey := values.Get("SecretAccessKey")
	if accessKeyId != "" {
		if err := iamlib.ValidateCallerSuppliedAccessKeyId(accessKeyId); err != nil {
			return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: err}
		}
	}
	if secretAccessKey != "" {
		if err := iamlib.ValidateCallerSuppliedSecretAccessKey(secretAccessKey); err != nil {
			return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: err}
		}
	}
	if owner := iamlib.FindAccessKeyOwner(s3cfg, accessKeyId); owner != nil {
		glog.V(4).Infof("CreateAccessKey: supplied AccessKeyId already in use by %s %s", owner.Type, owner.Name)
		return resp, &IamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("AccessKeyId is already in use")}
	}
	if (accessKeyId != "") != (secretAccessKey != "") {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("AccessKeyId and SecretAccessKey must be supplied together")}
	}
	if accessKeyId == "" {
		var err error
		accessKeyId, err = StringWithCharset(21, charsetUpper)
		if err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate access key: %w", err)}
		}
	}
	if secretAccessKey == "" {
		var err error
		secretAccessKey, err = StringWithCharset(42, charset)
		if err != nil {
			return resp, &IamError{Code: iam.ErrCodeServiceFailureException, Error: fmt.Errorf("failed to generate secret key: %w", err)}
		}
	}

	resp.CreateAccessKeyResult.AccessKey.AccessKeyId = &accessKeyId
	resp.CreateAccessKeyResult.AccessKey.SecretAccessKey = &secretAccessKey
	resp.CreateAccessKeyResult.AccessKey.UserName = &userName
	resp.CreateAccessKeyResult.AccessKey.Status = &status
	changed := false
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials,
				&iam_pb.Credential{AccessKey: accessKeyId, SecretKey: secretAccessKey, Status: accessKeyStatusActive})
			changed = true
			break
		}
	}
	if !changed {
		s3cfg.Identities = append(s3cfg.Identities,
			&iam_pb.Identity{
				Name: userName,
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: accessKeyId,
						SecretKey: secretAccessKey,
						Status:    accessKeyStatusActive,
					},
				},
			},
		)
	}
	return resp, nil
}

// UpdateAccessKey updates the status of an access key (Active or Inactive).
func (iama *IamApiServer) UpdateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *UpdateAccessKeyResponse, err *IamError) {
	resp = &UpdateAccessKeyResponse{}
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	status := values.Get("Status")

	if userName == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("UserName is required")}
	}
	if accessKeyId == "" {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("AccessKeyId is required")}
	}
	if err := validateAccessKeyStatus(status); err != nil {
		return resp, &IamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}

	for _, ident := range s3cfg.Identities {
		if ident.Name != userName {
			continue
		}
		for _, cred := range ident.Credentials {
			if cred.AccessKey == accessKeyId {
				cred.Status = status
				return resp, nil
			}
		}
		return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("the access key with id %s for user %s cannot be found", accessKeyId, userName)}
	}
	return resp, &IamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf(USER_DOES_NOT_EXIST, userName)}
}

func (iama *IamApiServer) DeleteAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp *DeleteAccessKeyResponse) {
	resp = &DeleteAccessKeyResponse{}
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			for i, cred := range ident.Credentials {
				if cred.AccessKey == accessKeyId {
					ident.Credentials = append(ident.Credentials[:i], ident.Credentials[i+1:]...)
					break
				}
			}
			break
		}
	}
	return resp
}

// handleImplicitUsername adds username who signs the request to values if 'username' is not specified.
// According to AWS documentation: "If you do not specify a user name, IAM determines the user name
// implicitly based on the Amazon Web Services access key ID signing the request."
// This function extracts the AccessKeyId from the SigV4 credential and looks up the corresponding
// identity name in the credential store.
func (iama *IamApiServer) handleImplicitUsername(r *http.Request, values url.Values) {
	if len(r.Header["Authorization"]) == 0 || values.Get("UserName") != "" {
		return
	}
	// Log presence of auth header without exposing sensitive signature material
	glog.V(4).Infof("Authorization header present, extracting access key")
	// Parse AWS SigV4 Authorization header format:
	// "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/iam/aws4_request, ..."
	s := strings.Split(r.Header["Authorization"][0], "Credential=")
	if len(s) < 2 {
		return
	}
	s = strings.Split(s[1], ",")
	if len(s) < 1 {
		return
	}
	s = strings.Split(s[0], "/")
	if len(s) < 1 {
		return
	}
	// s[0] is the AccessKeyId
	accessKeyId := s[0]
	if accessKeyId == "" {
		return
	}
	// Nil-guard: ensure iam is initialized before lookup
	if iama.iam == nil {
		glog.V(4).Infof("IAM not initialized, cannot look up access key")
		return
	}
	// Look up the identity by access key to get the username
	identity, _, found := iama.iam.LookupByAccessKey(accessKeyId)
	if !found {
		// Mask access key in logs - show only first 4 chars
		maskedKey := accessKeyId
		if len(accessKeyId) > 4 {
			maskedKey = accessKeyId[:4] + "***"
		}
		glog.V(4).Infof("Access key %s not found in credential store", maskedKey)
		return
	}
	values.Set("UserName", identity.Name)
}

func (iama *IamApiServer) DoActions(w http.ResponseWriter, r *http.Request) {
	// Lock to prevent concurrent read-modify-write race conditions
	policyLock.Lock()
	defer policyLock.Unlock()

	r, reqID := request_id.Ensure(r)

	if err := r.ParseForm(); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.s3ApiConfig.GetS3ApiConfiguration(s3cfg); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(4).Infof("DoActions: %+v", iamlib.RedactSensitiveFormValues(values))
	var response iamlib.RequestIDSetter
	changed := true
	switch r.Form.Get("Action") {
	case "ListUsers":
		response = iama.ListUsers(s3cfg, values)
		changed = false
	case "ListAccessKeys":
		iama.handleImplicitUsername(r, values)
		response = iama.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response = iama.CreateUser(s3cfg, values)
	case "GetUser":
		userName := values.Get("UserName")
		var err *IamError
		response, err = iama.GetUser(s3cfg, userName)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "UpdateUser":
		var err *IamError
		response, err = iama.UpdateUser(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "DeleteUser":
		userName := values.Get("UserName")
		var err *IamError
		response, err = iama.DeleteUser(s3cfg, userName)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "CreateAccessKey":
		iama.handleImplicitUsername(r, values)
		var err *IamError
		response, err = iama.CreateAccessKey(s3cfg, values)
		if err != nil {
			glog.Errorf("CreateAccessKey: %+v", err.Error)
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "DeleteAccessKey":
		iama.handleImplicitUsername(r, values)
		response = iama.DeleteAccessKey(s3cfg, values)
	case "UpdateAccessKey":
		iama.handleImplicitUsername(r, values)
		var err *IamError
		response, err = iama.UpdateAccessKey(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "CreatePolicy":
		var err *IamError
		response, err = iama.CreatePolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		// CreatePolicy persists the policy document via iama.s3ApiConfig.PutPolicies().
		// The `changed` flag is false because this does not modify the main s3cfg.Identities configuration.
		changed = false
	case "PutUserPolicy":
		var err *IamError
		response, err = iama.PutUserPolicy(s3cfg, values)
		if err != nil {
			glog.Errorf("PutUserPolicy:  %+v", err.Error)
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "GetUserPolicy":
		var err *IamError
		response, err = iama.GetUserPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "DeleteUserPolicy":
		var err *IamError
		response, err = iama.DeleteUserPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "ListUserPolicies":
		iama.handleImplicitUsername(r, values)
		var err *IamError
		response, err = iama.ListUserPolicies(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "GetPolicy":
		var err *IamError
		response, err = iama.GetPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "DeletePolicy":
		var err *IamError
		response, err = iama.DeletePolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "ListPolicies":
		var err *IamError
		response, err = iama.ListPolicies(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "AttachUserPolicy":
		var err *IamError
		response, err = iama.AttachUserPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "DetachUserPolicy":
		var err *IamError
		response, err = iama.DetachUserPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "ListAttachedUserPolicies":
		var err *IamError
		response, err = iama.ListAttachedUserPolicies(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	// Group actions
	case "CreateGroup":
		var err *IamError
		response, err = iama.CreateGroup(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "DeleteGroup":
		var err *IamError
		response, err = iama.DeleteGroup(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "UpdateGroup":
		var err *IamError
		response, err = iama.UpdateGroup(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "GetGroup":
		var err *IamError
		response, err = iama.GetGroup(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "ListGroups":
		response = iama.ListGroups(s3cfg, values)
		changed = false
	case "AddUserToGroup":
		var err *IamError
		response, err = iama.AddUserToGroup(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "RemoveUserFromGroup":
		var err *IamError
		response, err = iama.RemoveUserFromGroup(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "AttachGroupPolicy":
		var err *IamError
		response, err = iama.AttachGroupPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "DetachGroupPolicy":
		var err *IamError
		response, err = iama.DetachGroupPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
	case "ListAttachedGroupPolicies":
		var err *IamError
		response, err = iama.ListAttachedGroupPolicies(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "PutGroupPolicy":
		var err *IamError
		response, err = iama.PutGroupPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		// changed = true: PutGroupPolicy recomputes member Identity.Actions
	case "GetGroupPolicy":
		var err *IamError
		response, err = iama.GetGroupPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "DeleteGroupPolicy":
		var err *IamError
		response, err = iama.DeleteGroupPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		// changed = true: DeleteGroupPolicy recomputes member Identity.Actions
	case "ListGroupPolicies":
		var err *IamError
		response, err = iama.ListGroupPolicies(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	case "ListGroupsForUser":
		var err *IamError
		response, err = iama.ListGroupsForUser(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, reqID, err)
			return
		}
		changed = false
	default:
		errNotImplemented := s3err.GetAPIError(s3err.ErrNotImplemented)
		errorResponse := newErrorResponse(errNotImplemented.Code, errNotImplemented.Description, reqID)
		s3err.WriteXMLResponse(w, r, errNotImplemented.HTTPStatusCode, errorResponse)
		return
	}
	if changed {
		err := iama.s3ApiConfig.PutS3ApiConfiguration(s3cfg)
		if err != nil {
			var iamError = IamError{Code: iam.ErrCodeServiceFailureException, Error: err}
			writeIamErrorResponse(w, r, reqID, &iamError)
			return
		}
		// Reload in-memory identity maps so subsequent LookupByAccessKey calls
		// can see newly created or deleted keys immediately
		if iama.iam != nil {
			if err := iama.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
				glog.Warningf("Failed to reload IAM configuration after mutation: %v", err)
				// Don't fail the request since the persistent save succeeded
			}
		}
	}
	response.SetRequestId(reqID)
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
}
