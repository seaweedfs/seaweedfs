package s3api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/iam"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
)

// Role IAM actions handled by this file.
const (
	actionCreateRole               = "CreateRole"
	actionGetRole                  = "GetRole"
	actionListRoles                = "ListRoles"
	actionDeleteRole               = "DeleteRole"
	actionUpdateAssumeRolePolicy   = "UpdateAssumeRolePolicy"
	actionAttachRolePolicy         = "AttachRolePolicy"
	actionDetachRolePolicy         = "DetachRolePolicy"
	actionListAttachedRolePolicies = "ListAttachedRolePolicies"
)

// isRoleAction reports whether an action belongs to the role family.
func isRoleAction(action string) bool {
	switch action {
	case actionCreateRole, actionGetRole, actionListRoles, actionDeleteRole,
		actionUpdateAssumeRolePolicy, actionAttachRolePolicy, actionDetachRolePolicy,
		actionListAttachedRolePolicies:
		return true
	default:
		return false
	}
}

// dispatchRoleAction handles the role IAM actions. Roles live in the IAM
// manager's role store, not in S3ApiConfiguration, so like the OIDC provider
// actions they are dispatched before the configuration load. The boolean
// reports whether the action was recognised.
func (e *EmbeddedIamApi) dispatchRoleAction(ctx context.Context, values url.Values) (iamlib.RequestIDSetter, *iamError, bool) {
	if !isRoleAction(values.Get("Action")) {
		return nil, nil, false
	}
	mgr := e.oidcIAMManager()
	if mgr == nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: errors.New("role store not configured: start the S3 server with an IAM config")}, true
	}

	switch values.Get("Action") {
	case actionCreateRole:
		resp, err := e.createRole(ctx, mgr, values)
		return resp, err, true
	case actionGetRole:
		resp, err := e.getRole(ctx, mgr, values)
		return resp, err, true
	case actionListRoles:
		resp, err := e.listRoles(ctx, mgr)
		return resp, err, true
	case actionDeleteRole:
		resp, err := e.deleteRole(ctx, mgr, values)
		return resp, err, true
	case actionUpdateAssumeRolePolicy:
		resp, err := e.updateAssumeRolePolicy(ctx, mgr, values)
		return resp, err, true
	case actionAttachRolePolicy:
		resp, err := e.attachRolePolicy(ctx, mgr, values)
		return resp, err, true
	case actionDetachRolePolicy:
		resp, err := e.detachRolePolicy(ctx, mgr, values)
		return resp, err, true
	case actionListAttachedRolePolicies:
		resp, err := e.listAttachedRolePolicies(ctx, mgr, values)
		return resp, err, true
	}
	return nil, nil, false
}

// parseTrustPolicy decodes and validates an AssumeRolePolicyDocument.
func parseTrustPolicy(document string) (*policy.PolicyDocument, *iamError) {
	if strings.TrimSpace(document) == "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("AssumeRolePolicyDocument is required")}
	}
	var doc policy.PolicyDocument
	if err := json.Unmarshal([]byte(document), &doc); err != nil {
		return nil, &iamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: fmt.Errorf("parse trust policy: %w", err)}
	}
	if err := policy.ValidateTrustPolicyDocument(&doc); err != nil {
		return nil, &iamError{Code: iam.ErrCodeMalformedPolicyDocumentException, Error: err}
	}
	return &doc, nil
}

// requireRole loads the named role, mapping a missing role to NoSuchEntity.
func requireRole(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*integration.RoleDefinition, *iamError) {
	name := strings.TrimSpace(values.Get("RoleName"))
	if name == "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("RoleName is required")}
	}
	role, err := mgr.GetRole(ctx, name)
	if errors.Is(err, integration.ErrRoleNotFound) || (err == nil && role == nil) {
		return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("role %s not found", name)}
	}
	if err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return role, nil
}

// requireMutableRole is requireRole for actions that change or delete the
// role. A role loaded from the IAM config file is reloaded from it at every
// start, so a change made through the API would be silently reverted; it is
// refused instead.
func requireMutableRole(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*integration.RoleDefinition, *iamError) {
	role, iamErr := requireRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	if role.Source == integration.RoleSourceStaticConfig {
		return nil, &iamError{Code: iam.ErrCodeUnmodifiableEntityException, Error: fmt.Errorf("role %s is defined in the IAM config file; change it there", role.RoleName)}
	}
	return role, nil
}

// roleID is the role's stored ID. A role stored before IDs were recorded has
// none; it is reported with the ID a config-file role of that name would get.
func roleID(role *integration.RoleDefinition) string {
	if role.RoleId != "" {
		return role.RoleId
	}
	return integration.StaticRoleID(role.RoleName)
}

func toIAMRole(role *integration.RoleDefinition) iamlib.IAMRole {
	out := iamlib.IAMRole{
		Path:               "/",
		RoleName:           role.RoleName,
		RoleId:             roleID(role),
		Arn:                role.RoleArn,
		Description:        role.Description,
		MaxSessionDuration: role.MaxSessionDuration,
	}
	if !role.CreatedAt.IsZero() {
		out.CreateDate = role.CreatedAt.UTC().Format(time.RFC3339)
	}
	if role.TrustPolicy != nil {
		if doc, err := json.Marshal(role.TrustPolicy); err == nil {
			// AWS returns the document URL-encoded.
			out.AssumeRolePolicyDocument = url.PathEscape(string(doc))
		}
	}
	return out
}

func (e *EmbeddedIamApi) createRole(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.CreateRoleResponse, *iamError) {
	name := strings.TrimSpace(values.Get("RoleName"))
	if name == "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("RoleName is required")}
	}
	if err := integration.ValidateRoleName(name); err != nil {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}
	if path := values.Get("Path"); path != "" && path != "/" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("role paths are not supported: %s", path)}
	}
	if values.Get("Tags.member.1.Key") != "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("role tags are not supported")}
	}
	// Only a confirmed absence may proceed: an unreadable store must not let
	// CreateRole overwrite a role that exists, config-file roles included.
	existing, err := mgr.GetRole(ctx, name)
	if err == nil && existing != nil {
		return nil, &iamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: fmt.Errorf("role %s already exists", name)}
	}
	if err != nil && !errors.Is(err, integration.ErrRoleNotFound) {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	trust, iamErr := parseTrustPolicy(values.Get("AssumeRolePolicyDocument"))
	if iamErr != nil {
		return nil, iamErr
	}
	var maxSession int64
	if raw := values.Get("MaxSessionDuration"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: fmt.Errorf("MaxSessionDuration: %w", err)}
		}
		maxSession = n
	}

	role := &integration.RoleDefinition{
		RoleName:           name,
		TrustPolicy:        trust,
		Description:        values.Get("Description"),
		MaxSessionDuration: maxSession,
		CreatedAt:          time.Now().UTC(),
	}
	if err := mgr.CreateRole(ctx, "", name, role); err != nil {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}
	resp := &iamlib.CreateRoleResponse{}
	resp.CreateRoleResult.Role = toIAMRole(role)
	return resp, nil
}

func (e *EmbeddedIamApi) getRole(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.GetRoleResponse, *iamError) {
	role, iamErr := requireRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	resp := &iamlib.GetRoleResponse{}
	resp.GetRoleResult.Role = toIAMRole(role)
	return resp, nil
}

func (e *EmbeddedIamApi) listRoles(ctx context.Context, mgr *integration.IAMManager) (*iamlib.ListRolesResponse, *iamError) {
	roles, err := mgr.ListRoles(ctx)
	if err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	resp := &iamlib.ListRolesResponse{}
	resp.ListRolesResult.Roles = make([]*iamlib.IAMRole, 0, len(roles))
	for _, role := range roles {
		view := toIAMRole(role)
		resp.ListRolesResult.Roles = append(resp.ListRolesResult.Roles, &view)
	}
	return resp, nil
}

func (e *EmbeddedIamApi) deleteRole(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.DeleteRoleResponse, *iamError) {
	role, iamErr := requireMutableRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	// AWS refuses to delete a role that still has managed policies attached.
	if len(role.AttachedPolicies) > 0 {
		return nil, &iamError{Code: iam.ErrCodeDeleteConflictException, Error: fmt.Errorf("role %s has attached policies; detach them first", role.RoleName)}
	}
	if err := mgr.DeleteRole(ctx, role.RoleName); err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.DeleteRoleResponse{}, nil
}

func (e *EmbeddedIamApi) updateAssumeRolePolicy(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.UpdateAssumeRolePolicyResponse, *iamError) {
	role, iamErr := requireMutableRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	trust, iamErr := parseTrustPolicy(values.Get("PolicyDocument"))
	if iamErr != nil {
		return nil, iamErr
	}
	role.TrustPolicy = trust
	if err := mgr.CreateRole(ctx, "", role.RoleName, role); err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.UpdateAssumeRolePolicyResponse{}, nil
}

// rolePolicyName resolves PolicyArn to the name of an existing managed policy.
func (e *EmbeddedIamApi) rolePolicyName(ctx context.Context, values url.Values) (string, *iamError) {
	name, err := iamPolicyNameFromArn(values.Get("PolicyArn"))
	if err != nil {
		return "", &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}
	if e.credentialManager == nil {
		return "", &iamError{Code: iam.ErrCodeServiceFailureException, Error: errors.New("credential manager not configured")}
	}
	existing, err := e.credentialManager.GetPolicy(ctx, name)
	if err != nil {
		return "", &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	if existing == nil {
		return "", &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s not found", name)}
	}
	return name, nil
}

func (e *EmbeddedIamApi) attachRolePolicy(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.AttachRolePolicyResponse, *iamError) {
	role, iamErr := requireMutableRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	name, iamErr := e.rolePolicyName(ctx, values)
	if iamErr != nil {
		return nil, iamErr
	}
	if !slices.Contains(role.AttachedPolicies, name) {
		if len(role.AttachedPolicies) >= integration.MaxManagedPoliciesPerRole {
			return nil, &iamError{Code: iam.ErrCodeLimitExceededException,
				Error: fmt.Errorf("cannot attach more than %d managed policies to role %s", integration.MaxManagedPoliciesPerRole, role.RoleName)}
		}
		role.AttachedPolicies = append(role.AttachedPolicies, name)
		if err := mgr.CreateRole(ctx, "", role.RoleName, role); err != nil {
			return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
		}
	}
	return &iamlib.AttachRolePolicyResponse{}, nil
}

func (e *EmbeddedIamApi) detachRolePolicy(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.DetachRolePolicyResponse, *iamError) {
	role, iamErr := requireMutableRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	name, err := iamPolicyNameFromArn(values.Get("PolicyArn"))
	if err != nil {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}
	idx := slices.Index(role.AttachedPolicies, name)
	if idx < 0 {
		return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: fmt.Errorf("policy %s is not attached to role %s", name, role.RoleName)}
	}
	role.AttachedPolicies = slices.Delete(role.AttachedPolicies, idx, idx+1)
	if err := mgr.CreateRole(ctx, "", role.RoleName, role); err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.DetachRolePolicyResponse{}, nil
}

func (e *EmbeddedIamApi) listAttachedRolePolicies(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.ListAttachedRolePoliciesResponse, *iamError) {
	role, iamErr := requireRole(ctx, mgr, values)
	if iamErr != nil {
		return nil, iamErr
	}
	resp := &iamlib.ListAttachedRolePoliciesResponse{}
	resp.ListAttachedRolePoliciesResult.AttachedPolicies = make([]*iamlib.IAMAttachedPolicy, 0, len(role.AttachedPolicies))
	for _, name := range role.AttachedPolicies {
		resp.ListAttachedRolePoliciesResult.AttachedPolicies = append(resp.ListAttachedRolePoliciesResult.AttachedPolicies,
			&iamlib.IAMAttachedPolicy{PolicyName: name, PolicyArn: iamPolicyArn(name)})
	}
	return resp, nil
}
