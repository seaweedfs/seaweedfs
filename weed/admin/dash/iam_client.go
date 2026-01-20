package dash

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// IAMClient provides HTTP client for IAM API calls
type IAMClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewIAMClient creates a new IAM client
func NewIAMClient(endpoint string) *IAMClient {
	return &IAMClient{
		endpoint: strings.TrimSuffix(endpoint, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// IAMRole represents an IAM role for the Admin UI
type IAMRole struct {
	RoleName         string      `json:"roleName"`
	RoleArn          string      `json:"roleArn"`
	TrustPolicy      interface{} `json:"trustPolicy"`
	AttachedPolicies []string    `json:"attachedPolicies"`
	Description      string      `json:"description,omitempty"`
	TrustPolicyJSON    string    `json:"trustPolicyJson,omitempty"`
	MaxSessionDuration int       `json:"maxSessionDuration"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// RolesData contains data for the Roles management page
type RolesData struct {
	Username    string    `json:"username"`
	Roles       []IAMRole `json:"roles"`
	TotalRoles  int       `json:"total_roles"`
	LastUpdated time.Time `json:"last_updated"`
}

// CreateRoleRequest is the request structure for creating a role
type CreateRoleRequest struct {
	RoleName         string   `json:"roleName" binding:"required"`
	TrustPolicyJSON  string   `json:"trustPolicyJson" binding:"required"`
	AttachedPolicies   []string `json:"attachedPolicies"`
	Description        string   `json:"description"`
	MaxSessionDuration int      `json:"maxSessionDuration"`
}

// IAMAPIRole represents an IAM role from the XML API response
type IAMAPIRole struct {
	RoleName                 string `xml:"RoleName"`
	RoleId                   string `xml:"RoleId"`
	Arn                      string `xml:"Arn"`
	Path                     string `xml:"Path"`
	Description              string `xml:"Description"`
	AssumeRolePolicyDocument string `xml:"AssumeRolePolicyDocument"`
	MaxSessionDuration       int    `xml:"MaxSessionDuration"`
}

// ListRolesResult is the XML result from ListRoles
type ListRolesResult struct {
	Roles []*IAMAPIRole `xml:"Roles>member"`
}

// ListRolesResponse is the full ListRoles response
type ListRolesResponse struct {
	XMLName         xml.Name        `xml:"ListRolesResponse"`
	ListRolesResult ListRolesResult `xml:"ListRolesResult"`
}

// GetRoleResult is the XML result from GetRole
type GetRoleResult struct {
	Role IAMAPIRole `xml:"Role"`
}

// GetRoleResponse is the full GetRole response
type GetRoleResponse struct {
	XMLName       xml.Name      `xml:"GetRoleResponse"`
	GetRoleResult GetRoleResult `xml:"GetRoleResult"`
}

// CreateRoleResult is the XML result from CreateRole
type CreateRoleResult struct {
	Role IAMAPIRole `xml:"Role"`
}

// CreateRoleAPIResponse is the full CreateRole response
type CreateRoleAPIResponse struct {
	XMLName          xml.Name         `xml:"CreateRoleResponse"`
	CreateRoleResult CreateRoleResult `xml:"CreateRoleResult"`
}

// IAMErrorResponse represents an IAM API error
type IAMErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

// doIAMRequest performs an IAM API request
func (c *IAMClient) doIAMRequest(action string, params map[string]string) ([]byte, error) {
	// Build form data
	formData := url.Values{}
	formData.Set("Action", action)
	for k, v := range params {
		formData.Set(k, v)
	}

	// Create request
	req, err := http.NewRequest("POST", c.endpoint, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check for error response
	if resp.StatusCode != http.StatusOK {
		var errResp IAMErrorResponse
		if xml.Unmarshal(body, &errResp) == nil && errResp.Error.Code != "" {
			return nil, fmt.Errorf("IAM API error %s: %s", errResp.Error.Code, errResp.Error.Message)
		}
		return nil, fmt.Errorf("IAM API error (HTTP %d): %s", resp.StatusCode, string(body))
	}

	return fixNamespace(body), nil
}

// ListRoles retrieves all IAM roles
func (c *IAMClient) ListRoles() ([]IAMRole, error) {
	body, err := c.doIAMRequest("ListRoles", nil)
	if err != nil {
		return nil, err
	}

	var resp ListRolesResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Convert to IAMRole format
	var roles []IAMRole
	for _, r := range resp.ListRolesResult.Roles {
		trustPolicyJSON, _ := url.QueryUnescape(r.AssumeRolePolicyDocument)
		
		role := IAMRole{
			RoleName:        r.RoleName,
			RoleArn:         r.Arn,
			Description:        r.Description,
			TrustPolicyJSON:    trustPolicyJSON,
			MaxSessionDuration: r.MaxSessionDuration,
			CreatedAt:          time.Now(),
			UpdatedAt:          time.Now(),
		}

		// Fetch attached policies
		attachedPolicies, err := c.ListAttachedRolePolicies(r.RoleName)
		if err == nil {
			role.AttachedPolicies = attachedPolicies
		} else {
			glog.Warningf("Failed to list attached policies for role %s: %v", r.RoleName, err)
		}

		roles = append(roles, role)
	}

	return roles, nil
}

// GetRole retrieves a specific IAM role
func (c *IAMClient) GetRole(roleName string) (*IAMRole, error) {
	body, err := c.doIAMRequest("GetRole", map[string]string{"RoleName": roleName})
	if err != nil {
		return nil, err
	}

	var resp GetRoleResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	r := resp.GetRoleResult.Role
	trustPolicyJSON, _ := url.QueryUnescape(r.AssumeRolePolicyDocument)

	role := &IAMRole{
		RoleName:        r.RoleName,
		RoleArn:         r.Arn,
		Description:        r.Description,
		TrustPolicyJSON:    trustPolicyJSON,
		MaxSessionDuration: r.MaxSessionDuration,
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}

	// Fetch attached policies
	attachedPolicies, err := c.ListAttachedRolePolicies(r.RoleName)
	if err == nil {
		role.AttachedPolicies = attachedPolicies
	} else {
		glog.Warningf("Failed to list attached policies for role %s: %v", r.RoleName, err)
	}

	return role, nil
}

// CreateRole creates a new IAM role
func (c *IAMClient) CreateRole(req CreateRoleRequest) error {
	params := map[string]string{
		"RoleName":                 req.RoleName,
		"AssumeRolePolicyDocument": req.TrustPolicyJSON,
	}
	if req.Description != "" {
		params["Description"] = req.Description
	}
	if req.MaxSessionDuration > 0 {
		params["MaxSessionDuration"] = fmt.Sprintf("%d", req.MaxSessionDuration)
	}

	body, err := c.doIAMRequest("CreateRole", params)
	if err != nil {
		return err
	}

	// Parse response to verify success
	var resp CreateRoleAPIResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	glog.V(1).Infof("Created role: %s (ARN: %s)", resp.CreateRoleResult.Role.RoleName, resp.CreateRoleResult.Role.Arn)

	// Attach policies if specified
	for _, policyName := range req.AttachedPolicies {
		policyArn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
		if err := c.AttachRolePolicy(req.RoleName, policyArn); err != nil {
			glog.Warningf("Failed to attach policy %s to role %s: %v", policyName, req.RoleName, err)
		}
	}

	return nil
}

// DeleteRole deletes an IAM role
func (c *IAMClient) DeleteRole(roleName string) error {
	// First detach all policies (AWS requires this before deletion)
	policies, err := c.ListAttachedRolePolicies(roleName)
	if err == nil {
		for _, policy := range policies {
			if err := c.DetachRolePolicy(roleName, policy); err != nil {
				glog.Warningf("Failed to detach policy %s: %v", policy, err)
			}
		}
	}

	_, err = c.doIAMRequest("DeleteRole", map[string]string{"RoleName": roleName})
	return err
}

// AttachRolePolicy attaches a policy to a role
func (c *IAMClient) AttachRolePolicy(roleName, policyArn string) error {
	_, err := c.doIAMRequest("AttachRolePolicy", map[string]string{
		"RoleName":  roleName,
		"PolicyArn": policyArn,
	})
	return err
}

// DetachRolePolicy detaches a policy from a role
func (c *IAMClient) DetachRolePolicy(roleName, policyArn string) error {
	_, err := c.doIAMRequest("DetachRolePolicy", map[string]string{
		"RoleName":  roleName,
		"PolicyArn": policyArn,
	})
	return err
}

// ListAttachedRolePoliciesResult is the XML result
type ListAttachedRolePoliciesResult struct {
	AttachedPolicies []struct {
		PolicyName string `xml:"PolicyName"`
		PolicyArn  string `xml:"PolicyArn"`
	} `xml:"AttachedPolicies>member"`
}

// ListAttachedRolePoliciesResponse is the full response
type ListAttachedRolePoliciesAPIResponse struct {
	XMLName                        xml.Name                       `xml:"ListAttachedRolePoliciesResponse"`
	ListAttachedRolePoliciesResult ListAttachedRolePoliciesResult `xml:"ListAttachedRolePoliciesResult"`
}

// ListAttachedRolePolicies lists policies attached to a role
func (c *IAMClient) ListAttachedRolePolicies(roleName string) ([]string, error) {
	body, err := c.doIAMRequest("ListAttachedRolePolicies", map[string]string{"RoleName": roleName})
	if err != nil {
		return nil, err
	}

	var resp ListAttachedRolePoliciesAPIResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var policies []string
	for _, p := range resp.ListAttachedRolePoliciesResult.AttachedPolicies {
		policies = append(policies, p.PolicyArn)
	}

	return policies, nil
}

// UpdateRole updates a role's description and max session duration
func (c *IAMClient) UpdateRole(roleName, description string, maxSessionDuration int) error {
	params := map[string]string{"RoleName": roleName}
	if description != "" {
		params["Description"] = description
	}
	if maxSessionDuration > 0 {
		params["MaxSessionDuration"] = fmt.Sprintf("%d", maxSessionDuration)
	}
	_, err := c.doIAMRequest("UpdateRole", params)
	return err
}

// UpdateAssumeRolePolicy updates the trust policy of a role
func (c *IAMClient) UpdateAssumeRolePolicy(roleName, policyDocument string) error {
	_, err := c.doIAMRequest("UpdateAssumeRolePolicy", map[string]string{
		"RoleName":       roleName,
		"PolicyDocument": policyDocument,
	})
	return err
}

// IAMGroup represents an IAM group for the Admin UI
type IAMGroup struct {
	GroupName        string    `json:"groupName"`
	GroupId          string    `json:"groupId"`
	Members          []string  `json:"members"`
	AttachedPolicies []string  `json:"attachedPolicies"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// GroupsData contains data for the Groups management page
type GroupsData struct {
	Username    string     `json:"username"`
	Groups      []IAMGroup `json:"groups"`
	TotalGroups int        `json:"total_groups"`
	LastUpdated time.Time  `json:"last_updated"`
}

// CreateGroupRequest is the request structure for creating a group
type CreateGroupRequest struct {
	GroupName string `json:"groupName" binding:"required"`
}

// Group API Response Structures

type IAMAPIGroup struct {
	GroupName string `xml:"GroupName"`
	GroupId   string `xml:"GroupId"`
	Arn       string `xml:"Arn"`
	Path      string `xml:"Path"`
}

type ListGroupsResult struct {
	Groups []*IAMAPIGroup `xml:"Groups>member"`
}

type ListGroupsResponse struct {
	XMLName          xml.Name         `xml:"ListGroupsResponse"`
	ListGroupsResult ListGroupsResult `xml:"ListGroupsResult"`
}

type CreateGroupResult struct {
	Group IAMAPIGroup `xml:"Group"`
}

type CreateGroupAPIResponse struct {
	XMLName           xml.Name          `xml:"CreateGroupResponse"`
	CreateGroupResult CreateGroupResult `xml:"CreateGroupResult"`
}

// Group Methods

// ListGroups retrieves all IAM groups
func (c *IAMClient) ListGroups() ([]IAMGroup, error) {
	body, err := c.doIAMRequest("ListGroups", nil)
	if err != nil {
		return nil, err
	}

	var resp ListGroupsResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var groups []IAMGroup
	for _, g := range resp.ListGroupsResult.Groups {
		// Fetch members and policies for each group (N+1 queries but necessary for full view if not returned in List)
		// Optimization: Maybe only do this for detail view? But UI usually shows summary.
		// For now, let's keep it simple and just list basic info, or maybe fetch members if cheap.
		// ListGroups in IAM API generally doesn't return members/policies.
		// We'll leave members/policies empty in list view for performance, or fetch them if needed.
		// Let's TRY to fetch members to show count at least.
		
		members, _ := c.GetGroup(g.GroupName) // Reuse GetGroup which should return members
		
		group := IAMGroup{
			GroupName: g.GroupName,
			GroupId:   g.GroupId,
			CreatedAt: time.Now(), // API doesn't return CreateDate in simple struct? It should.
			UpdatedAt: time.Now(),
		}
		
		if members != nil {
			group.Members = members.Members
			group.AttachedPolicies = members.AttachedPolicies
		}

		groups = append(groups, group)
	}

	return groups, nil
}

// GetGroup retrieves a specific IAM group with members and policies/
// Note: IAM GetGroup returns Group info AND Users. But implementation detail might vary.
// Our GetGroup API response structure needs to be checked.
type GetGroupResult struct {
	Group IAMAPIGroup   `xml:"Group"`
	Users []*IAMAPIUser `xml:"Users>member"` // Users in the group
}

type IAMAPIUser struct {
	UserName string `xml:"UserName"`
	UserId   string `xml:"UserId"`
	Arn      string `xml:"Arn"`
}

type GetGroupResponse struct {
	XMLName        xml.Name       `xml:"GetGroupResponse"`
	GetGroupResult GetGroupResult `xml:"GetGroupResult"`
}

func (c *IAMClient) GetGroup(groupName string) (*IAMGroup, error) {
	body, err := c.doIAMRequest("GetGroup", map[string]string{"GroupName": groupName})
	if err != nil {
		return nil, err
	}

	var resp GetGroupResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Also need Attached Policies - separate call "ListAttachedGroupPolicies"
	policies, _ := c.ListAttachedGroupPolicies(groupName)

	var members []string
	for _, u := range resp.GetGroupResult.Users {
		members = append(members, u.UserName)
	}

	group := &IAMGroup{
		GroupName:        resp.GetGroupResult.Group.GroupName,
		GroupId:          resp.GetGroupResult.Group.GroupId,
		Members:          members,
		AttachedPolicies: policies,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}

	return group, nil
}

// CreateGroup creates a new IAM group
func (c *IAMClient) CreateGroup(req CreateGroupRequest) error {
	params := map[string]string{
		"GroupName": req.GroupName,
	}

	body, err := c.doIAMRequest("CreateGroup", params)
	if err != nil {
		return err
	}

	var resp CreateGroupAPIResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	return nil
}

// DeleteGroup deletes an IAM group
func (c *IAMClient) DeleteGroup(groupName string) error {
	// Need to check if group is empty? IAM usually requires empty group.
	// We might want to auto-clean members/policies?
	// For now, just call DeleteGroup.
	_, err := c.doIAMRequest("DeleteGroup", map[string]string{"GroupName": groupName})
	return err
}

// AddUserToGroup adds a user to a group
func (c *IAMClient) AddUserToGroup(groupName, userName string) error {
	_, err := c.doIAMRequest("AddUserToGroup", map[string]string{
		"GroupName": groupName,
		"UserName":  userName,
	})
	return err
}

// RemoveUserFromGroup removes a user from a group
func (c *IAMClient) RemoveUserFromGroup(groupName, userName string) error {
	_, err := c.doIAMRequest("RemoveUserFromGroup", map[string]string{
		"GroupName": groupName,
		"UserName":  userName,
	})
	return err
}

// ListAttachedGroupPoliciesResponse
type ListAttachedGroupPoliciesResponse struct {
	XMLName                         xml.Name                        `xml:"ListAttachedGroupPoliciesResponse"`
	ListAttachedGroupPoliciesResult ListAttachedGroupPoliciesResult `xml:"ListAttachedGroupPoliciesResult"`
}

type ListAttachedGroupPoliciesResult struct {
	AttachedPolicies []struct {
		PolicyName string `xml:"PolicyName"`
		PolicyArn  string `xml:"PolicyArn"`
	} `xml:"AttachedPolicies>member"`
}

// ListAttachedGroupPolicies lists policies attached to a group
func (c *IAMClient) ListAttachedGroupPolicies(groupName string) ([]string, error) {
	body, err := c.doIAMRequest("ListAttachedGroupPolicies", map[string]string{"GroupName": groupName})
	if err != nil {
		return nil, err
	}

	var resp ListAttachedGroupPoliciesResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var policies []string
	for _, p := range resp.ListAttachedGroupPoliciesResult.AttachedPolicies {
		policies = append(policies, p.PolicyArn)
	}

	return policies, nil
}

// AttachGroupPolicy attaches a policy to a group
func (c *IAMClient) AttachGroupPolicy(groupName, policyArn string) error {
	_, err := c.doIAMRequest("AttachGroupPolicy", map[string]string{
		"GroupName": groupName,
		"PolicyArn": policyArn,
	})
	return err
}

// DetachGroupPolicy detaches a policy from a group
func (c *IAMClient) DetachGroupPolicy(groupName, policyArn string) error {
	_, err := c.doIAMRequest("DetachGroupPolicy", map[string]string{
		"GroupName": groupName,
		"PolicyArn": policyArn,
	})
	return err
}

// SetEndpoint updates the IAM endpoint (for dynamic filer discovery)
func (c *IAMClient) SetEndpoint(endpoint string) {
	c.endpoint = strings.TrimSuffix(endpoint, "/")
}

// FixNamespace handles AWS XML namespace issues
func fixNamespace(data []byte) []byte {
	// Remove AWS namespace prefix if present
	data = bytes.ReplaceAll(data, []byte("https://iam.amazonaws.com/doc/2010-05-08/ "), []byte(""))
	return data
}

// ==========================================
// Policy Management Methods
// ==========================================

// ClientIAMPolicy represents an IAM policy for the Admin UI client
type ClientIAMPolicy struct {
	PolicyName  string    `json:"policyName"`
	PolicyId    string    `json:"policyId"`
	Arn         string    `json:"arn"`
	Path        string    `json:"path"`
	Description string    `json:"description,omitempty"`
	CreateDate  time.Time `json:"create_date"`
	UpdateDate  time.Time `json:"update_date"`
	VersionId   string    `json:"version_id"`
	Document    string    `json:"document,omitempty"` // For detailed view
	AttachmentCount int   `json:"attachment_count"`
}

// ClientPoliciesData contains data for the Policies management page
type ClientPoliciesData struct {
	Username      string            `json:"username"`
	Policies      []ClientIAMPolicy `json:"policies"`
	TotalPolicies int               `json:"total_policies"`
	LastUpdated   time.Time         `json:"last_updated"`
}

// ClientCreatePolicyRequest is the request structure for creating a policy
type ClientCreatePolicyRequest struct {
	Name        string      `json:"name" binding:"required"`
	Document    interface{} `json:"document" binding:"required"`
	Description string      `json:"description"`
}

// ClientUpdatePolicyRequest is the request structure for updating a policy
type ClientUpdatePolicyRequest struct {
	Document interface{} `json:"document" binding:"required"`
}

// IAMAPIPolicy represents an IAM policy from XML
type IAMAPIPolicy struct {
	PolicyName       string `xml:"PolicyName"`
	PolicyId         string `xml:"PolicyId"`
	Arn              string `xml:"Arn"`
	Path             string `xml:"Path"`
	DefaultVersionId string `xml:"DefaultVersionId"`
	AttachmentCount  int    `xml:"AttachmentCount"`
	Description      string `xml:"Description"`
	CreateDate       string `xml:"CreateDate"` // AWS returns ISO8601 string
	UpdateDate       string `xml:"UpdateDate"`
}

// ListPoliciesResponse
type ListPoliciesResult struct {
	Policies []*IAMAPIPolicy `xml:"Policies>member"`
	IsTruncated bool `xml:"IsTruncated"`
}

type ListPoliciesResponse struct {
	XMLName            xml.Name           `xml:"ListPoliciesResponse"`
	ListPoliciesResult ListPoliciesResult `xml:"ListPoliciesResult"`
}

// GetPolicyResponse
type GetPolicyResult struct {
	Policy IAMAPIPolicy `xml:"Policy"`
}

type GetPolicyResponse struct {
	XMLName         xml.Name        `xml:"GetPolicyResponse"`
	GetPolicyResult GetPolicyResult `xml:"GetPolicyResult"`
}

// CreatePolicyResponse
type CreatePolicyResult struct {
	Policy IAMAPIPolicy `xml:"Policy"`
}

type CreatePolicyAPIResponse struct {
	XMLName            xml.Name           `xml:"CreatePolicyResponse"`
	CreatePolicyResult CreatePolicyResult `xml:"CreatePolicyResult"`
}

// ListPolicies lists all managed policies
func (c *IAMClient) ListPolicies() ([]ClientIAMPolicy, error) {
	body, err := c.doIAMRequest("ListPolicies", nil)
	if err != nil {
		return nil, err
	}

	var resp ListPoliciesResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var policies []ClientIAMPolicy
	for _, p := range resp.ListPoliciesResult.Policies {
		createDate, err := time.Parse(time.RFC3339, p.CreateDate)
		if err != nil {
			glog.Warningf("Failed to parse CreateDate %s for policy %s: %v", p.CreateDate, p.PolicyName, err)
			createDate = time.Now() // Fallback
		}
		
		updateDate, err := time.Parse(time.RFC3339, p.UpdateDate)
		if err != nil {
			glog.Warningf("Failed to parse UpdateDate %s for policy %s: %v", p.UpdateDate, p.PolicyName, err)
			updateDate = time.Now() // Fallback
		}

		policies = append(policies, ClientIAMPolicy{
			PolicyName:      p.PolicyName,
			PolicyId:        p.PolicyId,
			Arn:             p.Arn,
			Path:            p.Path,
			Description:     p.Description,
			CreateDate:      createDate,
			UpdateDate:      updateDate,
			VersionId:       p.DefaultVersionId,
			AttachmentCount: p.AttachmentCount,
		})
	}
	return policies, nil
}

// GetPolicy retrieves a policy
func (c *IAMClient) GetPolicy(policyArn string) (*ClientIAMPolicy, error) {
	body, err := c.doIAMRequest("GetPolicy", map[string]string{"PolicyArn": policyArn})
	if err != nil {
		return nil, err
	}

	var resp GetPolicyResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	
	p := resp.GetPolicyResult.Policy
	createDate, err := time.Parse(time.RFC3339, p.CreateDate)
	if err != nil {
		glog.Warningf("Failed to parse CreateDate %s for policy %s: %v", p.CreateDate, p.PolicyName, err)
		createDate = time.Now()
	}
	
	updateDate, err := time.Parse(time.RFC3339, p.UpdateDate)
	if err != nil {
		glog.Warningf("Failed to parse UpdateDate %s for policy %s: %v", p.UpdateDate, p.PolicyName, err)
		updateDate = time.Now()
	}

	// Note: GetPolicy typically returns metadata. To get document we might need GetPolicyVersion?
	// But let's check if Admin UI currently expects document in GetPolicy.
	// AdminServer.GetPolicy returns *policy_engine.PolicyDocument which has everything.
	// IAM API separates Metadata (GetPolicy) and Document (GetPolicyVersion).
	// We might need to call GetPolicyVersion as well if document is needed.
	// For now let's just return what we have.
	
	policy := &ClientIAMPolicy{
		PolicyName:  p.PolicyName,
		PolicyId:    p.PolicyId,
		Arn:         p.Arn,
		Path:        p.Path,
		Description: p.Description,
		CreateDate:  createDate,
		UpdateDate:  updateDate,
		VersionId:   p.DefaultVersionId,
	}
	
	// If we need the document, we should fetch the default version
	// Only fetch if VersionId is present
	if p.DefaultVersionId != "" {
		doc, err := c.GetPolicyVersion(policyArn, p.DefaultVersionId)
		if err == nil {
			policy.Document = doc
		}
	}

	return policy, nil
}

// CreatePolicy creates a new managed policy
func (c *IAMClient) CreatePolicy(req ClientCreatePolicyRequest) error {
	// Marshal document to JSON string if it's an object/map
	var docStr string
	if str, ok := req.Document.(string); ok {
		docStr = str
	} else {
		docBytes, err := json.Marshal(req.Document)
		if err != nil {
			return fmt.Errorf("failed to marshal policy document: %w", err)
		}
		docStr = string(docBytes)
	}

	params := map[string]string{
		"PolicyName":     req.Name,
		"PolicyDocument": docStr,
	}
	if req.Description != "" {
		params["Description"] = req.Description
	}

	body, err := c.doIAMRequest("CreatePolicy", params)
	if err != nil {
		return err
	}

	var resp CreatePolicyAPIResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	return nil
}

// UpdatePolicy updates a managed policy using CreatePolicyVersion (simulated in-place update)
func (c *IAMClient) UpdatePolicy(name string, document interface{}) error {
	// Find the real ARN for the policy, as it might differ from the guessed one
	var arn string
	policies, err := c.ListPolicies()
	if err == nil {
		for _, p := range policies {
			if p.PolicyName == name {
				arn = p.Arn
				break
			}
		}
	}

	// Fallback if not found 
	if arn == "" {
		arn = fmt.Sprintf("arn:aws:iam:::policy/%s", name)
	}

	// Marshal document to JSON string if it's an object/map
	var docStr string
	if str, ok := document.(string); ok {
		docStr = str
	} else {
		docBytes, err := json.Marshal(document)
		if err != nil {
			return fmt.Errorf("failed to marshal policy document: %w", err)
		}
		docStr = string(docBytes)
	}
	
	// Use CreatePolicyVersion to update without deleting (preserves attachments)
	return c.CreatePolicyVersion(arn, docStr)
}

// CreatePolicyVersion creates a new policy version (updates the policy)
func (c *IAMClient) CreatePolicyVersion(policyArn string, document string) error {
	params := map[string]string{
		"PolicyArn":      policyArn,
		"PolicyDocument": document,
		"SetAsDefault":   "true", // Typically required for AWS to flip to new version immediately
	}

	_, err := c.doIAMRequest("CreatePolicyVersion", params)
	if err != nil {
		glog.V(1).Infof("Failed to create policy version for %s: %v", policyArn, err)
	}
	return err
}

// DeletePolicy deletes a managed policy
func (c *IAMClient) DeletePolicy(policyArn string) error {
	_, err := c.doIAMRequest("DeletePolicy", map[string]string{"PolicyArn": policyArn})
	return err
}

// GetPolicyVersionResult
type GetPolicyVersionResult struct {
	PolicyVersion struct {
		Document string `xml:"Document"`
	} `xml:"PolicyVersion"`
}

type GetPolicyVersionResponse struct {
	XMLName                xml.Name               `xml:"GetPolicyVersionResponse"`
	GetPolicyVersionResult GetPolicyVersionResult `xml:"GetPolicyVersionResult"`
}

// GetPolicyVersion retrieves a policy version document
func (c *IAMClient) GetPolicyVersion(policyArn, versionId string) (string, error) {
	body, err := c.doIAMRequest("GetPolicyVersion", map[string]string{
		"PolicyArn": policyArn,
		"VersionId": versionId,
	})
	if err != nil {
		return "", err
	}

	var resp GetPolicyVersionResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	doc, _ := url.QueryUnescape(resp.GetPolicyVersionResult.PolicyVersion.Document)
	return doc, nil
}

// User Management Methods

// IAMUser represents an IAM user
type IAMUser struct {
	UserName   string    `json:"userName"`
	UserId     string    `json:"userId"`
	Arn        string    `json:"arn"`
	CreateDate time.Time `json:"createDate"`
}

// UsersData contains data for the Users management page
type UsersData struct {
	Username    string    `json:"username"`
	Users       []IAMUser `json:"users"`
	TotalUsers  int       `json:"total_users"`
	LastUpdated time.Time `json:"last_updated"`
}

// IAMCreateUserRequest
type IAMCreateUserRequest struct {
	UserName string `json:"userName" binding:"required"`
}

// ListUsersResponse
type ListUsersResult struct {
	Users []*IAMAPIUserWithDate `xml:"Users>member"`
}

type IAMAPIUserWithDate struct {
	UserName   string `xml:"UserName"`
	UserId     string `xml:"UserId"`
	Arn        string `xml:"Arn"`
	CreateDate string `xml:"CreateDate"`
}

type ListUsersResponse struct {
	XMLName         xml.Name        `xml:"ListUsersResponse"`
	ListUsersResult ListUsersResult `xml:"ListUsersResult"`
}

// GetUserResponse
type GetUserResult struct {
	User IAMAPIUserWithDate `xml:"User"`
}

type GetUserResponse struct {
	XMLName       xml.Name      `xml:"GetUserResponse"`
	GetUserResult GetUserResult `xml:"GetUserResult"`
}

// CreateUserResponse
type CreateUserResult struct {
	User IAMAPIUserWithDate `xml:"User"`
}

type CreateUserAPIResponse struct {
	XMLName          xml.Name         `xml:"CreateUserResponse"`
	CreateUserResult CreateUserResult `xml:"CreateUserResult"`
}

// ListUsers lists all IAM users
func (c *IAMClient) ListUsers() ([]IAMUser, error) {
	body, err := c.doIAMRequest("ListUsers", nil)
	if err != nil {
		return nil, err
	}

	var resp ListUsersResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var users []IAMUser
	for _, u := range resp.ListUsersResult.Users {
		// Try parsing with standard RFC3339 first, then fallback to simpler formats if needed
		createDate, err := time.Parse(time.RFC3339, u.CreateDate)
		if err != nil {
			// Try parsing without timezone if RFC3339 fails (sometimes simplified ISO8601 is returned)
			createDate, err = time.Parse("2006-01-02T15:04:05", u.CreateDate)
			if err != nil {
				glog.Warningf("Failed to parse CreateDate %s for user %s: %v", u.CreateDate, u.UserName, err)
				createDate = time.Time{} // Return zero time
			}
		}
		users = append(users, IAMUser{
			UserName:   u.UserName,
			UserId:     u.UserId,
			Arn:        u.Arn,
			CreateDate: createDate,
		})
	}
	return users, nil
}

// GetUser retrieves a specific user
func (c *IAMClient) GetUser(userName string) (*IAMUser, error) {
	body, err := c.doIAMRequest("GetUser", map[string]string{"UserName": userName})
	if err != nil {
		return nil, err
	}

	var resp GetUserResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	u := resp.GetUserResult.User
	createDate, err := time.Parse(time.RFC3339, u.CreateDate)
	if err != nil {
		createDate, err = time.Parse("2006-01-02T15:04:05", u.CreateDate)
		if err != nil {
			glog.Warningf("Failed to parse CreateDate %s for user %s: %v", u.CreateDate, u.UserName, err)
			createDate = time.Time{}
		}
	}
	
	return &IAMUser{
		UserName:   u.UserName,
		UserId:     u.UserId,
		Arn:        u.Arn,
		CreateDate: createDate,
	}, nil
}

// CreateUser creates a new user
func (c *IAMClient) CreateUser(userName string) error {
	_, err := c.doIAMRequest("CreateUser", map[string]string{"UserName": userName})
	return err
}

// DeleteUser deletes a user
func (c *IAMClient) DeleteUser(userName string) error {
	_, err := c.doIAMRequest("DeleteUser", map[string]string{"UserName": userName})
	return err
}

// Access Key Methods

type IAMAccessKey struct {
	AccessKeyId string    `json:"accessKeyId"`
	Status      string    `json:"status"`
	CreateDate  time.Time `json:"createDate"`
	SecretKey   string    `json:"secretKey,omitempty"` // Only present on creation
}

type AccessKeysData struct {
	AccessKeys []IAMAccessKey `json:"accessKeys"`
}

// ListAccessKeysResponse
type ListAccessKeysResult struct {
	AccessKeyMetadata []*IAMAPIAccessKeyMetadata `xml:"AccessKeyMetadata>member"`
}

type IAMAPIAccessKeyMetadata struct {
	AccessKeyId string `xml:"AccessKeyId"`
	Status      string `xml:"Status"`
	CreateDate  string `xml:"CreateDate"`
	UserName    string `xml:"UserName"`
}

type ListAccessKeysResponse struct {
	XMLName              xml.Name             `xml:"ListAccessKeysResponse"`
	ListAccessKeysResult ListAccessKeysResult `xml:"ListAccessKeysResult"`
}

// CreateAccessKeyResponse
type CreateAccessKeyResult struct {
	AccessKey struct {
		AccessKeyId     string `xml:"AccessKeyId"`
		SecretAccessKey string `xml:"SecretAccessKey"`
		Status          string `xml:"Status"`
		CreateDate      string `xml:"CreateDate"`
		UserName        string `xml:"UserName"`
	} `xml:"AccessKey"`
}

type CreateAccessKeyAPIResponse struct {
	XMLName               xml.Name             `xml:"CreateAccessKeyResponse"`
	CreateAccessKeyResult CreateAccessKeyResult `xml:"CreateAccessKeyResult"`
}

// ListAccessKeys lists access keys for a user
func (c *IAMClient) ListAccessKeys(userName string) ([]IAMAccessKey, error) {
	body, err := c.doIAMRequest("ListAccessKeys", map[string]string{"UserName": userName})
	if err != nil {
		return nil, err
	}

	var resp ListAccessKeysResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var keys []IAMAccessKey
	for _, k := range resp.ListAccessKeysResult.AccessKeyMetadata {
		createDate, err := time.Parse(time.RFC3339, k.CreateDate)
		if err != nil {
			createDate, err = time.Parse("2006-01-02T15:04:05", k.CreateDate)
			if err != nil {
				glog.Warningf("Failed to parse CreateDate %s for access key %s: %v", k.CreateDate, k.AccessKeyId, err)
				createDate = time.Time{}
			}
		}
		keys = append(keys, IAMAccessKey{
			AccessKeyId: k.AccessKeyId,
			Status:      k.Status,
			CreateDate:  createDate,
		})
	}
	return keys, nil
}

// CreateAccessKey creates a new access key for a user
func (c *IAMClient) CreateAccessKey(userName string) (*IAMAccessKey, error) {
	body, err := c.doIAMRequest("CreateAccessKey", map[string]string{"UserName": userName})
	if err != nil {
		return nil, err
	}

	var resp CreateAccessKeyAPIResponse
	if err := xml.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	k := resp.CreateAccessKeyResult.AccessKey
	createDate, err := time.Parse(time.RFC3339, k.CreateDate)
	if err != nil {
		createDate, err = time.Parse("2006-01-02T15:04:05", k.CreateDate)
		if err != nil {
			glog.Warningf("Failed to parse CreateDate %s for new access key %s: %v", k.CreateDate, k.AccessKeyId, err)
			createDate = time.Time{}
		}
	}

	return &IAMAccessKey{
		AccessKeyId: k.AccessKeyId,
		SecretKey:   k.SecretAccessKey,
		Status:      k.Status,
		CreateDate:  createDate,
	}, nil
}

// DeleteAccessKey deletes an access key
func (c *IAMClient) DeleteAccessKey(userName, accessKeyId string) error {
	_, err := c.doIAMRequest("DeleteAccessKey", map[string]string{
		"UserName":    userName,
		"AccessKeyId": accessKeyId,
	})
	return err
}

// UpdateAccessKey updates access key status (Active/Inactive)
func (c *IAMClient) UpdateAccessKey(userName, accessKeyId, status string) error {
	_, err := c.doIAMRequest("UpdateAccessKey", map[string]string{
		"UserName":    userName,
		"AccessKeyId": accessKeyId,
		"Status":      status,
	})
	return err
}

