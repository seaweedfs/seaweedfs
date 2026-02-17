package policy_engine

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Action represents an S3 action - this should match the type in auth_credentials.go
type Action string

// Identity represents a user identity - this should match the type in auth_credentials.go
type Identity interface {
	CanDo(action Action, bucket string, objectKey string) bool
}

// PolicyBackedIAM provides policy-based access control with fallback to legacy IAM
type PolicyBackedIAM struct {
	policyEngine *PolicyEngine
	legacyIAM    LegacyIAM // Interface to delegate to existing IAM system
}

// LegacyIAM interface for delegating to existing IAM implementation
type LegacyIAM interface {
	authRequest(r *http.Request, action Action) (Identity, s3err.ErrorCode)
}

// NewPolicyBackedIAM creates a new policy-backed IAM system
func NewPolicyBackedIAM() *PolicyBackedIAM {
	return &PolicyBackedIAM{
		policyEngine: NewPolicyEngine(),
		legacyIAM:    nil, // Will be set when integrated with existing IAM
	}
}

// NewPolicyBackedIAMWithLegacy creates a new policy-backed IAM system with legacy IAM set
func NewPolicyBackedIAMWithLegacy(legacyIAM LegacyIAM) *PolicyBackedIAM {
	return &PolicyBackedIAM{
		policyEngine: NewPolicyEngine(),
		legacyIAM:    legacyIAM,
	}
}

// SetLegacyIAM sets the legacy IAM system for fallback
func (p *PolicyBackedIAM) SetLegacyIAM(legacyIAM LegacyIAM) {
	p.legacyIAM = legacyIAM
}

// SetBucketPolicy sets the policy for a bucket
func (p *PolicyBackedIAM) SetBucketPolicy(bucketName string, policyJSON string) error {
	return p.policyEngine.SetBucketPolicy(bucketName, policyJSON)
}

// GetBucketPolicy gets the policy for a bucket
func (p *PolicyBackedIAM) GetBucketPolicy(bucketName string) (*PolicyDocument, error) {
	return p.policyEngine.GetBucketPolicy(bucketName)
}

// DeleteBucketPolicy deletes the policy for a bucket
func (p *PolicyBackedIAM) DeleteBucketPolicy(bucketName string) error {
	return p.policyEngine.DeleteBucketPolicy(bucketName)
}

// CanDo checks if a principal can perform an action on a resource
func (p *PolicyBackedIAM) CanDo(action, bucketName, objectName, principal string, r *http.Request) bool {
	// If there's a bucket policy, evaluate it
	if p.policyEngine.HasPolicyForBucket(bucketName) {
		result := p.policyEngine.EvaluatePolicyForRequest(bucketName, objectName, action, principal, r)
		switch result {
		case PolicyResultAllow:
			return true
		case PolicyResultDeny:
			return false
		case PolicyResultIndeterminate:
			// Fall through to legacy system
		}
	}

	// No bucket policy or indeterminate result, use legacy conversion
	return p.evaluateLegacyAction(action, bucketName, objectName, principal)
}

// evaluateLegacyAction evaluates actions using legacy identity-based rules
func (p *PolicyBackedIAM) evaluateLegacyAction(action, bucketName, objectName, principal string) bool {
	// If we have a legacy IAM system to delegate to, use it
	if p.legacyIAM != nil {
		// Create a dummy request for legacy evaluation
		// In real implementation, this would use the actual request
		r := &http.Request{
			Header: make(http.Header),
		}

		// Convert the action string to Action type
		legacyAction := Action(action)

		// Use legacy IAM to check permission
		identity, errCode := p.legacyIAM.authRequest(r, legacyAction)
		if errCode != s3err.ErrNone {
			return false
		}

		// If we have an identity, check if it can perform the action
		if identity != nil {
			return identity.CanDo(legacyAction, bucketName, objectName)
		}
	}

	// No legacy IAM available, convert to policy and evaluate
	return p.evaluateUsingPolicyConversion(action, bucketName, objectName, principal)
}

// evaluateUsingPolicyConversion converts legacy action to policy and evaluates
func (p *PolicyBackedIAM) evaluateUsingPolicyConversion(action, bucketName, objectName, principal string) bool {
	// For now, use a conservative approach for legacy actions
	// In a real implementation, this would integrate with the existing identity system
	glog.V(2).Infof("Legacy action evaluation for %s on %s/%s by %s", action, bucketName, objectName, principal)

	// Return false to maintain security until proper legacy integration is implemented
	// This ensures no unintended access is granted
	return false
}

// extractBucketAndPrefix extracts bucket name and prefix from a resource pattern.
// Examples:
//
//	"bucket" -> bucket="bucket", prefix=""
//	"bucket/*" -> bucket="bucket", prefix=""
//	"bucket/prefix/*" -> bucket="bucket", prefix="prefix"
//	"bucket/a/b/c/*" -> bucket="bucket", prefix="a/b/c"
func extractBucketAndPrefix(pattern string) (string, string) {
	// Validate input
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "/" {
		return "", ""
	}

	// Remove trailing /* if present
	pattern = strings.TrimSuffix(pattern, "/*")

	// Remove a single trailing slash to avoid empty path segments
	if strings.HasSuffix(pattern, "/") {
		pattern = pattern[:len(pattern)-1]
	}
	if pattern == "" {
		return "", ""
	}

	// Split on the first /
	parts := strings.SplitN(pattern, "/", 2)
	bucket := strings.TrimSpace(parts[0])
	if bucket == "" {
		return "", ""
	}

	if len(parts) == 1 {
		// No slash, entire pattern is bucket
		return bucket, ""
	}
	// Has slash, first part is bucket, rest is prefix
	prefix := strings.Trim(parts[1], "/")
	return bucket, prefix
}

// buildObjectResourceArn generates ARNs for object-level access.
// It properly handles both bucket-level (all objects) and prefix-level access.
// Returns empty slice if bucket is invalid to prevent generating malformed ARNs.
func buildObjectResourceArn(resourcePattern string) []string {
	bucket, prefix := extractBucketAndPrefix(resourcePattern)
	// If bucket is empty, the pattern is invalid; avoid generating malformed ARNs
	if bucket == "" {
		return []string{}
	}
	if prefix != "" {
		// Prefix-based access: restrict to objects under this prefix
		return []string{fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, prefix)}
	}
	// Bucket-level access: all objects in bucket
	return []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
}

// ConvertIdentityToPolicy converts a legacy identity action to an AWS policy
func ConvertIdentityToPolicy(identityActions []string) (*PolicyDocument, error) {
	statements := make([]PolicyStatement, 0)

	for _, action := range identityActions {
		stmt, err := convertSingleAction(action)
		if err != nil {
			glog.Warningf("Failed to convert action %s: %v", action, err)
			continue
		}
		if stmt != nil {
			statements = append(statements, *stmt)
		}
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no valid statements generated")
	}

	return &PolicyDocument{
		Version:   PolicyVersion2012_10_17,
		Statement: statements,
	}, nil
}

// convertSingleAction converts a single legacy action to a policy statement.
// action format: "ActionType:ResourcePattern" (e.g., "Write:bucket/prefix/*")
func convertSingleAction(action string) (*PolicyStatement, error) {
	parts := strings.Split(action, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid action format: %s", action)
	}

	actionType := parts[0]
	resourcePattern := parts[1]

	var s3Actions []string
	var resources []string

	switch actionType {
	case "Read":
		// Read includes both object-level (GetObject, GetObjectAcl, GetObjectTagging, GetObjectVersions)
		// and bucket-level operations (ListBucket, GetBucketLocation, GetBucketVersioning, GetBucketCors, etc.)
		s3Actions = []string{
			"s3:GetObject",
			"s3:GetObjectVersion",
			"s3:GetObjectAcl",
			"s3:GetObjectVersionAcl",
			"s3:GetObjectTagging",
			"s3:GetObjectVersionTagging",
			"s3:ListBucket",
			"s3:ListBucketVersions",
			"s3:GetBucketLocation",
			"s3:GetBucketVersioning",
			"s3:GetBucketAcl",
			"s3:GetBucketCors",
			"s3:GetBucketTagging",
			"s3:GetBucketNotification",
		}
		bucket, _ := extractBucketAndPrefix(resourcePattern)
		objectResources := buildObjectResourceArn(resourcePattern)
		// Include both bucket ARN (for ListBucket* and Get*Bucket operations) and object ARNs (for GetObject* operations)
		if bucket != "" {
			resources = append([]string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}, objectResources...)
		} else {
			resources = objectResources
		}

	case "Write":
		// Write includes object-level writes (PutObject, DeleteObject, PutObjectAcl, DeleteObjectVersion, DeleteObjectTagging, PutObjectTagging)
		// and bucket-level writes (PutBucketVersioning, PutBucketCors, DeleteBucketCors, PutBucketAcl, PutBucketTagging, DeleteBucketTagging, PutBucketNotification)
		// and multipart upload operations (AbortMultipartUpload, ListMultipartUploads, ListParts).
		// ListMultipartUploads and ListParts are included because they are part of the multipart upload workflow
		// and require Write permissions to be meaningful (no point listing uploads if you can't abort/complete them).
		s3Actions = []string{
			"s3:PutObject",
			"s3:PutObjectAcl",
			"s3:PutObjectTagging",
			"s3:DeleteObject",
			"s3:DeleteObjectVersion",
			"s3:DeleteObjectTagging",
			"s3:AbortMultipartUpload",
			"s3:ListMultipartUploads",
			"s3:ListParts",
			"s3:PutBucketAcl",
			"s3:PutBucketCors",
			"s3:PutBucketTagging",
			"s3:PutBucketNotification",
			"s3:PutBucketVersioning",
			"s3:DeleteBucketTagging",
			"s3:DeleteBucketCors",
		}
		bucket, _ := extractBucketAndPrefix(resourcePattern)
		objectResources := buildObjectResourceArn(resourcePattern)
		// Include bucket ARN so bucket-level write operations (e.g., PutBucketVersioning, PutBucketCors)
		// have the correct resource, while still allowing object-level writes.
		if bucket != "" {
			resources = append([]string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}, objectResources...)
		} else {
			resources = objectResources
		}

	case "Admin":
		s3Actions = []string{"s3:*"}
		bucket, prefix := extractBucketAndPrefix(resourcePattern)
		if bucket == "" {
			// Invalid pattern, return error
			return nil, fmt.Errorf("Admin action requires a valid bucket name")
		}
		if prefix != "" {
			// Subpath admin access: restrict to objects under this prefix
			resources = []string{
				fmt.Sprintf("arn:aws:s3:::%s", bucket),
				fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, prefix),
			}
		} else {
			// Bucket-level admin access: full bucket permissions
			resources = []string{
				fmt.Sprintf("arn:aws:s3:::%s", bucket),
				fmt.Sprintf("arn:aws:s3:::%s/*", bucket),
			}
		}

	case "List":
		// List includes bucket listing operations and also ListAllMyBuckets
		s3Actions = []string{"s3:ListBucket", "s3:ListBucketVersions", "s3:ListAllMyBuckets"}
		// ListBucket actions only require bucket ARN, not object-level ARNs
		bucket, _ := extractBucketAndPrefix(resourcePattern)
		if bucket != "" {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}
		} else {
			// Invalid pattern, return empty resources to fail validation
			resources = []string{}
		}

	case "Tagging":
		// Tagging includes both object-level and bucket-level tagging operations
		s3Actions = []string{
			"s3:GetObjectTagging",
			"s3:PutObjectTagging",
			"s3:DeleteObjectTagging",
			"s3:GetBucketTagging",
			"s3:PutBucketTagging",
			"s3:DeleteBucketTagging",
		}
		bucket, _ := extractBucketAndPrefix(resourcePattern)
		objectResources := buildObjectResourceArn(resourcePattern)
		// Include bucket ARN so bucket-level tagging operations have the correct resource
		if bucket != "" {
			resources = append([]string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}, objectResources...)
		} else {
			resources = objectResources
		}

	case "BypassGovernanceRetention":
		s3Actions = []string{"s3:BypassGovernanceRetention"}
		resources = buildObjectResourceArn(resourcePattern)

	case "GetObjectRetention":
		s3Actions = []string{"s3:GetObjectRetention"}
		resources = buildObjectResourceArn(resourcePattern)

	case "PutObjectRetention":
		s3Actions = []string{"s3:PutObjectRetention"}
		resources = buildObjectResourceArn(resourcePattern)

	case "GetObjectLegalHold":
		s3Actions = []string{"s3:GetObjectLegalHold"}
		resources = buildObjectResourceArn(resourcePattern)

	case "PutObjectLegalHold":
		s3Actions = []string{"s3:PutObjectLegalHold"}
		resources = buildObjectResourceArn(resourcePattern)

	case "GetBucketObjectLockConfiguration":
		s3Actions = []string{"s3:GetBucketObjectLockConfiguration"}
		bucket, _ := extractBucketAndPrefix(resourcePattern)
		if bucket != "" {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}
		} else {
			// Invalid pattern, return empty resources to fail validation
			resources = []string{}
		}

	case "PutBucketObjectLockConfiguration":
		s3Actions = []string{"s3:PutBucketObjectLockConfiguration"}
		bucket, _ := extractBucketAndPrefix(resourcePattern)
		if bucket != "" {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}
		} else {
			// Invalid pattern, return empty resources to fail validation
			resources = []string{}
		}

	default:
		return nil, fmt.Errorf("unknown action type: %s", actionType)
	}

	return &PolicyStatement{
		Effect:   PolicyEffectAllow,
		Action:   NewStringOrStringSlice(s3Actions...),
		Resource: NewStringOrStringSlice(resources...),
	}, nil
}

// GetActionMappings returns the mapping of legacy actions to S3 actions
func GetActionMappings() map[string][]string {
	return map[string][]string{
		"Read": {
			"s3:GetObject",
			"s3:GetObjectVersion",
			"s3:GetObjectAcl",
			"s3:GetObjectVersionAcl",
			"s3:GetObjectTagging",
			"s3:GetObjectVersionTagging",
			"s3:ListBucket",
			"s3:ListBucketVersions",
			"s3:GetBucketLocation",
			"s3:GetBucketVersioning",
			"s3:GetBucketAcl",
			"s3:GetBucketCors",
			"s3:GetBucketTagging",
			"s3:GetBucketNotification",
		},
		"Write": {
			"s3:PutObject",
			"s3:PutObjectAcl",
			"s3:PutObjectTagging",
			"s3:DeleteObject",
			"s3:DeleteObjectVersion",
			"s3:DeleteObjectTagging",
			"s3:AbortMultipartUpload",
			"s3:ListMultipartUploads",
			"s3:ListParts",
			"s3:PutBucketAcl",
			"s3:PutBucketCors",
			"s3:PutBucketTagging",
			"s3:PutBucketNotification",
			"s3:PutBucketVersioning",
			"s3:DeleteBucketTagging",
			"s3:DeleteBucketCors",
		},
		"Admin": {
			"s3:*",
		},
		"List": {
			"s3:ListBucket",
			"s3:ListBucketVersions",
			"s3:ListAllMyBuckets",
		},
		"Tagging": {
			"s3:GetObjectTagging",
			"s3:PutObjectTagging",
			"s3:DeleteObjectTagging",
			"s3:GetBucketTagging",
			"s3:PutBucketTagging",
			"s3:DeleteBucketTagging",
		},
		"BypassGovernanceRetention": {
			"s3:BypassGovernanceRetention",
		},
		"GetObjectRetention": {
			"s3:GetObjectRetention",
		},
		"PutObjectRetention": {
			"s3:PutObjectRetention",
		},
		"GetObjectLegalHold": {
			"s3:GetObjectLegalHold",
		},
		"PutObjectLegalHold": {
			"s3:PutObjectLegalHold",
		},
		"GetBucketObjectLockConfiguration": {
			"s3:GetBucketObjectLockConfiguration",
		},
		"PutBucketObjectLockConfiguration": {
			"s3:PutBucketObjectLockConfiguration",
		},
	}
}

// ValidateActionMapping validates that a legacy action can be mapped to S3 actions
func ValidateActionMapping(action string) error {
	mappings := GetActionMappings()

	parts := strings.Split(action, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid action format: %s, expected format: 'ActionType:Resource'", action)
	}

	actionType := parts[0]
	resource := parts[1]

	if _, exists := mappings[actionType]; !exists {
		return fmt.Errorf("unknown action type: %s", actionType)
	}

	if resource == "" {
		return fmt.Errorf("resource cannot be empty")
	}

	return nil
}

// ConvertLegacyActions converts an array of legacy actions to S3 actions
func ConvertLegacyActions(legacyActions []string) ([]string, error) {
	mappings := GetActionMappings()
	s3Actions := make([]string, 0)

	for _, legacyAction := range legacyActions {
		if err := ValidateActionMapping(legacyAction); err != nil {
			return nil, err
		}

		parts := strings.Split(legacyAction, ":")
		actionType := parts[0]

		if actionType == "Admin" {
			// Admin gives all permissions, so we can just return s3:*
			return []string{"s3:*"}, nil
		}

		if mapped, exists := mappings[actionType]; exists {
			s3Actions = append(s3Actions, mapped...)
		}
	}

	// Remove duplicates
	uniqueActions := make([]string, 0)
	seen := make(map[string]bool)
	for _, action := range s3Actions {
		if !seen[action] {
			uniqueActions = append(uniqueActions, action)
			seen[action] = true
		}
	}

	return uniqueActions, nil
}

// GetResourcesFromLegacyAction extracts resources from a legacy action.
// It delegates to convertSingleAction to ensure consistent resource ARN generation
// across the codebase and avoid duplicating action-type-specific logic.
func GetResourcesFromLegacyAction(legacyAction string) ([]string, error) {
	stmt, err := convertSingleAction(legacyAction)
	if err != nil {
		return nil, err
	}
	return stmt.Resource.Strings(), nil
}

// CreatePolicyFromLegacyIdentity creates a policy document from legacy identity actions
func CreatePolicyFromLegacyIdentity(identityName string, actions []string) (*PolicyDocument, error) {
	statements := make([]PolicyStatement, 0)

	// Group actions by resource pattern
	resourceActions := make(map[string][]string)

	for _, action := range actions {
		// Validate action format before processing
		if err := ValidateActionMapping(action); err != nil {
			glog.Warningf("Skipping invalid action %q for identity %q: %v", action, identityName, err)
			continue
		}

		parts := strings.Split(action, ":")
		if len(parts) != 2 {
			continue
		}

		resourcePattern := parts[1]
		actionType := parts[0]

		if _, exists := resourceActions[resourcePattern]; !exists {
			resourceActions[resourcePattern] = make([]string, 0)
		}
		resourceActions[resourcePattern] = append(resourceActions[resourcePattern], actionType)
	}

	// Create statements for each resource pattern
	for resourcePattern, actionTypes := range resourceActions {
		s3Actions := make([]string, 0)
		resourceSet := make(map[string]struct{})

		// Collect S3 actions and aggregate resource ARNs from all action types.
		// Different action types have different resource ARN requirements:
		// - List: bucket-level ARNs only
		// - Read/Write/Tagging: object-level ARNs
		// - Admin: full bucket access
		// We must merge all required ARNs for the combined policy statement.
		for _, actionType := range actionTypes {
			if actionType == "Admin" {
				s3Actions = []string{"s3:*"}
				// Admin action determines the resources, so we can break after processing it.
				res, err := GetResourcesFromLegacyAction(fmt.Sprintf("Admin:%s", resourcePattern))
				if err != nil {
					glog.Warningf("Failed to get resources for Admin action on %s: %v", resourcePattern, err)
					resourceSet = nil // Invalidate to skip this statement
					break
				}
				for _, r := range res {
					resourceSet[r] = struct{}{}
				}
				break
			}

			if mapped, exists := GetActionMappings()[actionType]; exists {
				s3Actions = append(s3Actions, mapped...)
				res, err := GetResourcesFromLegacyAction(fmt.Sprintf("%s:%s", actionType, resourcePattern))
				if err != nil {
					glog.Warningf("Failed to get resources for %s action on %s: %v", actionType, resourcePattern, err)
					resourceSet = nil // Invalidate to skip this statement
					break
				}
				for _, r := range res {
					resourceSet[r] = struct{}{}
				}
			}
		}

		if resourceSet == nil || len(s3Actions) == 0 {
			continue
		}

		resources := make([]string, 0, len(resourceSet))
		for r := range resourceSet {
			resources = append(resources, r)
		}

		statement := PolicyStatement{
			Sid:      fmt.Sprintf("%s-%s", identityName, strings.ReplaceAll(resourcePattern, "/", "-")),
			Effect:   PolicyEffectAllow,
			Action:   NewStringOrStringSlice(s3Actions...),
			Resource: NewStringOrStringSlice(resources...),
		}

		statements = append(statements, statement)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no valid statements generated for identity %s", identityName)
	}

	return &PolicyDocument{
		Version:   PolicyVersion2012_10_17,
		Statement: statements,
	}, nil
}

// HasPolicyForBucket checks if a bucket has a policy
func (p *PolicyBackedIAM) HasPolicyForBucket(bucketName string) bool {
	return p.policyEngine.HasPolicyForBucket(bucketName)
}

// GetPolicyEngine returns the underlying policy engine
func (p *PolicyBackedIAM) GetPolicyEngine() *PolicyEngine {
	return p.policyEngine
}
