package policy_engine

import (
	"context"
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
	canDo(action Action, bucket string, objectKey string) bool
	GetActions() []string
}

// PolicyBackedIAM provides policy-based access control with fallback to legacy IAM
type PolicyBackedIAM struct {
	policyEngine *PolicyEngine
	legacyIAM    LegacyIAM    // Interface to delegate to existing IAM system
	policyReader PolicyReader // Interface to read named policies
}

// PolicyReader interface for reading named policies
type PolicyReader interface {
	GetPolicy(ctx context.Context, name string) (*PolicyDocument, error)
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

// SetPolicyReader sets the policy reader
func (p *PolicyBackedIAM) SetPolicyReader(policyReader PolicyReader) {
	p.policyReader = policyReader
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
			if identity.canDo(legacyAction, bucketName, objectName) {
				return true
			}
			
			// Check attached policies (policy:Name)
			if p.evaluateAttachedPolicies(identity, action, bucketName, objectName, r) {
				return true
			}
			
			return false
		}
	}

	// No legacy IAM available, convert to policy and evaluate
	return p.evaluateUsingPolicyConversion(action, bucketName, objectName, principal)
}

// evaluateAttachedPolicies checks if any attached policies allow the action
func (p *PolicyBackedIAM) evaluateAttachedPolicies(identity Identity, action, bucketName, objectName string, r *http.Request) bool {
	if p.policyReader == nil {
		return false
	}

	if idWithActions, ok := identity.(interface{ GetActions() []string }); ok {
		for _, act := range idWithActions.GetActions() {
			if strings.HasPrefix(act, "policy:") {
				policyName := strings.TrimPrefix(act, "policy:")
				
				// Look up policy document
				doc, err := p.policyReader.GetPolicy(context.Background(), policyName)
				if err != nil || doc == nil {
					continue
				}

				// Build arguments for evaluation
				resource := BuildResourceArn(bucketName, objectName)
				actionName := BuildActionName(action)
				
				// Extract conditions from request if available
				var conditions map[string][]string
				if r != nil {
					conditions = ExtractConditionValuesFromRequest(r)
				}
				
				// Identify principal - simplified, ideally should be passed down or extracted from identity
				principal := "" 
				if namedId, ok := identity.(interface{ GetName() string }); ok {
					principal = namedId.GetName() // Assuming GetName exists or similar
				}

				args := &PolicyEvaluationArgs{
					Action:     actionName,
					Resource:   resource,
					Principal:  principal,
					Conditions: conditions,
				}
				
				if p.policyEngine.EvaluatePolicyDocument(doc, args) == PolicyResultAllow {
					return true
				}
			}
		}
	}
	return false
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

// ConvertIdentityToPolicy converts a legacy identity action to an AWS policy
func ConvertIdentityToPolicy(identityActions []string, bucketName string) (*PolicyDocument, error) {
	statements := make([]PolicyStatement, 0)

	for _, action := range identityActions {
		stmt, err := convertSingleAction(action, bucketName)
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

// convertSingleAction converts a single legacy action to a policy statement
func convertSingleAction(action, bucketName string) (*PolicyStatement, error) {
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
		s3Actions = []string{"s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"}
		if strings.HasSuffix(resourcePattern, "/*") {
			// Object-level read access
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{
				fmt.Sprintf("arn:aws:s3:::%s", bucket),
				fmt.Sprintf("arn:aws:s3:::%s/*", bucket),
			}
		} else {
			// Bucket-level read access
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s", resourcePattern)}
		}

	case "Write":
		s3Actions = []string{"s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"}
		if strings.HasSuffix(resourcePattern, "/*") {
			// Object-level write access
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
		} else {
			// Bucket-level write access
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s", resourcePattern)}
		}

	case "Admin":
		s3Actions = []string{"s3:*"}
		resources = []string{
			fmt.Sprintf("arn:aws:s3:::%s", resourcePattern),
			fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern),
		}

	case "List":
		s3Actions = []string{"s3:ListBucket", "s3:ListBucketVersions"}
		if strings.HasSuffix(resourcePattern, "/*") {
			// Object-level list access - extract bucket from "bucket/prefix/*" pattern
			patternWithoutWildcard := strings.TrimSuffix(resourcePattern, "/*")
			parts := strings.SplitN(patternWithoutWildcard, "/", 2)
			bucket := parts[0]
			resources = []string{
				fmt.Sprintf("arn:aws:s3:::%s", bucket),
				fmt.Sprintf("arn:aws:s3:::%s/*", bucket),
			}
		} else {
			// Bucket-level list access
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s", resourcePattern)}
		}

	case "Tagging":
		s3Actions = []string{"s3:GetObjectTagging", "s3:PutObjectTagging", "s3:DeleteObjectTagging"}
		resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern)}

	case "BypassGovernanceRetention":
		s3Actions = []string{"s3:BypassGovernanceRetention"}
		if strings.HasSuffix(resourcePattern, "/*") {
			// Object-level bypass governance access
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
		} else {
			// Bucket-level bypass governance access
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern)}
		}

	case "GetObjectRetention":
		s3Actions = []string{"s3:GetObjectRetention"}
		if strings.HasSuffix(resourcePattern, "/*") {
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
		} else {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern)}
		}

	case "PutObjectRetention":
		s3Actions = []string{"s3:PutObjectRetention"}
		if strings.HasSuffix(resourcePattern, "/*") {
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
		} else {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern)}
		}

	case "GetObjectLegalHold":
		s3Actions = []string{"s3:GetObjectLegalHold"}
		if strings.HasSuffix(resourcePattern, "/*") {
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
		} else {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern)}
		}

	case "PutObjectLegalHold":
		s3Actions = []string{"s3:PutObjectLegalHold"}
		if strings.HasSuffix(resourcePattern, "/*") {
			bucket := strings.TrimSuffix(resourcePattern, "/*")
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)}
		} else {
			resources = []string{fmt.Sprintf("arn:aws:s3:::%s/*", resourcePattern)}
		}

	case "GetBucketObjectLockConfiguration":
		s3Actions = []string{"s3:GetBucketObjectLockConfiguration"}
		resources = []string{fmt.Sprintf("arn:aws:s3:::%s", resourcePattern)}

	case "PutBucketObjectLockConfiguration":
		s3Actions = []string{"s3:PutBucketObjectLockConfiguration"}
		resources = []string{fmt.Sprintf("arn:aws:s3:::%s", resourcePattern)}

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
		// Allow "policy" action type for attached policies
		if actionType == "policy" {
			if resource == "" {
				return fmt.Errorf("policy name cannot be empty")
			}
			return nil
		}
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

// GetResourcesFromLegacyAction extracts resources from a legacy action
func GetResourcesFromLegacyAction(legacyAction string) ([]string, error) {
	parts := strings.Split(legacyAction, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid action format: %s", legacyAction)
	}

	resourcePattern := parts[1]
	resources := make([]string, 0)

	if strings.HasSuffix(resourcePattern, "/*") {
		// Object-level access
		bucket := strings.TrimSuffix(resourcePattern, "/*")
		resources = append(resources, fmt.Sprintf("arn:aws:s3:::%s", bucket))
		resources = append(resources, fmt.Sprintf("arn:aws:s3:::%s/*", bucket))
	} else {
		// Bucket-level access
		resources = append(resources, fmt.Sprintf("arn:aws:s3:::%s", resourcePattern))
	}

	return resources, nil
}

// CreatePolicyFromLegacyIdentity creates a policy document from legacy identity actions
func CreatePolicyFromLegacyIdentity(identityName string, actions []string) (*PolicyDocument, error) {
	statements := make([]PolicyStatement, 0)

	// Group actions by resource pattern
	resourceActions := make(map[string][]string)

	for _, action := range actions {
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

		for _, actionType := range actionTypes {
			if actionType == "Admin" {
				s3Actions = []string{"s3:*"}
				break
			}

			if mapped, exists := GetActionMappings()[actionType]; exists {
				s3Actions = append(s3Actions, mapped...)
			}
		}

		resources, err := GetResourcesFromLegacyAction(fmt.Sprintf("dummy:%s", resourcePattern))
		if err != nil {
			continue
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
