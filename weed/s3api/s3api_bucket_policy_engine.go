package s3api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// BucketPolicyEngine wraps the policy_engine to provide bucket policy evaluation
type BucketPolicyEngine struct {
	engine *policy_engine.PolicyEngine
}

// NewBucketPolicyEngine creates a new bucket policy engine
func NewBucketPolicyEngine() *BucketPolicyEngine {
	return &BucketPolicyEngine{
		engine: policy_engine.NewPolicyEngine(),
	}
}

// LoadBucketPolicy loads a bucket policy into the engine from the filer entry
func (bpe *BucketPolicyEngine) LoadBucketPolicy(bucket string, entry *filer_pb.Entry) error {
	if entry == nil || entry.Extended == nil {
		return nil
	}

	policyJSON, exists := entry.Extended[BUCKET_POLICY_METADATA_KEY]
	if !exists || len(policyJSON) == 0 {
		// No policy for this bucket - remove it if it exists
		bpe.engine.DeleteBucketPolicy(bucket)
		return nil
	}

	// Set the policy in the engine
	if err := bpe.engine.SetBucketPolicy(bucket, string(policyJSON)); err != nil {
		glog.Errorf("Failed to load bucket policy for %s: %v", bucket, err)
		return err
	}

	glog.V(3).Infof("Loaded bucket policy for %s into policy engine", bucket)
	return nil
}

// LoadBucketPolicyFromCache loads a bucket policy from a cached BucketConfig
//
// This function uses a direct conversion function to efficiently convert between
// policy.PolicyDocument and policy_engine.PolicyDocument without JSON marshaling overhead.
func (bpe *BucketPolicyEngine) LoadBucketPolicyFromCache(bucket string, policyDoc *policy.PolicyDocument) error {
	if policyDoc == nil {
		// No policy for this bucket - remove it if it exists
		bpe.engine.DeleteBucketPolicy(bucket)
		return nil
	}

	// Convert policy.PolicyDocument to policy_engine.PolicyDocument using direct conversion
	// This is more efficient than JSON marshaling and provides better type safety
	enginePolicyDoc := ConvertPolicyDocumentToPolicyEngine(policyDoc)
	
	// Marshal the converted policy to JSON for storage in the engine
	policyJSON, err := json.Marshal(enginePolicyDoc)
	if err != nil {
		glog.Errorf("Failed to marshal bucket policy for %s: %v", bucket, err)
		return err
	}

	// Set the policy in the engine
	if err := bpe.engine.SetBucketPolicy(bucket, string(policyJSON)); err != nil {
		glog.Errorf("Failed to load bucket policy for %s: %v", bucket, err)
		return err
	}

	glog.V(4).Infof("Loaded bucket policy for %s into policy engine from cache", bucket)
	return nil
}

// DeleteBucketPolicy removes a bucket policy from the engine
func (bpe *BucketPolicyEngine) DeleteBucketPolicy(bucket string) error {
	return bpe.engine.DeleteBucketPolicy(bucket)
}

// EvaluatePolicy evaluates whether an action is allowed by bucket policy
// Returns: (allowed bool, evaluated bool, error)
// - allowed: whether the policy allows the action
// - evaluated: whether a policy was found and evaluated (false = no policy exists)
// - error: any error during evaluation
func (bpe *BucketPolicyEngine) EvaluatePolicy(bucket, object, action, principal string) (allowed bool, evaluated bool, err error) {
	// Validate required parameters
	if bucket == "" {
		return false, false, fmt.Errorf("bucket cannot be empty")
	}
	if action == "" {
		return false, false, fmt.Errorf("action cannot be empty")
	}

	// Convert action to S3 action format
	s3Action := convertActionToS3Format(action)

	// Build resource ARN
	resource := buildResourceARN(bucket, object)

	glog.V(4).Infof("EvaluatePolicy: bucket=%s, resource=%s, action=%s, principal=%s", bucket, resource, s3Action, principal)

	// Evaluate using the policy engine
	args := &policy_engine.PolicyEvaluationArgs{
		Action:    s3Action,
		Resource:  resource,
		Principal: principal,
	}

	result := bpe.engine.EvaluatePolicy(bucket, args)

	switch result {
	case policy_engine.PolicyResultAllow:
		glog.V(3).Infof("EvaluatePolicy: ALLOW - bucket=%s, action=%s, principal=%s", bucket, s3Action, principal)
		return true, true, nil
	case policy_engine.PolicyResultDeny:
		glog.V(3).Infof("EvaluatePolicy: DENY - bucket=%s, action=%s, principal=%s", bucket, s3Action, principal)
		return false, true, nil
	case policy_engine.PolicyResultIndeterminate:
		// No policy exists for this bucket
		glog.V(4).Infof("EvaluatePolicy: INDETERMINATE (no policy) - bucket=%s", bucket)
		return false, false, nil
	default:
		return false, false, fmt.Errorf("unknown policy result: %v", result)
	}
}

// convertActionToS3Format converts internal action strings to S3 action format
//
// KNOWN LIMITATION: The current Action type uses coarse-grained constants
// (ACTION_READ, ACTION_WRITE, etc.) that map to specific S3 actions, but these
// are used for multiple operations. For example, ACTION_WRITE is used for both
// PutObject and DeleteObject, but this function maps it to only s3:PutObject.
// This means bucket policies requiring fine-grained permissions (e.g., allowing
// s3:DeleteObject but not s3:PutObject) will not work correctly.
//
// TODO: Refactor to use specific S3 action strings throughout the S3 API handlers
// instead of coarse-grained Action constants. This is a major architectural change
// that should be done in a separate PR.
//
// This function explicitly maps all known actions to prevent security issues from
// overly permissive default behavior.
func convertActionToS3Format(action string) string {
	// Handle multipart actions that already have s3: prefix
	if strings.HasPrefix(action, "s3:") {
		return action
	}

	// Explicit mapping for all known actions
	switch action {
	// Basic operations
	case s3_constants.ACTION_READ:
		return "s3:GetObject"
	case s3_constants.ACTION_WRITE:
		return "s3:PutObject"
	case s3_constants.ACTION_LIST:
		return "s3:ListBucket"
	case s3_constants.ACTION_TAGGING:
		return "s3:PutObjectTagging"
	case s3_constants.ACTION_ADMIN:
		return "s3:*"

	// ACL operations
	case s3_constants.ACTION_READ_ACP:
		return "s3:GetObjectAcl"
	case s3_constants.ACTION_WRITE_ACP:
		return "s3:PutObjectAcl"

	// Bucket operations
	case s3_constants.ACTION_DELETE_BUCKET:
		return "s3:DeleteBucket"

	// Object Lock operations
	case s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION:
		return "s3:BypassGovernanceRetention"
	case s3_constants.ACTION_GET_OBJECT_RETENTION:
		return "s3:GetObjectRetention"
	case s3_constants.ACTION_PUT_OBJECT_RETENTION:
		return "s3:PutObjectRetention"
	case s3_constants.ACTION_GET_OBJECT_LEGAL_HOLD:
		return "s3:GetObjectLegalHold"
	case s3_constants.ACTION_PUT_OBJECT_LEGAL_HOLD:
		return "s3:PutObjectLegalHold"
	case s3_constants.ACTION_GET_BUCKET_OBJECT_LOCK_CONFIG:
		return "s3:GetBucketObjectLockConfiguration"
	case s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG:
		return "s3:PutBucketObjectLockConfiguration"

	default:
		// Log warning for unmapped actions to help catch issues
		glog.Warningf("convertActionToS3Format: unmapped action '%s', prefixing with 's3:'", action)
		// For unknown actions, prefix with s3: to maintain format consistency
		// This maintains backward compatibility while alerting developers
		return "s3:" + action
	}
}
