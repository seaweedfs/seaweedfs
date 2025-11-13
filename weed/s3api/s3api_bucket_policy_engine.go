package s3api

import (
	"encoding/json"
	"fmt"
	"net/http"

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
// This function uses a type-safe conversion function to convert between
// policy.PolicyDocument and policy_engine.PolicyDocument with explicit field mapping and error handling.
func (bpe *BucketPolicyEngine) LoadBucketPolicyFromCache(bucket string, policyDoc *policy.PolicyDocument) error {
	if policyDoc == nil {
		// No policy for this bucket - remove it if it exists
		bpe.engine.DeleteBucketPolicy(bucket)
		return nil
	}

	// Convert policy.PolicyDocument to policy_engine.PolicyDocument without a JSON round-trip
	// This removes the prior intermediate marshal/unmarshal and adds type safety
	enginePolicyDoc, err := ConvertPolicyDocumentToPolicyEngine(policyDoc)
	if err != nil {
		glog.Errorf("Failed to convert bucket policy for %s: %v", bucket, err)
		return fmt.Errorf("failed to convert bucket policy: %w", err)
	}
	
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
	s3Action := convertActionToS3Format(action, nil)

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

// EvaluatePolicyWithContext evaluates whether an action is allowed by bucket policy using HTTP request context
// This version uses the HTTP request to determine the actual S3 action more accurately
func (bpe *BucketPolicyEngine) EvaluatePolicyWithContext(bucket, object, action, principal string, r *http.Request) (allowed bool, evaluated bool, err error) {
	// Validate required parameters
	if bucket == "" {
		return false, false, fmt.Errorf("bucket cannot be empty")
	}
	if action == "" {
		return false, false, fmt.Errorf("action cannot be empty")
	}

	// Convert action to S3 action format using request context
	s3Action := convertActionToS3Format(action, r)

	// Build resource ARN
	resource := buildResourceARN(bucket, object)

	glog.V(4).Infof("EvaluatePolicyWithContext: bucket=%s, resource=%s, action=%s (from %s), principal=%s", 
		bucket, resource, s3Action, action, principal)

	// Evaluate using the policy engine
	args := &policy_engine.PolicyEvaluationArgs{
		Action:    s3Action,
		Resource:  resource,
		Principal: principal,
	}

	result := bpe.engine.EvaluatePolicy(bucket, args)

	switch result {
	case policy_engine.PolicyResultAllow:
		glog.V(3).Infof("EvaluatePolicyWithContext: ALLOW - bucket=%s, action=%s, principal=%s", bucket, s3Action, principal)
		return true, true, nil
	case policy_engine.PolicyResultDeny:
		glog.V(3).Infof("EvaluatePolicyWithContext: DENY - bucket=%s, action=%s, principal=%s", bucket, s3Action, principal)
		return false, true, nil
	case policy_engine.PolicyResultIndeterminate:
		// No policy exists for this bucket
		glog.V(4).Infof("EvaluatePolicyWithContext: INDETERMINATE (no policy) - bucket=%s", bucket)
		return false, false, nil
	default:
		return false, false, fmt.Errorf("unknown policy result: %v", result)
	}
}

// convertActionToS3Format converts internal action strings to S3 action format
// with optional HTTP request context for fine-grained action resolution.
//
// This function now uses the shared ResolveS3Action utility for consistent
// action resolution across the bucket policy engine and IAM integration.
//
// Parameters:
//   - action: The internal action constant (e.g., ACTION_WRITE, ACTION_READ)
//   - r: Optional HTTP request for context-aware resolution. If nil, uses legacy mapping.
func convertActionToS3Format(action string, r *http.Request) string {
	// If request context is provided, use the shared action resolver
	if r != nil {
		bucket, object := s3_constants.GetBucketAndObject(r)
		if resolvedAction := ResolveS3Action(r, action, bucket, object); resolvedAction != "" {
			return resolvedAction
		}
	}

	// Fallback to base action mapping
	return mapBaseActionToS3Format(action)
}

// NOTE: resolveS3ActionFromRequest has been replaced by the shared ResolveS3Action
// function in s3_action_resolver.go. This consolidates action resolution logic
// used by both the bucket policy engine and IAM integration.
