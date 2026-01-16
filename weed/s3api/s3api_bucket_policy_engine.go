package s3api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
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
// This function loads the policy directly into the engine
func (bpe *BucketPolicyEngine) LoadBucketPolicyFromCache(bucket string, policyDoc *policy_engine.PolicyDocument) error {
	if policyDoc == nil {
		// No policy for this bucket - remove it if it exists
		bpe.engine.DeleteBucketPolicy(bucket)
		return nil
	}

	// Policy is already in correct format, just load it
	// We need to re-marshal to string because SetBucketPolicy expects JSON string
	policyJSON, err := json.Marshal(policyDoc)
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

// HasPolicyForBucket checks if a bucket has a policy configured
func (bpe *BucketPolicyEngine) HasPolicyForBucket(bucket string) bool {
	return bpe.engine.HasPolicyForBucket(bucket)
}

// EvaluatePolicy evaluates whether an action is allowed by bucket policy
//
// Parameters:
//   - bucket: the bucket name
//   - object: the object key (can be empty for bucket-level operations)
//   - action: the action being performed (e.g., "Read", "Write")
//   - principal: the principal ARN or identifier
//   - r: the HTTP request (optional, used for condition evaluation and action resolution)
//   - objectEntry: the object's metadata from entry.Extended (can be nil at auth time,
//     should be passed when available for tag-based conditions like s3:ExistingObjectTag)
//
// Returns:
//   - allowed: whether the policy allows the action
//   - evaluated: whether a policy was found and evaluated (false = no policy exists)
//   - error: any error during evaluation
func (bpe *BucketPolicyEngine) EvaluatePolicy(bucket, object, action, principal string, r *http.Request, claims map[string]interface{}, objectEntry map[string][]byte) (allowed bool, evaluated bool, err error) {
	// Validate required parameters
	if bucket == "" {
		return false, false, fmt.Errorf("bucket cannot be empty")
	}
	if action == "" {
		return false, false, fmt.Errorf("action cannot be empty")
	}

	// Convert action to S3 action format
	// ResolveS3Action handles nil request internally (falls back to mapBaseActionToS3Format)
	s3Action := ResolveS3Action(r, action, bucket, object)

	// Build resource ARN
	resource := buildResourceARN(bucket, object)

	glog.V(4).Infof("EvaluatePolicy: bucket=%s, resource=%s, action=%s, principal=%s",
		bucket, resource, s3Action, principal)

	// Evaluate using the policy engine
	args := &policy_engine.PolicyEvaluationArgs{
		Action:      s3Action,
		Resource:    resource,
		Principal:   principal,
		ObjectEntry: objectEntry,
	}

	// glog.V(4).Infof("EvaluatePolicy [Wrapper]: bucket=%s, resource=%s, action=%s, principal=%s",
	// 	bucket, resource, s3Action, principal)

	// Extract conditions and claims from request if available
	if r != nil {
		args.Conditions = policy_engine.ExtractConditionValuesFromRequest(r)

		// Extract principal-related variables (aws:username, etc.) from principal ARN
		principalVars := policy_engine.ExtractPrincipalVariables(principal)
		for k, v := range principalVars {
			args.Conditions[k] = v
		}

		// Extract JWT claims if authenticated via JWT or STS
		if claims != nil {
			args.Claims = claims
		} else {
			// If claims were not provided directly, try to get them from context Identity?
			// But the caller is responsible for passing them.
			// Falling back to empty claims if not provided.
		}
	}

	result := bpe.engine.EvaluatePolicy(bucket, args)

	switch result {
	case policy_engine.PolicyResultAllow:
		// glog.V(4).Infof("EvaluatePolicy [Wrapper]: ALLOW - bucket=%s, action=%s, principal=%s", bucket, s3Action, principal)
		return true, true, nil
	case policy_engine.PolicyResultDeny:
		// glog.V(4).Infof("EvaluatePolicy [Wrapper]: DENY - bucket=%s, action=%s, principal=%s", bucket, s3Action, principal)
		return false, true, nil
	case policy_engine.PolicyResultIndeterminate:
		// No policy exists for this bucket
		// glog.V(4).Infof("EvaluatePolicy [Wrapper]: INDETERMINATE (no policy) - bucket=%s", bucket)
		return false, false, nil
	default:
		return false, false, fmt.Errorf("unknown policy result: %v", result)
	}
}
