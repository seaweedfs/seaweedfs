package s3api

import (
	"encoding/json"
	"fmt"
	"net/http"
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
// Context-Aware Resolution: When an HTTP request is provided, this function uses
// the HTTP method, path, and query parameters to accurately determine the specific
// S3 action. This solves the architectural limitation where coarse-grained internal
// actions (ACTION_READ, ACTION_WRITE) are used for multiple S3 operations.
//
// For example:
//   - ACTION_WRITE + DELETE /bucket/object → s3:DeleteObject
//   - ACTION_WRITE + PUT /bucket/object → s3:PutObject
//   - ACTION_READ + GET /bucket/object?acl → s3:GetObjectAcl
//
// This enables fine-grained bucket policies (e.g., allowing s3:DeleteObject but
// denying s3:PutObject) without requiring massive refactoring of handler registrations.
//
// Parameters:
//   - action: The internal action constant (e.g., ACTION_WRITE, ACTION_READ)
//   - r: Optional HTTP request for context-aware resolution. If nil, uses legacy mapping.
func convertActionToS3Format(action string, r *http.Request) string {
	// Handle actions that already have s3: prefix (e.g., multipart actions)
	if strings.HasPrefix(action, "s3:") {
		return action
	}

	// If request context is provided, use it for fine-grained action resolution
	if r != nil {
		if resolvedAction := resolveS3ActionFromRequest(action, r); resolvedAction != "" {
			return resolvedAction
		}
	}

	// Fallback to legacy coarse-grained mapping (for backward compatibility)
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

// resolveS3ActionFromRequest determines the specific S3 action from HTTP request context
// This enables fine-grained action resolution without changing handler registrations
func resolveS3ActionFromRequest(baseAction string, r *http.Request) string {
	if r == nil {
		return ""
	}

	method := r.Method
	query := r.URL.Query()
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Determine if this is an object or bucket operation
	hasObject := object != ""

	// Check for specific query parameters that indicate specific actions
	switch {
	// Multipart upload operations
	case query.Get("uploadId") != "" && query.Get("partNumber") != "":
		if method == http.MethodPut {
			return "s3:PutObject" // Upload part
		}
	case query.Get("uploadId") != "":
		switch method {
		case http.MethodPost:
			return "s3:PutObject" // Complete multipart
		case http.MethodDelete:
			return "s3:AbortMultipartUpload"
		case http.MethodGet:
			return "s3:ListParts"
		}
	case query.Get("uploads") != "":
		if method == http.MethodPost {
			return "s3:CreateMultipartUpload"
		} else if method == http.MethodGet {
			return "s3:ListMultipartUploads"
		}

	// ACL operations
	case query.Get("acl") != "":
		if hasObject {
			if method == http.MethodGet || method == http.MethodHead {
				return "s3:GetObjectAcl"
			} else if method == http.MethodPut {
				return "s3:PutObjectAcl"
			}
		} else {
			if method == http.MethodGet || method == http.MethodHead {
				return "s3:GetBucketAcl"
			} else if method == http.MethodPut {
				return "s3:PutBucketAcl"
			}
		}

	// Tagging operations
	case query.Get("tagging") != "":
		if hasObject {
			if method == http.MethodGet {
				return "s3:GetObjectTagging"
			} else if method == http.MethodPut {
				return "s3:PutObjectTagging"
			} else if method == http.MethodDelete {
				return "s3:DeleteObjectTagging"
			}
		} else {
			if method == http.MethodGet {
				return "s3:GetBucketTagging"
			} else if method == http.MethodPut {
				return "s3:PutBucketTagging"
			} else if method == http.MethodDelete {
				return "s3:DeleteBucketTagging"
			}
		}

	// Versioning
	case query.Get("versioning") != "":
		if method == http.MethodGet {
			return "s3:GetBucketVersioning"
		} else if method == http.MethodPut {
			return "s3:PutBucketVersioning"
		}
	case query.Get("versions") != "":
		return "s3:ListBucketVersions"

	// Policy operations
	case query.Get("policy") != "":
		if method == http.MethodGet {
			return "s3:GetBucketPolicy"
		} else if method == http.MethodPut {
			return "s3:PutBucketPolicy"
		} else if method == http.MethodDelete {
			return "s3:DeleteBucketPolicy"
		}

	// CORS operations
	case query.Get("cors") != "":
		if method == http.MethodGet {
			return "s3:GetBucketCors"
		} else if method == http.MethodPut {
			return "s3:PutBucketCors"
		} else if method == http.MethodDelete {
			return "s3:DeleteBucketCors"
		}

	// Lifecycle operations
	case query.Get("lifecycle") != "":
		if method == http.MethodGet {
			return "s3:GetBucketLifecycleConfiguration"
		} else if method == http.MethodPut {
			return "s3:PutBucketLifecycleConfiguration"
		} else if method == http.MethodDelete {
			return "s3:DeleteBucketLifecycle"
		}

	// Location
	case query.Get("location") != "":
		return "s3:GetBucketLocation"

	// Object Lock operations
	case query.Get("object-lock") != "":
		if method == http.MethodGet {
			return "s3:GetBucketObjectLockConfiguration"
		} else if method == http.MethodPut {
			return "s3:PutBucketObjectLockConfiguration"
		}
	case query.Get("retention") != "":
		if method == http.MethodGet {
			return "s3:GetObjectRetention"
		} else if method == http.MethodPut {
			return "s3:PutObjectRetention"
		}
	case query.Get("legal-hold") != "":
		if method == http.MethodGet {
			return "s3:GetObjectLegalHold"
		} else if method == http.MethodPut {
			return "s3:PutObjectLegalHold"
		}

	// Batch delete - check this early as it works on bucket level with no object
	case query.Has("delete"):
		return "s3:DeleteObject"
	}

	// Handle basic operations based on method and whether object exists
	// This is the critical fix for the DeleteObject issue
	if hasObject {
		// Object-level operations
		switch method {
		case http.MethodGet, http.MethodHead:
			if baseAction == s3_constants.ACTION_READ {
				return "s3:GetObject"
			}
		case http.MethodPut:
			if baseAction == s3_constants.ACTION_WRITE {
				// Check for copy operation
				if r.Header.Get("X-Amz-Copy-Source") != "" {
					return "s3:PutObject" // CopyObject also requires PutObject permission
				}
				return "s3:PutObject"
			}
		case http.MethodDelete:
			// CRITICAL FIX: Map DELETE method to s3:DeleteObject
			// Previously, ACTION_WRITE would map to s3:PutObject even for DELETE
			if baseAction == s3_constants.ACTION_WRITE {
				return "s3:DeleteObject"
			}
		case http.MethodPost:
			// POST without query params is typically multipart related
			if baseAction == s3_constants.ACTION_WRITE {
				return "s3:PutObject"
			}
		}
	} else if bucket != "" {
		// Bucket-level operations
		switch method {
		case http.MethodGet, http.MethodHead:
			if baseAction == s3_constants.ACTION_LIST {
				return "s3:ListBucket"
			} else if baseAction == s3_constants.ACTION_READ {
				return "s3:ListBucket"
			}
		case http.MethodPut:
			if baseAction == s3_constants.ACTION_WRITE {
				return "s3:CreateBucket"
			}
		case http.MethodDelete:
			if baseAction == s3_constants.ACTION_DELETE_BUCKET {
				return "s3:DeleteBucket"
			}
		case http.MethodPost:
			// POST to bucket typically is multipart or policy form upload
			if baseAction == s3_constants.ACTION_WRITE {
				return "s3:PutObject"
			}
		}
	}

	// No specific resolution found
	return ""
}
