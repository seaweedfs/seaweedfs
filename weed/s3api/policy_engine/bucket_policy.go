package policy_engine

import (
	"fmt"
	"strings"
)

// MaxBucketPolicySize mirrors AWS S3's 20 KB bucket-policy limit, enforced
// by both writers (the S3 gateway's PutBucketPolicy and the admin UI) so
// neither surface can store a document the other refuses to manage.
const MaxBucketPolicySize = 20 * 1024

// ValidateBucketPolicy performs bucket-specific policy validation, on top of
// the generic structural checks in ValidatePolicy. It enforces the rules
// that make a policy document valid as an S3 *bucket* policy specifically:
// every statement must name a Principal, and every Resource/NotResource/Action
// must scope to the given bucket.
//
// This is shared between the S3 gateway's PutBucketPolicy handler
// (weed/s3api/s3api_bucket_policy_handlers.go) and the admin UI
// (weed/admin/dash) so both enforce identical rules.
func ValidateBucketPolicy(policyDoc *PolicyDocument, bucket string) error {
	if policyDoc.Version != PolicyVersion2012_10_17 {
		return fmt.Errorf("unsupported policy version: %s (must be %s)", policyDoc.Version, PolicyVersion2012_10_17)
	}

	if len(policyDoc.Statement) == 0 {
		return fmt.Errorf("policy document must contain at least one statement")
	}

	for i, statement := range policyDoc.Statement {
		// Bucket policies must have Principal
		if statement.Principal == nil {
			return fmt.Errorf("statement %d: bucket policies must specify a Principal", i)
		}

		// Validate resources refer to this bucket
		for _, resource := range statement.Resource.Strings() {
			if !ResourceMatchesBucket(resource, bucket) {
				return fmt.Errorf("statement %d: resource %s does not match bucket %s", i, resource, bucket)
			}
		}

		// Validate NotResources refer to this bucket
		if statement.NotResource != nil {
			for _, notResource := range statement.NotResource.Strings() {
				if !ResourceMatchesBucket(notResource, bucket) {
					return fmt.Errorf("statement %d: NotResource %s does not match bucket %s", i, notResource, bucket)
				}
			}
		}

		// Validate actions are S3 actions
		for _, action := range statement.Action.Strings() {
			if !strings.HasPrefix(action, "s3:") {
				return fmt.Errorf("statement %d: bucket policies only support S3 actions, got %s", i, action)
			}
		}
	}

	return nil
}

// ResourceMatchesBucket checks if a resource ARN is valid for the given bucket.
func ResourceMatchesBucket(resource, bucket string) bool {
	// Accepted formats for S3 bucket policies:
	// AWS-style ARNs (standard):
	//   arn:aws:s3:::bucket-name
	//   arn:aws:s3:::bucket-name/*
	//   arn:aws:s3:::bucket-name/path/to/object
	// Simplified formats (for convenience):
	//   bucket-name
	//   bucket-name/*
	//   bucket-name/path/to/object

	var resourcePath string
	const awsPrefix = "arn:aws:s3:::"

	// Strip the optional ARN prefix to get the resource path
	if path, ok := strings.CutPrefix(resource, awsPrefix); ok {
		resourcePath = path
	} else {
		resourcePath = resource
	}

	// After stripping the optional ARN prefix, the resource path must
	// either match the bucket name exactly, or be a path within the bucket.
	return resourcePath == bucket ||
		resourcePath == bucket+"/*" ||
		strings.HasPrefix(resourcePath, bucket+"/")
}
