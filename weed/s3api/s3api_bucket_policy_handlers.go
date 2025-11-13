package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Bucket policy metadata key for storing policies in filer
const BUCKET_POLICY_METADATA_KEY = "s3-bucket-policy"

// GetBucketPolicyHandler handles GET bucket?policy requests
func (s3a *S3ApiServer) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	glog.V(3).Infof("GetBucketPolicyHandler: bucket=%s", bucket)

	// Get bucket policy from filer metadata
	policyDocument, err := s3a.getBucketPolicy(bucket)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketPolicy)
		} else {
			glog.Errorf("Failed to get bucket policy for %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}

	// Return policy as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(policyDocument); err != nil {
		glog.Errorf("Failed to encode bucket policy response: %v", err)
	}
}

// PutBucketPolicyHandler handles PUT bucket?policy requests
func (s3a *S3ApiServer) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	glog.V(3).Infof("PutBucketPolicyHandler: bucket=%s", bucket)

	// Read policy document from request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		glog.Errorf("Failed to read bucket policy request body: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPolicyDocument)
		return
	}
	defer r.Body.Close()

	// Parse and validate policy document
	var policyDoc policy.PolicyDocument
	if err := json.Unmarshal(body, &policyDoc); err != nil {
		glog.Errorf("Failed to parse bucket policy JSON: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPolicy)
		return
	}

	// Validate policy document structure
	if err := policy.ValidatePolicyDocument(&policyDoc); err != nil {
		glog.Errorf("Invalid bucket policy document: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPolicyDocument)
		return
	}

	// Additional bucket policy specific validation
	if err := s3a.validateBucketPolicy(&policyDoc, bucket); err != nil {
		glog.Errorf("Bucket policy validation failed: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPolicyDocument)
		return
	}

	// Store bucket policy
	if err := s3a.setBucketPolicy(bucket, &policyDoc); err != nil {
		glog.Errorf("Failed to store bucket policy for %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Update IAM integration with new bucket policy
	if s3a.iam.iamIntegration != nil {
		if err := s3a.updateBucketPolicyInIAM(bucket, &policyDoc); err != nil {
			glog.Errorf("Failed to update IAM with bucket policy: %v", err)
			// Don't fail the request, but log the warning
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// DeleteBucketPolicyHandler handles DELETE bucket?policy requests
func (s3a *S3ApiServer) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	glog.V(3).Infof("DeleteBucketPolicyHandler: bucket=%s", bucket)

	// Check if bucket policy exists
	if _, err := s3a.getBucketPolicy(bucket); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketPolicy)
		} else {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}

	// Delete bucket policy
	if err := s3a.deleteBucketPolicy(bucket); err != nil {
		glog.Errorf("Failed to delete bucket policy for %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Update IAM integration to remove bucket policy
	if s3a.iam.iamIntegration != nil {
		if err := s3a.removeBucketPolicyFromIAM(bucket); err != nil {
			glog.Errorf("Failed to remove bucket policy from IAM: %v", err)
			// Don't fail the request, but log the warning
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// Helper functions for bucket policy storage and retrieval

// getBucketPolicy retrieves a bucket policy from filer metadata
func (s3a *S3ApiServer) getBucketPolicy(bucket string) (*policy.PolicyDocument, error) {

	var policyDoc policy.PolicyDocument
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.option.BucketsPath,
			Name:      bucket,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %v", err)
		}

		if resp.Entry == nil {
			return fmt.Errorf("bucket policy not found: no entry")
		}

		policyJSON, exists := resp.Entry.Extended[BUCKET_POLICY_METADATA_KEY]
		if !exists || len(policyJSON) == 0 {
			return fmt.Errorf("bucket policy not found: no policy metadata")
		}

		if err := json.Unmarshal(policyJSON, &policyDoc); err != nil {
			return fmt.Errorf("failed to parse stored bucket policy: %v", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &policyDoc, nil
}

// setBucketPolicy stores a bucket policy in filer metadata
func (s3a *S3ApiServer) setBucketPolicy(bucket string, policyDoc *policy.PolicyDocument) error {
	// Serialize policy to JSON
	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		return fmt.Errorf("failed to serialize policy: %v", err)
	}

	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// First, get the current entry to preserve other attributes
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.option.BucketsPath,
			Name:      bucket,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %v", err)
		}

		entry := resp.Entry
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}

		// Set the bucket policy metadata
		entry.Extended[BUCKET_POLICY_METADATA_KEY] = policyJSON

		// Update the entry with new metadata
		_, err = client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: s3a.option.BucketsPath,
			Entry:     entry,
		})

		return err
	})
}

// deleteBucketPolicy removes a bucket policy from filer metadata
func (s3a *S3ApiServer) deleteBucketPolicy(bucket string) error {
	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Get the current entry
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.option.BucketsPath,
			Name:      bucket,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %v", err)
		}

		entry := resp.Entry
		if entry.Extended == nil {
			return nil // No policy to delete
		}

		// Remove the bucket policy metadata
		delete(entry.Extended, BUCKET_POLICY_METADATA_KEY)

		// Update the entry
		_, err = client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: s3a.option.BucketsPath,
			Entry:     entry,
		})

		return err
	})
}

// validateBucketPolicy performs bucket-specific policy validation
func (s3a *S3ApiServer) validateBucketPolicy(policyDoc *policy.PolicyDocument, bucket string) error {
	if policyDoc.Version != "2012-10-17" {
		return fmt.Errorf("unsupported policy version: %s (must be 2012-10-17)", policyDoc.Version)
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
		for _, resource := range statement.Resource {
			if !s3a.validateResourceForBucket(resource, bucket) {
				return fmt.Errorf("statement %d: resource %s does not match bucket %s", i, resource, bucket)
			}
		}

		// Validate actions are S3 actions
		for _, action := range statement.Action {
			if !strings.HasPrefix(action, "s3:") {
				return fmt.Errorf("statement %d: bucket policies only support S3 actions, got %s", i, action)
			}
		}
	}

	return nil
}

// validateResourceForBucket checks if a resource ARN is valid for the given bucket
func (s3a *S3ApiServer) validateResourceForBucket(resource, bucket string) bool {
	// Accepted formats for S3 bucket policies:
	// AWS-style ARNs (recommended):
	//   arn:aws:s3:::bucket-name
	//   arn:aws:s3:::bucket-name/*
	//   arn:aws:s3:::bucket-name/path/to/object
	// Legacy SeaweedFS ARNs (supported for backward compatibility):
	//   arn:seaweed:s3:::bucket-name
	//   arn:seaweed:s3:::bucket-name/*
	//   arn:seaweed:s3:::bucket-name/path/to/object
	// Simplified formats (for convenience):
	//   bucket-name
	//   bucket-name/*
	//   bucket-name/path/to/object

	var resourcePath string
	const awsPrefix = "arn:aws:s3:::"
	const seaweedPrefix = "arn:seaweed:s3:::"

	// Strip the optional ARN prefix to get the resource path
	if path, ok := strings.CutPrefix(resource, awsPrefix); ok {
		resourcePath = path
	} else if path, ok := strings.CutPrefix(resource, seaweedPrefix); ok {
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

// IAM integration functions

// updateBucketPolicyInIAM updates the IAM system with the new bucket policy
func (s3a *S3ApiServer) updateBucketPolicyInIAM(bucket string, policyDoc *policy.PolicyDocument) error {
	// This would integrate with our advanced IAM system
	// For now, we'll just log that the policy was updated
	glog.V(2).Infof("Updated bucket policy for %s in IAM system", bucket)

	// TODO: Integrate with IAM manager to store resource-based policies
	// s3a.iam.iamIntegration.iamManager.SetBucketPolicy(bucket, policyDoc)

	return nil
}

// removeBucketPolicyFromIAM removes the bucket policy from the IAM system
func (s3a *S3ApiServer) removeBucketPolicyFromIAM(bucket string) error {
	// This would remove the bucket policy from our advanced IAM system
	glog.V(2).Infof("Removed bucket policy for %s from IAM system", bucket)

	// TODO: Integrate with IAM manager to remove resource-based policies
	// s3a.iam.iamIntegration.iamManager.RemoveBucketPolicy(bucket)

	return nil
}

// GetPublicAccessBlockHandler Retrieves the PublicAccessBlock configuration for an S3 bucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html
func (s3a *S3ApiServer) GetPublicAccessBlockHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) PutPublicAccessBlockHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}

func (s3a *S3ApiServer) DeletePublicAccessBlockHandler(w http.ResponseWriter, r *http.Request) {
	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
}
