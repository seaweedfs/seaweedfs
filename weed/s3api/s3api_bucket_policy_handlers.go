package s3api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Bucket policy metadata key for storing policies in filer.
// Also consumed directly by weed/admin/dash for the admin UI's bucket
// policy management, so keep it exported and don't change its value
// without updating that package too.
const BUCKET_POLICY_METADATA_KEY = "s3-bucket-policy"

// Sentinel errors for bucket policy operations
var (
	ErrPolicyNotFound = errors.New("bucket policy not found")
	// ErrBucketNotFound is already defined in s3api_object_retention.go
)

// GetBucketPolicyHandler handles GET bucket?policy requests
func (s3a *S3ApiServer) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	glog.V(3).Infof("GetBucketPolicyHandler: bucket=%s", bucket)

	// Validate bucket exists first for correct error mapping
	_, err := s3a.getBucketEntry(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		} else {
			glog.Errorf("Failed to check bucket existence for %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}

	// Get bucket policy from filer metadata
	policyDocument, err := s3a.getBucketPolicy(bucket)
	if err != nil {
		if errors.Is(err, ErrPolicyNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketPolicy)
		} else if errors.Is(err, ErrBucketNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
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
	body, err := io.ReadAll(io.LimitReader(r.Body, policy_engine.MaxBucketPolicySize+1))
	if err != nil {
		glog.Errorf("Failed to read bucket policy request body: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPolicyDocument)
		return
	}
	defer r.Body.Close()

	if len(body) > policy_engine.MaxBucketPolicySize {
		s3err.WriteErrorResponse(w, r, s3err.ErrPolicyTooLarge)
		return
	}

	// Parse and validate policy document
	var policyDoc policy_engine.PolicyDocument
	if err := json.Unmarshal(body, &policyDoc); err != nil {
		glog.Errorf("Failed to parse bucket policy JSON: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedPolicy)
		return
	}

	// Validate core policy structure (Effect, Action, etc.)
	if err := policy_engine.ValidatePolicy(&policyDoc); err != nil {
		glog.Errorf("Policy validation failed: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPolicyDocument)
		return
	}

	// Additional bucket policy specific validation
	if err := policy_engine.ValidateBucketPolicy(&policyDoc, bucket); err != nil {
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

	// Immediately load into policy engine to avoid race condition
	// (The subscription system will also do this async, but we want immediate effect)
	if s3a.policyEngine != nil {
		if err := s3a.policyEngine.LoadBucketPolicyFromCache(bucket, &policyDoc); err != nil {
			glog.Warningf("Failed to immediately load bucket policy into engine for %s: %v", bucket, err)
			// Don't fail the request since the subscription will eventually sync it
		}
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

	// Validate bucket exists first for correct error mapping
	_, err := s3a.getBucketEntry(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		} else {
			glog.Errorf("Failed to check bucket existence for %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}

	// Check if bucket policy exists
	if _, err := s3a.getBucketPolicy(bucket); err != nil {
		if errors.Is(err, ErrPolicyNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketPolicy)
		} else if errors.Is(err, ErrBucketNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
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

	// Immediately remove from policy engine to avoid race condition
	// (The subscription system will also do this async, but we want immediate effect)
	if s3a.policyEngine != nil {
		if err := s3a.policyEngine.DeleteBucketPolicy(bucket); err != nil {
			glog.Warningf("Failed to immediately remove bucket policy from engine for %s: %v", bucket, err)
			// Don't fail the request since the subscription will eventually sync it
		}
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
// getBucketPolicy retrieves the bucket policy from filer
func (s3a *S3ApiServer) getBucketPolicy(bucket string) (*policy_engine.PolicyDocument, error) {

	var policyDoc policy_engine.PolicyDocument
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.bucketRoot(bucket),
			Name:      bucket,
		})
		if err != nil {
			// Return sentinel error for bucket not found
			return fmt.Errorf("%w: %v", ErrBucketNotFound, err)
		}

		if resp.Entry == nil {
			return ErrPolicyNotFound
		}

		policyJSON, exists := resp.Entry.Extended[BUCKET_POLICY_METADATA_KEY]
		if !exists || len(policyJSON) == 0 {
			return ErrPolicyNotFound
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
func (s3a *S3ApiServer) setBucketPolicy(bucket string, policyDoc *policy_engine.PolicyDocument) error {
	// Serialize policy to JSON
	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		return fmt.Errorf("failed to serialize policy: %v", err)
	}

	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// First, get the current entry to preserve other attributes
		resp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.bucketRoot(bucket),
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
			Directory: s3a.bucketRoot(bucket),
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
			Directory: s3a.bucketRoot(bucket),
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
			Directory: s3a.bucketRoot(bucket),
			Entry:     entry,
		})

		return err
	})
}

// IAM integration functions

// updateBucketPolicyInIAM updates the IAM system with the new bucket policy
func (s3a *S3ApiServer) updateBucketPolicyInIAM(bucket string, policyDoc *policy_engine.PolicyDocument) error {
	iamManager := s3a.bucketPolicyIAMManager()
	if iamManager == nil {
		return nil
	}

	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal policy: %w", err)
	}

	glog.V(2).Infof("Updated bucket policy for %s in IAM system", bucket)
	return iamManager.UpdateBucketPolicy(context.Background(), bucket, policyJSON)
}

// ensureBucketPolicyInIAM backfills the IAM mirror for a policy the
// subscription never saw change (one that predates the IAM integration).
// Called from the lazy bucket-config load; a present mirror is left alone.
func (s3a *S3ApiServer) ensureBucketPolicyInIAM(bucket string, policyDoc *policy_engine.PolicyDocument) {
	iamManager := s3a.bucketPolicyIAMManager()
	if iamManager == nil {
		return
	}

	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		glog.Warningf("backfill bucket policy for %s into IAM: marshal: %v", bucket, err)
		return
	}

	if err := iamManager.EnsureBucketPolicy(context.Background(), bucket, policyJSON); err != nil {
		glog.Warningf("backfill bucket policy for %s into IAM: %v", bucket, err)
	}
}

// removeBucketPolicyFromIAM removes the bucket policy from the IAM system
func (s3a *S3ApiServer) removeBucketPolicyFromIAM(bucket string) error {
	iamManager := s3a.bucketPolicyIAMManager()
	if iamManager == nil {
		return nil
	}

	glog.V(2).Infof("Removed bucket policy for %s from IAM system", bucket)
	return iamManager.RemoveBucketPolicy(context.Background(), bucket)
}

// bucketPolicyIAMManager returns the advanced-IAM manager the
// "bucket-policy:<bucket>" mirror lives in, or nil when the integration is
// not enabled.
func (s3a *S3ApiServer) bucketPolicyIAMManager() *integration.IAMManager {
	if s3a.iam == nil || s3a.iam.iamIntegration == nil {
		return nil
	}
	if s3Integration, ok := s3a.iam.iamIntegration.(*S3IAMIntegration); ok {
		return s3Integration.iamManager
	}
	return nil
}

// mirrorBucketPolicyToIAM keeps the "bucket-policy:<bucket>" IAM mirror in
// sync with the policy stored on a bucket's filer entry. Driven from the
// metadata subscription so it covers every writer - this gateway's own
// PutBucketPolicy, another gateway's, the admin UI, bucket deletion, and
// rename - where the handlers' direct calls only ever covered the first.
func (s3a *S3ApiServer) mirrorBucketPolicyToIAM(oldEntry, newEntry *filer_pb.Entry) {
	if s3a.bucketPolicyIAMManager() == nil {
		return
	}
	removeName, updateName, updatePolicy := bucketPolicyMirrorOps(oldEntry, newEntry)
	if removeName != "" {
		if err := s3a.removeBucketPolicyFromIAM(removeName); err != nil {
			glog.Warningf("remove bucket policy for %s from IAM: %v", removeName, err)
		}
	}
	if updateName == "" {
		return
	}
	var policyDoc policy_engine.PolicyDocument
	if err := json.Unmarshal(updatePolicy, &policyDoc); err != nil {
		glog.Warningf("mirror bucket policy for %s to IAM: parse: %v", updateName, err)
		return
	}
	if err := s3a.updateBucketPolicyInIAM(updateName, &policyDoc); err != nil {
		glog.Warningf("mirror bucket policy for %s to IAM: %v", updateName, err)
	}
}

// bucketPolicyMirrorOps computes what a bucket entry change means for the
// IAM mirror: a name whose mirror must be removed, and a (name, policy) to
// write. A rename delivers both entries under different names in one event,
// and the old name's mirror has to move even when the policy bytes are
// unchanged - equality only short-circuits same-name updates.
func bucketPolicyMirrorOps(oldEntry, newEntry *filer_pb.Entry) (removeName, updateName string, updatePolicy []byte) {
	var oldName, newName string
	var oldPolicy, newPolicy []byte
	if oldEntry != nil {
		oldName = oldEntry.Name
		oldPolicy = oldEntry.Extended[BUCKET_POLICY_METADATA_KEY]
	}
	if newEntry != nil {
		newName = newEntry.Name
		newPolicy = newEntry.Extended[BUCKET_POLICY_METADATA_KEY]
	}
	if oldName != "" && oldName != newName && len(oldPolicy) > 0 {
		removeName = oldName
		oldPolicy = nil
	}
	if newName == "" || bytes.Equal(oldPolicy, newPolicy) {
		return
	}
	if len(newPolicy) == 0 {
		removeName = newName
		return
	}
	updateName = newName
	updatePolicy = newPolicy
	return
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
