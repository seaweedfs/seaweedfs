package dash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// ErrInvalidBucketPolicy wraps a validation failure from SetBucketPolicy so
// callers (the HTTP handler) can map it to 400 instead of 500 without
// resorting to matching on the error string.
var ErrInvalidBucketPolicy = errors.New("invalid bucket policy")

// GetBucketPolicy returns the policy document stored on a bucket's filer
// entry, or (nil, nil, nil) if the bucket has no policy — that is not an
// error, it just means the caller (e.g. the admin UI) should show an empty
// editor instead of special-casing a 404. Stored bytes the current decoder
// rejects come back as (nil, raw, nil): a 500 here would leave the UI
// unable to show, fix, or even delete the one policy an operator most
// needs to remove — and DeleteBucketPolicy never reads the document.
func (s *AdminServer) GetBucketPolicy(bucketName string) (*policy_engine.PolicyDocument, []byte, error) {
	filerConfig, err := s.getFilerConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("get filer configuration: %w", err)
	}

	var doc *policy_engine.PolicyDocument
	var raw []byte
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerConfig.BucketsPath,
			Name:      bucketName,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, bucketName)
			}
			return fmt.Errorf("look up bucket %s: %w", bucketName, err)
		}

		policyJSON := resp.Entry.Extended[s3api.BUCKET_POLICY_METADATA_KEY]
		if len(policyJSON) == 0 {
			return nil
		}
		raw = policyJSON

		var parsed policy_engine.PolicyDocument
		if err := json.Unmarshal(policyJSON, &parsed); err == nil {
			doc = &parsed
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return doc, raw, nil
}

// SetBucketPolicy validates and stores a bucket policy, applying the exact
// same validation the S3 gateway's PutBucketPolicy enforces
// (policy_engine.ValidatePolicy + policy_engine.ValidateBucketPolicy), so
// the admin UI and the S3 API never disagree about what's a valid policy.
//
// Propagation to every S3 gateway is automatic: writing the
// s3-bucket-policy extended attribute drives the filer metadata log, which
// each gateway's onBucketMetadataChange subscription watches to rebuild its
// bucket policy cache and to maintain the advanced-IAM
// "bucket-policy:<bucket>" mirror (mirrorBucketPolicyToIAM in
// weed/s3api/s3api_bucket_policy_handlers.go). No separate notify step is
// needed here.
func (s *AdminServer) SetBucketPolicy(bucketName string, doc *policy_engine.PolicyDocument) error {
	if err := policy_engine.ValidatePolicy(doc); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBucketPolicy, err)
	}
	if err := policy_engine.ValidateBucketPolicy(doc, bucketName); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBucketPolicy, err)
	}

	policyJSON, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal policy document: %w", err)
	}
	if len(policyJSON) > policy_engine.MaxBucketPolicySize {
		return fmt.Errorf("%w: bucket policy is %d bytes, which exceeds the %d byte limit", ErrInvalidBucketPolicy, len(policyJSON), policy_engine.MaxBucketPolicySize)
	}

	filerConfig, err := s.getFilerConfig()
	if err != nil {
		return fmt.Errorf("get filer configuration: %w", err)
	}

	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// PATCH_EXTENDED is a no-op on a missing entry, so the existence
		// check has to happen here rather than fall out of the write.
		if _, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerConfig.BucketsPath,
			Name:      bucketName,
		}); err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, bucketName)
			}
			return fmt.Errorf("look up bucket %s: %w", bucketName, err)
		}

		bucketPath := filerConfig.BucketsPath + "/" + bucketName
		resp, err := client.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
			LockKey:   bucketPath,
			RouteKey:  s3_constants.ObjectWriteRouteKeyPrefix + bucketPath,
			Mutations: []*filer_pb.ObjectMutation{bucketPolicyMutation(filerConfig.BucketsPath, bucketName, policyJSON)},
		})
		if err != nil {
			return fmt.Errorf("failed to update bucket policy: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("failed to update bucket policy: %s", resp.Error)
		}
		return nil
	})
}

// DeleteBucketPolicy clears the bucket policy stored on a bucket's filer
// entry. Deleting a policy that doesn't exist is a success, matching
// DeleteBucketLifecycle's idempotent behavior.
func (s *AdminServer) DeleteBucketPolicy(bucketName string) error {
	filerConfig, err := s.getFilerConfig()
	if err != nil {
		return fmt.Errorf("get filer configuration: %w", err)
	}

	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		if _, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerConfig.BucketsPath,
			Name:      bucketName,
		}); err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, bucketName)
			}
			return fmt.Errorf("look up bucket %s: %w", bucketName, err)
		}

		bucketPath := filerConfig.BucketsPath + "/" + bucketName
		resp, err := client.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
			LockKey:   bucketPath,
			RouteKey:  s3_constants.ObjectWriteRouteKeyPrefix + bucketPath,
			Mutations: []*filer_pb.ObjectMutation{bucketPolicyMutation(filerConfig.BucketsPath, bucketName, nil)},
		})
		if err != nil {
			return fmt.Errorf("failed to delete bucket policy: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("failed to delete bucket policy: %s", resp.Error)
		}
		return nil
	})
}

// bucketPolicyMutation patches only the policy key rather than writing the
// whole entry back: the filer re-reads and merges under the bucket path
// lock, so a concurrent owner/quota/versioning/lifecycle change is
// preserved instead of being reverted by a stale snapshot. Same pattern as
// bucketLifecycleMutation. A nil/empty policyJSON clears the key.
func bucketPolicyMutation(bucketsPath, bucketName string, policyJSON []byte) *filer_pb.ObjectMutation {
	mutation := &filer_pb.ObjectMutation{
		Type:      filer_pb.ObjectMutation_PATCH_EXTENDED,
		Directory: bucketsPath,
		Name:      bucketName,
	}
	if len(policyJSON) > 0 {
		mutation.SetExtended = map[string][]byte{
			s3api.BUCKET_POLICY_METADATA_KEY: policyJSON,
		}
		return mutation
	}
	mutation.DeleteExtended = []string{s3api.BUCKET_POLICY_METADATA_KEY}
	return mutation
}
