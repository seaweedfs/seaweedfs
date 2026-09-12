package s3api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// putBucketQuotaMaxBodyBytes caps the request body to prevent DoS via large
// payloads. The valid payload is a few hundred bytes.
const putBucketQuotaMaxBodyBytes = 64 * 1024

// bucketQuotaRequest is the JSON body for PUT /{bucket}?seaweedfs-quota.
//
// SeaweedFS stores quota on the bucket's filer entry:
//   - positive value: quota enabled, enforced server-side
//   - negative value: quota disabled but size retained
//   - zero: no quota
//
// The quota_unit field accepts B, KB, MB, GB, TB (case-insensitive).
// quota_size is the numeric size in the given unit.
// quota_enabled false with a positive size stores a negative (disabled) quota.
type bucketQuotaRequest struct {
	QuotaSize    int64  `json:"quota_size"`
	QuotaUnit    string `json:"quota_unit"`
	QuotaEnabled bool   `json:"quota_enabled"`
}

// bucketQuotaResponse is the JSON body for GET /{bucket}?seaweedfs-quota.
type bucketQuotaResponse struct {
	QuotaSize    int64  `json:"quota_size"`
	QuotaUnit    string `json:"quota_unit"`
	QuotaEnabled bool   `json:"quota_enabled"`
}

// PutBucketQuotaHandler handles PUT /{bucket}?seaweedfs-quota.
//
// This is a SeaweedFS-specific S3 extension that allows setting a bucket's
// storage quota through the S3 API, authenticated via SigV4 and authorized via
// the s3:PutBucketQuota IAM permission. It avoids the need for a separate
// admin API credential for integrations like Apache CloudStack.
func (s3a *S3ApiServer) PutBucketQuotaHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketQuotaHandler %s", bucket)

	if bucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketName)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, putBucketQuotaMaxBodyBytes)
	defer r.Body.Close()

	var req bucketQuotaRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}
	// Reject trailing data after the JSON object to prevent malformed payloads
	// from being silently accepted.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeQuotaError(w, r, http.StatusBadRequest, "unexpected trailing data after JSON object")
		return
	}

	if req.QuotaEnabled && req.QuotaSize <= 0 {
		writeQuotaError(w, r, http.StatusBadRequest, "quota_size must be > 0 when quota_enabled is true")
		return
	}

	normalizedUnit, err := normalizeQuotaUnit(req.QuotaUnit)
	if err != nil {
		writeQuotaError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	req.QuotaUnit = normalizedUnit

	quotaBytes, err := convertQuotaToBytes(req.QuotaSize, normalizedUnit)
	if err != nil {
		writeQuotaError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	var quota int64
	switch {
	case req.QuotaEnabled && quotaBytes > 0:
		quota = quotaBytes
	case !req.QuotaEnabled && quotaBytes > 0:
		quota = -quotaBytes
	default:
		quota = 0
	}

	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		lookupResp, err := client.LookupDirectoryEntry(r.Context(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.option.BucketsPath,
			Name:      bucket,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return filer_pb.ErrNotFound
			}
			return fmt.Errorf("failed to look up bucket: %w", err)
		}
		bucketEntry := lookupResp.Entry
		bucketEntry.Quota = quota

		_, err = client.UpdateEntry(r.Context(), &filer_pb.UpdateEntryRequest{
			Directory: s3a.option.BucketsPath,
			Entry:     bucketEntry,
		})
		if err != nil {
			return fmt.Errorf("failed to update bucket quota: %w", err)
		}

		if quota <= 0 {
			if _, err := filer.ClearBucketReadOnly(r.Context(), client, s3a.option.BucketsPath, bucket); err != nil {
				return fmt.Errorf("failed to clear bucket read-only flag: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("PutBucketQuotaHandler %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetBucketQuotaHandler handles GET /{bucket}?seaweedfs-quota.
//
// Returns the current quota configuration for the bucket as JSON.
func (s3a *S3ApiServer) GetBucketQuotaHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketQuotaHandler %s", bucket)

	if bucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketName)
		return
	}

	entry, err := s3a.getBucketEntry(bucket)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Return the absolute quota magnitude as quota_size; the sign only
	// encodes enabled/disabled state internally. This makes the response
	// round-trippable: a client can send the same JSON back via PUT without
	// the negative sentinel being interpreted as zero.
	quotaSize := entry.Quota
	if quotaSize < 0 {
		quotaSize = -quotaSize
	}
	resp := bucketQuotaResponse{
		QuotaSize:    quotaSize,
		QuotaUnit:    "B",
		QuotaEnabled: entry.Quota > 0,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		glog.Errorf("GetBucketQuotaHandler %s: failed to encode response: %v", bucket, err)
	}
}

// writeQuotaError writes a JSON error response for quota operations.
func writeQuotaError(w http.ResponseWriter, r *http.Request, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": message}); err != nil {
		glog.V(1).Infof("failed to write quota error response: %v", err)
	}
}

// normalizeQuotaUnit normalizes the quota unit string to a canonical uppercase form.
func normalizeQuotaUnit(unit string) (string, error) {
	switch unit {
	case "", "B", "b":
		return "B", nil
	case "KB", "kb":
		return "KB", nil
	case "MB", "mb":
		return "MB", nil
	case "GB", "gb":
		return "GB", nil
	case "TB", "tb":
		return "TB", nil
	default:
		return "", fmt.Errorf("unsupported quota_unit %q (supported: B, KB, MB, GB, TB)", unit)
	}
}

// convertQuotaToBytes converts a quota size + unit to bytes.
// Returns an error if the result would overflow int64.
func convertQuotaToBytes(size int64, unit string) (int64, error) {
	if size <= 0 {
		return 0, nil
	}
	var multiplier int64
	switch unit {
	case "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	case "MB":
		multiplier = 1024 * 1024
	case "KB":
		multiplier = 1024
	case "B":
		multiplier = 1
	default:
		return 0, fmt.Errorf("unsupported quota_unit %q", unit)
	}
	if multiplier > 0 && size > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("quota_size %d %s overflows maximum bytes (%d)", size, unit, math.MaxInt64)
	}
	return size * multiplier, nil
}
