package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// handleCreateTableBucket creates a new table bucket
func (h *S3TablesHandler) handleCreateTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	principal := h.getAccountID(r)
	if !CanCreateTableBucket(principal, principal, "") {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table buckets")
		return NewAuthError("CreateTableBucket", principal, "not authorized to create table buckets")
	}

	var req CreateTableBucketRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Validate bucket name
	if err := validateBucketName(req.Name); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketPath := getTableBucketPath(req.Name)

	// Check if bucket already exists and ensure no conflict with object store buckets
	tableBucketExists := false
	s3BucketExists := false
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(r.Context(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		bucketsPath := resp.DirBuckets
		if bucketsPath == "" {
			bucketsPath = s3_constants.DefaultBucketsPath
		}
		_, err = filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: bucketsPath,
			Name:      req.Name,
		})
		if err != nil {
			if !errors.Is(err, filer_pb.ErrNotFound) {
				return err
			}
		} else {
			s3BucketExists = true
		}
		_, err = filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: TablesPath,
			Name:      req.Name,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return nil
			}
			return err
		}
		tableBucketExists = true
		return nil
	})

	if err != nil {
		glog.Errorf("S3Tables: failed to check bucket existence: %v", err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to check bucket existence")
		return err
	}

	if s3BucketExists {
		h.writeError(w, http.StatusConflict, ErrCodeBucketAlreadyExists, fmt.Sprintf("bucket name %s is already used by an object store bucket", req.Name))
		return fmt.Errorf("bucket name conflicts with object store bucket")
	}

	if tableBucketExists {
		h.writeError(w, http.StatusConflict, ErrCodeBucketAlreadyExists, fmt.Sprintf("table bucket %s already exists", req.Name))
		return fmt.Errorf("bucket already exists")
	}

	// Create the bucket directory and set metadata as extended attributes
	now := time.Now()
	metadata := &tableBucketMetadata{
		Name:           req.Name,
		CreatedAt:      now,
		OwnerAccountID: h.getAccountID(r),
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		glog.Errorf("S3Tables: failed to marshal metadata: %v", err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal metadata")
		return err
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Ensure root tables directory exists
		if !h.entryExists(r.Context(), client, TablesPath) {
			if err := h.createDirectory(r.Context(), client, TablesPath); err != nil {
				return fmt.Errorf("failed to create root tables directory: %w", err)
			}
		}

		// Create bucket directory
		if err := h.createDirectory(r.Context(), client, bucketPath); err != nil {
			return err
		}

		// Set metadata as extended attribute
		if err := h.setExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata, metadataBytes); err != nil {
			return err
		}

		// Set tags if provided
		if len(req.Tags) > 0 {
			tagsBytes, err := json.Marshal(req.Tags)
			if err != nil {
				return fmt.Errorf("failed to marshal tags: %w", err)
			}
			if err := h.setExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyTags, tagsBytes); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		glog.Errorf("S3Tables: failed to create table bucket %s: %v", req.Name, err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create table bucket")
		return err
	}

	resp := &CreateTableBucketResponse{
		ARN: h.generateTableBucketARN(metadata.OwnerAccountID, req.Name),
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}
