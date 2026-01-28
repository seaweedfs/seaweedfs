package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleCreateTableBucket creates a new table bucket
func (h *S3TablesHandler) handleCreateTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	accountID := h.getAccountID(r)
	principal := h.getPrincipalFromRequest(r)
	if !CanCreateTableBucket(principal, accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table buckets")
		return NewAuthError("CreateTableBucket", principal, "not authorized to create table buckets")
	}

	var req CreateTableBucketRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "name is required")
		return fmt.Errorf("name is required")
	}

	// Validate bucket name length
	if len(req.Name) < 3 || len(req.Name) > 63 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "bucket name must be between 3 and 63 characters")
		return fmt.Errorf("invalid bucket name length")
	}

	// Validate bucket name characters [a-z0-9_-]
	if !isValidBucketName(req.Name) {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "bucket name must contain only lowercase letters, numbers, hyphens, and underscores")
		return fmt.Errorf("invalid bucket name characters")
	}

	bucketPath := getTableBucketPath(req.Name)

	// Check if bucket already exists
	exists := false
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: TablesPath,
			Name:      req.Name,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return nil
			}
			return err
		}
		exists = true
		return nil
	})

	if err != nil {
		glog.Errorf("S3Tables: failed to check bucket existence: %v", err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to check bucket existence")
		return err
	}

	if exists {
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
		ARN: h.generateTableBucketARN(r, req.Name),
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}
