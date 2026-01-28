package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handlePutTableBucketPolicy puts a policy on a table bucket
func (h *S3TablesHandler) handlePutTableBucketPolicy(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req PutTableBucketPolicyRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	if req.ResourcePolicy == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "resourcePolicy is required")
		return fmt.Errorf("resourcePolicy is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if bucket exists and get metadata for ownership check
	bucketPath := getTableBucketPath(bucketName)
	var bucketMetadata tableBucketMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table bucket: %v", err))
		}
		return err
	}

	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanPutTableBucketPolicy(principal, bucketMetadata.OwnerID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to put table bucket policy")
		return NewAuthError("PutTableBucketPolicy", principal, "not authorized to put table bucket policy")
	}

	// Write policy
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy, []byte(req.ResourcePolicy))
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to put table bucket policy")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handleGetTableBucketPolicy gets the policy of a table bucket
func (h *S3TablesHandler) handleGetTableBucketPolicy(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req GetTableBucketPolicyRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketPath := getTableBucketPath(bucketName)
	var policy []byte
	var bucketMetadata tableBucketMetadata

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Get metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Get policy
		policy, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
			return err
		}
		if errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchPolicy, "table bucket policy not found")
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table bucket policy: %v", err))
		return err
	}

	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanGetTableBucketPolicy(principal, bucketMetadata.OwnerID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to get table bucket policy")
		return NewAuthError("GetTableBucketPolicy", principal, "not authorized to get table bucket policy")
	}

	resp := &GetTableBucketPolicyResponse{
		ResourcePolicy: string(policy),
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleDeleteTableBucketPolicy deletes the policy of a table bucket
func (h *S3TablesHandler) handleDeleteTableBucketPolicy(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req DeleteTableBucketPolicyRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketPath := getTableBucketPath(bucketName)

	// Check if bucket exists and get metadata for ownership check
	var bucketMetadata tableBucketMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table bucket: %v", err))
		}
		return err
	}

	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanDeleteTableBucketPolicy(principal, bucketMetadata.OwnerID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table bucket policy")
		return NewAuthError("DeleteTableBucketPolicy", principal, "not authorized to delete table bucket policy")
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
	})

	if err != nil && !errors.Is(err, ErrAttributeNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table bucket policy")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handlePutTablePolicy puts a policy on a table
func (h *S3TablesHandler) handlePutTablePolicy(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req PutTablePolicyRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" || len(req.Namespace) == 0 || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.ResourcePolicy == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "resourcePolicy is required")
		return fmt.Errorf("resourcePolicy is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if table exists
	tableName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}
	tablePath := getTablePath(bucketName, namespaceName, tableName)

	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanPutTablePolicy(principal, metadata.OwnerID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to put table policy")
		return NewAuthError("PutTablePolicy", principal, "not authorized to put table policy")
	}

	// Write policy
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy, []byte(req.ResourcePolicy))
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to put table policy")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handleGetTablePolicy gets the policy of a table
func (h *S3TablesHandler) handleGetTablePolicy(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req GetTablePolicyRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" || len(req.Namespace) == 0 || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tableName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}
	tablePath := getTablePath(bucketName, namespaceName, tableName)
	var policy []byte
	var metadata tableMetadataInternal

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Get metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		// Get policy
		policy, err = h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
			return err
		}
		if errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchPolicy, "table policy not found")
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table policy: %v", err))
		return err
	}

	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanGetTablePolicy(principal, metadata.OwnerID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to get table policy")
		return NewAuthError("GetTablePolicy", principal, "not authorized to get table policy")
	}

	resp := &GetTablePolicyResponse{
		ResourcePolicy: string(policy),
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleDeleteTablePolicy deletes the policy of a table
func (h *S3TablesHandler) handleDeleteTablePolicy(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req DeleteTablePolicyRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" || len(req.Namespace) == 0 || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tableName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}
	tablePath := getTablePath(bucketName, namespaceName, tableName)

	// Check if table exists
	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanDeleteTablePolicy(principal, metadata.OwnerID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table policy")
		return NewAuthError("DeleteTablePolicy", principal, "not authorized to delete table policy")
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
	})

	if err != nil && !errors.Is(err, ErrAttributeNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table policy")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handleTagResource adds tags to a resource
func (h *S3TablesHandler) handleTagResource(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req TagResourceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.ResourceARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "resourceArn is required")
		return fmt.Errorf("resourceArn is required")
	}

	if len(req.Tags) == 0 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tags are required")
		return fmt.Errorf("tags are required")
	}

	// Parse resource ARN to determine if it's a bucket or table
	resourcePath, extendedKey, rType, err := h.resolveResourcePath(req.ResourceARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Read existing tags and merge, AND check permissions based on metadata ownership
	existingTags := make(map[string]string)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}

		var ownerID string
		if rType == ResourceTypeTable {
			var meta tableMetadataInternal
			if err := json.Unmarshal(data, &meta); err != nil {
				return err
			}
			ownerID = meta.OwnerID
		} else {
			var meta tableBucketMetadata
			if err := json.Unmarshal(data, &meta); err != nil {
				return err
			}
			ownerID = meta.OwnerID
		}

		// Check Permission inside the closure because we just got the ID
		principal := h.getPrincipalFromRequest(r)
		if !CanManageTags(principal, ownerID) {
			return NewAuthError("TagResource", principal, "not authorized to tag resource")
		}

		// Read existing tags
		data, err = h.getExtendedAttribute(r.Context(), client, resourcePath, extendedKey)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				return nil // No existing tags, which is fine.
			}
			return err // Propagate other errors.
		}
		return json.Unmarshal(data, &existingTags)
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			errorCode := ErrCodeNoSuchBucket
			if rType == ResourceTypeTable {
				errorCode = ErrCodeNoSuchTable
			}
			h.writeError(w, http.StatusNotFound, errorCode, "resource not found")
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to read existing tags: %v", err))
		}
		return err
	}

	// Merge new tags
	for k, v := range req.Tags {
		existingTags[k] = v
	}

	// Write merged tags
	tagsBytes, err := json.Marshal(existingTags)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal tags")
		return fmt.Errorf("failed to marshal tags: %w", err)
	}
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(r.Context(), client, resourcePath, extendedKey, tagsBytes)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to tag resource")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handleListTagsForResource lists tags for a resource
func (h *S3TablesHandler) handleListTagsForResource(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req ListTagsForResourceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.ResourceARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "resourceArn is required")
		return fmt.Errorf("resourceArn is required")
	}

	resourcePath, extendedKey, rType, err := h.resolveResourcePath(req.ResourceARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tags := make(map[string]string)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}

		var ownerID string
		if rType == ResourceTypeTable {
			var meta tableMetadataInternal
			if err := json.Unmarshal(data, &meta); err != nil {
				return err
			}
			ownerID = meta.OwnerID
		} else {
			var meta tableBucketMetadata
			if err := json.Unmarshal(data, &meta); err != nil {
				return err
			}
			ownerID = meta.OwnerID
		}

		// Check Permission
		principal := h.getPrincipalFromRequest(r)
		if !CheckPermission("ListTagsForResource", principal, ownerID) {
			return NewAuthError("ListTagsForResource", principal, "not authorized to list tags for resource")
		}

		data, err = h.getExtendedAttribute(r.Context(), client, resourcePath, extendedKey)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				return nil // No tags is not an error.
			}
			return err // Propagate other errors.
		}
		return json.Unmarshal(data, &tags)
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			errorCode := ErrCodeNoSuchBucket
			if rType == ResourceTypeTable {
				errorCode = ErrCodeNoSuchTable
			}
			h.writeError(w, http.StatusNotFound, errorCode, "resource not found")
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list tags: %v", err))
		}
		return err
	}

	resp := &ListTagsForResourceResponse{
		Tags: tags,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleUntagResource removes tags from a resource
func (h *S3TablesHandler) handleUntagResource(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req UntagResourceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.ResourceARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "resourceArn is required")
		return fmt.Errorf("resourceArn is required")
	}

	if len(req.TagKeys) == 0 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tagKeys are required")
		return fmt.Errorf("tagKeys are required")
	}

	resourcePath, extendedKey, rType, err := h.resolveResourcePath(req.ResourceARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Read existing tags, check permission
	tags := make(map[string]string)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}

		var ownerID string
		if rType == ResourceTypeTable {
			var meta tableMetadataInternal
			if err := json.Unmarshal(data, &meta); err != nil {
				return err
			}
			ownerID = meta.OwnerID
		} else {
			var meta tableBucketMetadata
			if err := json.Unmarshal(data, &meta); err != nil {
				return err
			}
			ownerID = meta.OwnerID
		}

		// Check Permission
		principal := h.getPrincipalFromRequest(r)
		if !CanManageTags(principal, ownerID) {
			return NewAuthError("UntagResource", principal, "not authorized to untag resource")
		}

		data, err = h.getExtendedAttribute(r.Context(), client, resourcePath, extendedKey)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				return nil
			}
			return err
		}
		return json.Unmarshal(data, &tags)
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			errorCode := ErrCodeNoSuchBucket
			if rType == ResourceTypeTable {
				errorCode = ErrCodeNoSuchTable
			}
			h.writeError(w, http.StatusNotFound, errorCode, "resource not found")
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to read existing tags")
		}
		return err
	}

	// Remove specified tags
	for _, key := range req.TagKeys {
		delete(tags, key)
	}

	// Write updated tags
	tagsBytes, err := json.Marshal(tags)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal tags")
		return fmt.Errorf("failed to marshal tags: %w", err)
	}
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(r.Context(), client, resourcePath, extendedKey, tagsBytes)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to untag resource")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// resolveResourcePath determines the resource path and extended attribute key from a resource ARN
func (h *S3TablesHandler) resolveResourcePath(resourceARN string) (path string, key string, rType ResourceType, err error) {
	// Try parsing as table ARN first
	bucketName, namespace, tableName, err := parseTableFromARN(resourceARN)
	if err == nil {
		return getTablePath(bucketName, namespace, tableName), ExtendedKeyTags, ResourceTypeTable, nil
	}

	// Try parsing as bucket ARN
	bucketName, err = parseBucketNameFromARN(resourceARN)
	if err == nil {
		return getTableBucketPath(bucketName), ExtendedKeyTags, ResourceTypeBucket, nil
	}

	return "", "", "", fmt.Errorf("invalid resource ARN: %s", resourceARN)
}
