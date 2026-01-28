package s3tables

import (
	"encoding/json"
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

	// Check if bucket exists
	bucketPath := getTableBucketPath(bucketName)
	var bucketExists bool
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(client, bucketPath, ExtendedKeyMetadata)
		bucketExists = err == nil
		return nil
	})

	if !bucketExists {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return fmt.Errorf("bucket not found")
	}

	// Write policy
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(client, bucketPath, ExtendedKeyPolicy, []byte(req.ResourcePolicy))
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
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var readErr error
		policy, readErr = h.getExtendedAttribute(client, bucketPath, ExtendedKeyPolicy)
		return readErr
	})

	if err != nil {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchPolicy, "table bucket policy not found")
		return err
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
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteExtendedAttribute(client, bucketPath, ExtendedKeyPolicy)
	})

	if err != nil {
		// Ignore error if policy doesn't exist
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

	if req.TableBucketARN == "" || req.Namespace == "" || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
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
	tablePath := getTablePath(bucketName, req.Namespace, req.Name)
	var tableExists bool
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(client, tablePath, ExtendedKeyMetadata)
		tableExists = err == nil
		return nil
	})

	if !tableExists {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", req.Name))
		return fmt.Errorf("table not found")
	}

	// Write policy
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(client, tablePath, ExtendedKeyPolicy, []byte(req.ResourcePolicy))
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

	if req.TableBucketARN == "" || req.Namespace == "" || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tablePath := getTablePath(bucketName, req.Namespace, req.Name)
	var policy []byte
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var readErr error
		policy, readErr = h.getExtendedAttribute(client, tablePath, ExtendedKeyPolicy)
		return readErr
	})

	if err != nil {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchPolicy, "table policy not found")
		return err
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

	if req.TableBucketARN == "" || req.Namespace == "" || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tablePath := getTablePath(bucketName, req.Namespace, req.Name)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteExtendedAttribute(client, tablePath, ExtendedKeyPolicy)
	})

	if err != nil {
		// Ignore error if policy doesn't exist
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
	resourcePath, extendedKey, err := h.resolveResourcePath(req.ResourceARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Read existing tags and merge
	existingTags := make(map[string]string)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(client, resourcePath, extendedKey)
		if err == nil {
			json.Unmarshal(data, &existingTags)
		}
		return nil
	})

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
		return h.setExtendedAttribute(client, resourcePath, extendedKey, tagsBytes)
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

	resourcePath, extendedKey, err := h.resolveResourcePath(req.ResourceARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tags := make(map[string]string)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(client, resourcePath, extendedKey)
		if err != nil {
			return nil // Return empty tags if not found
		}
		return json.Unmarshal(data, &tags)
	})

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

	resourcePath, extendedKey, err := h.resolveResourcePath(req.ResourceARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Read existing tags
	tags := make(map[string]string)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(client, resourcePath, extendedKey)
		if err != nil {
			return nil
		}
		return json.Unmarshal(data, &tags)
	})

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
		return h.setExtendedAttribute(client, resourcePath, extendedKey, tagsBytes)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to untag resource")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// resolveResourcePath determines the resource path and extended attribute key from a resource ARN
func (h *S3TablesHandler) resolveResourcePath(resourceARN string) (path string, key string, err error) {
	// Try parsing as table ARN first
	bucketName, namespace, tableName, err := parseTableFromARN(resourceARN)
	if err == nil {
		return getTablePath(bucketName, namespace, tableName), ExtendedKeyTags, nil
	}

	// Try parsing as bucket ARN
	bucketName, err = parseBucketNameFromARN(resourceARN)
	if err == nil {
		return getTableBucketPath(bucketName), ExtendedKeyTags, nil
	}

	return "", "", fmt.Errorf("invalid resource ARN: %s", resourceARN)
}
