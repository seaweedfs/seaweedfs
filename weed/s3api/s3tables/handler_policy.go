package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// extractResourceOwnerAndBucket extracts ownership info, bucket name and the
// creation time that tells the resource apart from one created again under its
// name. It consolidates the repeated pattern used in handleTagResource,
// handleListTagsForResource, and handleUntagResource.
func (h *S3TablesHandler) extractResourceOwnerAndBucket(
	data []byte,
	resourcePath string,
	rType ResourceType,
) (ownerAccountID, bucketName string, createdAt time.Time, err error) {
	// Extract bucket name from resource path (format: /buckets/{bucket}/... for both tables and buckets)
	parts := strings.Split(strings.Trim(resourcePath, "/"), "/")
	if len(parts) >= 2 {
		bucketName = parts[1]
	}

	if rType == ResourceTypeTable {
		var meta tableMetadataInternal
		if err := json.Unmarshal(data, &meta); err != nil {
			return "", "", time.Time{}, err
		}
		ownerAccountID, createdAt = meta.OwnerAccountID, meta.CreatedAt
	} else {
		var meta tableBucketMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return "", "", time.Time{}, err
		}
		ownerAccountID, createdAt = meta.OwnerAccountID, meta.CreatedAt
	}
	return ownerAccountID, bucketName, createdAt, nil
}

type maxTagsKey struct{}

var errTooManyTags = errors.New("too many tags")

// WithMaxTags caps how many tags a TagResource run through Execute may leave
// on its resource. In-process only, like WithTableBucketCreatedAt.
func WithMaxTags(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, maxTagsKey{}, n)
}

// updateTags applies set and remove to raw, the tags the record holds at the
// write, so concurrent tag writes do not undo each other.
func updateTags(raw []byte, set map[string]string, remove []string) (map[string]string, error) {
	tags := make(map[string]string)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &tags); err != nil {
			return nil, err
		}
	}
	for k, v := range set {
		tags[k] = v
	}
	for _, k := range remove {
		delete(tags, k)
	}
	return tags, nil
}

func writeTags(extended map[string][]byte, key string, tags map[string]string) error {
	data, err := json.Marshal(tags)
	if err != nil {
		return err
	}
	extended[key] = data
	return nil
}

// requireBoundTableBucket answers not found when ctx binds the request to a
// table bucket other than the one now named bucketName.
func (h *S3TablesHandler) requireBoundTableBucket(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName string) error {
	if _, bound := ctx.Value(tableBucketCreatedAtKey{}).(time.Time); !bound {
		return nil
	}
	data, err := h.getExtendedAttribute(ctx, client, GetTableBucketPath(bucketName), ExtendedKeyMetadata)
	if errors.Is(err, ErrAttributeNotFound) {
		return filer_pb.ErrNotFound
	} else if err != nil {
		return err
	}
	var meta tableBucketMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}
	if otherTableBucket(ctx, meta.CreatedAt) {
		return filer_pb.ErrNotFound
	}
	return nil
}

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

	if err := validatePolicyDocument(req.ResourcePolicy); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if bucket exists and get metadata for ownership check
	bucketPath := GetTableBucketPath(bucketName)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}
		if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
			return err
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table bucket: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	ctx := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		TableBucketTags:  bucketTags,
		ResourceTags:     bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}
	if deniedByPolicy("PutTableBucketPolicy", principal, bucketPolicy, bucketARN, ctx) ||
		!CheckPermissionWithContext("PutTableBucketPolicy", principal, bucketMetadata.OwnerAccountID, "", bucketARN, ctx) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to put table bucket policy")
		return NewAuthError("PutTableBucketPolicy", principal, "not authorized to put table bucket policy")
	}

	// Conditional on the policy this request was authorized against: a Deny
	// landing mid-request sends the caller back through authorization.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateEntryExtended(r.Context(), client, bucketPath, func(extended map[string][]byte) error {
			if err := requireTableBucketCreatedAt(extended, bucketPath, bucketMetadata.CreatedAt); err != nil {
				return err
			}
			if !bytes.Equal(extended[ExtendedKeyPolicy], []byte(bucketPolicy)) {
				return fmt.Errorf("%w: %s", ErrConcurrentUpdate, ExtendedKeyPolicy)
			}
			extended[ExtendedKeyPolicy] = []byte(req.ResourcePolicy)
			return nil
		})
	})

	if err != nil {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table bucket policy changed concurrently, retry the request")
		} else if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to put table bucket policy")
		}
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

	bucketPath := GetTableBucketPath(bucketName)
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

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("GetTableBucketPolicy", principal, bucketMetadata.OwnerAccountID, string(policy), bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
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

	bucketPath := GetTableBucketPath(bucketName)

	// Check if bucket exists and get metadata for ownership check
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err != nil {
			if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to read bucket policy: %w", err)
			}
			// Policy not found is not an error; bucketPolicy remains empty
		} else {
			bucketPolicy = string(policyData)
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

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("DeleteTableBucketPolicy", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table bucket policy")
		return NewAuthError("DeleteTableBucketPolicy", principal, "not authorized to delete table bucket policy")
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateEntryExtended(r.Context(), client, bucketPath, func(extended map[string][]byte) error {
			if err := requireTableBucketCreatedAt(extended, bucketPath, bucketMetadata.CreatedAt); err != nil {
				return err
			}
			if !bytes.Equal(extended[ExtendedKeyPolicy], []byte(bucketPolicy)) {
				return fmt.Errorf("%w: %s", ErrConcurrentUpdate, ExtendedKeyPolicy)
			}
			delete(extended, ExtendedKeyPolicy)
			return nil
		})
	})

	if err != nil {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table bucket policy changed concurrently, retry the request")
		} else if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table bucket policy")
		}
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

	if err := validatePolicyDocument(req.ResourcePolicy); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
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
	tablePath := GetTablePath(bucketName, namespaceName, tableName)
	bucketPath := GetTableBucketPath(bucketName)

	var metadata tableMetadataInternal
	var bucketPolicy, tablePolicy string
	var bucketTags, tableTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readTableMetadataAttr(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
			return err
		}
		if bucketTags, err = h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		}
		if tablePolicy, err = h.readPolicy(r, client, tablePath); err != nil {
			return err
		}
		tableTags, err = h.readTags(r.Context(), client, tablePath)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	ctx := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableName:        tableName,
		TableBucketTags:  bucketTags,
		ResourceTags:     tableTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}
	if deniedByPolicy("PutTablePolicy", principal, tablePolicy, tableARN, ctx) ||
		!CheckPermissionWithContext("PutTablePolicy", principal, metadata.OwnerAccountID, bucketPolicy, tableARN, ctx) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to put table policy")
		return NewAuthError("PutTablePolicy", principal, "not authorized to put table policy")
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateCatalogRecord(r.Context(), client, tablePath, func(extended map[string][]byte) error {
			if err := requireTableCreatedAt(extended, tablePath, metadata.CreatedAt); err != nil {
				return err
			}
			if !bytes.Equal(extended[ExtendedKeyPolicy], []byte(tablePolicy)) {
				return fmt.Errorf("%w: %s", ErrConcurrentUpdate, ExtendedKeyPolicy)
			}
			extended[ExtendedKeyPolicy] = []byte(req.ResourcePolicy)
			return nil
		})
	})

	if err != nil {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table policy changed concurrently, retry the request")
		} else if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to put table policy")
		}
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
	tablePath := GetTablePath(bucketName, namespaceName, tableName)
	bucketPath := GetTableBucketPath(bucketName)
	var policy []byte
	var metadata tableMetadataInternal
	var bucketPolicy string
	var bucketTags, tableTags map[string]string

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Get metadata for ownership check
		data, err := h.readTableMetadataAttr(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		// Get policy
		policy, err = h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		if err != nil {
			return err
		}

		if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
			return err
		}
		if bucketTags, err = h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		}
		tableTags, err = h.readTags(r.Context(), client, tablePath)
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

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	ctx := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableName:        tableName,
		TableBucketTags:  bucketTags,
		ResourceTags:     tableTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}
	if deniedByPolicy("GetTablePolicy", principal, string(policy), tableARN, ctx) ||
		!CheckPermissionWithContext("GetTablePolicy", principal, metadata.OwnerAccountID, bucketPolicy, tableARN, ctx) {
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
	tablePath := GetTablePath(bucketName, namespaceName, tableName)
	bucketPath := GetTableBucketPath(bucketName)

	// Check if table exists
	var metadata tableMetadataInternal
	var bucketPolicy, tablePolicy string
	var bucketTags, tableTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readTableMetadataAttr(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
			return err
		}
		if bucketTags, err = h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		}
		if tablePolicy, err = h.readPolicy(r, client, tablePath); err != nil {
			return err
		}
		tableTags, err = h.readTags(r.Context(), client, tablePath)
		return err
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	ctx := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableName:        tableName,
		TableBucketTags:  bucketTags,
		ResourceTags:     tableTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}
	if deniedByPolicy("DeleteTablePolicy", principal, tablePolicy, tableARN, ctx) ||
		!CheckPermissionWithContext("DeleteTablePolicy", principal, metadata.OwnerAccountID, bucketPolicy, tableARN, ctx) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table policy")
		return NewAuthError("DeleteTablePolicy", principal, "not authorized to delete table policy")
	}

	// The removal is conditional on the table and the policy this request was
	// authorized against, as PutTablePolicy's write is.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateCatalogRecord(r.Context(), client, tablePath, func(extended map[string][]byte) error {
			if err := requireTableCreatedAt(extended, tablePath, metadata.CreatedAt); err != nil {
				return err
			}
			if !bytes.Equal(extended[ExtendedKeyPolicy], []byte(tablePolicy)) {
				return fmt.Errorf("%w: %s", ErrConcurrentUpdate, ExtendedKeyPolicy)
			}
			delete(extended, ExtendedKeyPolicy)
			return nil
		})
	})

	if err != nil && !errors.Is(err, ErrAttributeNotFound) {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table policy changed concurrently, retry the request")
		} else if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table policy")
		}
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
	var bucketPolicy, tablePolicy string
	var bucketTags map[string]string
	var tableNamespace, tableName string
	var createdAt time.Time
	requestTagKeys := mapKeys(req.Tags)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
				return h.absentTagTargetError(r, client, "TagResource", "not authorized to tag resource", req.ResourceARN, rType)
			}
			return err
		}

		ownerAccountID, bucketName, resourceCreatedAt, err := h.extractResourceOwnerAndBucket(data, resourcePath, rType)
		if err != nil {
			return err
		}
		createdAt = resourceCreatedAt

		// Fetch bucket policy if we have a bucket name
		if bucketName != "" {
			bucketPath := GetTableBucketPath(bucketName)
			if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
				return err
			}
			bucketTags, err = h.readTags(r.Context(), client, bucketPath)
			if err != nil {
				return err
			}
		}

		if rType == ResourceTypeTable {
			if _, tableNamespace, tableName, err = parseTableFromARN(req.ResourceARN); err != nil {
				return err
			}
			if tablePolicy, err = h.readPolicy(r, client, resourcePath); err != nil {
				return err
			}
		}

		// Read existing tags
		data, err = h.getExtendedAttribute(r.Context(), client, resourcePath, extendedKey)
		if err != nil {
			if !errors.Is(err, ErrAttributeNotFound) {
				return err
			}
		} else if err := json.Unmarshal(data, &existingTags); err != nil {
			return err
		}

		// Checked after the reads, so they all came from the bound bucket.
		if err := h.requireBoundTableBucket(r.Context(), client, bucketName); err != nil {
			return err
		}
		var resourceARN string
		if rType == ResourceTypeTable {
			resourceARN = h.generateTableARN(ownerAccountID, bucketName, tableNamespace+"/"+tableName)
		} else {
			resourceARN = h.generateTableBucketARN(ownerAccountID, bucketName)
		}
		principal := h.getAccountID(r)
		identityActions := getIdentityActions(r)
		ctx := &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        tableNamespace,
			TableName:        tableName,
			TableBucketTags:  bucketTags,
			RequestTags:      req.Tags,
			TagKeys:          requestTagKeys,
			ResourceTags:     existingTags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		}
		if deniedByPolicy("TagResource", principal, tablePolicy, resourceARN, ctx) ||
			!CheckPermissionWithContext("TagResource", principal, ownerAccountID, bucketPolicy, resourceARN, ctx) {
			return NewAuthError("TagResource", principal, "not authorized to tag resource")
		}
		return nil
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

	sameResource := requireTableCreatedAt
	if rType != ResourceTypeTable {
		sameResource = requireTableBucketCreatedAt
	}
	maxTags, limited := r.Context().Value(maxTagsKey{}).(int)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateCatalogRecord(r.Context(), client, resourcePath, func(extended map[string][]byte) error {
			if err := sameResource(extended, resourcePath, createdAt); err != nil {
				return err
			}
			tags, err := updateTags(extended[extendedKey], req.Tags, nil)
			if err != nil {
				return err
			}
			if limited && len(tags) > maxTags {
				return errTooManyTags
			}
			return writeTags(extended, extendedKey, tags)
		})
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			errorCode := ErrCodeNoSuchBucket
			if rType == ResourceTypeTable {
				errorCode = ErrCodeNoSuchTable
			}
			h.writeError(w, http.StatusNotFound, errorCode, "resource not found")
			return err
		}
		if errors.Is(err, errTooManyTags) {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest,
				fmt.Sprintf("a resource can have at most %d tags", maxTags))
			return err
		}
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "resource tags changed concurrently, retry the request")
			return err
		}
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
	var bucketPolicy, tablePolicy string
	var bucketTags map[string]string
	var tableNamespace, tableName string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
				return h.absentTagTargetError(r, client, "ListTagsForResource", "not authorized to list tags for resource", req.ResourceARN, rType)
			}
			return err
		}

		ownerAccountID, bucketName, _, err := h.extractResourceOwnerAndBucket(data, resourcePath, rType)
		if err != nil {
			return err
		}

		// Fetch bucket policy if we have a bucket name
		if bucketName != "" {
			bucketPath := GetTableBucketPath(bucketName)
			if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
				return err
			}
			bucketTags, err = h.readTags(r.Context(), client, bucketPath)
			if err != nil {
				return err
			}
		}

		if rType == ResourceTypeTable {
			if _, tableNamespace, tableName, err = parseTableFromARN(req.ResourceARN); err != nil {
				return err
			}
			if tablePolicy, err = h.readPolicy(r, client, resourcePath); err != nil {
				return err
			}
		}

		data, err = h.getExtendedAttribute(r.Context(), client, resourcePath, extendedKey)
		if err != nil {
			if !errors.Is(err, ErrAttributeNotFound) {
				return err
			}
		} else if err := json.Unmarshal(data, &tags); err != nil {
			return err
		}

		// Checked after the reads, so they all came from the bound bucket.
		if err := h.requireBoundTableBucket(r.Context(), client, bucketName); err != nil {
			return err
		}
		var resourceARN string
		if rType == ResourceTypeTable {
			resourceARN = h.generateTableARN(ownerAccountID, bucketName, tableNamespace+"/"+tableName)
		} else {
			resourceARN = h.generateTableBucketARN(ownerAccountID, bucketName)
		}
		principal := h.getAccountID(r)
		identityActions := getIdentityActions(r)
		ctx := &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        tableNamespace,
			TableName:        tableName,
			TableBucketTags:  bucketTags,
			ResourceTags:     tags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		}
		if deniedByPolicy("ListTagsForResource", principal, tablePolicy, resourceARN, ctx) ||
			!CheckPermissionWithContext("ListTagsForResource", principal, ownerAccountID, bucketPolicy, resourceARN, ctx) {
			return NewAuthError("ListTagsForResource", principal, "not authorized to list tags for resource")
		}
		return nil
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
	var bucketPolicy, tablePolicy string
	var bucketTags map[string]string
	var tableNamespace, tableName string
	var createdAt time.Time
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
				return h.absentTagTargetError(r, client, "UntagResource", "not authorized to untag resource", req.ResourceARN, rType)
			}
			return err
		}

		ownerAccountID, bucketName, resourceCreatedAt, err := h.extractResourceOwnerAndBucket(data, resourcePath, rType)
		if err != nil {
			return err
		}
		createdAt = resourceCreatedAt

		// Fetch bucket policy if we have a bucket name
		if bucketName != "" {
			bucketPath := GetTableBucketPath(bucketName)
			if bucketPolicy, err = h.readPolicy(r, client, bucketPath); err != nil {
				return err
			}
			bucketTags, err = h.readTags(r.Context(), client, bucketPath)
			if err != nil {
				return err
			}
		}

		if rType == ResourceTypeTable {
			if _, tableNamespace, tableName, err = parseTableFromARN(req.ResourceARN); err != nil {
				return err
			}
			if tablePolicy, err = h.readPolicy(r, client, resourcePath); err != nil {
				return err
			}
		}

		data, err = h.getExtendedAttribute(r.Context(), client, resourcePath, extendedKey)
		if err != nil {
			if !errors.Is(err, ErrAttributeNotFound) {
				return err
			}
		} else if err := json.Unmarshal(data, &tags); err != nil {
			return err
		}

		// Checked after the reads, so they all came from the bound bucket.
		if err := h.requireBoundTableBucket(r.Context(), client, bucketName); err != nil {
			return err
		}
		var resourceARN string
		if rType == ResourceTypeTable {
			resourceARN = h.generateTableARN(ownerAccountID, bucketName, tableNamespace+"/"+tableName)
		} else {
			resourceARN = h.generateTableBucketARN(ownerAccountID, bucketName)
		}
		principal := h.getAccountID(r)
		identityActions := getIdentityActions(r)
		ctx := &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        tableNamespace,
			TableName:        tableName,
			TableBucketTags:  bucketTags,
			TagKeys:          req.TagKeys,
			ResourceTags:     tags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		}
		if deniedByPolicy("UntagResource", principal, tablePolicy, resourceARN, ctx) ||
			!CheckPermissionWithContext("UntagResource", principal, ownerAccountID, bucketPolicy, resourceARN, ctx) {
			return NewAuthError("UntagResource", principal, "not authorized to untag resource")
		}
		return nil
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

	sameResource := requireTableCreatedAt
	if rType != ResourceTypeTable {
		sameResource = requireTableBucketCreatedAt
	}
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateCatalogRecord(r.Context(), client, resourcePath, func(extended map[string][]byte) error {
			if err := sameResource(extended, resourcePath, createdAt); err != nil {
				return err
			}
			tags, err := updateTags(extended[extendedKey], nil, req.TagKeys)
			if err != nil {
				return err
			}
			return writeTags(extended, extendedKey, tags)
		})
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			errorCode := ErrCodeNoSuchBucket
			if rType == ResourceTypeTable {
				errorCode = ErrCodeNoSuchTable
			}
			h.writeError(w, http.StatusNotFound, errorCode, "resource not found")
			return err
		}
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "resource tags changed concurrently, retry the request")
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to untag resource")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// absentTagTargetError resolves a tag operation when the resource's catalog
// metadata did not load: a caller the operation would refuse gets its denial,
// everyone else the same not-found an absent name gets.
func (h *S3TablesHandler) absentTagTargetError(r *http.Request, client filer_pb.SeaweedFilerClient, operation, deniedMessage, resourceARN string, rType ResourceType) error {
	var bucketName, namespace, name string
	if rType == ResourceTypeTable {
		bucketName, namespace, name, _ = parseTableFromARN(resourceARN)
	} else {
		bucketName, _ = parseBucketNameFromARN(resourceARN)
	}
	if !h.authorizeAbsentTarget(r, &singleClient{client}, operation, bucketName, namespace, name, resourceARN) {
		return NewAuthError(operation, h.getAccountID(r), deniedMessage)
	}
	return filer_pb.ErrNotFound
}

// resolveResourcePath determines the resource path and extended attribute key from a resource ARN
func (h *S3TablesHandler) resolveResourcePath(resourceARN string) (path string, key string, rType ResourceType, err error) {
	// Try parsing as table ARN first
	bucketName, namespace, tableName, err := parseTableFromARN(resourceARN)
	if err == nil {
		return GetTablePath(bucketName, namespace, tableName), ExtendedKeyTags, ResourceTypeTable, nil
	}

	// Try parsing as bucket ARN
	bucketName, err = parseBucketNameFromARN(resourceARN)
	if err == nil {
		return GetTableBucketPath(bucketName), ExtendedKeyTags, ResourceTypeBucket, nil
	}

	return "", "", "", fmt.Errorf("invalid resource ARN: %s", resourceARN)
}
