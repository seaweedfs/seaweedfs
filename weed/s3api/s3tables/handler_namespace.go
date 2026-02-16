package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleCreateNamespace creates a new namespace in a table bucket
func (h *S3TablesHandler) handleCreateNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	glog.Errorf("S3Tables: handleCreateNamespace called")
	var req CreateNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		glog.Errorf("S3Tables: handleCreateNamespace failed to read request body: %v", err)
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	if len(req.Namespace) == 0 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "namespace is required")
		return fmt.Errorf("namespace is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if table bucket exists
	bucketPath := GetTableBucketPath(bucketName)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	ownerAccountID := h.getAccountID(r)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				dir, name := splitPath(bucketPath)
				entryResp, lookupErr := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
					Directory: dir,
					Name:      name,
				})
				if lookupErr != nil {
					return lookupErr
				}
				if entryResp.Entry == nil || !IsTableBucketEntry(entryResp.Entry) {
					return filer_pb.ErrNotFound
				}
				bucketMetadata = tableBucketMetadata{
					Name:           bucketName,
					CreatedAt:      time.Now(),
					OwnerAccountID: ownerAccountID,
				}
				metadataBytes, err := json.Marshal(&bucketMetadata)
				if err != nil {
					return fmt.Errorf("failed to marshal bucket metadata: %w", err)
				}
				if err := h.setExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata, metadataBytes); err != nil {
					return err
				}
			} else {
				return err
			}
		} else if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
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
	glog.Infof("S3Tables: CreateNamespace permission check - principal=%s, owner=%s, actions=%v", principal, bucketMetadata.OwnerAccountID, identityActions)
	if !CheckPermissionWithContext("CreateNamespace", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
		DefaultAllow:    h.defaultAllow,
	}) {
		glog.Infof("S3Tables: Permission denied for CreateNamespace - principal=%s, owner=%s", principal, bucketMetadata.OwnerAccountID)
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
		return ErrAccessDenied
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)

	// Check if namespace already exists
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		return err
	})

	if err == nil {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceAlreadyExists, fmt.Sprintf("namespace %s already exists", namespaceName))
		return fmt.Errorf("namespace already exists")
	} else if !errors.Is(err, filer_pb.ErrNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		return err
	}

	// Create the namespace with bucket owner to maintain consistency
	// (authorization above ensures the caller has permission to create in this bucket)
	now := time.Now()
	metadata := &namespaceMetadata{
		Namespace:      req.Namespace,
		CreatedAt:      now,
		OwnerAccountID: bucketMetadata.OwnerAccountID,
		Properties:     req.Properties,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal namespace metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Create namespace directory
		if err := h.createDirectory(r.Context(), client, namespacePath); err != nil {
			return err
		}

		// Set metadata as extended attribute
		if err := h.setExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata, metadataBytes); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create namespace")
		return err
	}

	resp := &CreateNamespaceResponse{
		Namespace:      req.Namespace,
		TableBucketARN: req.TableBucketARN,
		Properties:     req.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleGetNamespace gets details of a namespace
func (h *S3TablesHandler) handleGetNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
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

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	// Get namespace and bucket policy
	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("GetNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
		DefaultAllow:    h.defaultAllow,
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, "namespace not found")
		return ErrAccessDenied
	}

	resp := &GetNamespaceResponse{
		Namespace:      metadata.Namespace,
		CreatedAt:      metadata.CreatedAt,
		OwnerAccountID: metadata.OwnerAccountID,
		Properties:     metadata.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListNamespaces lists all namespaces in a table bucket
func (h *S3TablesHandler) handleListNamespaces(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req ListNamespacesRequest
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

	maxNamespaces := req.MaxNamespaces
	if maxNamespaces <= 0 {
		maxNamespaces = 100
	}

	bucketPath := GetTableBucketPath(bucketName)

	// Check permission (check bucket ownership)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			glog.V(0).Infof("S3Tables: handleListNamespaces getExtendedAttribute error: %v (type: %T)", err, err)
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		glog.V(0).Infof("S3Tables: handleListNamespaces metadata fetch error: %v (isNotFound: %v, isAttrNotFound: %v)",
			err, errors.Is(err, filer_pb.ErrNotFound), errors.Is(err, ErrAttributeNotFound))
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespaces: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	glog.V(0).Infof("S3Tables: handleListNamespaces about to check permission: principal=%s owner=%s defaultAllow=%v",
		principal, bucketMetadata.OwnerAccountID, h.defaultAllow)
	if !CheckPermissionWithContext("ListNamespaces", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
		DefaultAllow:    h.defaultAllow,
	}) {
		glog.V(0).Infof("S3Tables: handleListNamespaces permission check FAILED")
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return ErrAccessDenied
	}
	glog.V(0).Infof("S3Tables: handleListNamespaces permission check PASSED")

	var namespaces []NamespaceSummary

	lastFileName := req.ContinuationToken
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for len(namespaces) < maxNamespaces {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          bucketPath,
				Limit:              uint32(maxNamespaces * 2),
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "" || lastFileName == req.ContinuationToken,
			})
			if err != nil {
				return err
			}

			hasMore := false
			for {
				entry, respErr := resp.Recv()
				if respErr != nil {
					if respErr == io.EOF {
						break
					}
					return respErr
				}
				if entry.Entry == nil {
					continue
				}

				// Skip the start item if it was included in the previous page
				if len(namespaces) == 0 && req.ContinuationToken != "" && entry.Entry.Name == req.ContinuationToken {
					continue
				}

				hasMore = true
				lastFileName = entry.Entry.Name

				if !entry.Entry.IsDirectory {
					continue
				}

				// Skip hidden entries
				if strings.HasPrefix(entry.Entry.Name, ".") {
					continue
				}

				// Apply prefix filter
				if req.Prefix != "" && !strings.HasPrefix(entry.Entry.Name, req.Prefix) {
					continue
				}

				// Read metadata from extended attribute
				data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
				if !ok {
					continue
				}

				var metadata namespaceMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					continue
				}

				if metadata.OwnerAccountID != bucketMetadata.OwnerAccountID {
					continue
				}

				namespaces = append(namespaces, NamespaceSummary{
					Namespace: metadata.Namespace,
					CreatedAt: metadata.CreatedAt,
				})

				if len(namespaces) >= maxNamespaces {
					return nil
				}
			}

			if !hasMore {
				break
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			namespaces = []NamespaceSummary{}
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespaces: %v", err))
			return err
		}
	}

	paginationToken := ""
	if len(namespaces) >= maxNamespaces {
		paginationToken = lastFileName
	}

	resp := &ListNamespacesResponse{
		Namespaces:        namespaces,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleDeleteNamespace deletes a namespace from a table bucket
func (h *S3TablesHandler) handleDeleteNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req DeleteNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
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

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	// Check if namespace exists and get metadata for permission check
	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace metadata: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("DeleteNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
		DefaultAllow:    h.defaultAllow,
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, "namespace not found")
		return ErrAccessDenied
	}

	// Check if namespace is empty
	hasChildren := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
			Directory: namespacePath,
			Limit:     10,
		})
		if err != nil {
			return err
		}

		for {
			entry, err := resp.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if entry.Entry != nil && !strings.HasPrefix(entry.Entry.Name, ".") {
				hasChildren = true
				break
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespace entries: %v", err))
		}
		return err
	}

	if hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceNotEmpty, "namespace is not empty")
		return fmt.Errorf("namespace not empty")
	}

	// Delete the namespace
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(r.Context(), client, namespacePath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete namespace")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
