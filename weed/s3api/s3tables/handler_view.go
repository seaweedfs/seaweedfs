package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleCreateView creates a new view in a namespace. Views share the table
// storage layout but are tagged ExtendedKeyEntryType="view". Names are unique
// across tables and views in a namespace.
func (h *S3TablesHandler) handleCreateView(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req CreateViewRequest
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

	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "name is required")
		return fmt.Errorf("name is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	viewName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if namespace exists
	namespacePath := GetNamespacePath(bucketName, namespaceName)
	var namespaceMetadata namespaceMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &namespaceMetadata)
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", namespaceName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		}
		return err
	}

	accountID := h.getAccountID(r)
	bucketPath := GetTableBucketPath(bucketName)
	namespacePolicy, bucketPolicy, bucketTags, bucketMetadata, err := h.loadAuthContext(r, filerClient, namespacePath, bucketPath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to fetch policies: %v", err))
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	if !h.authorizeViewOp(r, "CreateView", accountID, namespaceMetadata.OwnerAccountID, bucketMetadata.OwnerAccountID, namespacePolicy, bucketPolicy, bucketARN, bucketName, namespaceName, viewName, bucketTags) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create view in this namespace")
		return ErrAccessDenied
	}

	viewPath := GetTablePath(bucketName, namespaceName, viewName)

	// Reject if a table or view already exists at this name.
	var existingMetadata tableMetadataInternal
	var existingIsTable bool
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, viewPath)
		if err != nil {
			return err
		}
		if entryType(entry.Extended) != EntryTypeView {
			existingIsTable = true
			return nil
		}
		data, ok := entry.Extended[ExtendedKeyMetadata]
		if !ok {
			return fmt.Errorf("%w: %s", ErrAttributeNotFound, ExtendedKeyMetadata)
		}
		return json.Unmarshal(data, &existingMetadata)
	})
	if err == nil {
		if existingIsTable {
			h.writeError(w, http.StatusConflict, ErrCodeViewAlreadyExists, fmt.Sprintf("a table named %s already exists", viewName))
			return fmt.Errorf("table name conflict: %s", viewName)
		}
		viewARN := h.generateViewARN(existingMetadata.OwnerAccountID, bucketName, namespaceName+"/"+viewName)
		h.writeJSON(w, http.StatusOK, &CreateViewResponse{
			ViewARN:          viewARN,
			VersionToken:     existingMetadata.VersionToken,
			MetadataLocation: existingMetadata.MetadataLocation,
		})
		return nil
	} else if !errors.Is(err, filer_pb.ErrNotFound) && !errors.Is(err, ErrAttributeNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check view: %v", err))
		return err
	}

	now := time.Now()
	versionToken := generateVersionToken()
	metadata := &tableMetadataInternal{
		Name:             viewName,
		Namespace:        namespaceName,
		Format:           "ICEBERG",
		CreatedAt:        now,
		ModifiedAt:       now,
		OwnerAccountID:   namespaceMetadata.OwnerAccountID,
		VersionToken:     versionToken,
		MetadataVersion:  max(req.MetadataVersion, 1),
		MetadataLocation: req.MetadataLocation,
		Metadata:         req.Metadata,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal view metadata")
		return err
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if err := h.ensureDirectory(r.Context(), client, viewPath); err != nil {
			return err
		}
		return h.setExtendedAttributes(r.Context(), client, viewPath, map[string][]byte{
			ExtendedKeyMetadata:  metadataBytes,
			ExtendedKeyEntryType: []byte(EntryTypeView),
		})
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create view")
		return err
	}

	viewARN := h.generateViewARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+viewName)
	h.writeJSON(w, http.StatusOK, &CreateViewResponse{
		ViewARN:          viewARN,
		VersionToken:     versionToken,
		MetadataLocation: metadata.MetadataLocation,
	})
	return nil
}

// handleGetView returns the metadata pointer for a view.
func (h *S3TablesHandler) handleGetView(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetViewRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, namespace, viewName, err := h.parseViewRef(req.TableBucketARN, req.Namespace, req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	viewPath := GetTablePath(bucketName, namespace, viewName)
	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, viewPath)
		if err != nil {
			return err
		}
		if entryType(entry.Extended) != EntryTypeView {
			return filer_pb.ErrNotFound
		}
		data, ok := entry.Extended[ExtendedKeyMetadata]
		if !ok {
			return fmt.Errorf("%w: %s", ErrAttributeNotFound, ExtendedKeyMetadata)
		}
		return json.Unmarshal(data, &metadata)
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchView, fmt.Sprintf("view %s not found", viewName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get view: %v", err))
		}
		return err
	}

	accountID := h.getAccountID(r)
	viewPolicy, bucketPolicy, bucketTags, bucketMetadata, err := h.loadAuthContext(r, filerClient, viewPath, GetTableBucketPath(bucketName))
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to fetch policies: %v", err))
		return err
	}
	viewARN := h.generateViewARN(metadata.OwnerAccountID, bucketName, namespace+"/"+viewName)
	if !h.authorizeViewOp(r, "GetView", accountID, metadata.OwnerAccountID, bucketMetadata.OwnerAccountID, viewPolicy, bucketPolicy, viewARN, bucketName, namespace, viewName, bucketTags) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchView, fmt.Sprintf("view %s not found", viewName))
		return ErrAccessDenied
	}

	h.writeJSON(w, http.StatusOK, &GetViewResponse{
		Name:             metadata.Name,
		ViewARN:          viewARN,
		Namespace:        expandNamespace(metadata.Namespace),
		CreatedAt:        metadata.CreatedAt,
		ModifiedAt:       metadata.ModifiedAt,
		OwnerAccountID:   metadata.OwnerAccountID,
		MetadataLocation: metadata.MetadataLocation,
		MetadataVersion:  metadata.MetadataVersion,
		VersionToken:     metadata.VersionToken,
		Metadata:         metadata.Metadata,
	})
	return nil
}

// handleListViews lists views in a namespace, excluding table entries.
func (h *S3TablesHandler) handleListViews(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req ListViewsRequest
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

	maxViews := req.MaxViews
	if maxViews <= 0 {
		maxViews = 100
	}
	const maxViewsLimit = 1000
	if maxViews > maxViewsLimit {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "MaxViews exceeds maximum allowed value")
		return fmt.Errorf("invalid maxViews value: %d", maxViews)
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	var views []ViewSummary
	var paginationToken string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		namespacePath := GetNamespacePath(bucketName, namespaceName)
		bucketPath := GetTableBucketPath(bucketName)
		var nsMeta namespaceMetadata
		data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &nsMeta); err != nil {
			return err
		}

		namespacePolicy, bucketPolicy, bucketTags, bucketMeta, err := h.loadAuthContext(r, &singleClient{client}, namespacePath, bucketPath)
		if err != nil {
			return err
		}
		bucketARN := h.generateTableBucketARN(bucketMeta.OwnerAccountID, bucketName)
		if !h.authorizeViewOp(r, "ListViews", h.getAccountID(r), nsMeta.OwnerAccountID, bucketMeta.OwnerAccountID, namespacePolicy, bucketPolicy, bucketARN, bucketName, namespaceName, "", bucketTags) {
			return ErrAccessDenied
		}

		views, paginationToken, err = h.listViewsInNamespace(r, client, bucketName, namespaceName, req.Prefix, req.ContinuationToken, maxViews)
		return err
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			views = []ViewSummary{}
			paginationToken = ""
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "Access Denied")
			return err
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list views: %v", err))
			return err
		}
	}

	h.writeJSON(w, http.StatusOK, &ListViewsResponse{
		Views:             views,
		ContinuationToken: paginationToken,
	})
	return nil
}

func (h *S3TablesHandler) listViewsInNamespace(r *http.Request, client filer_pb.SeaweedFilerClient, bucketName, namespaceName, prefix, continuationToken string, maxViews int) ([]ViewSummary, string, error) {
	namespacePath := GetNamespacePath(bucketName, namespaceName)
	var views []ViewSummary
	lastFileName := continuationToken
	ctx := r.Context()

	for len(views) < maxViews {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          namespacePath,
			Limit:              uint32(maxViews * 2),
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: lastFileName == "" || lastFileName == continuationToken,
		})
		if err != nil {
			return nil, "", err
		}

		hasMore := false
		for {
			entry, respErr := resp.Recv()
			if respErr != nil {
				if respErr == io.EOF {
					break
				}
				return nil, "", respErr
			}
			if entry.Entry == nil {
				continue
			}
			if len(views) == 0 && continuationToken != "" && entry.Entry.Name == continuationToken {
				continue
			}

			hasMore = true
			lastFileName = entry.Entry.Name

			if !entry.Entry.IsDirectory || strings.HasPrefix(entry.Entry.Name, ".") {
				continue
			}
			if prefix != "" && !strings.HasPrefix(entry.Entry.Name, prefix) {
				continue
			}
			// Only include view entries; skip tables and untagged entries.
			if entryType(entry.Entry.Extended) != EntryTypeView {
				continue
			}
			data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
			if !ok {
				continue
			}
			var metadata tableMetadataInternal
			if err := json.Unmarshal(data, &metadata); err != nil {
				continue
			}

			views = append(views, ViewSummary{
				Name:       entry.Entry.Name,
				ViewARN:    h.generateViewARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+entry.Entry.Name),
				Namespace:  expandNamespace(namespaceName),
				CreatedAt:  metadata.CreatedAt,
				ModifiedAt: metadata.ModifiedAt,
			})

			if len(views) >= maxViews {
				return views, lastFileName, nil
			}
		}

		if !hasMore {
			break
		}
	}

	if len(views) < maxViews {
		lastFileName = ""
	}
	return views, lastFileName, nil
}

// handleUpdateView replaces the metadata pointer for a view, mirroring UpdateTable.
func (h *S3TablesHandler) handleUpdateView(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req UpdateViewRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, namespaceName, viewName, err := h.parseViewRef(req.TableBucketARN, req.Namespace, req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	viewPath := GetTablePath(bucketName, namespaceName, viewName)
	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, viewPath)
		if err != nil {
			return err
		}
		if entryType(entry.Extended) != EntryTypeView {
			return filer_pb.ErrNotFound
		}
		data, ok := entry.Extended[ExtendedKeyMetadata]
		if !ok {
			return fmt.Errorf("%w: %s", ErrAttributeNotFound, ExtendedKeyMetadata)
		}
		return json.Unmarshal(data, &metadata)
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchView, "view not found")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		}
		return err
	}

	accountID := h.getAccountID(r)
	viewPolicy, bucketPolicy, bucketTags, bucketMetadata, err := h.loadAuthContext(r, filerClient, viewPath, GetTableBucketPath(bucketName))
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		return err
	}
	viewARN := h.generateViewARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+viewName)
	if !h.authorizeViewOp(r, "UpdateView", accountID, metadata.OwnerAccountID, bucketMetadata.OwnerAccountID, viewPolicy, bucketPolicy, viewARN, bucketName, namespaceName, viewName, bucketTags) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to update view")
		return NewAuthError("UpdateView", accountID, "not authorized to update view")
	}

	if req.VersionToken != "" && req.VersionToken != metadata.VersionToken {
		h.writeError(w, http.StatusConflict, ErrCodeConflict, "Version token mismatch")
		return ErrVersionTokenMismatch
	}

	if req.Metadata != nil {
		if metadata.Metadata == nil {
			metadata.Metadata = &TableMetadata{}
		}
		if req.Metadata.Iceberg != nil {
			if metadata.Metadata.Iceberg == nil {
				metadata.Metadata.Iceberg = &IcebergMetadata{}
			}
			if req.Metadata.Iceberg.TableUUID != "" {
				metadata.Metadata.Iceberg.TableUUID = req.Metadata.Iceberg.TableUUID
			}
		}
		if len(req.Metadata.FullMetadata) > 0 {
			metadata.Metadata.FullMetadata = req.Metadata.FullMetadata
		}
	}
	if req.MetadataLocation != "" {
		metadata.MetadataLocation = req.MetadataLocation
	}
	if req.MetadataVersion > 0 {
		metadata.MetadataVersion = req.MetadataVersion
	} else if metadata.MetadataVersion == 0 {
		metadata.MetadataVersion = 1
	}
	metadata.ModifiedAt = time.Now()
	metadata.VersionToken = generateVersionToken()

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal metadata")
		return err
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.setExtendedAttribute(r.Context(), client, viewPath, ExtendedKeyMetadata, metadataBytes)
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to update metadata")
		return err
	}

	h.writeJSON(w, http.StatusOK, &UpdateViewResponse{
		ViewARN:          viewARN,
		MetadataLocation: metadata.MetadataLocation,
		VersionToken:     metadata.VersionToken,
	})
	return nil
}

// handleDeleteView deletes a view.
func (h *S3TablesHandler) handleDeleteView(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req DeleteViewRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, namespaceName, viewName, err := h.parseViewRef(req.TableBucketARN, req.Namespace, req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	viewPath := GetTablePath(bucketName, namespaceName, viewName)
	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, viewPath)
		if err != nil {
			return err
		}
		if entryType(entry.Extended) != EntryTypeView {
			return filer_pb.ErrNotFound
		}
		data, ok := entry.Extended[ExtendedKeyMetadata]
		if !ok {
			return fmt.Errorf("%w: %s", ErrAttributeNotFound, ExtendedKeyMetadata)
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}
		if req.VersionToken != "" && metadata.VersionToken != req.VersionToken {
			return ErrVersionTokenMismatch
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchView, fmt.Sprintf("view %s not found", viewName))
		} else if errors.Is(err, ErrVersionTokenMismatch) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "version token mismatch")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check view: %v", err))
		}
		return err
	}

	accountID := h.getAccountID(r)
	viewPolicy, bucketPolicy, bucketTags, bucketMetadata, err := h.loadAuthContext(r, filerClient, viewPath, GetTableBucketPath(bucketName))
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		return err
	}
	viewARN := h.generateViewARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+viewName)
	if !h.authorizeViewOp(r, "DeleteView", accountID, metadata.OwnerAccountID, bucketMetadata.OwnerAccountID, viewPolicy, bucketPolicy, viewARN, bucketName, namespaceName, viewName, bucketTags) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete view")
		return NewAuthError("DeleteView", accountID, "not authorized to delete view")
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(r.Context(), client, viewPath)
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete view")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// parseViewRef validates and resolves a (bucketARN, namespace, name) triple.
func (h *S3TablesHandler) parseViewRef(bucketARN string, namespace []string, name string) (bucketName, namespaceName, viewName string, err error) {
	if bucketARN == "" || len(namespace) == 0 || name == "" {
		return "", "", "", fmt.Errorf("tableBucketARN, namespace, and name are required")
	}
	if bucketName, err = parseBucketNameFromARN(bucketARN); err != nil {
		return "", "", "", err
	}
	if namespaceName, err = validateNamespace(namespace); err != nil {
		return "", "", "", err
	}
	if viewName, err = validateTableName(name); err != nil {
		return "", "", "", err
	}
	return bucketName, namespaceName, viewName, nil
}

// loadAuthContext fetches the resource policy, bucket policy, bucket tags and
// bucket metadata used to authorize a view operation.
func (h *S3TablesHandler) loadAuthContext(r *http.Request, filerClient FilerClient, resourcePath, bucketPath string) (resourcePolicy, bucketPolicy string, bucketTags map[string]string, bucketMetadata tableBucketMetadata, err error) {
	bucketTags = map[string]string{}
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, e := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if e == nil {
			if e := json.Unmarshal(data, &bucketMetadata); e != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", e)
			}
		} else if !errors.Is(e, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %w", e)
		}

		policyData, e := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyPolicy)
		if e == nil {
			resourcePolicy = string(policyData)
		} else if !errors.Is(e, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch resource policy: %w", e)
		}

		policyData, e = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if e == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(e, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %w", e)
		}
		if tags, e := h.readTags(r.Context(), client, bucketPath); e != nil {
			return e
		} else if tags != nil {
			bucketTags = tags
		}
		return nil
	})
	return resourcePolicy, bucketPolicy, bucketTags, bucketMetadata, err
}

// authorizeViewOp evaluates the resource and bucket policies for a view operation.
func (h *S3TablesHandler) authorizeViewOp(r *http.Request, operation, principal, resourceOwner, bucketOwner, resourcePolicy, bucketPolicy, resourceARN, bucketName, namespace, viewName string, bucketTags map[string]string) bool {
	identityActions := getIdentityActions(r)
	ctx := &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespace,
		TableName:       viewName,
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
		DefaultAllow:    h.defaultAllowFor(r),
	}
	resourceAllowed := CheckPermissionWithContext(operation, principal, resourceOwner, resourcePolicy, resourceARN, ctx)
	bucketARN := h.generateTableBucketARN(bucketOwner, bucketName)
	bucketAllowed := CheckPermissionWithContext(operation, principal, bucketOwner, bucketPolicy, bucketARN, ctx)
	return resourceAllowed || bucketAllowed
}

// singleClient adapts a bound filer client to the FilerClient interface so the
// shared helpers can be reused inside an existing WithFilerClient closure.
type singleClient struct {
	client filer_pb.SeaweedFilerClient
}

func (s *singleClient) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(s.client)
}
