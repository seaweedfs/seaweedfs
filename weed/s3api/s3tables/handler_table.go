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

// handleCreateTable creates a new table in a namespace
func (h *S3TablesHandler) handleCreateTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req CreateTableRequest
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

	if req.Format == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "format is required")
		return fmt.Errorf("format is required")
	}

	// Validate format
	if req.Format != "ICEBERG" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "only ICEBERG format is supported")
		return fmt.Errorf("invalid format")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Validate table name
	tableName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if namespace exists
	namespacePath := getNamespacePath(bucketName, namespaceName)
	var namespaceMetadata namespaceMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &namespaceMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal namespace metadata: %w", err)
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", namespaceName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		}
		return err
	}

	// Authorize table creation using policy framework (namespace + bucket policies)
	accountID := h.getAccountID(r)
	bucketPath := getTableBucketPath(bucketName)
	namespacePolicy := ""
	bucketPolicy := ""
	bucketTags := map[string]string{}
	var data []byte
	var bucketMetadata tableBucketMetadata

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Fetch bucket metadata to use correct owner for bucket policy evaluation
		data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %v", err)
		}

		// Fetch namespace policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyPolicy)
		if err == nil {
			namespacePolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch namespace policy: %v", err)
		}

		// Fetch bucket policy if it exists
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		} else if tags != nil {
			bucketTags = tags
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to fetch policies: %v", err))
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	identityActions := getIdentityActions(r)
	nsAllowed := CheckPermissionWithContext("CreateTable", accountID, namespaceMetadata.OwnerAccountID, namespacePolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableName:       tableName,
		RequestTags:     req.Tags,
		TagKeys:         mapKeys(req.Tags),
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
	})
	bucketAllowed := CheckPermissionWithContext("CreateTable", accountID, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableName:       tableName,
		RequestTags:     req.Tags,
		TagKeys:         mapKeys(req.Tags),
		TableBucketTags: bucketTags,
		IdentityActions: identityActions,
	})

	if !nsAllowed && !bucketAllowed {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table in this namespace")
		return ErrAccessDenied
	}

	tablePath := getTablePath(bucketName, namespaceName, tableName)

	// Check if table already exists
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		return err
	})

	if err == nil {
		h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists, fmt.Sprintf("table %s already exists", tableName))
		return fmt.Errorf("table already exists")
	} else if !errors.Is(err, filer_pb.ErrNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		return err
	}

	// Create the table
	now := time.Now()
	versionToken := generateVersionToken()

	metadata := &tableMetadataInternal{
		Name:           tableName,
		Namespace:      namespaceName,
		Format:         req.Format,
		CreatedAt:      now,
		ModifiedAt:     now,
		OwnerAccountID: namespaceMetadata.OwnerAccountID, // Inherit namespace owner for consistency
		VersionToken:   versionToken,
		Metadata:       req.Metadata,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal table metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Create table directory
		if err := h.createDirectory(r.Context(), client, tablePath); err != nil {
			return err
		}

		// Create data subdirectory for Iceberg files
		dataPath := tablePath + "/data"
		if err := h.createDirectory(r.Context(), client, dataPath); err != nil {
			return err
		}

		// Set metadata as extended attribute
		if err := h.setExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata, metadataBytes); err != nil {
			return err
		}

		// Set tags if provided
		if len(req.Tags) > 0 {
			tagsBytes, err := json.Marshal(req.Tags)
			if err != nil {
				return fmt.Errorf("failed to marshal tags: %w", err)
			}
			if err := h.setExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyTags, tagsBytes); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create table")
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)

	resp := &CreateTableResponse{
		TableARN:     tableARN,
		VersionToken: versionToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleGetTable gets details of a table
func (h *S3TablesHandler) handleGetTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req GetTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	var bucketName, namespace, tableName string
	var err error

	// Support getting by ARN or by bucket/namespace/name
	if req.TableARN != "" {
		bucketName, namespace, tableName, err = parseTableFromARN(req.TableARN)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
	} else if req.TableBucketARN != "" && len(req.Namespace) > 0 && req.Name != "" {
		bucketName, err = parseBucketNameFromARN(req.TableBucketARN)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
		namespace, err = validateNamespace(req.Namespace)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
		tableName, err = validateTableName(req.Name)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
	} else {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "either tableARN or (tableBucketARN, namespace, name) is required")
		return fmt.Errorf("missing required parameters")
	}

	tablePath := getTablePath(bucketName, namespace, tableName)

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
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table: %v", err))
		}
		return err
	}

	// Authorize access to the table using policy framework
	accountID := h.getAccountID(r)
	bucketPath := getTableBucketPath(bucketName)
	tablePolicy := ""
	bucketPolicy := ""
	bucketTags := map[string]string{}
	tableTags := map[string]string{}
	var bucketMetadata tableBucketMetadata

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Fetch bucket metadata to use correct owner for bucket policy evaluation
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %v", err)
		}

		// Fetch table policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		if err == nil {
			tablePolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch table policy: %v", err)
		}
		if tags, err := h.readTags(r.Context(), client, tablePath); err != nil {
			return err
		} else if tags != nil {
			tableTags = tags
		}

		// Fetch bucket policy if it exists
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		} else if tags != nil {
			bucketTags = tags
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to fetch policies: %v", err))
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespace+"/"+tableName)
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	identityActions := getIdentityActions(r)
	tableAllowed := CheckPermissionWithContext("GetTable", accountID, metadata.OwnerAccountID, tablePolicy, tableARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespace,
		TableName:       tableName,
		ResourceTags:    tableTags,
		IdentityActions: identityActions,
	})
	bucketAllowed := CheckPermissionWithContext("GetTable", accountID, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespace,
		TableName:       tableName,
		TableBucketTags: bucketTags,
		ResourceTags:    tableTags,
		IdentityActions: identityActions,
	})

	if !tableAllowed && !bucketAllowed {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return ErrAccessDenied
	}

	resp := &GetTableResponse{
		Name:             metadata.Name,
		TableARN:         tableARN,
		Namespace:        []string{metadata.Namespace},
		Format:           metadata.Format,
		CreatedAt:        metadata.CreatedAt,
		ModifiedAt:       metadata.ModifiedAt,
		OwnerAccountID:   metadata.OwnerAccountID,
		MetadataLocation: metadata.MetadataLocation,
		VersionToken:     metadata.VersionToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListTables lists all tables in a namespace or bucket
func (h *S3TablesHandler) handleListTables(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req ListTablesRequest
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

	maxTables := req.MaxTables
	if maxTables <= 0 {
		maxTables = 100
	}
	// Cap to prevent uint32 overflow when used in uint32(maxTables*2)
	const maxTablesLimit = 1000
	if maxTables > maxTablesLimit {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "MaxTables exceeds maximum allowed value")
		return fmt.Errorf("invalid maxTables value: %d", maxTables)
	}

	// Pre-validate namespace before calling WithFilerClient to return 400 on validation errors
	var namespaceName string
	if len(req.Namespace) > 0 {
		var err error
		namespaceName, err = validateNamespace(req.Namespace)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
	}

	var tables []TableSummary
	var paginationToken string

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var err error
		accountID := h.getAccountID(r)

		if len(req.Namespace) > 0 {
			// Namespace has already been validated above
			namespacePath := getNamespacePath(bucketName, namespaceName)
			bucketPath := getTableBucketPath(bucketName)
			var nsMeta namespaceMetadata
			var bucketMeta tableBucketMetadata
			var namespacePolicy, bucketPolicy string
			bucketTags := map[string]string{}

			// Fetch namespace metadata and policy
			data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
			if err != nil {
				return err // Not Found handled by caller
			}
			if err := json.Unmarshal(data, &nsMeta); err != nil {
				return err
			}

			// Fetch namespace policy if it exists
			policyData, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyPolicy)
			if err == nil {
				namespacePolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch namespace policy: %v", err)
			}

			// Fetch bucket metadata and policy
			data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
			if err == nil {
				if err := json.Unmarshal(data, &bucketMeta); err != nil {
					return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
				}
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket metadata: %v", err)
			}

			policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
			if err == nil {
				bucketPolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket policy: %v", err)
			}
			if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
				return err
			} else if tags != nil {
				bucketTags = tags
			}

			bucketARN := h.generateTableBucketARN(bucketMeta.OwnerAccountID, bucketName)
			identityActions := getIdentityActions(r)
			nsAllowed := CheckPermissionWithContext("ListTables", accountID, nsMeta.OwnerAccountID, namespacePolicy, bucketARN, &PolicyContext{
				TableBucketName: bucketName,
				Namespace:       namespaceName,
				IdentityActions: identityActions,
			})
			bucketAllowed := CheckPermissionWithContext("ListTables", accountID, bucketMeta.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
				TableBucketName: bucketName,
				Namespace:       namespaceName,
				TableBucketTags: bucketTags,
				IdentityActions: identityActions,
			})
			if !nsAllowed && !bucketAllowed {
				return ErrAccessDenied
			}

			tables, paginationToken, err = h.listTablesInNamespaceWithClient(r, client, bucketName, namespaceName, req.Prefix, req.ContinuationToken, maxTables)
		} else {
			// List tables across all namespaces in bucket
			bucketPath := getTableBucketPath(bucketName)
			var bucketMeta tableBucketMetadata
			var bucketPolicy string
			bucketTags := map[string]string{}

			// Fetch bucket metadata and policy
			data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &bucketMeta); err != nil {
				return err
			}

			// Fetch bucket policy if it exists
			policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
			if err == nil {
				bucketPolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket policy: %v", err)
			}
			if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
				return err
			} else if tags != nil {
				bucketTags = tags
			}

			bucketARN := h.generateTableBucketARN(bucketMeta.OwnerAccountID, bucketName)
			identityActions := getIdentityActions(r)
			if !CheckPermissionWithContext("ListTables", accountID, bucketMeta.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
				TableBucketName: bucketName,
				TableBucketTags: bucketTags,
				IdentityActions: identityActions,
			}) {
				return ErrAccessDenied
			}

			tables, paginationToken, err = h.listTablesInAllNamespaces(r, client, bucketName, req.Prefix, req.ContinuationToken, maxTables)
		}
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// If the bucket or namespace directory is not found, return an empty result
			tables = []TableSummary{}
			paginationToken = ""
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "Access Denied")
			return err
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list tables: %v", err))
			return err
		}
	}

	resp := &ListTablesResponse{
		Tables:            tables,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// listTablesInNamespaceWithClient lists tables in a specific namespace
func (h *S3TablesHandler) listTablesInNamespaceWithClient(r *http.Request, client filer_pb.SeaweedFilerClient, bucketName, namespaceName, prefix, continuationToken string, maxTables int) ([]TableSummary, string, error) {
	namespacePath := getNamespacePath(bucketName, namespaceName)
	return h.listTablesWithClient(r, client, namespacePath, bucketName, namespaceName, prefix, continuationToken, maxTables)
}

func (h *S3TablesHandler) listTablesWithClient(r *http.Request, client filer_pb.SeaweedFilerClient, dirPath, bucketName, namespaceName, prefix, continuationToken string, maxTables int) ([]TableSummary, string, error) {
	var tables []TableSummary
	lastFileName := continuationToken
	ctx := r.Context()

	for len(tables) < maxTables {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dirPath,
			Limit:              uint32(maxTables * 2),
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

			// Skip the start item if it was included in the previous page
			if len(tables) == 0 && continuationToken != "" && entry.Entry.Name == continuationToken {
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
			if prefix != "" && !strings.HasPrefix(entry.Entry.Name, prefix) {
				continue
			}

			// Read table metadata from extended attribute
			data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
			if !ok {
				continue
			}

			var metadata tableMetadataInternal
			if err := json.Unmarshal(data, &metadata); err != nil {
				continue
			}

			// Note: Authorization (ownership or policy-based access) is checked at the handler level
			// before calling this function. This filter is removed to allow policy-based sharing.
			// The caller has already been verified to have ListTables permission for this namespace/bucket.

			tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+entry.Entry.Name)

			tables = append(tables, TableSummary{
				Name:       entry.Entry.Name,
				TableARN:   tableARN,
				Namespace:  []string{namespaceName},
				CreatedAt:  metadata.CreatedAt,
				ModifiedAt: metadata.ModifiedAt,
			})

			if len(tables) >= maxTables {
				return tables, lastFileName, nil
			}
		}

		if !hasMore {
			break
		}
	}

	if len(tables) < maxTables {
		lastFileName = ""
	}
	return tables, lastFileName, nil
}

func (h *S3TablesHandler) listTablesInAllNamespaces(r *http.Request, client filer_pb.SeaweedFilerClient, bucketName, prefix, continuationToken string, maxTables int) ([]TableSummary, string, error) {
	bucketPath := getTableBucketPath(bucketName)
	ctx := r.Context()

	var continuationNamespace string
	var startTableName string
	if continuationToken != "" {
		if parts := strings.SplitN(continuationToken, "/", 2); len(parts) == 2 {
			continuationNamespace = parts[0]
			startTableName = parts[1]
		} else {
			continuationNamespace = continuationToken
		}
	}

	var tables []TableSummary
	lastNamespace := continuationNamespace
	for {
		// List namespaces in batches
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          bucketPath,
			Limit:              100,
			StartFromFileName:  lastNamespace,
			InclusiveStartFrom: (lastNamespace == continuationNamespace && startTableName != "") || (lastNamespace == "" && continuationNamespace == ""),
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

			hasMore = true
			lastNamespace = entry.Entry.Name

			if !entry.Entry.IsDirectory || strings.HasPrefix(entry.Entry.Name, ".") {
				continue
			}

			namespace := entry.Entry.Name
			tableNameFilter := ""
			if namespace == continuationNamespace {
				tableNameFilter = startTableName
			}

			nsTables, nsToken, err := h.listTablesInNamespaceWithClient(r, client, bucketName, namespace, prefix, tableNameFilter, maxTables-len(tables))
			if err != nil {
				glog.Warningf("S3Tables: failed to list tables in namespace %s/%s: %v", bucketName, namespace, err)
				continue
			}

			tables = append(tables, nsTables...)

			if namespace == continuationNamespace {
				startTableName = ""
			}

			if len(tables) >= maxTables {
				paginationToken := namespace + "/" + nsToken
				if nsToken == "" {
					// If we hit the limit exactly at the end of a namespace, the next token should be the next namespace
					paginationToken = namespace // This will start from the NEXT namespace in the outer loop
				}
				return tables, paginationToken, nil
			}
		}

		if !hasMore {
			break
		}
	}

	return tables, "", nil
}

// handleDeleteTable deletes a table from a namespace
func (h *S3TablesHandler) handleDeleteTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req DeleteTableRequest
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

	// Check if table exists and enforce VersionToken if provided
	var metadata tableMetadataInternal
	var tablePolicy string
	var bucketPolicy string
	var bucketTags map[string]string
	var tableTags map[string]string
	var bucketMetadata tableBucketMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		if req.VersionToken != "" {
			if metadata.VersionToken != req.VersionToken {
				return ErrVersionTokenMismatch
			}
		}

		// Fetch table policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				// No table policy set; proceed with empty policy
			} else {
				return fmt.Errorf("failed to fetch table policy: %w", err)
			}
		} else {
			tablePolicy = string(policyData)
		}

		tableTags, err = h.readTags(r.Context(), client, tablePath)
		if err != nil {
			return err
		}

		bucketPath := getTableBucketPath(bucketName)
		data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %w", err)
		}
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err != nil {
			if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket policy: %w", err)
			}
		} else {
			bucketPolicy = string(policyData)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else if errors.Is(err, ErrVersionTokenMismatch) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "version token mismatch")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	tableAllowed := CheckPermissionWithContext("DeleteTable", principal, metadata.OwnerAccountID, tablePolicy, tableARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableName:       tableName,
		TableBucketTags: bucketTags,
		ResourceTags:    tableTags,
		IdentityActions: identityActions,
	})
	bucketAllowed := CheckPermissionWithContext("DeleteTable", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableName:       tableName,
		TableBucketTags: bucketTags,
		ResourceTags:    tableTags,
		IdentityActions: identityActions,
	})
	if !tableAllowed && !bucketAllowed {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table")
		return NewAuthError("DeleteTable", principal, "not authorized to delete table")
	}

	// Delete the table
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(r.Context(), client, tablePath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
