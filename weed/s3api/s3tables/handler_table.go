package s3tables

import (
	"context"
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
	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanCreateTable(principal, h.accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table")
		return NewAuthError("CreateTable", principal, "not authorized to create table")
	}

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
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", namespaceName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		}
		return err
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
		Name:         tableName,
		Namespace:    namespaceName,
		Format:       req.Format,
		CreatedAt:    now,
		ModifiedAt:   now,
		OwnerID:      h.accountID,
		VersionToken: versionToken,
		Schema:       req.Metadata,
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

	tableARN := h.generateTableARN(bucketName, namespaceName+"/"+tableName)

	resp := &CreateTableResponse{
		TableARN:     tableARN,
		VersionToken: versionToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleGetTable gets details of a table
func (h *S3TablesHandler) handleGetTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanGetTable(principal, h.accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to get table")
		return NewAuthError("GetTable", principal, "not authorized to get table")
	}

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
		return json.Unmarshal(data, &metadata)
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table: %v", err))
		}
		return err
	}

	tableARN := h.generateTableARN(bucketName, namespace+"/"+tableName)

	resp := &GetTableResponse{
		Name:             metadata.Name,
		TableARN:         tableARN,
		Namespace:        []string{metadata.Namespace},
		Format:           metadata.Format,
		CreatedAt:        metadata.CreatedAt,
		ModifiedAt:       metadata.ModifiedAt,
		OwnerAccountID:   metadata.OwnerID,
		MetadataLocation: metadata.MetadataLocation,
		VersionToken:     metadata.VersionToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListTables lists all tables in a namespace or bucket
func (h *S3TablesHandler) handleListTables(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanListTables(principal, h.accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to list tables")
		return NewAuthError("ListTables", principal, "not authorized to list tables")
	}

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

	var tables []TableSummary

	// If namespace is specified, list tables in that namespace only
	if len(req.Namespace) > 0 {
		namespaceName, err := validateNamespace(req.Namespace)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return h.listTablesInNamespaceWithClient(r.Context(), client, bucketName, namespaceName, req.Prefix, req.ContinuationToken, maxTables, &tables)
		})
	} else {
		// List tables in all namespaces
		err = h.listTablesInAllNamespaces(r.Context(), filerClient, bucketName, req.Prefix, req.ContinuationToken, maxTables, &tables)
	}

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list tables: %v", err))
		return err
	}

	paginationToken := ""
	if len(tables) >= maxTables && len(tables) > 0 {
		// This is tricky for cross-namespace listing. For now, we'll store the full path if possible,
		// but standard S3 tables usually lists within a namespace.
		// If we are listing within a namespace, lastFileName is just the table name.
		// If we are listing all namespaces, we might need a more complex token.
		// For simplicity, let's assume if we hit the limit, we return the last seen entry's path-related info.
		if len(req.Namespace) > 0 {
			paginationToken = tables[len(tables)-1].Name
		} else {
			// For all-namespaces listing, we'd need to encode the namespace too.
			// Let's use namespace/name as token.
			lastTable := tables[len(tables)-1]
			paginationToken = lastTable.Namespace[0] + "/" + lastTable.Name
		}
	}

	resp := &ListTablesResponse{
		Tables:            tables,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

func (h *S3TablesHandler) listTablesInNamespaceWithClient(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, namespace, prefix, continuationToken string, maxTables int, tables *[]TableSummary) error {
	namespacePath := getNamespacePath(bucketName, namespace)

	lastFileName := continuationToken
	for len(*tables) < maxTables {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          namespacePath,
			Limit:              uint32(maxTables * 2),
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: lastFileName == "" || lastFileName == continuationToken,
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
			if len(*tables) == 0 && continuationToken != "" && entry.Entry.Name == continuationToken {
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

			tableARN := h.generateTableARN(bucketName, namespace+"/"+entry.Entry.Name)

			*tables = append(*tables, TableSummary{
				Name:       metadata.Name,
				TableARN:   tableARN,
				Namespace:  []string{namespace},
				CreatedAt:  metadata.CreatedAt,
				ModifiedAt: metadata.ModifiedAt,
			})

			if len(*tables) >= maxTables {
				return nil
			}
		}

		if !hasMore {
			break
		}
	}

	return nil
}

func (h *S3TablesHandler) listTablesInAllNamespaces(ctx context.Context, filerClient FilerClient, bucketName, prefix, continuationToken string, maxTables int, tables *[]TableSummary) error {
	bucketPath := getTableBucketPath(bucketName)

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

	lastNamespace := continuationNamespace
	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for {
			// List namespaces in batches
			resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
				Directory:          bucketPath,
				Limit:              100,
				StartFromFileName:  lastNamespace,
				InclusiveStartFrom: lastNamespace == continuationNamespace && continuationNamespace != "" || lastNamespace == "",
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

				// Skip the start item if it was the continuation namespace but we already processed it
				// (handled by the startTableName clearing logic below)
				if lastNamespace == continuationNamespace && continuationNamespace != "" && entry.Entry.Name == continuationNamespace && startTableName == "" && len(*tables) > 0 {
					continue
				}

				hasMore = true
				lastNamespace = entry.Entry.Name

				if !entry.Entry.IsDirectory {
					continue
				}

				// Skip hidden entries
				if strings.HasPrefix(entry.Entry.Name, ".") {
					continue
				}

				namespace := entry.Entry.Name

				// List tables in this namespace
				tableNameFilter := ""
				if namespace == continuationNamespace {
					tableNameFilter = startTableName
				}

				if err := h.listTablesInNamespaceWithClient(ctx, client, bucketName, namespace, prefix, tableNameFilter, maxTables-len(*tables), tables); err != nil {
					glog.Warningf("S3Tables: failed to list tables in namespace %s/%s: %v", bucketName, namespace, err)
					continue
				}

				// Clear startTableName after the first matching namespace is processed
				if namespace == continuationNamespace {
					startTableName = ""
				}

				if len(*tables) >= maxTables {
					return nil
				}
			}

			if !hasMore {
				break
			}
		}

		return nil
	})
}

// handleDeleteTable deletes a table from a namespace
func (h *S3TablesHandler) handleDeleteTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanDeleteTable(principal, h.accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table")
		return NewAuthError("DeleteTable", principal, "not authorized to delete table")
	}

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

	// Check if table exists
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
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
