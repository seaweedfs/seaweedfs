package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

var errTableNameRequired = errors.New("table name is required")

func validateCreateTableRequest(req CreateTableRequest) error {
	if req.Name == "" {
		return errTableNameRequired
	}
	return nil
}

// handleListTables lists tables in a namespace.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	pageToken, pageSize, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	listReq := &s3tables.ListTablesRequest{
		TableBucketARN:    bucketARN,
		Namespace:         namespace,
		ContinuationToken: pageToken,
		MaxTables:         pageSize,
	}
	var listResp s3tables.ListTablesResponse

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListTables", listReq, &listResp, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: ListTables error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Convert to Iceberg format
	identifiers := make([]TableIdentifier, 0, len(listResp.Tables))
	for _, t := range listResp.Tables {
		identifiers = append(identifiers, TableIdentifier{
			Namespace: namespace,
			Name:      t.Name,
		})
	}

	result := ListTablesResponse{
		NextPageToken: listResp.ContinuationToken,
		Identifiers:   identifiers,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateTable creates a new table.
func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	if err := validateCreateTableRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}
	if req.StageCreate && !isStageCreateEnabled() {
		writeError(w, http.StatusNotImplemented, "NotImplementedException", "stage-create is disabled")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Generate UUID for the new table
	tableUUID := uuid.New()
	tablePath := path.Join(flattenNamespacePath(namespace), req.Name)
	location := strings.TrimSuffix(req.Location, "/")
	if location == "" {
		if req.Properties != nil {
			if warehouse := strings.TrimSuffix(req.Properties["warehouse"], "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, tablePath)
			}
		}
		if location == "" {
			if warehouse := strings.TrimSuffix(os.Getenv("ICEBERG_WAREHOUSE"), "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, tablePath)
			}
		}
		if location == "" {
			location = fmt.Sprintf("s3://%s/%s", bucketName, tablePath)
		}
	} else {
		parsedBucket, parsedPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid table location: "+err.Error())
			return
		}
		if parsedPath == "" {
			location = fmt.Sprintf("s3://%s/%s", parsedBucket, tablePath)
		}
	}

	// Build proper Iceberg table metadata using iceberg-go types
	metadata := newTableMetadata(tableUUID, location, req.Schema, req.PartitionSpec, req.WriteOrder, req.Properties)
	if metadata == nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build table metadata")
		return
	}

	// Serialize metadata to JSON
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: "+err.Error())
		return
	}

	tableName := req.Name
	metadataFileName := "v1.metadata.json" // Initial version is always 1
	metadataLocation := fmt.Sprintf("%s/metadata/%s", location, metadataFileName)
	metadataBucket, metadataPath, err := parseS3Location(location)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid table location: "+err.Error())
		return
	}

	// Authoritative existence check: ask the catalog whether a table is registered
	// at this name. If it is, short-circuit with the existing table (idempotent
	// CreateTable). Any leftover objects at the target path from a previous
	// lifecycle of the same name are purged by handleDropTable on the way out —
	// we deliberately do not clean storage here to keep CreateTable free of
	// destructive side effects. See issue #9074.
	existsReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var existsResp s3tables.GetTableResponse
	existsErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", existsReq, &existsResp, identityName)
	})
	if existsErr == nil {
		// Table already registered. Return the existing definition so CTAS/IF NOT
		// EXISTS flows see a stable response instead of a 409.
		result := buildLoadTableResult(existsResp, bucketName, namespace, tableName)
		writeJSON(w, http.StatusOK, result)
		return
	}
	if !isNoSuchTableError(existsErr) {
		glog.V(1).Infof("Iceberg: CreateTable existence check failed for %s.%s: %v", flattenNamespacePath(namespace), tableName, existsErr)
		writeError(w, http.StatusInternalServerError, "InternalServerError", existsErr.Error())
		return
	}

	// Stage-create persists metadata in the internal staged area and skips S3Tables registration.
	if req.StageCreate {
		stagedTablePath := stageCreateStagedTablePath(namespace, tableName, tableUUID)
		if err := s.saveMetadataFile(r.Context(), metadataBucket, stagedTablePath, metadataFileName, metadataBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save staged metadata file: "+err.Error())
			return
		}
		stagedMetadataLocation := fmt.Sprintf("s3://%s/%s/metadata/%s", metadataBucket, stagedTablePath, metadataFileName)
		if markerErr := s.writeStageCreateMarker(r.Context(), bucketName, namespace, tableName, tableUUID, location, stagedMetadataLocation); markerErr != nil {
			glog.V(1).Infof("Iceberg: failed to persist stage-create marker for %s.%s: %v", flattenNamespacePath(namespace), tableName, markerErr)
		}
		result := LoadTableResult{
			MetadataLocation: metadataLocation,
			Metadata:         metadata,
			Config:           make(iceberg.Properties),
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
		return
	}

	// Use S3 Tables manager to create table
	createReq := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
		Format:         "ICEBERG",
		Metadata: &s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{
				TableUUID: tableUUID.String(),
			},
			FullMetadata: metadataBytes,
		},
		MetadataLocation: metadataLocation,
		MetadataVersion:  1,
	}
	var createResp s3tables.CreateTableResponse

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateTable", createReq, &createResp, identityName)
	})

	if err != nil {
		if tableErr, ok := err.(*s3tables.S3TablesError); ok && tableErr.Type == s3tables.ErrCodeTableAlreadyExists {
			getReq := &s3tables.GetTableRequest{
				TableBucketARN: bucketARN,
				Namespace:      namespace,
				Name:           tableName,
			}
			var getResp s3tables.GetTableResponse
			getErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				mgrClient := s3tables.NewManagerClient(client)
				return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
			})
			if getErr != nil {
				writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
				return
			}
			result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
			writeJSON(w, http.StatusOK, result)
			return
		}
		if strings.Contains(err.Error(), "already exists") {
			getReq := &s3tables.GetTableRequest{
				TableBucketARN: bucketARN,
				Namespace:      namespace,
				Name:           tableName,
			}
			var getResp s3tables.GetTableResponse
			getErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				mgrClient := s3tables.NewManagerClient(client)
				return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
			})
			if getErr != nil {
				writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
				return
			}
			result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
			writeJSON(w, http.StatusOK, result)
			return
		}
		glog.V(1).Infof("Iceberg: CreateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Use returned location if available, otherwise fallback to local one
	finalLocation := createResp.MetadataLocation
	if finalLocation == "" {
		finalLocation = metadataLocation
	}
	if markerErr := s.deleteStageCreateMarkers(r.Context(), bucketName, namespace, tableName); markerErr != nil {
		glog.V(1).Infof("Iceberg: failed to cleanup stage-create markers for %s.%s after create: %v", flattenNamespacePath(namespace), tableName, markerErr)
	}

	result := LoadTableResult{
		MetadataLocation: finalLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
	writeJSON(w, http.StatusOK, result)
}

// handleLoadTable loads table metadata.
func (s *Server) handleLoadTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
	})

	if err != nil {

		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: LoadTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
	writeJSON(w, http.StatusOK, result)
}

func buildLoadTableResult(getResp s3tables.GetTableResponse, bucketName string, namespace []string, tableName string) LoadTableResult {
	location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
	if location == "" {
		location = fmt.Sprintf("s3://%s/%s", bucketName, path.Join(flattenNamespacePath(namespace), tableName))
	}
	tableUUID := uuid.Nil
	if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
		if parsed, err := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); err == nil {
			tableUUID = parsed
		}
	}
	// Use Nil UUID if not found in storage (legacy table)
	// Stability is guaranteed by not generating random UUIDs on read

	var metadata table.Metadata
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		var err error
		metadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
		if err != nil {
			glog.Warningf("Iceberg: Failed to parse persisted metadata for %s: %v", tableName, err)
			// Attempt to reconstruct from IcebergMetadata if available, otherwise synthetic
			// TODO: Extract schema/spec from getResp.Metadata.Iceberg if FullMetadata fails but partial info exists?
			// For now, fallback to empty metadata
			metadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
		}
	} else {
		// No full metadata, create synthetic
		// TODO: If we had stored schema in IcebergMetadata, we would pass it here
		metadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
	}

	return LoadTableResult{
		MetadataLocation: getResp.MetadataLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
}

// handleTableExists checks if a table exists.
func (s *Server) handleTableExists(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
	})

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropTable deletes a table.
func (s *Server) handleDropTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Resolve the table's storage location from the catalog before the
	// delete, so we can purge data files afterwards. The catalog is the
	// authoritative owner of this mapping — if the table is not registered,
	// there is nothing to clean up and DeleteTable will return NoSuchTable.
	var storedMetadataLocation string
	lookupReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var lookupResp s3tables.GetTableResponse
	lookupErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", lookupReq, &lookupResp, identityName)
	})
	if lookupErr == nil {
		storedMetadataLocation = lookupResp.MetadataLocation
	}

	deleteReq := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteTable", deleteReq, nil, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: DropTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Purge data files that lived under the dropped table's location, so a
	// subsequent CREATE TABLE at the same name (issue #9074) does not trip
	// Trino's "non-empty location" pre-check. We only purge when the catalog
	// told us a location: if GetTable above failed, there is no mapping to
	// trust and we leave storage alone.
	if storedMetadataLocation != "" {
		if tableLoc := tableLocationFromMetadataLocation(storedMetadataLocation); tableLoc != "" {
			if dataBucket, dataPath, parseErr := parseS3Location(tableLoc); parseErr == nil {
				if cleanupErr := s.cleanupStaleTableLocation(r.Context(), dataBucket, dataPath); cleanupErr != nil {
					glog.V(1).Infof("Iceberg: failed to purge dropped table location s3://%s/%s: %v", dataBucket, dataPath, cleanupErr)
				}
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// isNoSuchTableError reports whether an error from the S3 Tables manager
// indicates the target table is not registered in the catalog.
func isNoSuchTableError(err error) bool {
	if err == nil {
		return false
	}
	var tableErr *s3tables.S3TablesError
	if errors.As(err, &tableErr) {
		return tableErr.Type == s3tables.ErrCodeNoSuchTable || tableErr.Type == s3tables.ErrCodeNoSuchNamespace
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no such table")
}

// cleanupStaleTableLocation purges any existing filer entry at the target
// table path inside the regular S3 bucket so that engines which verify an
// empty location (e.g. Trino CTAS) can proceed after a DROP. Callers must
// have confirmed via the catalog that no table is registered at this name —
// live tables must never be touched by this helper. Missing paths are not
// an error.
//
// The tablePath is validated to contain only safe, relative segments before
// being joined with the bucket prefix, so a maliciously crafted location
// (e.g. containing "..") cannot escape the bucket subtree and delete data
// outside of it.
func (s *Server) cleanupStaleTableLocation(ctx context.Context, bucketName, tablePath string) error {
	tablePath = strings.Trim(tablePath, "/")
	if bucketName == "" || tablePath == "" {
		return nil
	}
	// Reject absolute paths and any traversal segments. path.Clean would
	// silently collapse "foo/../bar" into "bar", masking an attempted
	// escape, so we check each raw segment explicitly.
	for _, segment := range strings.Split(tablePath, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.ContainsAny(segment, `\`) {
			return fmt.Errorf("cleanupStaleTableLocation: refusing unsafe table path %q", tablePath)
		}
	}
	parentDir := path.Join(s3_constants.DefaultBucketsPath, bucketName, path.Dir(tablePath))
	name := path.Base(tablePath)
	if name == "" || name == "." || name == ".." || name == "/" {
		return fmt.Errorf("cleanupStaleTableLocation: refusing unsafe table path %q", tablePath)
	}
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.DoRemove(ctx, client, parentDir, name, true, true, true, false, nil)
		if err == nil || errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		return err
	})
}

// newTableMetadata creates a new table.Metadata object with the given parameters.
// Uses iceberg-go's MetadataBuilder pattern for proper spec compliance.
func newTableMetadata(
	tableUUID uuid.UUID,
	location string,
	schema *iceberg.Schema,
	partitionSpec *iceberg.PartitionSpec,
	sortOrder *table.SortOrder,
	props iceberg.Properties,
) table.Metadata {
	// Add schema - use provided or create empty schema
	var s *iceberg.Schema
	if schema != nil {
		s = schema
	} else {
		s = iceberg.NewSchema(0)
	}

	// Add partition spec
	var pSpec *iceberg.PartitionSpec
	if partitionSpec != nil {
		pSpec = partitionSpec
	} else {
		unpartitioned := iceberg.NewPartitionSpecID(0)
		pSpec = &unpartitioned
	}

	// Add sort order
	var so table.SortOrder
	if sortOrder != nil {
		so = *sortOrder
	} else {
		so = table.UnsortedSortOrder
	}

	// Create properties map if nil
	if props == nil {
		props = make(iceberg.Properties)
	}

	// Create metadata directly using the constructor which ensures spec compliance for V2
	metadata, err := table.NewMetadataWithUUID(s, pSpec, so, location, props, tableUUID)
	if err != nil {
		glog.Errorf("Failed to create metadata: %v", err)
		return nil
	}

	return metadata
}
