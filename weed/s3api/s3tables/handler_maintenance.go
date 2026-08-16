package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Maintenance types are scoped: AWS configures unreferenced file removal on the
// table bucket and compaction and snapshot management on the table.
var (
	bucketMaintenanceTypes = map[string]bool{
		MaintenanceTypeIcebergUnreferencedFileRemoval: true,
	}
	tableMaintenanceTypes = map[string]bool{
		MaintenanceTypeIcebergCompaction:         true,
		MaintenanceTypeIcebergSnapshotManagement: true,
	}
)

// handlePutTableBucketMaintenanceConfiguration sets a maintenance configuration on a table bucket
func (h *S3TablesHandler) handlePutTableBucketMaintenanceConfiguration(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req PutTableBucketMaintenanceConfigurationRequest
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

	if err := validateMaintenanceValue(req.Type, bucketMaintenanceTypes, req.Value); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if _, err := h.authorizeMaintenanceBucket(r, filerClient, "PutTableBucketMaintenanceConfiguration", bucketName); err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return err
	}

	if err := h.putMaintenanceConfiguration(r, filerClient, GetTableBucketPath(bucketName), req.Type, req.Value); err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handleGetTableBucketMaintenanceConfiguration gets the maintenance configuration of a table bucket
func (h *S3TablesHandler) handleGetTableBucketMaintenanceConfiguration(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetTableBucketMaintenanceConfigurationRequest
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

	bucketARN, err := h.authorizeMaintenanceBucket(r, filerClient, "GetTableBucketMaintenanceConfiguration", bucketName)
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return err
	}

	config, err := h.readMaintenanceConfiguration(r, filerClient, GetTableBucketPath(bucketName))
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return err
	}

	h.writeJSON(w, http.StatusOK, &GetTableBucketMaintenanceConfigurationResponse{
		TableBucketARN: bucketARN,
		Configuration:  config,
	})
	return nil
}

// handlePutTableMaintenanceConfiguration sets a maintenance configuration on a table
func (h *S3TablesHandler) handlePutTableMaintenanceConfiguration(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req PutTableMaintenanceConfigurationRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, namespaceName, tableName, err := h.parseTableTarget(req.TableBucketARN, req.Namespace, req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if err := validateMaintenanceValue(req.Type, tableMaintenanceTypes, req.Value); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if _, err := h.authorizeMaintenanceTable(r, filerClient, "PutTableMaintenanceConfiguration", bucketName, namespaceName, tableName); err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)
	if err := h.putMaintenanceConfiguration(r, filerClient, tablePath, req.Type, req.Value); err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// handleGetTableMaintenanceConfiguration gets the maintenance configuration of a table
func (h *S3TablesHandler) handleGetTableMaintenanceConfiguration(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetTableMaintenanceConfigurationRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, namespaceName, tableName, err := h.parseTableTarget(req.TableBucketARN, req.Namespace, req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tableARN, err := h.authorizeMaintenanceTable(r, filerClient, "GetTableMaintenanceConfiguration", bucketName, namespaceName, tableName)
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)
	config, err := h.readMaintenanceConfiguration(r, filerClient, tablePath)
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	h.writeJSON(w, http.StatusOK, &GetTableMaintenanceConfigurationResponse{
		TableARN:      tableARN,
		Namespace:     []string{namespaceName},
		Name:          tableName,
		Configuration: config,
	})
	return nil
}

// handleGetTableMaintenanceJobStatus reports the outcome of the last maintenance run on a table
func (h *S3TablesHandler) handleGetTableMaintenanceJobStatus(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetTableMaintenanceJobStatusRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, namespaceName, tableName, err := h.parseTableTarget(req.TableBucketARN, req.Namespace, req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tableARN, err := h.authorizeMaintenanceTable(r, filerClient, "GetTableMaintenanceJobStatus", bucketName, namespaceName, tableName)
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)

	config, err := h.readMaintenanceConfiguration(r, filerClient, tablePath)
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	recorded := MaintenanceJobStatus{}
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMaintenanceStatus)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				return nil
			}
			return err
		}
		return json.Unmarshal(data, &recorded)
	})
	if err != nil {
		h.writeMaintenanceError(w, err, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	h.writeJSON(w, http.StatusOK, &GetTableMaintenanceJobStatusResponse{
		TableARN: tableARN,
		Status:   buildJobStatusResponse(recorded, config),
	})
	return nil
}

// buildJobStatusResponse reports every job type: what the worker recorded, or
// Disabled when the configuration switched the type off, or Not_Yet_Run.
func buildJobStatusResponse(recorded MaintenanceJobStatus, config MaintenanceConfiguration) MaintenanceJobStatus {
	jobTypes := []string{
		MaintenanceTypeIcebergCompaction,
		MaintenanceTypeIcebergSnapshotManagement,
		MaintenanceTypeIcebergUnreferencedFileRemoval,
	}

	status := make(MaintenanceJobStatus, len(jobTypes))
	for _, jobType := range jobTypes {
		if value, ok := config[jobType]; ok && value != nil && value.Status == MaintenanceStatusDisabled {
			status[jobType] = &MaintenanceJobStatusValue{Status: MaintenanceJobStatusDisabled}
			continue
		}
		if value, ok := recorded[jobType]; ok && value != nil {
			status[jobType] = value
			continue
		}
		status[jobType] = &MaintenanceJobStatusValue{Status: MaintenanceJobStatusNotYetRun}
	}
	return status
}

// putMaintenanceConfiguration merges one maintenance type into the stored
// configuration, leaving the other types alone.
func (h *S3TablesHandler) putMaintenanceConfiguration(
	r *http.Request,
	filerClient FilerClient,
	resourcePath, maintenanceType string,
	value *MaintenanceConfigurationValue,
) error {
	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.updateExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMaintenance, func(current []byte) ([]byte, error) {
			config := MaintenanceConfiguration{}
			if len(current) > 0 {
				if err := json.Unmarshal(current, &config); err != nil {
					return nil, fmt.Errorf("failed to unmarshal maintenance configuration: %w", err)
				}
			}
			config[maintenanceType] = value
			return json.Marshal(config)
		})
	})
}

// readMaintenanceConfiguration returns the stored configuration, or an empty
// one when the resource has never been configured.
func (h *S3TablesHandler) readMaintenanceConfiguration(r *http.Request, filerClient FilerClient, resourcePath string) (MaintenanceConfiguration, error) {
	config := MaintenanceConfiguration{}

	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, resourcePath, ExtendedKeyMaintenance)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				return nil
			}
			return err
		}
		return json.Unmarshal(data, &config)
	})
	if err != nil {
		return nil, err
	}

	return config, nil
}

// parseTableTarget validates the bucket/namespace/table triple every
// table-scoped maintenance request carries.
func (h *S3TablesHandler) parseTableTarget(tableBucketARN string, namespace []string, name string) (bucketName, namespaceName, tableName string, err error) {
	if tableBucketARN == "" || len(namespace) == 0 || name == "" {
		return "", "", "", fmt.Errorf("tableBucketARN, namespace, and name are required")
	}
	if namespaceName, err = validateNamespace(namespace); err != nil {
		return "", "", "", err
	}
	if bucketName, err = parseBucketNameFromARN(tableBucketARN); err != nil {
		return "", "", "", err
	}
	if tableName, err = validateTableName(name); err != nil {
		return "", "", "", err
	}
	return bucketName, namespaceName, tableName, nil
}

// authorizeMaintenanceBucket checks the caller may perform operation on the
// table bucket, returning the bucket ARN.
func (h *S3TablesHandler) authorizeMaintenanceBucket(r *http.Request, filerClient FilerClient, operation, bucketName string) (string, error) {
	bucketPath := GetTableBucketPath(bucketName)

	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}
		bucketPolicy, err = h.readBucketPolicy(r, client, bucketPath)
		return err
	})
	if err != nil {
		return "", err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	if !CheckPermissionWithContext(operation, principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TableBucketName: bucketName,
		IdentityActions: getIdentityActions(r),
		DefaultAllow:    h.defaultAllowFor(r),
	}) {
		return "", NewAuthError(operation, principal, "not authorized to "+operation)
	}

	return bucketARN, nil
}

// authorizeMaintenanceTable checks the caller may perform operation on the
// table, returning the table ARN.
func (h *S3TablesHandler) authorizeMaintenanceTable(r *http.Request, filerClient FilerClient, operation, bucketName, namespaceName, tableName string) (string, error) {
	tablePath := GetTablePath(bucketName, namespaceName, tableName)
	bucketPath := GetTableBucketPath(bucketName)

	var metadata tableMetadataInternal
	var bucketPolicy string
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}
		bucketPolicy, err = h.readBucketPolicy(r, client, bucketPath)
		return err
	})
	if err != nil {
		return "", err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	principal := h.getAccountID(r)
	if !CheckPermissionWithContext(operation, principal, metadata.OwnerAccountID, bucketPolicy, tableARN, &PolicyContext{
		TableBucketName: bucketName,
		Namespace:       namespaceName,
		TableName:       tableName,
		IdentityActions: getIdentityActions(r),
		DefaultAllow:    h.defaultAllowFor(r),
	}) {
		return "", NewAuthError(operation, principal, "not authorized to "+operation)
	}

	return tableARN, nil
}

// readBucketPolicy returns the bucket policy, or an empty string when the
// bucket has none.
func (h *S3TablesHandler) readBucketPolicy(r *http.Request, client filer_pb.SeaweedFilerClient, bucketPath string) (string, error) {
	data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read bucket policy: %w", err)
	}
	return string(data), nil
}

// writeMaintenanceError maps the shared failure modes of the maintenance
// handlers onto responses.
func (h *S3TablesHandler) writeMaintenanceError(w http.ResponseWriter, err error, notFoundCode, notFoundMessage string) {
	switch {
	case errors.Is(err, filer_pb.ErrNotFound):
		h.writeError(w, http.StatusNotFound, notFoundCode, notFoundMessage)
	case isAuthError(err):
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, err.Error())
	case errors.Is(err, ErrConcurrentUpdate):
		h.writeError(w, http.StatusConflict, ErrCodeConflict, "maintenance configuration changed concurrently, retry the request")
	default:
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
	}
}

// validateMaintenanceValue rejects a type that does not belong to the scope and
// settings that do not name that same type.
func validateMaintenanceValue(maintenanceType string, allowed map[string]bool, value *MaintenanceConfigurationValue) error {
	if maintenanceType == "" {
		return fmt.Errorf("type is required")
	}
	if !allowed[maintenanceType] {
		return fmt.Errorf("unsupported maintenance type %q", maintenanceType)
	}
	if value == nil {
		return fmt.Errorf("value is required")
	}

	switch value.Status {
	case MaintenanceStatusEnabled, MaintenanceStatusDisabled:
	case "":
		return fmt.Errorf("value.status is required")
	default:
		return fmt.Errorf("invalid value.status %q, expected %q or %q", value.Status, MaintenanceStatusEnabled, MaintenanceStatusDisabled)
	}

	if value.Settings != nil && !settingsMatchType(maintenanceType, value.Settings) {
		return fmt.Errorf("value.settings must only contain %s", maintenanceType)
	}
	if value.Settings != nil && value.Settings.IcebergCompaction != nil {
		if err := validateCompactionStrategy(value.Settings.IcebergCompaction.Strategy); err != nil {
			return err
		}
	}
	return nil
}

// validateCompactionStrategy rejects a strategy the maintenance worker cannot
// carry out, rather than accepting it and quietly compacting some other way.
func validateCompactionStrategy(strategy string) error {
	switch strategy {
	case "", CompactionStrategyAuto, CompactionStrategyBinpack, CompactionStrategySort:
		return nil
	case CompactionStrategyZOrder:
		return fmt.Errorf("compaction strategy %q is not supported", strategy)
	default:
		return fmt.Errorf("invalid compaction strategy %q, expected one of %q, %q, %q",
			strategy, CompactionStrategyAuto, CompactionStrategyBinpack, CompactionStrategySort)
	}
}

func settingsMatchType(maintenanceType string, settings *MaintenanceSettings) bool {
	switch maintenanceType {
	case MaintenanceTypeIcebergCompaction:
		return settings.IcebergSnapshotManagement == nil && settings.IcebergUnreferencedFileRemoval == nil
	case MaintenanceTypeIcebergSnapshotManagement:
		return settings.IcebergCompaction == nil && settings.IcebergUnreferencedFileRemoval == nil
	case MaintenanceTypeIcebergUnreferencedFileRemoval:
		return settings.IcebergCompaction == nil && settings.IcebergSnapshotManagement == nil
	}
	return false
}
