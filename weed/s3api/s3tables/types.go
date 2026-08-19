package s3tables

import (
	"encoding/json"
	"strings"
	"time"
)

// Table bucket types

type TableBucket struct {
	ARN            string    `json:"arn"`
	Name           string    `json:"name"`
	OwnerAccountID string    `json:"ownerAccountId"`
	CreatedAt      time.Time `json:"createdAt"`
	Format         string    `json:"format,omitempty"`
}

type CreateTableBucketRequest struct {
	Name string            `json:"name"`
	Tags map[string]string `json:"tags,omitempty"`
	// Format is the table format this bucket holds. A bucket is a catalog and a
	// catalog serves one protocol, so declaring it here is what lets a caller be
	// told where to connect. Empty means ICEBERG, which is what AWS S3 Tables
	// serves and therefore what an SDK that has never heard of this field means.
	Format string `json:"format,omitempty"`
}

type CreateTableBucketResponse struct {
	ARN string `json:"arn"`
}

type GetTableBucketRequest struct {
	TableBucketARN string `json:"tableBucketARN"`
}

type GetTableBucketResponse struct {
	ARN            string    `json:"arn"`
	Name           string    `json:"name"`
	OwnerAccountID string    `json:"ownerAccountId"`
	CreatedAt      time.Time `json:"createdAt"`
	// Format is empty for a bucket created before formats were declared. Such a
	// bucket accepts any format, which is what it did when it was made.
	Format string `json:"format,omitempty"`
}

type ListTableBucketsRequest struct {
	Prefix            string `json:"prefix,omitempty"`
	ContinuationToken string `json:"continuationToken,omitempty"`
	MaxBuckets        int    `json:"maxBuckets,omitempty"`
}

type TableBucketSummary struct {
	ARN       string    `json:"arn"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	Format    string    `json:"format,omitempty"`
}

type ListTableBucketsResponse struct {
	TableBuckets      []TableBucketSummary `json:"tableBuckets"`
	ContinuationToken string               `json:"continuationToken,omitempty"`
}

type DeleteTableBucketRequest struct {
	TableBucketARN string `json:"tableBucketARN"`
}

// Table bucket policy types

type PutTableBucketPolicyRequest struct {
	TableBucketARN string `json:"tableBucketARN"`
	ResourcePolicy string `json:"resourcePolicy"`
}

type GetTableBucketPolicyRequest struct {
	TableBucketARN string `json:"tableBucketARN"`
}

type GetTableBucketPolicyResponse struct {
	ResourcePolicy string `json:"resourcePolicy"`
}

type DeleteTableBucketPolicyRequest struct {
	TableBucketARN string `json:"tableBucketARN"`
}

// Namespace types

type Namespace struct {
	Namespace      []string          `json:"namespace"`
	CreatedAt      time.Time         `json:"createdAt"`
	OwnerAccountID string            `json:"ownerAccountId"`
	Properties     map[string]string `json:"properties,omitempty"`
}

type CreateNamespaceRequest struct {
	TableBucketARN string            `json:"tableBucketARN"`
	Namespace      []string          `json:"namespace"`
	Properties     map[string]string `json:"properties,omitempty"`
}

type CreateNamespaceResponse struct {
	Namespace      []string          `json:"namespace"`
	TableBucketARN string            `json:"tableBucketARN"`
	Properties     map[string]string `json:"properties,omitempty"`
}

type GetNamespaceRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
}

type UpdateNamespaceRequest struct {
	TableBucketARN string            `json:"tableBucketARN"`
	Namespace      []string          `json:"namespace"`
	Properties     map[string]string `json:"properties,omitempty"`
}

type UpdateNamespaceResponse struct {
	Namespace      []string          `json:"namespace"`
	TableBucketARN string            `json:"tableBucketARN"`
	Properties     map[string]string `json:"properties,omitempty"`
}

type GetNamespaceResponse struct {
	Namespace      []string          `json:"namespace"`
	CreatedAt      time.Time         `json:"createdAt"`
	OwnerAccountID string            `json:"ownerAccountId"`
	Properties     map[string]string `json:"properties,omitempty"`
}

type ListNamespacesRequest struct {
	TableBucketARN    string `json:"tableBucketARN"`
	Prefix            string `json:"prefix,omitempty"`
	ContinuationToken string `json:"continuationToken,omitempty"`
	MaxNamespaces     int    `json:"maxNamespaces,omitempty"`
}

type NamespaceSummary struct {
	Namespace []string  `json:"namespace"`
	CreatedAt time.Time `json:"createdAt"`
}

type ListNamespacesResponse struct {
	Namespaces        []NamespaceSummary `json:"namespaces"`
	ContinuationToken string             `json:"continuationToken,omitempty"`
}

type DeleteNamespaceRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
}

// Table types

type IcebergSchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
}

type IcebergSchema struct {
	Fields []IcebergSchemaField `json:"fields"`
}

type IcebergMetadata struct {
	Schema    IcebergSchema `json:"schema"`
	TableUUID string        `json:"tableUuid,omitempty"`
}

type TableMetadata struct {
	Iceberg      *IcebergMetadata `json:"iceberg,omitempty"`
	FullMetadata json.RawMessage  `json:"fullMetadata,omitempty"`
}

type Table struct {
	Name             string         `json:"name"`
	TableARN         string         `json:"tableARN"`
	Namespace        []string       `json:"namespace"`
	Format           string         `json:"format"`
	CreatedAt        time.Time      `json:"createdAt"`
	ModifiedAt       time.Time      `json:"modifiedAt"`
	OwnerAccountID   string         `json:"ownerAccountId"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
}

type CreateTableRequest struct {
	TableBucketARN   string            `json:"tableBucketARN"`
	Namespace        []string          `json:"namespace"`
	Name             string            `json:"name"`
	Format           string            `json:"format"`
	Metadata         *TableMetadata    `json:"metadata,omitempty"`
	MetadataVersion  int               `json:"metadataVersion,omitempty"`
	MetadataLocation string            `json:"metadataLocation,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

type CreateTableResponse struct {
	TableARN         string `json:"tableARN"`
	VersionToken     string `json:"versionToken"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
}

type RegisterTableRequest struct {
	TableBucketARN   string   `json:"tableBucketARN"`
	Namespace        []string `json:"namespace"`
	Name             string   `json:"name"`
	MetadataLocation string   `json:"metadataLocation"`
}

type RegisterTableResponse struct {
	TableARN         string `json:"tableARN"`
	VersionToken     string `json:"versionToken"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
}

type GetTableRequest struct {
	TableBucketARN string   `json:"tableBucketARN,omitempty"`
	Namespace      []string `json:"namespace,omitempty"`
	Name           string   `json:"name,omitempty"`
	TableARN       string   `json:"tableARN,omitempty"`
}

type GetTableResponse struct {
	Name             string         `json:"name"`
	TableARN         string         `json:"tableARN"`
	Namespace        []string       `json:"namespace"`
	Format           string         `json:"format"`
	CreatedAt        time.Time      `json:"createdAt"`
	ModifiedAt       time.Time      `json:"modifiedAt"`
	OwnerAccountID   string         `json:"ownerAccountId"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
	VersionToken     string         `json:"versionToken"`
	MetadataVersion  int            `json:"metadataVersion"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
}

type ListTablesRequest struct {
	TableBucketARN    string   `json:"tableBucketARN"`
	Namespace         []string `json:"namespace,omitempty"`
	Prefix            string   `json:"prefix,omitempty"`
	ContinuationToken string   `json:"continuationToken,omitempty"`
	MaxTables         int      `json:"maxTables,omitempty"`
}

type TableSummary struct {
	Name      string   `json:"name"`
	TableARN  string   `json:"tableARN"`
	Namespace []string `json:"namespace"`
	// Format lets a caller tell an Iceberg table from a catalog-only one without
	// a GetTable per row. AWS omits it; listing a mixed catalog needs it.
	Format           string    `json:"format,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
	ModifiedAt       time.Time `json:"modifiedAt"`
	MetadataLocation string    `json:"metadataLocation,omitempty"`
}

type ListTablesResponse struct {
	Tables            []TableSummary `json:"tables"`
	ContinuationToken string         `json:"continuationToken,omitempty"`
}

type DeleteTableRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
	VersionToken   string   `json:"versionToken,omitempty"`
}

type UpdateTableRequest struct {
	TableBucketARN   string         `json:"tableBucketARN"`
	Namespace        []string       `json:"namespace"`
	Name             string         `json:"name"`
	VersionToken     string         `json:"versionToken,omitempty"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
	MetadataVersion  int            `json:"metadataVersion,omitempty"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
}

type UpdateTableResponse struct {
	TableARN         string `json:"tableARN"`
	VersionToken     string `json:"versionToken"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
}

type RenameTableRequest struct {
	TableBucketARN  string   `json:"tableBucketARN"`
	SourceNamespace []string `json:"sourceNamespace"`
	SourceName      string   `json:"sourceName"`
	DestNamespace   []string `json:"destNamespace"`
	DestName        string   `json:"destName"`
}

type RenameTableResponse struct {
	TableARN         string `json:"tableARN"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
}

// View types
//
// Views are stored exactly like tables (a filer directory carrying a metadata
// pointer xattr) but tagged with ExtendedKeyEntryType="view". They reuse
// TableMetadata for the metadata pointer.

type CreateViewRequest struct {
	TableBucketARN   string         `json:"tableBucketARN"`
	Namespace        []string       `json:"namespace"`
	Name             string         `json:"name"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
	MetadataVersion  int            `json:"metadataVersion,omitempty"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
}

type CreateViewResponse struct {
	ViewARN          string `json:"viewARN"`
	VersionToken     string `json:"versionToken"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
}

type GetViewRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
}

type GetViewResponse struct {
	Name             string         `json:"name"`
	ViewARN          string         `json:"viewARN"`
	Namespace        []string       `json:"namespace"`
	CreatedAt        time.Time      `json:"createdAt"`
	ModifiedAt       time.Time      `json:"modifiedAt"`
	OwnerAccountID   string         `json:"ownerAccountId"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
	VersionToken     string         `json:"versionToken"`
	MetadataVersion  int            `json:"metadataVersion"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
}

type ListViewsRequest struct {
	TableBucketARN    string   `json:"tableBucketARN"`
	Namespace         []string `json:"namespace,omitempty"`
	Prefix            string   `json:"prefix,omitempty"`
	ContinuationToken string   `json:"continuationToken,omitempty"`
	MaxViews          int      `json:"maxViews,omitempty"`
}

type ViewSummary struct {
	Name             string    `json:"name"`
	ViewARN          string    `json:"viewARN"`
	Namespace        []string  `json:"namespace"`
	CreatedAt        time.Time `json:"createdAt"`
	ModifiedAt       time.Time `json:"modifiedAt"`
	MetadataLocation string    `json:"metadataLocation,omitempty"`
}

type ListViewsResponse struct {
	Views             []ViewSummary `json:"views"`
	ContinuationToken string        `json:"continuationToken,omitempty"`
}

type UpdateViewRequest struct {
	TableBucketARN   string         `json:"tableBucketARN"`
	Namespace        []string       `json:"namespace"`
	Name             string         `json:"name"`
	VersionToken     string         `json:"versionToken,omitempty"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
	MetadataVersion  int            `json:"metadataVersion,omitempty"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
}

type UpdateViewResponse struct {
	ViewARN          string `json:"viewARN"`
	VersionToken     string `json:"versionToken"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
}

type DeleteViewRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
	VersionToken   string   `json:"versionToken,omitempty"`
}

// Table policy types

type PutTablePolicyRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
	ResourcePolicy string   `json:"resourcePolicy"`
}

type GetTablePolicyRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
}

type GetTablePolicyResponse struct {
	ResourcePolicy string `json:"resourcePolicy"`
}

type DeleteTablePolicyRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
}

// Maintenance configuration types

const (
	MaintenanceTypeIcebergCompaction              = "icebergCompaction"
	MaintenanceTypeIcebergSnapshotManagement      = "icebergSnapshotManagement"
	MaintenanceTypeIcebergUnreferencedFileRemoval = "icebergUnreferencedFileRemoval"

	MaintenanceStatusEnabled  = "enabled"
	MaintenanceStatusDisabled = "disabled"
)

type IcebergCompactionSettings struct {
	Strategy         string `json:"strategy,omitempty"`
	TargetFileSizeMB *int64 `json:"targetFileSizeMB,omitempty"`
}

// Compaction strategies. AWS also defines z-order, which the maintenance
// worker cannot produce, so it is rejected rather than silently binpacked.
const (
	CompactionStrategyAuto    = "auto"
	CompactionStrategyBinpack = "binpack"
	CompactionStrategySort    = "sort"
	CompactionStrategyZOrder  = "z-order"
)

type IcebergSnapshotManagementSettings struct {
	MinSnapshotsToKeep  *int64 `json:"minSnapshotsToKeep,omitempty"`
	MaxSnapshotAgeHours *int64 `json:"maxSnapshotAgeHours,omitempty"`
}

type IcebergUnreferencedFileRemovalSettings struct {
	UnreferencedDays *int64 `json:"unreferencedDays,omitempty"`
	NonCurrentDays   *int64 `json:"nonCurrentDays,omitempty"`
}

type MaintenanceSettings struct {
	IcebergCompaction              *IcebergCompactionSettings              `json:"icebergCompaction,omitempty"`
	IcebergSnapshotManagement      *IcebergSnapshotManagementSettings      `json:"icebergSnapshotManagement,omitempty"`
	IcebergUnreferencedFileRemoval *IcebergUnreferencedFileRemovalSettings `json:"icebergUnreferencedFileRemoval,omitempty"`
}

type MaintenanceConfigurationValue struct {
	Status   string               `json:"status,omitempty"`
	Settings *MaintenanceSettings `json:"settings,omitempty"`
}

// MaintenanceConfiguration maps a maintenance type to its configuration. It is
// stored verbatim as the maintenance extended attribute so Get is a passthrough.
type MaintenanceConfiguration map[string]*MaintenanceConfigurationValue

// MergeMaintenanceConfiguration overlays a table's configuration on its
// bucket's. Unreferenced file removal is only configurable on the bucket, so
// anything reading a table's effective configuration has to consult both.
func MergeMaintenanceConfiguration(bucket, table MaintenanceConfiguration) MaintenanceConfiguration {
	if len(bucket) == 0 {
		return table
	}
	if len(table) == 0 {
		return bucket
	}

	merged := make(MaintenanceConfiguration, len(bucket)+len(table))
	for k, v := range bucket {
		merged[k] = v
	}
	for k, v := range table {
		merged[k] = v
	}
	return merged
}

type PutTableBucketMaintenanceConfigurationRequest struct {
	TableBucketARN string                         `json:"tableBucketARN"`
	Type           string                         `json:"type"`
	Value          *MaintenanceConfigurationValue `json:"value"`
}

type GetTableBucketMaintenanceConfigurationRequest struct {
	TableBucketARN string `json:"tableBucketARN"`
}

type GetTableBucketMaintenanceConfigurationResponse struct {
	TableBucketARN string                   `json:"tableBucketARN"`
	Configuration  MaintenanceConfiguration `json:"configuration"`
}

type PutTableMaintenanceConfigurationRequest struct {
	TableBucketARN string                         `json:"tableBucketARN"`
	Namespace      []string                       `json:"namespace"`
	Name           string                         `json:"name"`
	Type           string                         `json:"type"`
	Value          *MaintenanceConfigurationValue `json:"value"`
}

type GetTableMaintenanceConfigurationRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
}

type GetTableMaintenanceConfigurationResponse struct {
	TableARN      string                   `json:"tableARN"`
	Namespace     []string                 `json:"namespace"`
	Name          string                   `json:"name"`
	Configuration MaintenanceConfiguration `json:"configuration"`
}

// Maintenance job status types

const (
	MaintenanceJobStatusNotYetRun  = "Not_Yet_Run"
	MaintenanceJobStatusSuccessful = "Successful"
	MaintenanceJobStatusFailed     = "Failed"
	MaintenanceJobStatusDisabled   = "Disabled"
)

type MaintenanceJobStatusValue struct {
	Status           string     `json:"status"`
	LastRunTimestamp *time.Time `json:"lastRunTimestamp,omitempty"`
	FailureMessage   string     `json:"failureMessage,omitempty"`
}

// MaintenanceJobStatus maps a maintenance type to the outcome of its last run.
// The worker writes it; the control plane only reads it.
type MaintenanceJobStatus map[string]*MaintenanceJobStatusValue

type GetTableMaintenanceJobStatusRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
	Name           string   `json:"name"`
}

type GetTableMaintenanceJobStatusResponse struct {
	TableARN string               `json:"tableARN"`
	Status   MaintenanceJobStatus `json:"status"`
}

// Tagging types

type TagResourceRequest struct {
	ResourceARN string            `json:"resourceArn"`
	Tags        map[string]string `json:"tags"`
}

type ListTagsForResourceRequest struct {
	ResourceARN string `json:"resourceArn"`
}

type ListTagsForResourceResponse struct {
	Tags map[string]string `json:"tags"`
}

type UntagResourceRequest struct {
	ResourceARN string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

// Error types

type S3TablesError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func (e *S3TablesError) Error() string {
	return e.Message
}

// Table formats a catalog entry may declare.
//
// ICEBERG tables carry metadata the catalog maintains and the maintenance
// worker rewrites. LANCE is catalog-only: the entry records a name and the
// dataset root in MetadataLocation, and the Lance client owns every byte under
// it. Nothing in this package interprets a catalog-only table's files.
const (
	FormatIceberg = "ICEBERG"
	FormatLance   = "LANCE"
)

// IsCatalogOnlyFormat reports whether the catalog only records where a table of
// this format lives, without understanding its files.
func IsCatalogOnlyFormat(format string) bool {
	return format == FormatLance
}

// NormalizeFormat folds a caller's spelling onto the canonical one and reports
// whether it names a format this catalog serves.
func NormalizeFormat(format string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case FormatIceberg:
		return FormatIceberg, true
	case FormatLance:
		return FormatLance, true
	default:
		return "", false
	}
}

// Error codes
const (
	ErrCodeBucketAlreadyExists    = "BucketAlreadyExists"
	ErrCodeBucketNotEmpty         = "BucketNotEmpty"
	ErrCodeNoSuchBucket           = "NoSuchBucket"
	ErrCodeNoSuchNamespace        = "NoSuchNamespace"
	ErrCodeNoSuchTable            = "NoSuchTable"
	ErrCodeNamespaceAlreadyExists = "NamespaceAlreadyExists"
	ErrCodeNamespaceNotEmpty      = "NamespaceNotEmpty"
	ErrCodeTableAlreadyExists     = "TableAlreadyExists"
	ErrCodeNoSuchView             = "NoSuchView"
	ErrCodeViewAlreadyExists      = "ViewAlreadyExists"
	ErrCodeAccessDenied           = "AccessDenied"
	ErrCodeInvalidRequest         = "InvalidRequest"
	ErrCodeInternalError          = "InternalError"
	ErrCodeNoSuchPolicy           = "NoSuchPolicy"
	ErrCodeConflict               = "Conflict"
)
