package s3tables

import "time"

// Table bucket types

type TableBucket struct {
	ARN            string    `json:"arn"`
	Name           string    `json:"name"`
	OwnerAccountID string    `json:"ownerAccountId"`
	CreatedAt      time.Time `json:"createdAt"`
}

type CreateTableBucketRequest struct {
	Name string            `json:"name"`
	Tags map[string]string `json:"tags,omitempty"`
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
	Namespace      []string  `json:"namespace"`
	CreatedAt      time.Time `json:"createdAt"`
	OwnerAccountID string    `json:"ownerAccountId"`
}

type CreateNamespaceRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
}

type CreateNamespaceResponse struct {
	Namespace      []string `json:"namespace"`
	TableBucketARN string   `json:"tableBucketARN"`
}

type GetNamespaceRequest struct {
	TableBucketARN string   `json:"tableBucketARN"`
	Namespace      []string `json:"namespace"`
}

type GetNamespaceResponse struct {
	Namespace      []string  `json:"namespace"`
	CreatedAt      time.Time `json:"createdAt"`
	OwnerAccountID string    `json:"ownerAccountId"`
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
	Iceberg *IcebergMetadata `json:"iceberg,omitempty"`
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
	TableBucketARN string            `json:"tableBucketARN"`
	Namespace      []string          `json:"namespace"`
	Name           string            `json:"name"`
	Format         string            `json:"format"`
	Metadata       *TableMetadata    `json:"metadata,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

type CreateTableResponse struct {
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
	Name             string    `json:"name"`
	TableARN         string    `json:"tableARN"`
	Namespace        []string  `json:"namespace"`
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
	ErrCodeAccessDenied           = "AccessDenied"
	ErrCodeInvalidRequest         = "InvalidRequest"
	ErrCodeInternalError          = "InternalError"
	ErrCodeNoSuchPolicy           = "NoSuchPolicy"
	ErrCodeConflict               = "Conflict"
)
