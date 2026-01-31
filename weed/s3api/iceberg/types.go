// Package iceberg defines types for the Iceberg REST Catalog API.
package iceberg

// CatalogConfig is returned by GET /v1/config.
type CatalogConfig struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
	Endpoints []string          `json:"endpoints,omitempty"`
}

// ErrorModel represents an Iceberg error.
type ErrorModel struct {
	Message string   `json:"message"`
	Type    string   `json:"type"`
	Code    int      `json:"code"`
	Stack   []string `json:"stack,omitempty"`
}

// ErrorResponse wraps an error in the standard Iceberg format.
type ErrorResponse struct {
	Error ErrorModel `json:"error"`
}

// Namespace is a reference to one or more levels of a namespace.
type Namespace []string

// TableIdentifier identifies a table within a namespace.
type TableIdentifier struct {
	Namespace Namespace `json:"namespace"`
	Name      string    `json:"name"`
}

// ListNamespacesResponse is returned by GET /v1/namespaces.
type ListNamespacesResponse struct {
	NextPageToken string      `json:"next-page-token,omitempty"`
	Namespaces    []Namespace `json:"namespaces"`
}

// CreateNamespaceRequest is sent to POST /v1/namespaces.
type CreateNamespaceRequest struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties,omitempty"`
}

// CreateNamespaceResponse is returned by POST /v1/namespaces.
type CreateNamespaceResponse struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties,omitempty"`
}

// GetNamespaceResponse is returned by GET /v1/namespaces/{namespace}.
type GetNamespaceResponse struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties,omitempty"`
}

// ListTablesResponse is returned by GET /v1/namespaces/{namespace}/tables.
type ListTablesResponse struct {
	NextPageToken string            `json:"next-page-token,omitempty"`
	Identifiers   []TableIdentifier `json:"identifiers"`
}

// Schema represents an Iceberg table schema.
type Schema struct {
	Type     string        `json:"type"`
	SchemaID int           `json:"schema-id"`
	Fields   []SchemaField `json:"fields,omitempty"`
}

// SchemaField represents a field in a schema.
type SchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Doc      string `json:"doc,omitempty"`
}

// PartitionSpec represents partition specification.
type PartitionSpec struct {
	SpecID int                  `json:"spec-id"`
	Fields []PartitionSpecField `json:"fields,omitempty"`
}

// PartitionSpecField represents a partition field.
type PartitionSpecField struct {
	FieldID   int    `json:"field-id"`
	SourceID  int    `json:"source-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// SortOrder represents sort order specification.
type SortOrder struct {
	OrderID int              `json:"order-id"`
	Fields  []SortOrderField `json:"fields,omitempty"`
}

// SortOrderField represents a sort field.
type SortOrderField struct {
	SourceID  int    `json:"source-id"`
	Transform string `json:"transform"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

// Snapshot represents an Iceberg table snapshot.
type Snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number,omitempty"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list,omitempty"`
	Summary          map[string]string `json:"summary,omitempty"`
	SchemaID         *int              `json:"schema-id,omitempty"`
}

// TableMetadata represents Iceberg table metadata.
type TableMetadata struct {
	FormatVersion      int               `json:"format-version"`
	TableUUID          string            `json:"table-uuid"`
	Location           string            `json:"location,omitempty"`
	LastUpdatedMs      int64             `json:"last-updated-ms,omitempty"`
	Properties         map[string]string `json:"properties,omitempty"`
	Schemas            []Schema          `json:"schemas,omitempty"`
	CurrentSchemaID    int               `json:"current-schema-id,omitempty"`
	LastColumnID       int               `json:"last-column-id,omitempty"`
	PartitionSpecs     []PartitionSpec   `json:"partition-specs,omitempty"`
	DefaultSpecID      int               `json:"default-spec-id,omitempty"`
	LastPartitionID    int               `json:"last-partition-id,omitempty"`
	SortOrders         []SortOrder       `json:"sort-orders,omitempty"`
	DefaultSortOrderID int               `json:"default-sort-order-id,omitempty"`
	Snapshots          []Snapshot        `json:"snapshots,omitempty"`
	CurrentSnapshotID  *int64            `json:"current-snapshot-id,omitempty"`
	LastSequenceNumber int64             `json:"last-sequence-number,omitempty"`
}

// CreateTableRequest is sent to POST /v1/namespaces/{namespace}/tables.
type CreateTableRequest struct {
	Name          string            `json:"name"`
	Location      string            `json:"location,omitempty"`
	Schema        *Schema           `json:"schema,omitempty"`
	PartitionSpec *PartitionSpec    `json:"partition-spec,omitempty"`
	WriteOrder    *SortOrder        `json:"write-order,omitempty"`
	StageCreate   bool              `json:"stage-create,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
}

// LoadTableResult is returned by GET/POST table endpoints.
type LoadTableResult struct {
	MetadataLocation string            `json:"metadata-location,omitempty"`
	Metadata         TableMetadata     `json:"metadata"`
	Config           map[string]string `json:"config,omitempty"`
}

// CommitTableRequest is sent to POST /v1/namespaces/{namespace}/tables/{table}.
type CommitTableRequest struct {
	Identifier   *TableIdentifier   `json:"identifier,omitempty"`
	Requirements []TableRequirement `json:"requirements"`
	Updates      []TableUpdate      `json:"updates"`
}

// TableRequirement represents a requirement for table commit.
type TableRequirement struct {
	Type       string `json:"type"`
	Ref        string `json:"ref,omitempty"`
	SnapshotID *int64 `json:"snapshot-id,omitempty"`
}

// TableUpdate represents an update to table metadata.
type TableUpdate struct {
	Action string `json:"action"`
	// Additional fields depend on the action type
}
