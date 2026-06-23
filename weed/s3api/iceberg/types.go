// Package iceberg defines types for the Iceberg REST Catalog API.
// This package uses types from github.com/apache/iceberg-go for spec compliance.
package iceberg

import (
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
)

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
	Properties map[string]string `json:"properties"`
}

// GetNamespaceResponse is returned by GET /v1/namespaces/{namespace}.
type GetNamespaceResponse struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

// UpdateNamespacePropertiesRequest is sent to POST /v1/namespaces/{namespace}/properties.
type UpdateNamespacePropertiesRequest struct {
	Removals []string          `json:"removals,omitempty"`
	Updates  map[string]string `json:"updates,omitempty"`
}

// UpdateNamespacePropertiesResponse is returned by POST /v1/namespaces/{namespace}/properties.
type UpdateNamespacePropertiesResponse struct {
	Removed []string `json:"removed"`
	Updated []string `json:"updated"`
	Missing []string `json:"missing"`
}

// ListTablesResponse is returned by GET /v1/namespaces/{namespace}/tables.
type ListTablesResponse struct {
	NextPageToken string            `json:"next-page-token,omitempty"`
	Identifiers   []TableIdentifier `json:"identifiers"`
}

// CreateTableRequest is sent to POST /v1/namespaces/{namespace}/tables.
// Uses iceberg-go types for Schema, PartitionSpec, and SortOrder.
type CreateTableRequest struct {
	Name          string                 `json:"name"`
	Location      string                 `json:"location,omitempty"`
	Schema        *iceberg.Schema        `json:"schema,omitempty"`
	PartitionSpec *iceberg.PartitionSpec `json:"partition-spec,omitempty"`
	WriteOrder    *table.SortOrder       `json:"write-order,omitempty"`
	StageCreate   bool                   `json:"stage-create,omitempty"`
	Properties    iceberg.Properties     `json:"properties,omitempty"`
}

// RegisterTableRequest is sent to POST /v1/namespaces/{namespace}/register.
type RegisterTableRequest struct {
	Name             string `json:"name"`
	MetadataLocation string `json:"metadata-location"`
}

type LoadTableResult struct {
	MetadataLocation string             `json:"metadata-location,omitempty"`
	Metadata         table.Metadata     `json:"metadata"`
	Config           iceberg.Properties `json:"config"`
}

// loadTableResultAlias is used for custom JSON unmarshaling.
type loadTableResultAlias struct {
	MetadataLocation string             `json:"metadata-location,omitempty"`
	RawMetadata      json.RawMessage    `json:"metadata"`
	Config           iceberg.Properties `json:"config,omitempty"`
}

// MarshalJSON serializes LoadTableResult while backfilling spec-required
// metadata fields that iceberg-go v0.5.0 drops via `omitempty`. Without this,
// strict Iceberg REST clients (Java/Spark/Trino) reject otherwise valid
// responses for empty tables with errors like
// "Cannot parse missing long current-snapshot-id".
func (r LoadTableResult) MarshalJSON() ([]byte, error) {
	metaBytes, err := json.Marshal(r.Metadata)
	if err != nil {
		return nil, err
	}
	metaBytes = ensureMetadataSpecCompliance(metaBytes)

	return json.Marshal(struct {
		MetadataLocation string             `json:"metadata-location,omitempty"`
		Metadata         json.RawMessage    `json:"metadata"`
		Config           iceberg.Properties `json:"config"`
	}{
		MetadataLocation: r.MetadataLocation,
		Metadata:         metaBytes,
		Config:           r.Config,
	})
}

// UnmarshalJSON implements custom unmarshaling for LoadTableResult
// to properly parse table.Metadata using iceberg-go's parser.
func (r *LoadTableResult) UnmarshalJSON(data []byte) error {
	var alias loadTableResultAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	r.MetadataLocation = alias.MetadataLocation
	r.Config = alias.Config

	if len(alias.RawMetadata) > 0 {
		metadata, err := table.ParseMetadataBytes(alias.RawMetadata)
		if err != nil {
			return err
		}
		r.Metadata = metadata
	}

	return nil
}

// CommitTableRequest is sent to POST /v1/namespaces/{namespace}/tables/{table}.
type CommitTableRequest struct {
	Identifier   *TableIdentifier   `json:"identifier,omitempty"`
	Requirements table.Requirements `json:"requirements"`
	Updates      table.Updates      `json:"updates"`
}

// CommitTableResponse is returned by POST table commit operations.
type CommitTableResponse struct {
	MetadataLocation string         `json:"metadata-location"`
	Metadata         table.Metadata `json:"metadata"`
}

// commitTableResponseAlias is used for custom JSON unmarshaling.
type commitTableResponseAlias struct {
	MetadataLocation string          `json:"metadata-location"`
	RawMetadata      json.RawMessage `json:"metadata"`
}

// MarshalJSON mirrors LoadTableResult.MarshalJSON: it backfills spec-required
// fields that iceberg-go v0.5.0 omits, so commit responses also parse cleanly
// on strict Iceberg clients.
func (r CommitTableResponse) MarshalJSON() ([]byte, error) {
	metaBytes, err := json.Marshal(r.Metadata)
	if err != nil {
		return nil, err
	}
	metaBytes = ensureMetadataSpecCompliance(metaBytes)

	return json.Marshal(struct {
		MetadataLocation string          `json:"metadata-location"`
		Metadata         json.RawMessage `json:"metadata"`
	}{
		MetadataLocation: r.MetadataLocation,
		Metadata:         metaBytes,
	})
}

// UnmarshalJSON implements custom unmarshaling for CommitTableResponse.
func (r *CommitTableResponse) UnmarshalJSON(data []byte) error {
	var alias commitTableResponseAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	r.MetadataLocation = alias.MetadataLocation

	if len(alias.RawMetadata) > 0 {
		metadata, err := table.ParseMetadataBytes(alias.RawMetadata)
		if err != nil {
			return err
		}
		r.Metadata = metadata
	}

	return nil
}

// ListViewsResponse is returned by GET /v1/namespaces/{namespace}/views.
type ListViewsResponse struct {
	NextPageToken string            `json:"next-page-token,omitempty"`
	Identifiers   []TableIdentifier `json:"identifiers"`
}

// CreateViewRequest is sent to POST /v1/namespaces/{namespace}/views.
type CreateViewRequest struct {
	Name        string             `json:"name"`
	Schema      *iceberg.Schema    `json:"schema"`
	Location    string             `json:"location,omitempty"`
	Properties  iceberg.Properties `json:"properties,omitempty"`
	ViewVersion *view.Version      `json:"view-version"`
}

// ViewResponse is returned by view create/update and load operations.
type ViewResponse struct {
	MetadataLocation string             `json:"metadata-location"`
	Metadata         view.Metadata      `json:"metadata"`
	Config           iceberg.Properties `json:"config"`
}

// viewResponseAlias is used for custom JSON unmarshaling.
type viewResponseAlias struct {
	MetadataLocation string             `json:"metadata-location"`
	RawMetadata      json.RawMessage    `json:"metadata"`
	Config           iceberg.Properties `json:"config,omitempty"`
}

// UnmarshalJSON parses view.Metadata using iceberg-go's view parser.
func (r *ViewResponse) UnmarshalJSON(data []byte) error {
	var alias viewResponseAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	r.MetadataLocation = alias.MetadataLocation
	r.Config = alias.Config
	if len(alias.RawMetadata) > 0 {
		metadata, err := view.ParseMetadataBytes(alias.RawMetadata)
		if err != nil {
			return err
		}
		r.Metadata = metadata
	}
	return nil
}

// UpdateViewRequest is sent to POST /v1/namespaces/{namespace}/views/{view}.
type UpdateViewRequest struct {
	Identifier   *TableIdentifier  `json:"identifier,omitempty"`
	Requirements view.Requirements `json:"requirements"`
	Updates      view.Updates      `json:"updates"`
}
