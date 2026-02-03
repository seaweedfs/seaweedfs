// Package iceberg defines types for the Iceberg REST Catalog API.
// This package uses types from github.com/apache/iceberg-go for spec compliance.
package iceberg

import (
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
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
