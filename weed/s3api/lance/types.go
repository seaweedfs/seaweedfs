// Package lance serves the Lance Namespace REST spec over the same table
// buckets the Iceberg REST catalog uses. A Lance table is a catalog entry with
// format LANCE: the namespace records where the dataset lives and vends
// credentials for it, and the Lance client owns everything under that location.
package lance

// Modes shared by create and register. The spec matches them case-insensitively
// and accepts both PascalCase and snake_case.
const (
	modeCreate    = "create"
	modeExistOk   = "existok"
	modeOverwrite = "overwrite"
	modeFail      = "fail"
	modeSkip      = "skip"

	behaviorRestrict = "restrict"
	behaviorCascade  = "cascade"
)

type CreateNamespaceRequest struct {
	ID         []string          `json:"id,omitempty"`
	Mode       string            `json:"mode,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type CreateNamespaceResponse struct {
	Properties map[string]string `json:"properties"`
}

type ListNamespacesResponse struct {
	Namespaces []string `json:"namespaces"`
	PageToken  string   `json:"page_token,omitempty"`
}

type DescribeNamespaceRequest struct {
	ID []string `json:"id,omitempty"`
}

type DescribeNamespaceResponse struct {
	Properties map[string]string `json:"properties"`
}

type DropNamespaceRequest struct {
	ID       []string `json:"id,omitempty"`
	Mode     string   `json:"mode,omitempty"`
	Behavior string   `json:"behavior,omitempty"`
}

type DropNamespaceResponse struct {
	Properties map[string]string `json:"properties,omitempty"`
}

type NamespaceExistsRequest struct {
	ID []string `json:"id,omitempty"`
}

type ListTablesResponse struct {
	Tables    []string `json:"tables"`
	PageToken string   `json:"page_token,omitempty"`
}

type DeclareTableRequest struct {
	ID              []string          `json:"id,omitempty"`
	Location        string            `json:"location,omitempty"`
	VendCredentials *bool             `json:"vend_credentials,omitempty"`
	Properties      map[string]string `json:"properties,omitempty"`
}

type DeclareTableResponse struct {
	Location       string            `json:"location"`
	StorageOptions map[string]string `json:"storage_options,omitempty"`
	Properties     map[string]string `json:"properties"`
	// ManagedVersioning stays false: the dataset owns its version history,
	// because this store can order commits without the catalog in the path.
	ManagedVersioning bool `json:"managed_versioning"`
}

type DescribeTableRequest struct {
	ID      []string `json:"id,omitempty"`
	Version *int64   `json:"version,omitempty"`
	Tag     string   `json:"tag,omitempty"`
	Branch  string   `json:"branch,omitempty"`
	// The REST spec carries these as query parameters, but clients also put
	// them in the body, so honour both.
	WithTableURI         *bool `json:"with_table_uri,omitempty"`
	LoadDetailedMetadata *bool `json:"load_detailed_metadata,omitempty"`
	CheckDeclared        *bool `json:"check_declared,omitempty"`
	VendCredentials      *bool `json:"vend_credentials,omitempty"`
}

type DescribeTableResponse struct {
	Table          string            `json:"table,omitempty"`
	Namespace      []string          `json:"namespace,omitempty"`
	Version        *int64            `json:"version,omitempty"`
	Location       string            `json:"location"`
	TableURI       string            `json:"table_uri,omitempty"`
	StorageOptions map[string]string `json:"storage_options,omitempty"`
	Properties     map[string]string `json:"properties"`
	// ManagedVersioning stays false: the dataset owns its version history,
	// because this store can order commits without the catalog in the path.
	ManagedVersioning bool  `json:"managed_versioning"`
	IsOnlyDeclared    *bool `json:"is_only_declared,omitempty"`
}

type TableExistsRequest struct {
	ID []string `json:"id,omitempty"`
}

type RegisterTableRequest struct {
	ID         []string          `json:"id,omitempty"`
	Location   string            `json:"location"`
	Mode       string            `json:"mode,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type RegisterTableResponse struct {
	Location   string            `json:"location"`
	Properties map[string]string `json:"properties"`
}

type DeregisterTableRequest struct {
	ID []string `json:"id,omitempty"`
}

type DeregisterTableResponse struct {
	ID         []string          `json:"id"`
	Location   string            `json:"location"`
	Properties map[string]string `json:"properties"`
}

type DropTableRequest struct {
	ID []string `json:"id,omitempty"`
}

type DropTableResponse struct {
	ID         []string          `json:"id"`
	Location   string            `json:"location"`
	Properties map[string]string `json:"properties"`
}

type RenameTableRequest struct {
	ID    []string `json:"id,omitempty"`
	NewID []string `json:"new_id"`
}

type RenameTableResponse struct{}
